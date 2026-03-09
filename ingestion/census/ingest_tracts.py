"""
US Census TIGER/Line Tract Boundary Ingestion
==============================================

Downloads census tract boundary shapefiles from the US Census Bureau
and loads them into PostGIS as the spatial foundation for risk scoring.

Data source:
  https://www2.census.gov/geo/tiger/TIGER2023/TRACT/

Each state has its own shapefile: tl_2023_{state_fips}_tract.zip
Fields used: GEOID (11-char), STATEFP, COUNTYFP, NAME, ALAND

Usage:
    # Ingest tracts for Texas (48) and Florida (12)
    python -m ingestion.census.ingest_tracts --state 48 12

    # Ingest all 50 states (takes ~10 minutes)
    python -m ingestion.census.ingest_tracts --all
"""

import argparse
import io
import logging
import os
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

CENSUS_TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER2023/TRACT"

# All 50 states + DC FIPS codes
ALL_STATE_FIPS = [
    "01","02","04","05","06","08","09","10","11","12",
    "13","15","16","17","18","19","20","21","22","23",
    "24","25","26","27","28","29","30","31","32","33",
    "34","35","36","37","38","39","40","41","42","44",
    "45","46","47","48","49","50","51","53","54","55","56",
]


def download_tract_shapefile(state_fips: str) -> gpd.GeoDataFrame:
    """
    Download and parse census tract shapefile for a single state.

    Args:
        state_fips: 2-digit state FIPS code (zero-padded)

    Returns:
        GeoDataFrame with tract boundaries in EPSG:4326
    """
    filename = f"tl_2023_{state_fips}_tract.zip"
    url = f"{CENSUS_TIGER_BASE}/{filename}"

    logger.info("Downloading census tracts for state %s from %s", state_fips, url)

    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()

    # Load shapefile directly from zip bytes into GeoPandas
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / filename
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmpdir)

        # Find the .shp file
        shp_files = list(Path(tmpdir).glob("*.shp"))
        if not shp_files:
            raise FileNotFoundError(f"No .shp file found in {filename}")

        gdf = gpd.read_file(shp_files[0])

    # Reproject to WGS84 (EPSG:4326) for PostGIS storage
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    logger.info(
        "Downloaded %d census tracts for state %s", len(gdf), state_fips
    )
    return gdf


def upsert_tracts(gdf: gpd.GeoDataFrame, state_fips: str, db: Session) -> int:
    """
    Upsert census tract boundaries into PostGIS.

    Uses delete-then-insert per state for clean refreshes.

    Args:
        gdf: GeoDataFrame from download_tract_shapefile()
        state_fips: 2-digit state FIPS
        db: SQLAlchemy session

    Returns:
        Number of tracts inserted
    """
    if gdf.empty:
        logger.warning("Empty GeoDataFrame for state %s", state_fips)
        return 0

    # Delete existing tracts for this state
    deleted = db.execute(
        text("DELETE FROM census_tracts WHERE state_fips = :fips"),
        {"fips": state_fips},
    ).rowcount
    if deleted:
        logger.info("Deleted %d existing tracts for state %s", deleted, state_fips)

    inserted = 0
    for _, row in gdf.iterrows():
        geom = row.get("geometry")
        if geom is None or geom.is_empty:
            continue

        # Normalize to MultiPolygon
        if geom.geom_type == "Polygon":
            from shapely.geometry import MultiPolygon
            geom = MultiPolygon([geom])

        # GEOID is 11 chars: 2 state + 3 county + 6 tract
        geoid = str(row.get("GEOID", "")).zfill(11)
        county_fips = geoid[:5]  # first 5 chars = state + county

        db.execute(
            text("""
                INSERT INTO census_tracts
                    (geoid, state_fips, county_fips, name, land_area_sqm, geom)
                VALUES
                    (:geoid, :state_fips, :county_fips, :name, :land_area,
                     ST_GeomFromEWKT(:geom))
                ON CONFLICT (geoid) DO UPDATE SET
                    name = EXCLUDED.name,
                    land_area_sqm = EXCLUDED.land_area_sqm,
                    geom = EXCLUDED.geom
            """),
            {
                "geoid": geoid,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "name": str(row.get("NAME", "")),
                "land_area": float(row.get("ALAND", 0) or 0),
                "geom": f"SRID=4326;{geom.wkt}",
            },
        )
        inserted += 1

        if inserted % 500 == 0:
            db.commit()
            logger.info("Committed %d tracts for state %s...", inserted, state_fips)

    db.commit()
    logger.info(
        "Tract upsert complete: %d tracts inserted for state %s",
        inserted,
        state_fips,
    )
    return inserted


def run_ingestion(state_fips_list: list[str]) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = SessionLocal()
    total = 0
    try:
        for fips in state_fips_list:
            fips = fips.zfill(2)
            logger.info("=== Census tracts: state %s ===", fips)
            try:
                gdf = download_tract_shapefile(fips)
                count = upsert_tracts(gdf, fips, db)
                total += count
                logger.info("State %s: %d tracts ingested", fips, count)
            except Exception as e:
                logger.error("Failed state %s: %s", fips, e)
                db.rollback()
                continue

        logger.info("=== Census ingestion complete. Total tracts: %d ===", total)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest Census TIGER/Line tract boundaries"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--state", nargs="+", help="2-digit state FIPS codes (e.g. --state 48 12)"
    )
    group.add_argument(
        "--all", action="store_true", help="Ingest all 50 states + DC"
    )
    args = parser.parse_args()

    states = ALL_STATE_FIPS if args.all else args.state
    run_ingestion(states)
