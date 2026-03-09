"""
NIFC Active Wildfire Perimeter Ingestion
=========================================

Downloads active wildfire incident perimeters from the National Interagency
Fire Center (NIFC) ArcGIS REST API and loads them into PostGIS.

Data source:
  NIFC Open Data — Wildfire Perimeters (current year)
  https://data-nifc.opendata.arcgis.com/

Each feature includes fire name, acres burned, containment %, geometry, and
IRWIN ID (Integrated Reporting of Wildland-Fire Information unique identifier).

Usage:
    python -m ingestion.nifc.ingest_wildfires
"""

import argparse
import logging
from datetime import UTC, datetime

import requests
from shapely.geometry import MultiPolygon, shape
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# NIFC ArcGIS REST endpoint — current-year wildfire perimeters
NIFC_PERIMETERS_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services"
    "/WFIGS_Interagency_Perimeters/FeatureServer/0/query"
)

# US state FIPS lookup from state abbreviation
STATE_ABBR_TO_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}


def fetch_wildfire_perimeters() -> list[dict]:
    """
    Fetch active wildfire perimeters from NIFC ArcGIS REST API.

    Returns:
        List of GeoJSON-like feature dicts with geometry and attributes.
    """
    params = {
        "where": "1=1",
        "outFields": (
            "attr_IrwinID,poly_IncidentName,poly_GISAcres,"
            "attr_PercentContained,attr_FireCause,"
            "attr_FireDiscoveryDateTime,attr_ModifiedOnDateTime_dt,"
            "attr_POOState"
        ),
        "outSR": "4326",
        "f": "geojson",
        "resultRecordCount": 2000,
    }

    logger.info("Fetching wildfire perimeters from NIFC...")
    resp = requests.get(NIFC_PERIMETERS_URL, params=params, timeout=120)
    resp.raise_for_status()

    data = resp.json()
    features = data.get("features", [])
    logger.info("Fetched %d wildfire perimeter features from NIFC", len(features))
    return features


def _parse_geometry(feature: dict) -> MultiPolygon | None:
    """Parse and normalize feature geometry to MultiPolygon."""
    geom_dict = feature.get("geometry")
    if not geom_dict:
        return None

    try:
        geom = shape(geom_dict)
    except Exception:
        return None

    if geom.is_empty:
        return None

    if geom.geom_type == "Polygon":
        return MultiPolygon([geom])
    if geom.geom_type == "MultiPolygon":
        return geom

    # GeometryCollection — extract polygons
    if geom.geom_type == "GeometryCollection":
        polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        if polys:
            all_polys = []
            for p in polys:
                if p.geom_type == "Polygon":
                    all_polys.append(p)
                else:
                    all_polys.extend(p.geoms)
            return MultiPolygon(all_polys)

    return None


def _parse_epoch_ms(val) -> datetime | None:
    """Convert epoch milliseconds to UTC datetime."""
    if not val:
        return None
    try:
        return datetime.fromtimestamp(int(val) / 1000, tz=UTC).replace(tzinfo=None)
    except (ValueError, TypeError, OSError):
        return None


def upsert_wildfires(features: list[dict], db: Session) -> tuple[int, int]:
    """
    Upsert wildfire perimeter features into PostGIS.

    Returns:
        Tuple of (inserted, updated) counts.
    """
    inserted = 0
    updated = 0

    for feature in features:
        props = feature.get("properties", {})
        irwin_id = props.get("attr_IrwinID")
        if not irwin_id:
            continue

        geom = _parse_geometry(feature)
        if geom is None:
            continue

        incident_name = props.get("poly_IncidentName", "Unknown")
        acres = props.get("poly_GISAcres")
        contained = props.get("attr_PercentContained")
        fire_cause = props.get("attr_FireCause")
        state_abbr = props.get("attr_POOState", "")
        state_fips = STATE_ABBR_TO_FIPS.get(state_abbr.strip().upper()) if state_abbr else None

        start_date = _parse_epoch_ms(props.get("attr_FireDiscoveryDateTime"))
        updated_at = _parse_epoch_ms(props.get("attr_ModifiedOnDateTime_dt"))

        wkt = f"SRID=4326;{geom.wkt}"

        # Check if exists
        exists = db.execute(
            text("SELECT id FROM wildfire_incidents WHERE irwin_id = :irwin_id"),
            {"irwin_id": irwin_id},
        ).scalar()

        if exists:
            db.execute(
                text("""
                    UPDATE wildfire_incidents SET
                        incident_name = :name,
                        acres_burned = :acres,
                        percent_contained = :contained,
                        fire_cause = :cause,
                        state_fips = :state,
                        start_date = :start,
                        updated_at = :updated,
                        geom = ST_GeomFromEWKT(:geom),
                        ingested_at = NOW()
                    WHERE irwin_id = :irwin_id
                """),
                {
                    "irwin_id": irwin_id,
                    "name": incident_name,
                    "acres": float(acres) if acres else None,
                    "contained": float(contained) if contained else None,
                    "cause": fire_cause,
                    "state": state_fips,
                    "start": start_date,
                    "updated": updated_at,
                    "geom": wkt,
                },
            )
            updated += 1
        else:
            db.execute(
                text("""
                    INSERT INTO wildfire_incidents
                        (irwin_id, incident_name, acres_burned, percent_contained,
                         fire_cause, state_fips, start_date, updated_at, geom)
                    VALUES
                        (:irwin_id, :name, :acres, :contained,
                         :cause, :state, :start, :updated,
                         ST_GeomFromEWKT(:geom))
                """),
                {
                    "irwin_id": irwin_id,
                    "name": incident_name,
                    "acres": float(acres) if acres else None,
                    "contained": float(contained) if contained else None,
                    "cause": fire_cause,
                    "state": state_fips,
                    "start": start_date,
                    "updated": updated_at,
                    "geom": wkt,
                },
            )
            inserted += 1

        if (inserted + updated) % 100 == 0:
            db.commit()
            logger.info("Progress: %d inserted, %d updated...", inserted, updated)

    db.commit()
    logger.info(
        "Wildfire upsert complete: %d inserted, %d updated",
        inserted,
        updated,
    )
    return inserted, updated


def run_ingestion() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = SessionLocal()
    try:
        features = fetch_wildfire_perimeters()
        inserted, updated = upsert_wildfires(features, db)
        logger.info(
            "NIFC wildfire ingestion complete. Inserted: %d, Updated: %d",
            inserted,
            updated,
        )
    except Exception as e:
        logger.error("NIFC wildfire ingestion failed: %s", e)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest active wildfire perimeters from NIFC")
    parser.parse_args()
    run_ingestion()
