"""
HIFLD Critical Infrastructure Ingestion
=========================================

Downloads critical infrastructure facility data from the Homeland
Infrastructure Foundation-Level Data (HIFLD) ArcGIS REST APIs and
loads them into PostGIS.

Data sources:
  - Hospitals: HIFLD Hospitals_1 FeatureServer
  - Public Schools: HIFLD Public_Schools FeatureServer
  - Power Plants: HIFLD Power_Plants FeatureServer

Usage:
    python -m ingestion.hifld.ingest_infrastructure
    python -m ingestion.hifld.ingest_infrastructure --types hospital power_plant
"""

import argparse
import logging

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# HIFLD ArcGIS REST endpoints
HIFLD_HOSPITALS_URL = (
    "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services"
    "/Hospitals2/FeatureServer/0/query"
)
HIFLD_SCHOOLS_URL = (
    "https://services.arcgis.com/XG15cJAlne2vxtgt/ArcGIS/rest/services"
    "/Public_Schools/FeatureServer/3/query"
)
HIFLD_POWER_PLANTS_URL = (
    "https://services.arcgis.com/XG15cJAlne2vxtgt/ArcGIS/rest/services"
    "/Power_Plants/FeatureServer/0/query"
)

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


def _fetch_paginated(url: str, out_fields: str, facility_type: str) -> list[dict]:
    """
    Fetch all features from an ArcGIS REST endpoint using offset pagination.

    ArcGIS limits results to ~2000 per request; this function pages through
    using resultOffset until all records are retrieved.
    """
    all_features: list[dict] = []
    offset = 0
    page_size = 2000

    while True:
        params = {
            "where": "1=1",
            "outFields": out_fields,
            "outSR": "4326",
            "f": "geojson",
            "resultRecordCount": page_size,
            "resultOffset": offset,
        }

        logger.info("Fetching %s from HIFLD (offset=%d)...", facility_type, offset)
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()

        data = resp.json()
        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += len(features)

        if len(features) < page_size:
            break

    logger.info("Fetched %d total %s features from HIFLD", len(all_features), facility_type)
    return all_features


def fetch_hospitals() -> list[dict]:
    """Fetch hospital facilities from HIFLD."""
    return _fetch_paginated(
        HIFLD_HOSPITALS_URL,
        "ID,NAME,ADDRESS,CITY,STATE,COUNTYFIPS,BEDS,STATUS,LATITUDE,LONGITUDE",
        "hospital",
    )


def fetch_schools() -> list[dict]:
    """Fetch public school facilities from HIFLD."""
    return _fetch_paginated(
        HIFLD_SCHOOLS_URL,
        "FID,NAME,ADDRESS,CITY,STATE,COUNTYFIPS,ENROLLMENT,STATUS,LATITUDE,LONGITUDE",
        "school",
    )


def fetch_power_plants() -> list[dict]:
    """Fetch power plant facilities from HIFLD."""
    return _fetch_paginated(
        HIFLD_POWER_PLANTS_URL,
        "OBJECTID,NAME,ADDRESS,CITY,STATE,COUNTYFIPS,TOTAL_MW,STATUS,LATITUDE,LONGITUDE",
        "power_plant",
    )


def _resolve_state_fips(props: dict) -> str | None:
    """Resolve state FIPS from COUNTYFIPS or STATE abbreviation."""
    county_fips = props.get("COUNTYFIPS", "")
    if county_fips and len(str(county_fips)) >= 2:
        return str(county_fips).zfill(5)[:2]
    state_abbr = props.get("STATE", "")
    if state_abbr:
        return STATE_ABBR_TO_FIPS.get(state_abbr.strip().upper())
    return None


def _resolve_county_fips(props: dict) -> str | None:
    """Resolve 5-digit county FIPS code."""
    county_fips = props.get("COUNTYFIPS", "")
    if county_fips:
        return str(county_fips).zfill(5)
    return None


def upsert_infrastructure(
    features: list[dict],
    facility_type: str,
    id_field: str,
    capacity_field: str,
    db: Session,
) -> tuple[int, int]:
    """
    Upsert infrastructure features into PostGIS.

    Returns:
        Tuple of (inserted, updated) counts.
    """
    inserted = 0
    updated = 0

    for feature in features:
        props = feature.get("properties", {})
        raw_id = props.get(id_field)
        if not raw_id:
            continue

        hifld_id = f"{facility_type}_{raw_id}"

        lat = props.get("LATITUDE")
        lon = props.get("LONGITUDE")
        if not lat or not lon:
            continue

        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            continue

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue

        name = props.get("NAME", "Unknown")
        address = props.get("ADDRESS")
        city = props.get("CITY")
        state_fips = _resolve_state_fips(props)
        county_fips = _resolve_county_fips(props)
        status = props.get("STATUS")

        capacity_raw = props.get(capacity_field)
        try:
            capacity = int(float(capacity_raw)) if capacity_raw else None
        except (ValueError, TypeError):
            capacity = None

        wkt = f"SRID=4326;POINT({lon} {lat})"

        exists = db.execute(
            text("SELECT id FROM critical_infrastructure WHERE hifld_id = :hifld_id"),
            {"hifld_id": hifld_id},
        ).scalar()

        params = {
            "hifld_id": hifld_id,
            "ftype": facility_type,
            "name": name,
            "address": address,
            "city": city,
            "state": state_fips,
            "county": county_fips,
            "capacity": capacity,
            "status": status,
            "lat": lat,
            "lon": lon,
            "geom": wkt,
        }

        if exists:
            db.execute(
                text("""
                    UPDATE critical_infrastructure SET
                        facility_type = :ftype, name = :name, address = :address,
                        city = :city, state_fips = :state, county_fips = :county,
                        capacity = :capacity, status = :status,
                        latitude = :lat, longitude = :lon,
                        geom = ST_GeomFromEWKT(:geom), ingested_at = NOW()
                    WHERE hifld_id = :hifld_id
                """),
                params,
            )
            updated += 1
        else:
            db.execute(
                text("""
                    INSERT INTO critical_infrastructure
                        (hifld_id, facility_type, name, address, city,
                         state_fips, county_fips, capacity, status,
                         latitude, longitude, geom)
                    VALUES
                        (:hifld_id, :ftype, :name, :address, :city,
                         :state, :county, :capacity, :status,
                         :lat, :lon, ST_GeomFromEWKT(:geom))
                """),
                params,
            )
            inserted += 1

        if (inserted + updated) % 500 == 0:
            db.commit()
            logger.info(
                "Progress (%s): %d inserted, %d updated...",
                facility_type,
                inserted,
                updated,
            )

    db.commit()
    logger.info("%s upsert complete: %d inserted, %d updated", facility_type, inserted, updated)
    return inserted, updated


def run_ingestion(facility_types: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if facility_types is None:
        facility_types = ["hospital", "school", "power_plant"]

    db = SessionLocal()
    try:
        total_ins = 0
        total_upd = 0

        if "hospital" in facility_types:
            features = fetch_hospitals()
            ins, upd = upsert_infrastructure(features, "hospital", "ID", "BEDS", db)
            total_ins += ins
            total_upd += upd

        if "school" in facility_types:
            features = fetch_schools()
            ins, upd = upsert_infrastructure(features, "school", "FID", "ENROLLMENT", db)
            total_ins += ins
            total_upd += upd

        if "power_plant" in facility_types:
            features = fetch_power_plants()
            ins, upd = upsert_infrastructure(features, "power_plant", "OBJECTID", "TOTAL_MW", db)
            total_ins += ins
            total_upd += upd

        logger.info(
            "HIFLD infrastructure ingestion complete. Inserted: %d, Updated: %d",
            total_ins,
            total_upd,
        )
    except Exception as e:
        logger.error("HIFLD infrastructure ingestion failed: %s", e)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest critical infrastructure from HIFLD")
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["hospital", "school", "power_plant"],
        default=["hospital", "school", "power_plant"],
        help="Facility types to ingest",
    )
    args = parser.parse_args()
    run_ingestion(facility_types=args.types)
