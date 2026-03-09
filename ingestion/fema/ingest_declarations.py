"""
FEMA Disaster Declarations Ingestion
=====================================

Downloads all historical disaster declarations from the FEMA OpenFEMA API
and loads them into PostGIS. Data goes back to 1953.

API endpoint:
  https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries

Usage:
    python -m ingestion.fema.ingest_declarations
    python -m ingestion.fema.ingest_declarations --since 2000
"""

import argparse
import logging
from datetime import datetime

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

FEMA_API_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

PAGE_SIZE = 1000


def fetch_declarations(since_year: int = 2000) -> list[dict]:
    """
    Fetch all disaster declarations from FEMA API since a given year.

    Uses offset pagination ($skip / $top) to retrieve all records.
    """
    all_records: list[dict] = []
    skip = 0

    date_filter = f"declarationDate ge '{since_year}-01-01T00:00:00.000Z'"

    # First request: get total count
    params = {
        "$filter": date_filter,
        "$top": PAGE_SIZE,
        "$skip": skip,
        "$count": "true",
        "$orderby": "declarationDate desc",
    }

    logger.info("Fetching FEMA disaster declarations since %d...", since_year)
    resp = requests.get(FEMA_API_URL, params=params, timeout=120)
    resp.raise_for_status()

    data = resp.json()
    total_count = data.get("metadata", {}).get("count", 0)
    records = data.get("DisasterDeclarationsSummaries", [])
    all_records.extend(records)
    skip += len(records)

    logger.info("FEMA API reports %d total declarations since %d", total_count, since_year)

    # Continue paginating
    while skip < total_count and records:
        params = {
            "$filter": date_filter,
            "$top": PAGE_SIZE,
            "$skip": skip,
            "$orderby": "declarationDate desc",
        }

        logger.info("Fetching FEMA declarations (offset=%d/%d)...", skip, total_count)
        resp = requests.get(FEMA_API_URL, params=params, timeout=120)
        resp.raise_for_status()

        data = resp.json()
        records = data.get("DisasterDeclarationsSummaries", [])
        if not records:
            break

        all_records.extend(records)
        skip += len(records)

    logger.info("Fetched %d total FEMA declarations", len(all_records))
    return all_records


def _parse_date(value: str | None) -> datetime | None:
    """Parse ISO date string from FEMA API."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _build_county_fips(state_fips: str | None, county_code: str | None) -> str | None:
    """Build 5-digit county FIPS from state + county codes."""
    if not state_fips or not county_code:
        return None
    try:
        return f"{state_fips.zfill(2)}{county_code.zfill(3)}"
    except (ValueError, AttributeError):
        return None


def upsert_declarations(records: list[dict], db: Session) -> tuple[int, int]:
    """
    Upsert FEMA disaster declarations into PostGIS.

    Returns:
        Tuple of (inserted, updated) counts.
    """
    inserted = 0
    updated = 0

    for record in records:
        fema_id = record.get("femaDeclarationString")
        if not fema_id:
            continue

        declaration_date = _parse_date(record.get("declarationDate"))
        if not declaration_date:
            continue

        state = record.get("state", "")
        state_fips = record.get("fipsStateCode")
        county_code = record.get("fipsCountyCode")
        county_fips = _build_county_fips(state_fips, county_code)

        params = {
            "fema_id": fema_id,
            "disaster_number": record.get("disasterNumber"),
            "state": state,
            "state_fips": state_fips,
            "county_fips": county_fips,
            "declaration_type": record.get("declarationType", ""),
            "incident_type": record.get("incidentType", "Unknown"),
            "declaration_title": record.get("declarationTitle"),
            "declaration_date": declaration_date,
            "incident_begin": _parse_date(record.get("incidentBeginDate")),
            "incident_end": _parse_date(record.get("incidentEndDate")),
            "designated_area": record.get("designatedArea"),
            "fema_region": record.get("region"),
        }

        exists = db.execute(
            text("SELECT id FROM disaster_declarations WHERE fema_id = :fema_id"),
            {"fema_id": fema_id},
        ).scalar()

        if exists:
            db.execute(
                text("""
                    UPDATE disaster_declarations SET
                        disaster_number = :disaster_number,
                        state = :state,
                        state_fips = :state_fips,
                        county_fips = :county_fips,
                        declaration_type = :declaration_type,
                        incident_type = :incident_type,
                        declaration_title = :declaration_title,
                        declaration_date = :declaration_date,
                        incident_begin_date = :incident_begin,
                        incident_end_date = :incident_end,
                        designated_area = :designated_area,
                        fema_region = :fema_region,
                        ingested_at = NOW()
                    WHERE fema_id = :fema_id
                """),
                params,
            )
            updated += 1
        else:
            db.execute(
                text("""
                    INSERT INTO disaster_declarations
                        (fema_id, disaster_number, state, state_fips,
                         county_fips, declaration_type, incident_type,
                         declaration_title, declaration_date,
                         incident_begin_date, incident_end_date,
                         designated_area, fema_region)
                    VALUES
                        (:fema_id, :disaster_number, :state, :state_fips,
                         :county_fips, :declaration_type, :incident_type,
                         :declaration_title, :declaration_date,
                         :incident_begin, :incident_end,
                         :designated_area, :fema_region)
                """),
                params,
            )
            inserted += 1

        if (inserted + updated) % 500 == 0:
            db.commit()
            logger.info("Progress: %d inserted, %d updated...", inserted, updated)

    db.commit()
    logger.info(
        "FEMA declarations upsert complete: %d inserted, %d updated",
        inserted,
        updated,
    )
    return inserted, updated


def run_ingestion(since_year: int = 2000) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = SessionLocal()
    try:
        records = fetch_declarations(since_year=since_year)
        inserted, updated = upsert_declarations(records, db)
        logger.info(
            "FEMA disaster declarations ingestion complete. " "Inserted: %d, Updated: %d",
            inserted,
            updated,
        )
    except Exception as e:
        logger.error("FEMA declarations ingestion failed: %s", e)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FEMA disaster declarations")
    parser.add_argument(
        "--since",
        type=int,
        default=2000,
        help="Start year for historical data (default: 2000)",
    )
    args = parser.parse_args()
    run_ingestion(since_year=args.since)
