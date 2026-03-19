"""
FEMA Disaster Cost Summaries Ingestion
=======================================

Downloads disaster cost/funding data from the FEMA OpenFEMA API
(FemaWebDisasterSummaries endpoint) and loads into PostGIS.

API endpoint:
  https://www.fema.gov/api/open/v1/FemaWebDisasterSummaries

Usage:
    python -m ingestion.fema.ingest_costs
"""

import logging

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

FEMA_COST_URL = "https://www.fema.gov/api/open/v1/FemaWebDisasterSummaries"

PAGE_SIZE = 1000


def fetch_cost_summaries() -> list[dict]:
    """
    Fetch all disaster cost summaries from FEMA API.

    Uses offset pagination ($skip / $top) to retrieve all records.
    """
    all_records: list[dict] = []
    skip = 0

    fields = (
        "disasterNumber,"
        "totalAmountIhpApproved,"
        "totalAmountHaApproved,"
        "totalAmountOnaApproved,"
        "totalObligatedAmountPa,"
        "totalObligatedAmountHmgp"
    )

    logger.info("Fetching FEMA disaster cost summaries...")

    while True:
        params = {
            "$select": fields,
            "$top": PAGE_SIZE,
            "$skip": skip,
            "$orderby": "disasterNumber desc",
        }

        resp = requests.get(FEMA_COST_URL, params=params, timeout=120)
        resp.raise_for_status()

        data = resp.json()
        records = data.get("FemaWebDisasterSummaries", [])
        if not records:
            break

        all_records.extend(records)
        skip += len(records)
        logger.info("Fetched %d cost records so far...", len(all_records))

        if len(records) < PAGE_SIZE:
            break

    logger.info("Fetched %d total cost summaries", len(all_records))
    return all_records


def upsert_costs(records: list[dict], db: Session) -> tuple[int, int]:
    """
    Upsert cost summaries into PostGIS.

    Returns:
        Tuple of (inserted, updated) counts.
    """
    inserted = 0
    updated = 0

    for record in records:
        dn = record.get("disasterNumber")
        if not dn:
            continue

        ihp = record.get("totalAmountIhpApproved") or 0.0
        ha = record.get("totalAmountHaApproved") or 0.0
        ona = record.get("totalAmountOnaApproved") or 0.0
        pa = record.get("totalObligatedAmountPa") or 0.0
        hmgp = record.get("totalObligatedAmountHmgp") or 0.0
        total = ihp + ha + ona + pa + hmgp

        if total == 0:
            continue

        params = {
            "dn": dn,
            "ihp": ihp,
            "ha": ha,
            "ona": ona,
            "pa": pa,
            "hmgp": hmgp,
            "total": total,
        }

        exists = db.execute(
            text("SELECT id FROM disaster_costs WHERE disaster_number = :dn"),
            {"dn": dn},
        ).scalar()

        if exists:
            db.execute(
                text("""
                    UPDATE disaster_costs SET
                        total_ihp_approved = :ihp,
                        total_ha_approved = :ha,
                        total_ona_approved = :ona,
                        total_pa_obligated = :pa,
                        total_hmgp_obligated = :hmgp,
                        total_cost = :total,
                        ingested_at = NOW()
                    WHERE disaster_number = :dn
                """),
                params,
            )
            updated += 1
        else:
            db.execute(
                text("""
                    INSERT INTO disaster_costs
                        (disaster_number, total_ihp_approved, total_ha_approved,
                         total_ona_approved, total_pa_obligated,
                         total_hmgp_obligated, total_cost)
                    VALUES (:dn, :ihp, :ha, :ona, :pa, :hmgp, :total)
                """),
                params,
            )
            inserted += 1

        if (inserted + updated) % 500 == 0:
            db.commit()

    db.commit()
    logger.info(
        "FEMA cost upsert complete: %d inserted, %d updated",
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
        records = fetch_cost_summaries()
        inserted, updated = upsert_costs(records, db)
        logger.info(
            "FEMA cost ingestion complete. Inserted: %d, Updated: %d",
            inserted,
            updated,
        )
    except Exception as e:
        logger.error("FEMA cost ingestion failed: %s", e)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    run_ingestion()
