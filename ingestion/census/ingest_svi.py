"""
CDC/ATSDR Social Vulnerability Index (SVI) Ingestion
=====================================================

Downloads the CDC SVI dataset and loads tract-level vulnerability
scores into PostGIS. SVI is a 0–1 composite of socioeconomic,
household composition, minority status, and housing/transportation factors.

Data source:
  https://www.atsdr.cdc.gov/placeandhealth/svi/data_documentation_download.html
  Direct CSV: https://svi.cdc.gov/Documents/Data/2022_SVI_Data/CSV/SVI2022_US.csv

Fields used:
  FIPS (11-char GEOID), RPL_THEMES (overall percentile rank 0–1),
  RPL_THEME1..4 (component ranks)

Usage:
    python -m ingestion.census.ingest_svi
"""

import logging

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# CDC SVI 2022 national CSV (~35MB)
SVI_CSV_URL = "https://svi.cdc.gov/Documents/Data/2022_SVI_Data/CSV/SVI2022_US.csv"

# Fallback: smaller state-level files if national fails
SVI_STATE_URL = (
    "https://svi.cdc.gov/Documents/Data/2022_SVI_Data/CSV/States/SVI2022_{state_abbr}.csv"
)


def fetch_svi_data() -> pd.DataFrame:
    """
    Download CDC SVI national CSV and return as DataFrame.

    Selects only the columns needed for ResilienceMap:
    - FIPS: 11-digit census tract GEOID
    - RPL_THEMES: overall SVI percentile rank (0–1), -999 = no data
    - RPL_THEME1: socioeconomic rank
    - RPL_THEME2: household composition rank
    - RPL_THEME3: minority status rank
    - RPL_THEME4: housing/transportation rank
    """
    logger.info("Downloading CDC SVI 2022 national CSV...")

    resp = requests.get(SVI_CSV_URL, timeout=120, stream=True)
    resp.raise_for_status()

    # Read CSV, dtype FIPS as string to preserve leading zeros
    df = pd.read_csv(
        resp.url,
        dtype={"FIPS": str},
        usecols=["FIPS", "RPL_THEMES", "RPL_THEME1", "RPL_THEME2", "RPL_THEME3", "RPL_THEME4"],
        low_memory=False,
    )

    # CDC uses -999 for missing data — replace with NaN
    df.replace(-999, float("nan"), inplace=True)

    # Zero-pad FIPS to 11 chars
    df["FIPS"] = df["FIPS"].str.zfill(11)

    # Drop rows with no overall SVI score
    df = df.dropna(subset=["RPL_THEMES"])

    logger.info("SVI data loaded: %d census tracts", len(df))
    return df


def upsert_svi(df: pd.DataFrame, db: Session) -> int:
    """
    Upsert SVI scores into a dedicated svi_scores table.
    Also updates the social_vulnerability_score in risk_scores if present.

    Args:
        df: DataFrame from fetch_svi_data()
        db: SQLAlchemy session

    Returns:
        Number of records upserted
    """
    # Ensure svi_scores table exists
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS svi_scores (
            geoid           CHAR(11) PRIMARY KEY,
            overall_rank    FLOAT,
            socioeconomic   FLOAT,
            household_comp  FLOAT,
            minority_status FLOAT,
            housing_trans   FLOAT,
            updated_at      TIMESTAMP DEFAULT NOW()
        )
    """))
    db.commit()

    inserted = 0
    for _, row in df.iterrows():
        db.execute(
            text("""
                INSERT INTO svi_scores
                    (geoid, overall_rank, socioeconomic, household_comp,
                     minority_status, housing_trans, updated_at)
                VALUES
                    (:geoid, :overall, :soc, :hh, :min, :ht, NOW())
                ON CONFLICT (geoid) DO UPDATE SET
                    overall_rank    = EXCLUDED.overall_rank,
                    socioeconomic   = EXCLUDED.socioeconomic,
                    household_comp  = EXCLUDED.household_comp,
                    minority_status = EXCLUDED.minority_status,
                    housing_trans   = EXCLUDED.housing_trans,
                    updated_at      = NOW()
            """),
            {
                "geoid": row["FIPS"],
                "overall": float(row["RPL_THEMES"]),
                "soc": float(row.get("RPL_THEME1") or 0),
                "hh": float(row.get("RPL_THEME2") or 0),
                "min": float(row.get("RPL_THEME3") or 0),
                "ht": float(row.get("RPL_THEME4") or 0),
            },
        )
        inserted += 1

        if inserted % 1000 == 0:
            db.commit()
            logger.info("Committed %d SVI records...", inserted)

    db.commit()

    # Backfill social_vulnerability_score in risk_scores table
    updated = db.execute(text("""
        UPDATE risk_scores rs
        SET social_vulnerability_score = sv.overall_rank
        FROM svi_scores sv
        WHERE rs.tract_geoid = sv.geoid
          AND sv.overall_rank IS NOT NULL
    """)).rowcount
    db.commit()

    logger.info("SVI upsert complete: %d records. Backfilled %d risk scores.", inserted, updated)
    return inserted


def run_ingestion() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = SessionLocal()
    try:
        df = fetch_svi_data()
        count = upsert_svi(df, db)
        logger.info("CDC SVI ingestion complete. %d tracts loaded.", count)
    except Exception as e:
        logger.error("SVI ingestion failed: %s", e)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    run_ingestion()
