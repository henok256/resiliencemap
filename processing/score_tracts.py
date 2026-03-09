"""
Composite Risk Scoring Engine
==============================

Computes a composite disaster risk score (0.0 – 1.0) for each US census tract
by combining flood, seismic, storm, wildfire, and social vulnerability components.

Scoring weights (tunable via config):
  - Flood risk:               30%  (FEMA SFHA coverage %)
  - Seismic risk:             20%  (USGS magnitude-weighted proximity)
  - Storm exposure:           20%  (NOAA active alert recency & severity)
  - Wildfire risk:            20%  (NIFC perimeter proximity & acreage)
  - Social vulnerability:     10%  (CDC/ATSDR SVI score)

Usage:
    python -m processing.score_tracts --county 48201   # Harris County, TX
    python -m processing.score_tracts --state 48       # All counties in Texas
"""

import argparse
import logging
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# Scoring weights — must sum to 1.0
WEIGHTS = {
    "flood": 0.30,
    "seismic": 0.20,
    "storm": 0.20,
    "wildfire": 0.20,
    "social_vulnerability": 0.10,
}

# Severity multipliers for NOAA storm alerts
SEVERITY_WEIGHTS = {
    "Extreme": 1.0,
    "Severe": 0.75,
    "Moderate": 0.50,
    "Minor": 0.25,
}


def compute_flood_score(tract_geoid: str, db: Session) -> float:
    """
    Compute flood risk score for a tract as the fraction of its area
    that intersects FEMA Special Flood Hazard Areas (SFHA).

    Returns a value between 0.0 (no flood risk) and 1.0 (fully within SFHA).
    """
    result = db.execute(
        text("""
            SELECT
                COALESCE(
                    ST_Area(ST_Intersection(ct.geom, ST_Union(fz.geom))) /
                    NULLIF(ST_Area(ct.geom), 0),
                    0
                ) AS sfha_fraction
            FROM census_tracts ct
            LEFT JOIN flood_zones fz
                ON ST_Intersects(ct.geom, fz.geom)
                AND fz.sfha_tf = 'T'
            WHERE ct.geoid = :geoid
            GROUP BY ct.geom
        """),
        {"geoid": tract_geoid},
    ).scalar()

    return float(min(result or 0.0, 1.0))


def compute_seismic_score(tract_geoid: str, db: Session, days_back: int = 365) -> float:
    """
    Compute seismic risk score based on magnitude-weighted proximity of
    recent earthquakes to the tract centroid.

    Score = sum of (magnitude^2 / distance_km) for each nearby earthquake,
    normalized to [0, 1] by capping at a reference value.

    Only considers earthquakes within 500km and in the last `days_back` days.
    """
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    result = db.execute(
        text("""
            WITH tract_centroid AS (
                SELECT ST_Centroid(geom) AS pt
                FROM census_tracts
                WHERE geoid = :geoid
            ),
            nearby_quakes AS (
                SELECT
                    sh.magnitude,
                    ST_Distance(
                        tc.pt::geography,
                        sh.geom::geography
                    ) / 1000.0 AS dist_km
                FROM seismic_hazard sh, tract_centroid tc
                WHERE
                    sh.event_time >= :cutoff
                    AND ST_DWithin(tc.pt::geography, sh.geom::geography, 500000)
            )
            SELECT COALESCE(SUM(POWER(magnitude, 2) / GREATEST(dist_km, 1.0)), 0)
            FROM nearby_quakes
        """),
        {"geoid": tract_geoid, "cutoff": cutoff},
    ).scalar()

    raw_score = float(result or 0.0)
    # Normalize: a raw score of 200+ saturates to 1.0
    # (calibrated against historical high-seismicity areas like LA basin)
    return float(min(raw_score / 200.0, 1.0))


def compute_storm_score(tract_geoid: str, db: Session) -> float:
    """
    Compute storm exposure score based on active NOAA alerts
    whose geometry intersects the census tract.

    Higher severity → higher contribution to score.
    """
    now = datetime.utcnow()

    rows = db.execute(
        text("""
            SELECT sa.severity
            FROM storm_alerts sa
            JOIN census_tracts ct ON ST_Intersects(ct.geom, sa.geom)
            WHERE
                ct.geoid = :geoid
                AND sa.expires > :now
                AND sa.geom IS NOT NULL
        """),
        {"geoid": tract_geoid, "now": now},
    ).fetchall()

    if not rows:
        return 0.0

    total = sum(SEVERITY_WEIGHTS.get(row[0], 0.1) for row in rows)
    # Cap at 1.0: 4 simultaneous severe alerts = max score
    return float(min(total / 4.0, 1.0))


def compute_wildfire_score(tract_geoid: str, db: Session, days_back: int = 365) -> float:
    """
    Compute wildfire risk score based on proximity and size of active
    wildfire incidents relative to the census tract.

    Score considers:
      - Direct intersection: tract overlapping an active fire perimeter
      - Proximity: nearby fires within 100km weighted by acreage and containment
      - Containment: uncontained fires contribute more than contained ones

    Returns a value between 0.0 (no wildfire risk) and 1.0 (maximum risk).
    """
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    result = db.execute(
        text("""
            WITH tract_centroid AS (
                SELECT ST_Centroid(geom) AS pt, geom AS tract_geom
                FROM census_tracts
                WHERE geoid = :geoid
            ),
            nearby_fires AS (
                SELECT
                    wi.acres_burned,
                    wi.percent_contained,
                    ST_Intersects(tc.tract_geom, wi.geom) AS overlaps,
                    ST_Distance(
                        tc.pt::geography,
                        ST_Centroid(wi.geom)::geography
                    ) / 1000.0 AS dist_km
                FROM wildfire_incidents wi, tract_centroid tc
                WHERE
                    wi.start_date >= :cutoff
                    AND ST_DWithin(tc.pt::geography, wi.geom::geography, 100000)
            )
            SELECT COALESCE(SUM(
                COALESCE(acres_burned, 100.0)
                * (100.0 - COALESCE(percent_contained, 0)) / 100.0
                * CASE WHEN overlaps THEN 5.0 ELSE 1.0 END
                / GREATEST(dist_km, 1.0)
            ), 0)
            FROM nearby_fires
        """),
        {"geoid": tract_geoid, "cutoff": cutoff},
    ).scalar()

    raw_score = float(result or 0.0)
    # Normalize: a large nearby uncontained fire (~10k acres at 10km) ≈ 1.0
    return float(min(raw_score / 5000.0, 1.0))


def compute_social_vulnerability_score(tract_geoid: str, db: Session) -> float:
    """
    Placeholder for CDC/ATSDR Social Vulnerability Index (SVI) score.

    SVI is a 0–1 composite of income, housing, racial minority status,
    transportation, and household composition factors.

    Phase 2: ingest actual SVI CSV from:
    https://www.atsdr.cdc.gov/placeandhealth/svi/data_documentation_download.html

    For now returns 0.5 (neutral) as a placeholder.
    """
    # TODO Phase 2: join against a svi_scores table populated from CDC data
    return 0.5


def compute_composite_score(
    flood: float,
    seismic: float,
    storm: float,
    wildfire: float,
    svi: float,
) -> float:
    """
    Compute weighted composite score from component scores.
    All inputs and output are in [0, 1].
    """
    score = (
        WEIGHTS["flood"] * flood
        + WEIGHTS["seismic"] * seismic
        + WEIGHTS["storm"] * storm
        + WEIGHTS["wildfire"] * wildfire
        + WEIGHTS["social_vulnerability"] * svi
    )
    return round(min(max(score, 0.0), 1.0), 6)


def score_county(county_fips: str, db: Session) -> int:
    """
    Compute and persist risk scores for all census tracts in a county.

    Args:
        county_fips: 5-digit county FIPS code
        db: SQLAlchemy session

    Returns:
        Number of tracts scored
    """
    tract_rows = db.execute(
        text("SELECT geoid FROM census_tracts WHERE county_fips = :fips"),
        {"fips": county_fips},
    ).fetchall()

    if not tract_rows:
        logger.warning("No census tracts found for county %s", county_fips)
        return 0

    logger.info("Scoring %d tracts for county %s", len(tract_rows), county_fips)
    scored = 0

    for (geoid,) in tract_rows:
        flood = compute_flood_score(geoid, db)
        seismic = compute_seismic_score(geoid, db)
        storm = compute_storm_score(geoid, db)
        wildfire = compute_wildfire_score(geoid, db)
        svi = compute_social_vulnerability_score(geoid, db)
        composite = compute_composite_score(flood, seismic, storm, wildfire, svi)

        # Upsert risk score
        db.execute(
            text("""
                INSERT INTO risk_scores
                    (tract_geoid, county_fips, flood_score, seismic_score,
                     storm_score, wildfire_score, social_vulnerability_score,
                     composite_score, computed_at)
                VALUES
                    (:geoid, :county, :flood, :seismic, :storm, :wildfire,
                     :svi, :composite, NOW())
                ON CONFLICT (tract_geoid)
                DO UPDATE SET
                    flood_score = EXCLUDED.flood_score,
                    seismic_score = EXCLUDED.seismic_score,
                    storm_score = EXCLUDED.storm_score,
                    wildfire_score = EXCLUDED.wildfire_score,
                    social_vulnerability_score = EXCLUDED.social_vulnerability_score,
                    composite_score = EXCLUDED.composite_score,
                    computed_at = EXCLUDED.computed_at
            """),
            {
                "geoid": geoid,
                "county": county_fips,
                "flood": flood,
                "seismic": seismic,
                "storm": storm,
                "wildfire": wildfire,
                "svi": svi,
                "composite": composite,
            },
        )
        scored += 1

        if scored % 100 == 0:
            db.commit()
            logger.info("Scored %d / %d tracts...", scored, len(tract_rows))

    db.commit()
    logger.info("County %s scoring complete: %d tracts", county_fips, scored)
    return scored


def run_scoring(county_fips: str | None = None, state_fips: str | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = SessionLocal()
    try:
        if county_fips:
            counties = [county_fips]
        elif state_fips:
            rows = db.execute(
                text("SELECT DISTINCT county_fips FROM census_tracts WHERE state_fips = :fips"),
                {"fips": state_fips},
            ).fetchall()
            counties = [r[0] for r in rows]
        else:
            raise ValueError("Must provide --county or --state")

        total = 0
        for county in counties:
            total += score_county(county, db)

        logger.info("=== Scoring complete. Total tracts scored: %d ===", total)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute risk scores for census tracts")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--county", help="5-digit county FIPS (e.g. 48201)")
    group.add_argument("--state", help="2-digit state FIPS (e.g. 48)")
    args = parser.parse_args()

    run_scoring(county_fips=args.county, state_fips=args.state)
