"""
NOAA National Weather Service Alert Ingestion
==============================================

Fetches active weather watches, warnings, and advisories from the
NOAA NWS public API (no API key required) and upserts them into PostGIS.

Data source: https://api.weather.gov/alerts/active
Format: GeoJSON FeatureCollection with CAP (Common Alerting Protocol) properties

Usage:
    python -m ingestion.noaa.ingest_alerts
    python -m ingestion.noaa.ingest_alerts --state TX
    python -m ingestion.noaa.ingest_alerts --severity Extreme Severe
"""

import argparse
import logging
from datetime import UTC, datetime

import requests
from shapely.geometry import MultiPolygon, shape
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)
settings = get_settings()

NOAA_ALERTS_URL = f"{settings.noaa_api_base}/alerts/active"

# NOAA severity tiers (from CAP standard)
SEVERITY_ORDER = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1, "Unknown": 0}


def fetch_active_alerts(
    state: str | None = None,
    severity: list[str] | None = None,
) -> list[dict]:
    """
    Fetch active NWS alerts from api.weather.gov.

    Args:
        state: optional 2-letter state code to filter (e.g. 'TX', 'CA')
        severity: optional list of severity levels to filter

    Returns:
        List of GeoJSON feature dicts
    """
    params: dict = {"status": "actual", "message_type": "alert"}

    if state:
        params["area"] = state.upper()

    if severity:
        params["severity"] = ",".join(severity)

    headers = {
        "User-Agent": (
            "ResilienceMap/0.1 (https://github.com/henok256/resiliencemap;"
            " open-source disaster risk platform)"
        ),
        "Accept": "application/geo+json",
    }

    logger.info("Fetching NOAA active alerts (state=%s, severity=%s)", state, severity)

    resp = requests.get(NOAA_ALERTS_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    features = data.get("features", [])
    logger.info("NOAA returned %d active alerts", len(features))
    return features


def _parse_geometry(feature: dict) -> str | None:
    """
    Parse GeoJSON geometry from a NOAA alert feature into WKT for PostGIS.

    NOAA alerts may have:
    - A direct geometry on the feature
    - geometry=null (alert covers a zone described in properties)

    Returns WKT string or None if no geometry available.
    """
    geom_data = feature.get("geometry")
    if not geom_data:
        return None

    try:
        geom = shape(geom_data)

        # Normalize everything to MultiPolygon for schema consistency
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        elif geom.geom_type == "MultiPolygon":
            pass  # already correct
        elif geom.geom_type == "GeometryCollection":
            polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
            if not polys:
                return None
            geom = MultiPolygon(
                [p for g in polys for p in (g.geoms if g.geom_type == "MultiPolygon" else [g])]
            )
        else:
            return None

        if geom.is_empty or not geom.is_valid:
            geom = geom.buffer(0)  # attempt repair

        return f"SRID=4326;{geom.wkt}"

    except Exception as e:
        logger.warning("Could not parse geometry: %s", e)
        return None


def _parse_datetime(dt_str: str | None) -> datetime | None:
    """Parse ISO 8601 datetime string from NOAA into naive UTC datetime."""
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(UTC).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def upsert_alerts(features: list[dict], db: Session) -> tuple[int, int]:
    """
    Upsert NOAA alert records into storm_alerts table.

    Strategy: INSERT new alerts, UPDATE existing ones by noaa_id.

    Returns:
        Tuple of (inserted, updated) counts
    """
    inserted = 0
    updated = 0

    for feature in features:
        props = feature.get("properties", {})
        noaa_id = props.get("id")

        if not noaa_id:
            continue

        geom_wkt = _parse_geometry(feature)

        # Check if record already exists
        existing = db.execute(
            text("SELECT id FROM storm_alerts WHERE noaa_id = :id"),
            {"id": noaa_id},
        ).scalar()

        alert_data = {
            "noaa_id": noaa_id,
            "event": props.get("event", "Unknown"),
            "severity": props.get("severity"),
            "certainty": props.get("certainty"),
            "headline": props.get("headline"),
            "description": (props.get("description") or "")[:2000],  # truncate
            "effective": _parse_datetime(props.get("effective")),
            "expires": _parse_datetime(props.get("expires")),
        }

        if existing:
            # Update existing record
            db.execute(
                text("""
                    UPDATE storm_alerts SET
                        event = :event,
                        severity = :severity,
                        certainty = :certainty,
                        headline = :headline,
                        description = :description,
                        effective = :effective,
                        expires = :expires,
                        geom = CASE WHEN :geom IS NOT NULL
                                    THEN ST_GeomFromEWKT(:geom)
                                    ELSE geom END,
                        ingested_at = NOW()
                    WHERE noaa_id = :noaa_id
                """),
                {**alert_data, "geom": geom_wkt},
            )
            updated += 1
        else:
            # Insert new record
            if geom_wkt:
                db.execute(
                    text("""
                        INSERT INTO storm_alerts
                            (noaa_id, event, severity, certainty, headline,
                             description, effective, expires, geom, ingested_at)
                        VALUES
                            (:noaa_id, :event, :severity, :certainty, :headline,
                             :description, :effective, :expires,
                             ST_GeomFromEWKT(:geom), NOW())
                    """),
                    {**alert_data, "geom": geom_wkt},
                )
            else:
                db.execute(
                    text("""
                        INSERT INTO storm_alerts
                            (noaa_id, event, severity, certainty, headline,
                             description, effective, expires, ingested_at)
                        VALUES
                            (:noaa_id, :event, :severity, :certainty, :headline,
                             :description, :effective, :expires, NOW())
                    """),
                    alert_data,
                )
            inserted += 1

    db.commit()

    # Clean up expired alerts older than 48 hours
    deleted = db.execute(text("""
            DELETE FROM storm_alerts
            WHERE expires < NOW() - INTERVAL '48 hours'
        """)).rowcount
    db.commit()

    if deleted:
        logger.info("Purged %d expired alerts", deleted)

    logger.info("Alert upsert complete: %d inserted, %d updated", inserted, updated)
    return inserted, updated


def run_ingestion(state: str | None = None, severity: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = SessionLocal()
    try:
        features = fetch_active_alerts(state=state, severity=severity)
        inserted, updated = upsert_alerts(features, db)
        logger.info("NOAA ingestion complete. Inserted: %d, Updated: %d", inserted, updated)
    except Exception as e:
        logger.error("NOAA ingestion failed: %s", e)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest active NOAA NWS storm alerts")
    parser.add_argument("--state", help="2-letter state code (e.g. TX)")
    parser.add_argument(
        "--severity",
        nargs="+",
        choices=["Extreme", "Severe", "Moderate", "Minor"],
        help="Filter by severity level(s)",
    )
    args = parser.parse_args()
    run_ingestion(state=args.state, severity=args.severity)
