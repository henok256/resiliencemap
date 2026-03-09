"""
ResilienceMap Automated Ingestion Scheduler
============================================

Runs data ingestion jobs on a schedule using APScheduler.
Designed to run as a long-lived background process (separate Docker service).

Schedule:
  - NOAA storm alerts:    every 1 hour   (data changes frequently)
  - USGS earthquakes:     every 6 hours  (new events accumulate)
  - NIFC wildfires:       every 12 hours (perimeters update daily)
  - FEMA flood zones:     every 7 days   (rarely updated)

Usage:
    python -m scripts.run_scheduler
"""

import logging
import sys

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ingestion.census.ingest_svi import run_ingestion as run_svi
from ingestion.nifc.ingest_wildfires import run_ingestion as run_wildfires
from ingestion.noaa.ingest_alerts import run_ingestion as run_noaa
from ingestion.usgs.ingest_earthquakes import run_ingestion as run_usgs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def job_noaa_alerts() -> None:
    """Hourly: ingest all active NOAA NWS storm alerts."""
    logger.info("=== SCHEDULED JOB: NOAA alerts ingestion ===")
    run_noaa()


def job_usgs_earthquakes() -> None:
    """Every 6 hours: ingest recent USGS earthquakes (last 30 days, M2.5+)."""
    logger.info("=== SCHEDULED JOB: USGS earthquake ingestion ===")
    run_usgs(days_back=30, min_magnitude=2.5)


def job_wildfire_incidents() -> None:
    """Every 12 hours: ingest active NIFC wildfire perimeters."""
    logger.info("=== SCHEDULED JOB: NIFC wildfire ingestion ===")
    run_wildfires()


def job_svi_refresh() -> None:
    """Weekly: refresh CDC SVI scores (published annually, weekly check is sufficient)."""
    logger.info("=== SCHEDULED JOB: CDC SVI refresh ===")
    run_svi()


def on_job_event(event: JobExecutionEvent) -> None:
    if event.exception:
        logger.error("Job %s FAILED: %s", event.job_id, event.exception)
    else:
        logger.info("Job %s completed successfully", event.job_id)


def main() -> None:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_listener(on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # NOAA: every hour at minute 5 (stagger from top of hour)
    scheduler.add_job(
        job_noaa_alerts,
        trigger=IntervalTrigger(hours=1),
        id="noaa_alerts",
        name="NOAA NWS Alert Ingestion",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # USGS: every 6 hours
    scheduler.add_job(
        job_usgs_earthquakes,
        trigger=IntervalTrigger(hours=6),
        id="usgs_earthquakes",
        name="USGS Earthquake Ingestion",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # NIFC Wildfires: every 12 hours (perimeters updated daily)
    scheduler.add_job(
        job_wildfire_incidents,
        trigger=IntervalTrigger(hours=12),
        id="nifc_wildfires",
        name="NIFC Wildfire Perimeter Ingestion",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # CDC SVI: weekly (data published annually, weekly check is sufficient)
    scheduler.add_job(
        job_svi_refresh,
        trigger=CronTrigger(day_of_week="sun", hour=3),
        id="cdc_svi",
        name="CDC SVI Refresh",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    logger.info("ResilienceMap scheduler started. Jobs registered:")
    for job in scheduler.get_jobs():
        logger.info("  - %s (%s)", job.name, job.id)

    # Run all jobs immediately on startup so data is fresh from the start
    logger.info("Running initial ingestion on startup...")
    try:
        job_noaa_alerts()
    except Exception as e:
        logger.error("Initial NOAA ingestion failed: %s", e)

    try:
        job_usgs_earthquakes()
    except Exception as e:
        logger.error("Initial USGS ingestion failed: %s", e)

    try:
        job_wildfire_incidents()
    except Exception as e:
        logger.error("Initial NIFC wildfire ingestion failed: %s", e)

    logger.info("Initial ingestion complete. Scheduler now running...")
    scheduler.start()


if __name__ == "__main__":
    main()
