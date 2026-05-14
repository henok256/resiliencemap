# Changelog

All notable changes to ResilienceMap are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

---

## [1.0.0] — General Availability — 2025

### Added
- **State-level risk API endpoint** (`GET /api/v1/risk/state/{state_fips}`)
  - Aggregates tract-level scores across all counties in a state
  - Returns county count, tract count, avg/max composite scores, and top N tracts
  - Covers all 50 states + DC; 404 when scoring data is not yet available
- **State-filtered analytics on dashboard** (`docs/index.html`)
  - Searching a state filters the yearly declarations chart to that state's history
  - State summary card shows total declarations, peak year, last-5-year count
  - Top 10 States bar chart highlights the selected state in blue
  - Population exposure counter scopes to selected state
- **Shareable state URLs** — selecting a state updates `#state=TX` in the URL; page restores state on load
- **Integration test suite** (`tests/integration/`) — 41 tests covering all API route groups
  - Health, Risk (county/tract/state/top), Alerts, Hazards GeoJSON, Disasters (declarations, trends, costs)
  - SA 2.0 savepoint-based transaction isolation; no data persists between tests
- **National impact statement** (`docs/IMPACT.md`)
  - Maps platform to Stafford Act, FEMA Strategic Plan 2022–2026, PPD-8, and BIL BRIC program
  - Quantified coverage: 109M+ people, $372.6B tracked, 84k+ tracts, 6 federal agencies
- **Monthly FEMA aid auto-refresh** (`.github/workflows/update-fema-aid.yml`)
  - Downloads FEMA bulk CSVs on the 1st of each month
  - Calculates 2000-present total and updates `FEMA_TOTAL_AID` constant via `sed`
  - Commits and pushes only when the value changes

### Fixed
- **SVI scoring placeholder removed** — `compute_social_vulnerability_score` now queries the `svi_scores` table populated by `ingestion.census.ingest_svi`; falls back to 0.5 (neutral) only when SVI data has not been ingested
- **Risk scoring weights synced across all files** — docs previously showed three different weight sets; code is now authoritative: Flood 30% / Seismic 20% / Storm 20% / Wildfire 20% / SVI 10%
- **FEMA API degradation** — replaced live `$filter` queries (returning 503) with embedded static dataset (`FEMA_STATIC`) built from FEMA bulk CSV download
- **Live hazard counters** — earthquakes, fires, and facilities now show real counts via parallel count-only endpoint calls instead of hardcoded API cap values
- **Federal Disaster Aid "N/A"** — replaced with `FEMA_TOTAL_AID` static constant ($372.6B) auto-refreshed monthly
- **FEMA NFHL WMS 404** — updated endpoint path from `gis/nfhl/rest` to `arcgis/rest`
- **`svi_scores` table** added to `init_db.sql` so it is created on first Docker startup

### Changed
- **README** rewritten with national impact framing: Mission section, National Scale & Impact metrics table, The Problem This Solves section
- **CONTRIBUTING.md** updated with mission alignment section, "Why It Matters" column in contribution areas, non-code contribution paths, and citation section
- **`docs/methodology.md`** updated: added wildfire as a scored component with full methodology section; corrected all component weights

---

## [0.3.0] — Phase 3 — 2025

### Added
- **Census TIGER/Line tract boundary ingestion** (`ingestion/census/ingest_tracts.py`)
  - Downloads state shapefiles from Census Bureau, reprojects to EPSG:4326
  - ON CONFLICT upsert — safe to re-run; supports all 50 states + DC
- **CDC/ATSDR Social Vulnerability Index ingestion** (`ingestion/census/ingest_svi.py`)
  - Downloads 2022 national SVI CSV, loads into `svi_scores` table
  - Backfills real SVI scores replacing Phase 1/2 placeholder (0.5)
- **GitHub Pages live dashboard** (`docs/index.html`)
  - Fully static — fetches NOAA & USGS APIs directly, no backend needed
  - Live at: https://henok256.github.io/resiliencemap
  - FEMA flood zones via WMS tile service; auto-refresh every 5 minutes
- **Technical blog post** (`docs/blog_post.md`) with academic citations for SSRN/Medium
- **PyPI-ready metadata** — updated `pyproject.toml` with project URLs and classifiers
- Version bumped to 0.3.0

## [0.2.0] — Phase 2 — 2025

### Added
- **NOAA NWS storm alert ingestion** (`ingestion/noaa/ingest_alerts.py`)
  - Fetches all active watches, warnings, and advisories from `api.weather.gov`
  - Parses CAP geometry into PostGIS MULTIPOLYGON
  - Idempotent upsert — safe to run on any schedule
  - Auto-purge of alerts expired >48 hours
- **Leaflet.js interactive map dashboard** (`dashboard/index.html`)
  - Dark-themed full-screen map centered on CONUS
  - Layer toggles: Storm Alerts, Earthquakes, FEMA Flood Zones, Risk Scores
  - Severity-colored storm alert markers with popup details
  - Magnitude-scaled earthquake circle markers
  - FEMA flood zone polygons colored by SFHA status
  - Sidebar with live stats and active alert list
  - Auto-refresh every 5 minutes
  - Graceful demo mode when API is not running
- **APScheduler automated ingestion** (`scripts/run_scheduler.py`)
  - NOAA alerts: every 1 hour
  - USGS earthquakes: every 6 hours
  - Runs initial ingestion on startup
  - Job event logging with error reporting
- **Unit tests** for NOAA geometry and datetime parsing (`tests/unit/test_noaa_ingestion.py`)
- **CHANGELOG.md** — this file

---

## [0.1.0] — Phase 1 — 2025

### Added
- Project scaffold: FastAPI app, PostGIS schema, Docker Compose
- **FEMA NFHL flood zone ingestion** (`ingestion/fema/ingest_flood_zones.py`)
  - Fetches Special Flood Hazard Area polygons per state
  - Bounding-box spatial filtering for all 50 states
  - Batched upsert with state-level delete-then-insert
- **USGS earthquake ingestion** (`ingestion/usgs/ingest_earthquakes.py`)
  - FDSN event service integration
  - Configurable lookback window and minimum magnitude
  - Idempotent insert by USGS event ID
- **Composite risk scoring engine** (`processing/score_tracts.py`)
  - Flood score: FEMA SFHA area intersection ratio
  - Seismic score: magnitude²/distance proximity model
  - Storm score: NOAA severity-weighted alert intersection
  - Social vulnerability: CDC/ATSDR SVI (Phase 2 placeholder)
  - Weighted composite: 30% flood / 20% seismic / 20% storm / 20% wildfire / 10% SVI
- **FastAPI REST API** with endpoints:
  - `GET /api/v1/risk/county/{fips}` — county risk summary
  - `GET /api/v1/risk/tract/{geoid}` — single tract risk score
  - `GET /api/v1/risk/top` — highest-risk tracts nationally
  - `GET /api/v1/hazards/geojson` — flood and seismic GeoJSON
  - `GET /api/v1/alerts/active` — active storm alerts
  - `GET /health` — service health check
- **Methodology documentation** (`docs/methodology.md`) with academic references
- MIT License, CONTRIBUTING.md, .gitignore
- GitHub Actions CI pipeline
- Unit tests for risk scoring engine
