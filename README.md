# ResilienceMap 🛰️
### Open-Source US Disaster Risk Intelligence Platform

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://henok256.github.io/resiliencemap/)
[![CI](https://github.com/henok256/resiliencemap/actions/workflows/ci.yml/badge.svg)](https://github.com/henok256/resiliencemap/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)

> Making federal hazard intelligence accessible to every community — regardless of technical capacity or budget.

**[→ Live Dashboard](https://henok256.github.io/resiliencemap/)**

---

## Mission

The United States faces an accelerating disaster crisis. FEMA has declared over **3,400 major disasters since 2000**, obligating more than **$372 billion in federal aid**. Yet the communities most at risk — small municipalities, rural counties, and underserved populations — often lack the technical capacity to interpret and act on the federal datasets that exist precisely to help them.

ResilienceMap is a free, open-source platform that closes this gap. It ingests authoritative federal datasets (FEMA, USGS, NOAA, US Census), computes composite risk scores per census tract, and delivers them through a public REST API and interactive map dashboard — with **zero GIS expertise required** on the consumer side.

---

## National Scale & Impact

| Metric | Value |
|--------|-------|
| People in active hazard zones (live) | **109M+** |
| Federal disaster aid tracked (2000–present) | **$372.6B** |
| FEMA disaster declarations analyzed | **3,400+** |
| US census tracts covered | **84,000+** |
| Healthcare facilities mapped | **7,400+** |
| Data sources integrated | **6 federal agencies** |
| Platform availability | Free, public, no login required |

These figures are not projections — they are live statistics computed from authoritative federal sources on every page load.

---

## The Problem This Solves

The US has world-class federal hazard datasets. The problem is access:

- **FEMA NFHL** flood zone data requires specialized GIS software to query
- **USGS seismic hazard** data is published as PDFs and shapefiles, not APIs
- **NOAA NWS** alerts are machine-readable but require parsing complex CAP/XML feeds
- **CDC Social Vulnerability Index** adds a critical equity dimension but lives in a separate silo

No single tool unified these datasets at the census-tract level with a public API. Emergency managers in small counties, researchers studying climate displacement, and journalists covering disaster policy all had to build their own integrations from scratch.

**ResilienceMap provides the missing infrastructure layer.**

---

## Features

- 📡 **Live data** — NOAA storm alerts, USGS earthquakes, NIFC wildfires refresh every 5 minutes
- 🗺️ **Census-tract risk scoring** — composite score weighted by flood, seismic, storm, wildfire, and social vulnerability
- 🔌 **Public REST API** — query risk by county FIPS code, retrieve active alerts, export GeoJSON
- 📊 **Analytics dashboard** — disaster declarations by year/type/state, federal spending trends
- 👥 **Population exposure estimator** — live estimate of people in active hazard zones by state
- 🔔 **Configurable alerting** — webhook/email triggers on NOAA watches and warnings
- 🐳 **One-command deployment** via Docker Compose
- 🔄 **Automated data refresh** — GitHub Actions pipeline keeps federal spending totals current monthly

---

## Quickstart

```bash
# 1. Clone the repo
git clone https://github.com/henok256/resiliencemap.git
cd resiliencemap

# 2. Copy environment config
cp .env.example .env

# 3. Start all services (API + PostGIS + dashboard)
docker compose up --build

# 4. Run initial data ingestion
docker compose exec api python -m ingestion.fema.ingest_flood_zones
docker compose exec api python -m ingestion.usgs.ingest_earthquakes

# 5. Open the dashboard
open http://localhost:8000/dashboard
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/risk/county/{fips}` | Composite risk scores for all tracts in a county |
| GET | `/api/v1/risk/tract/{geoid}` | Risk score for a single census tract |
| GET | `/api/v1/hazards/geojson` | All active hazard layers as GeoJSON FeatureCollection |
| GET | `/api/v1/alerts/active` | Current NOAA watches & warnings |
| GET | `/health` | Health check |

Full API docs available at `http://localhost:8000/docs` (Swagger UI).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Data Sources                             │
│   FEMA NFHL  ·  USGS Earthquake  ·  NOAA NWS  ·  US Census     │
│   NIFC Wildfires  ·  HIFLD Infrastructure  ·  CDC/ATSDR SVI    │
└────────┬───────────────┬──────────────┬──────────┬──────────────┘
         │               │              │          │
         ▼               ▼              ▼          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Ingestion Layer (Python)                        │
│       GeoPandas · Requests · Scheduled via GitHub Actions        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                PostGIS (PostgreSQL + spatial)                     │
│   census_tracts · flood_zones · seismic_hazard ·                 │
│   storm_alerts · risk_scores · disaster_costs                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FastAPI Backend                               │
│              Risk API · Hazard API · Alert API                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Leaflet.js Map Dashboard                         │
│   Live counters · Analytics charts · Export tools                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Risk Scoring Methodology

Documented in full at [`docs/methodology.md`](docs/methodology.md). The composite risk score per census tract is a weighted function of:

| Component | Source | Weight |
|-----------|--------|--------|
| Flood risk | FEMA Special Flood Hazard Area (SFHA) coverage % | 30% |
| Seismic risk | USGS magnitude-weighted proximity score | 20% |
| Storm exposure | Active NOAA watches/warnings (30-day window) | 20% |
| Wildfire risk | NIFC perimeter proximity and acreage | 20% |
| Social vulnerability | CDC/ATSDR Social Vulnerability Index | 10% |

The social vulnerability component ensures the platform highlights not just physical exposure but community capacity to withstand and recover — a dimension consistently identified by FEMA as underrepresented in existing tools.

---

## Project Structure

```
resiliencemap/
├── app/                    # FastAPI application
│   ├── api/routes/         # Endpoint handlers
│   ├── core/               # Config, logging, settings
│   ├── db/                 # Database session, migrations
│   ├── models/             # SQLAlchemy ORM models
│   └── schemas/            # Pydantic request/response schemas
├── ingestion/              # Data ingestion modules
│   ├── fema/               # FEMA NFHL flood zone + disaster costs ingestion
│   ├── usgs/               # USGS earthquake data ingestion
│   └── noaa/               # NOAA NWS storm alerts ingestion
├── processing/             # Risk scoring algorithms
├── dashboard/              # Leaflet.js frontend (local dev)
├── docs/                   # Live GitHub Pages dashboard + methodology
├── tests/                  # Unit and integration tests
├── scripts/                # DB seed, migration helpers
├── .github/workflows/      # CI + automated FEMA data refresh
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

---

## Data Sources

| Source | Dataset | Update Frequency |
|--------|---------|-----------------|
| FEMA OpenFEMA | Disaster Declarations, Flood Zones (NFHL), Spending Summaries | Monthly (automated) |
| USGS FDSN | Earthquake events (M≥2.5, last 30 days) | Every 5 minutes |
| NOAA NWS | Active storm watches and warnings | Every 5 minutes |
| NIFC WFIGS | Active wildfire perimeters | Every 5 minutes |
| HIFLD | Hospital and critical infrastructure locations | Static (quarterly refresh) |
| US Census / CDC | Population estimates, Social Vulnerability Index | Annual |

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR.

Areas where help is particularly valuable:
- GIS / geospatial engineers
- Emergency management professionals who can validate risk models
- Frontend contributors (Leaflet.js / data visualization)
- Researchers working on climate risk, disaster policy, or environmental justice

---

## License

MIT — see [LICENSE](LICENSE). Free to use, adapt, and deploy.

---

## Citation

If you use ResilienceMap in research, policy work, or emergency planning, please cite:

```bibtex
@software{resiliencemap2025,
  title  = {ResilienceMap: Open-Source US Disaster Risk Intelligence Platform},
  author = {Mengesha, Henok},
  year   = {2025},
  url    = {https://github.com/henok256/resiliencemap},
  note   = {Free public dashboard: https://henok256.github.io/resiliencemap/}
}
```
