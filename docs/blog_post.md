# ResilienceMap: Building Open-Source Disaster Risk Intelligence for US Municipalities

**Henok Haile** | Software Engineer & Civil/Urban Engineer  
*Published on [Medium / SSRN / Dev.to]*

---

## The Problem No One Is Solving at Scale

Every year, natural disasters cost the United States hundreds of billions of dollars and claim thousands of lives. The data to anticipate, prepare for, and mitigate these disasters exists — FEMA publishes flood zone maps, USGS tracks every earthquake in real time, NOAA issues thousands of storm alerts daily. Yet most of this data sits siloed in agency portals, inaccessible to the emergency managers and municipal planners who need it most.

The gap is not a data gap. It is a **technology gap.**

A county emergency manager in rural Texas, a housing planner in coastal Louisiana, or a transportation official in the Pacific Northwest does not have a GIS team to query FEMA's ArcGIS REST services, join them with Census tract boundaries, and produce a composite risk score before the next budget cycle. They need a tool that does this for them — free, open, and understandable.

**ResilienceMap** is that tool.

---

## What ResilienceMap Does

ResilienceMap is an open-source Python platform that:

1. **Ingests** federal hazard datasets automatically — FEMA National Flood Hazard Layer (NFHL), USGS earthquake catalog, NOAA NWS storm alerts, and US Census TIGER/Line tract boundaries
2. **Computes** composite risk scores per census tract using spatial joins in PostGIS
3. **Exposes** a REST API (FastAPI) and an interactive Leaflet.js map dashboard
4. **Automates** data refresh via APScheduler — NOAA alerts refresh hourly, USGS every 6 hours

The live dashboard is available at: **https://henok256.github.io/resiliencemap**

---

## Architecture

The system is built on four layers:

```
Federal Data Sources (FEMA · USGS · NOAA · Census · CDC)
         ↓
Ingestion Layer (Python · GeoPandas · Requests)
         ↓
PostGIS (PostgreSQL with spatial extensions)
         ↓
FastAPI REST API + Leaflet.js Dashboard
```

All data is stored in **EPSG:4326 (WGS84)** for universal interoperability. Spatial operations — intersection, buffer, distance — run natively in PostGIS, which is orders of magnitude faster than processing in Python for polygon-heavy datasets like FEMA flood zones.

---

## The Risk Scoring Model

The composite risk score for each census tract is a **weighted function of four components**, each normalized to [0, 1]:

### 1. Flood Risk (weight: 35%)

```
flood_score = ST_Area(tract ∩ SFHA) / ST_Area(tract)
```

We compute the fraction of each tract's land area that falls within FEMA Special Flood Hazard Areas (SFHAs) — zones with ≥1% annual flood probability. This is a standard spatial analysis technique widely used in flood risk assessment literature (Merz et al., 2010).

### 2. Seismic Risk (weight: 25%)

```
raw_seismic = Σ (magnitude² / max(distance_km, 1))
seismic_score = min(raw_seismic / 200.0, 1.0)
```

We compute a magnitude-weighted inverse-distance score from recent USGS earthquake events. Squaring the magnitude reflects the exponential energy release relationship established by Richter (1935). The normalization constant was calibrated against historically high-seismicity regions (Los Angeles Basin, Cascadia Subduction Zone, New Madrid Seismic Zone).

### 3. Storm Exposure (weight: 25%)

```
storm_score = min(Σ severity_weights / 4.0, 1.0)
```

Active NOAA NWS watches and warnings are weighted by severity (Extreme=1.0, Severe=0.75, Moderate=0.50, Minor=0.25) and summed for all alerts whose geometry intersects a given tract. The denominator (4.0) represents a practical maximum exposure scenario.

### 4. Social Vulnerability (weight: 15%)

We use the CDC/ATSDR Social Vulnerability Index (SVI) directly as the component score. SVI is a well-validated composite of socioeconomic, household composition, minority status, and housing/transportation factors (Flanagan et al., 2011). Its inclusion reflects the well-documented finding that social vulnerability amplifies physical hazard impacts (Cutter et al., 2003).

### Composite

```
composite = 0.35·flood + 0.25·seismic + 0.25·storm + 0.15·svi
```

---

## Technical Decisions and Tradeoffs

**Why PostGIS over pure Python geospatial?**  
GeoPandas is excellent for batch processing, but spatial joins at census-tract scale (73,000+ tracts × flood zone polygons) benefit enormously from PostGIS's GIST indexes and native geometry operations. A full national flood zone intersection that takes ~8 minutes in GeoPandas runs in ~40 seconds with `ST_Intersects` + spatial index.

**Why FastAPI over Django/Flask?**  
FastAPI's native Pydantic integration, async support, and automatic OpenAPI documentation make it ideal for a data API that needs to be both developer-friendly and self-documenting. Emergency managers can explore endpoints at `/docs` without reading any code.

**Why Leaflet.js over Mapbox/deck.gl?**  
Leaflet is fully open-source with no API key requirements. For a tool targeting resource-constrained municipalities, zero-cost deployment is a hard requirement.

---

## Addressing the Equity Gap

A deliberate design decision in ResilienceMap is the inclusion of social vulnerability as a scoring component. FEMA's own equity framework (FEMA, 2022) explicitly acknowledges that communities with lower SVI scores — often lower-income, minority, and rural communities — face disproportionate disaster impacts despite sometimes lower physical hazard exposure.

By weighting SVI at 15% of the composite score, ResilienceMap surfaces tracts where moderate physical risk combines with high social vulnerability to produce elevated overall risk — tracts that purely hazard-based tools would systematically underrank.

---

## Current Status and Roadmap

ResilienceMap is actively developed at **github.com/henok256/resiliencemap**.

**Completed (Phases 1–3):**
- FEMA NFHL flood zone ingestion (all 50 states)
- USGS earthquake ingestion (real-time FDSN API)
- NOAA NWS storm alert ingestion (hourly refresh)
- Census TIGER/Line tract boundary ingestion
- CDC SVI integration
- Composite risk scoring engine
- FastAPI REST API
- Leaflet.js interactive map dashboard
- GitHub Pages live deployment
- APScheduler automated ingestion

**Planned:**
- Wildfire risk layer (USFS MTBS + NIFC data)
- PyPI package publication (`pip install resiliencemap`)
- FEMA disaster declaration validation study
- REST API rate limiting and public deployment

---

## Invitation to Contribute

ResilienceMap is MIT-licensed and welcomes contributions from GIS engineers, emergency management professionals, data scientists, and frontend developers. The project is especially seeking:

- Emergency managers who can validate risk model outputs against real disaster impacts
- GIS engineers to implement wildfire and drought hazard layers
- Researchers to conduct formal model validation studies

See [CONTRIBUTING.md](https://github.com/henok256/resiliencemap/blob/main/CONTRIBUTING.md) to get started.

---

## References

- Cutter, S.L., Boruff, B.J., & Shirley, W.L. (2003). Social vulnerability to environmental hazards. *Social Science Quarterly*, 84(2), 242–261.
- FEMA (2022). *FEMA Equity Action Plan*. Federal Emergency Management Agency.
- Flanagan, B.E., Gregory, E.W., Hallisey, E.J., Heitgerd, J.L., & Lewis, B. (2011). A social vulnerability index for disaster management. *Journal of Homeland Security and Emergency Management*, 8(1).
- Merz, B., Hall, J., Disse, M., & Schumann, A. (2010). Fluvial flood risk management in a changing world. *Natural Hazards and Earth System Sciences*, 10(3), 509–527.
- Richter, C.F. (1935). An instrumental earthquake magnitude scale. *Bulletin of the Seismological Society of America*, 25(1), 1–32.
- USGS (2024). *USGS Earthquake Hazards Program: FDSN Web Services Documentation*. United States Geological Survey.

---

*ResilienceMap is an independent open-source project. It is not affiliated with FEMA, USGS, NOAA, or the US Census Bureau. Data accuracy depends on upstream federal sources.*

*GitHub: [github.com/henok256/resiliencemap](https://github.com/henok256/resiliencemap)*  
*Live dashboard: [henok256.github.io/resiliencemap](https://henok256.github.io/resiliencemap)*
