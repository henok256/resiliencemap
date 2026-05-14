# ResilienceMap: National Impact Statement

**Platform:** ResilienceMap — Open-Source US Disaster Risk Intelligence  
**Live Dashboard:** https://henok256.github.io/resiliencemap/  
**Repository:** https://github.com/henok256/resiliencemap  
**Author:** Henok Mengesha  
**Last Updated:** 2025

---

## Executive Summary

ResilienceMap is a free, open-source platform that unifies six federal hazard datasets — FEMA, USGS, NOAA, NIFC, HIFLD, and the US Census Bureau — into a single public API and interactive dashboard. It is the only freely available tool that delivers census-tract-level composite disaster risk scores without requiring GIS expertise, a software license, or institutional affiliation.

The platform is in active public use, covers **84,000+ US census tracts**, tracks more than **$372 billion in federal disaster aid**, and monitors hazard exposure for an estimated **109 million Americans** in real time.

---

## 1. The Problem at National Scale

The United States has invested heavily in federal hazard datasets. FEMA's National Flood Hazard Layer (NFHL) is the world's most comprehensive flood zone dataset. USGS publishes real-time earthquake data globally. NOAA issues thousands of weather alerts annually. The CDC Social Vulnerability Index adds critical equity context.

Despite this investment, the data is practically inaccessible to the communities that need it most:

| Federal Dataset | Format | Barrier to Use |
|----------------|--------|----------------|
| FEMA NFHL flood zones | Shapefiles, WMS tiles | Requires ArcGIS or QGIS expertise |
| USGS seismic hazard | PDFs, shapefiles | No census-tract-level API |
| NOAA NWS alerts | CAP/XML feed | Complex parsing, no spatial join |
| CDC Social Vulnerability Index | CSV (tract-level) | No integration with hazard layers |

Emergency managers in small counties, rural municipalities, and tribal nations — the communities with the least technical capacity — cannot use these datasets without significant GIS expertise or expensive software. The communities most at risk are precisely those least equipped to act on the available information.

**ResilienceMap closes this gap.** It performs the spatial joins, normalizes the schemas, computes composite risk scores, and exposes everything through a public REST API and zero-installation map interface.

---

## 2. Alignment with Federal Priorities

### Robert T. Stafford Disaster Relief and Emergency Assistance Act (42 U.S.C. § 5121 et seq.)

The Stafford Act directs FEMA to support state and local governments in building disaster resilience, with explicit emphasis on pre-disaster mitigation and equitable access to federal resources. ResilienceMap directly advances this mandate by:

- Making FEMA disaster declaration and spending data accessible to non-technical users
- Integrating the CDC Social Vulnerability Index to surface equity dimensions that FEMA itself has identified as underrepresented in existing tools
- Providing a free, open-source alternative to commercial GIS platforms that small municipalities cannot afford

### FEMA Strategic Plan 2022–2026

FEMA's current strategic plan identifies three goals: (1) instill equity as a foundation of emergency management, (2) lead the whole-of-community in climate resilience, and (3) promote and sustain a ready FEMA. ResilienceMap contributes to all three:

- **Equity:** The SVI component in the composite risk score explicitly weights social vulnerability alongside physical hazard
- **Climate resilience:** Real-time NOAA and USGS feeds capture both acute hazards (storms, earthquakes) and flood zone exposure relevant to long-term climate risk
- **Ready FEMA:** By reducing the technical barrier to federal datasets, the platform multiplies the effective reach of FEMA's existing data infrastructure at zero cost to the agency

### National Preparedness Goal (Presidential Policy Directive 8)

PPD-8 established community resilience as a core national security objective and directed the development of tools that help communities "develop and maintain the capabilities necessary to prevent, protect against, mitigate, respond to, and recover from" hazards. ResilienceMap is a direct implementation of this objective for the 30,000+ local emergency management offices that lack the budget for commercial solutions.

### Bipartisan Infrastructure Law (2021) — BRIC Program

The Building Resilient Infrastructure and Communities (BRIC) program under the BIL allocates billions in pre-disaster mitigation grants, with priority for data-driven risk assessments. Applicants to BRIC grants require exactly the kind of census-tract-level risk scoring that ResilienceMap provides free of charge. The platform effectively lowers the barrier to competitive grant applications for under-resourced municipalities.

---

## 3. Quantified Impact

| Metric | Value | Source |
|--------|-------|--------|
| People in active hazard zones (live estimate) | 109M+ | USGS + NOAA + NIFC live feeds |
| Federal disaster aid tracked | $372.6B | FEMA OpenFEMA (2000–present) |
| FEMA disaster declarations analyzed | 3,400+ | FEMA DisasterDeclarationsSummaries |
| US census tracts covered | 84,000+ | US Census TIGER/Line |
| Healthcare facilities mapped | 7,400+ | HIFLD Critical Infrastructure |
| Federal agencies integrated | 6 | FEMA, USGS, NOAA, NIFC, HIFLD, CDC |
| Cost to end users | $0 | Free, no login required |
| Required software expertise | None | Browser-based, public URL |

These figures are computed from live federal data sources on every page load and are independently verifiable.

---

## 4. Technical Contribution

### What Existed Before

Before ResilienceMap, accessing multi-hazard census-tract risk scores required:
- A PostGIS database with FEMA NFHL shapefiles loaded (requires ~100 GB storage, GIS expertise)
- An ArcGIS or QGIS license ($500–$3,500/year for municipalities)
- Custom scripts to join FEMA, USGS, NOAA, and Census data (weeks of engineering time)
- No public API existed for on-demand tract-level composite risk scores

### What ResilienceMap Provides

- **Public REST API** — `/api/v1/risk/county/{fips}`, `/api/v1/risk/tract/{geoid}`, `/api/v1/hazards/geojson`, `/api/v1/alerts/active`
- **Zero-friction dashboard** — Leaflet.js map with live hazard overlays, no installation required
- **Composite risk scoring** — Flood (30%), Seismic (20%), Storm (20%), Wildfire (20%), Social Vulnerability (10%) weighted methodology documented in `methodology.md`
- **Open infrastructure** — MIT license, Docker Compose deployment, GitHub Actions automation; any municipality can self-host
- **Real-time feeds** — USGS earthquakes, NOAA alerts, and NIFC wildfires refresh every 5 minutes

### Methodological Innovation

The composite risk score integrates physical hazard (flood, seismic, storm) with social vulnerability at the census-tract level. This cross-domain integration — combining engineering hazard metrics with a sociological vulnerability index — is consistent with the most recent emergency management literature and addresses a gap FEMA has explicitly acknowledged in its own equity-focused strategic planning. The methodology is fully documented, reproducible, and open to peer review.

---

## 5. Accessibility and Equity Dimension

Commercial disaster risk platforms (RMS, AIR Worldwide, Verisk) charge licensing fees that place them out of reach for small and rural governments. Academic tools exist but require institutional access and technical expertise. ResilienceMap is the only tool in this category that is:

1. Entirely free with no registration
2. Browser-based (no software to install)
3. Backed by a public API usable from any programming language
4. Fully open-source with a documented deployment path

The 2020 US Census identified more than 19,000 incorporated places in the United States. The majority have populations under 5,000 and no dedicated GIS staff. ResilienceMap is designed specifically for this population.

---

## 6. Reproducibility and Auditability

All data sources used by ResilienceMap are in the public domain. All algorithms are published in this repository. Any researcher, journalist, or government auditor can:
- Reproduce any risk score from raw federal data
- Fork the repository and deploy an independent instance
- Extend the methodology for regional or domain-specific use cases

This transparency is a deliberate design choice that distinguishes ResilienceMap from proprietary alternatives and makes it suitable as a reference tool in academic research, policy analysis, and litigation support.

---

## 7. References

- FEMA (2022). *FEMA Strategic Plan 2022–2026*. Federal Emergency Management Agency. https://www.fema.gov/strategic-plan
- FEMA (2023). *National Flood Insurance Program: Flood Insurance Claims and Policy Data*. Federal Emergency Management Agency.
- Cutter, S.L., Boruff, B.J., & Shirley, W.L. (2003). Social vulnerability to environmental hazards. *Social Science Quarterly*, 84(2), 242–261.
- Fothergill, A., & Peek, L. (2004). Poverty and disasters in the United States. *Natural Hazards*, 32(1), 89–110.
- US Department of Homeland Security (2015). *National Preparedness Goal*, 2nd ed. Presidential Policy Directive 8.
- US Congress (2021). *Infrastructure Investment and Jobs Act (Bipartisan Infrastructure Law)*, P.L. 117-58. Building Resilient Infrastructure and Communities (BRIC) provisions.
- Stafford, R.T. (1988). *Robert T. Stafford Disaster Relief and Emergency Assistance Act*, 42 U.S.C. § 5121 et seq.
