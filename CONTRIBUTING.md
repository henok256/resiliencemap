# Contributing to ResilienceMap

ResilienceMap is a public-benefit, open-source platform that makes federal disaster risk intelligence accessible to every US community — regardless of technical capacity or budget. Every contribution directly strengthens disaster preparedness tooling for municipalities, researchers, and emergency managers across the country.

We welcome contributors from geospatial engineering, emergency management, software development, data science, and policy research.

---

## Mission Alignment

Before contributing, it helps to understand what this project is trying to accomplish. ResilienceMap exists because the communities most exposed to disaster risk — small municipalities, rural counties, tribal nations, and low-income neighborhoods — are precisely those least equipped to access and act on the federal datasets that exist to help them.

Contributions should advance one or more of these outcomes:

- **Broaden coverage** — more hazard types, more census tracts, more data sources
- **Improve accuracy** — better risk scoring methodology, validated against real outcomes
- **Lower barriers** — simpler deployment, better documentation, more accessible UI
- **Advance equity** — surface disproportionate risk in vulnerable communities

If your contribution advances one of these goals, it belongs here.

---

## Getting Started

1. **Fork** the repository and clone your fork locally
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e ".[dev]"
   ```
3. Copy `.env.example` to `.env` and fill in values
4. Start PostGIS locally: `docker compose up db -d`
5. Run tests to confirm your setup: `pytest`

---

## Development Workflow

- Branch naming: `feature/your-feature`, `fix/issue-description`, `docs/topic`
- Commit style: [Conventional Commits](https://www.conventionalcommits.org/)
  - `feat: add NOAA alert webhook support`
  - `fix: correct FEMA projection transformation`
  - `docs: update methodology for seismic scoring`
- Open a PR against `main` with a clear description of what changed and why

---

## Priority Contribution Areas

These are the areas where contributions have the most direct public impact:

| Area | Skills Needed | Why It Matters |
|------|--------------|----------------|
| Risk model validation | Emergency management, GIS | Scores need to be tested against observed disaster outcomes before practitioners can rely on them |
| Additional hazard layers (wildfire, drought, extreme heat) | Python, GeoPandas | Wildfire and heat are the fastest-growing disaster categories in the US and are currently underrepresented |
| Tribal and rural coverage | GIS, federal data expertise | FEMA NFHL has significant coverage gaps in rural and tribal areas; these communities carry disproportionate risk |
| Social vulnerability integration | Data science, public health | Expanding beyond CDC SVI to include health system capacity, housing stability, and language access |
| Frontend dashboard improvements | Leaflet.js, JavaScript | The primary interface for non-technical users — clarity here directly affects who can use the platform |
| API performance and reliability | FastAPI, PostGIS | Enables use by third-party tools, government integrations, and researchers |
| Documentation and tutorials | Technical writing | Lowers the barrier for emergency managers and planners who are not software engineers |

---

## Code Standards

- Python 3.11+, type hints throughout
- `ruff` for linting, `black` for formatting
- Minimum 80% test coverage for new modules
- All geospatial operations must preserve or explicitly set CRS (EPSG:4326 for storage)
- New data sources must include a documented provenance statement (agency, dataset name, update frequency, license)

---

## Contributing Data or Methodology

If you are an emergency management professional, GIS specialist, or researcher, non-code contributions are equally valuable:

- **Methodology review** — critique or validate the [risk scoring methodology](methodology.md)
- **Ground-truth data** — share observed disaster outcomes that can be used to validate model predictions
- **Policy context** — help ensure the platform aligns with how emergency managers actually make decisions
- **Accessibility feedback** — test the dashboard as a non-technical user and report what's unclear

Open a GitHub Issue with the label `methodology` or `data-source` to start the conversation.

---

## Reporting Issues

Please use GitHub Issues with the appropriate label:

- `bug` — something is broken
- `enhancement` — new feature or improvement
- `data-source` — upstream data issues (FEMA/USGS/NOAA API changes, coverage gaps)
- `methodology` — questions or concerns about the risk scoring model
- `documentation` — gaps or errors in docs
- `equity` — coverage gaps or bias that disadvantages specific communities

---

## Code of Conduct

This project serves vulnerable communities. Treat all contributors and community members with respect. Constructive criticism of ideas is welcome; personal attacks are not. We hold ourselves to the same standard of rigor and care we expect in the disaster management systems we're trying to improve.

---

## Citation

If you use ResilienceMap in research or policy work, please cite it so the project can demonstrate impact:

```bibtex
@software{resiliencemap2025,
  title  = {ResilienceMap: Open-Source US Disaster Risk Intelligence Platform},
  author = {Mengesha, Henok},
  year   = {2025},
  url    = {https://github.com/henok256/resiliencemap},
  note   = {Free public dashboard: https://henok256.github.io/resiliencemap/}
}
```
