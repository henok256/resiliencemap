import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.hazard import RiskScore
from app.schemas.responses import CountyRiskResponse, RiskScoreResponse, StateRiskResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/risk/county/{fips}", response_model=CountyRiskResponse)
def get_county_risk(fips: str, db: Session = Depends(get_db)) -> CountyRiskResponse:
    """
    Return composite risk scores for all census tracts in a county.

    - **fips**: 5-digit county FIPS code (e.g. '06037' for Los Angeles County)
    """
    rows = db.execute(select(RiskScore).where(RiskScore.county_fips == fips)).scalars().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No risk data found for county FIPS {fips}. "
            "Run ingestion and risk scoring first.",
        )

    tracts = [RiskScoreResponse.model_validate(r) for r in rows]
    scores = [t.composite_score for t in tracts]

    return CountyRiskResponse(
        county_fips=fips,
        tract_count=len(tracts),
        avg_composite_score=round(sum(scores) / len(scores), 4),
        max_composite_score=round(max(scores), 4),
        tracts=tracts,
    )


@router.get("/risk/tract/{geoid}", response_model=RiskScoreResponse)
def get_tract_risk(geoid: str, db: Session = Depends(get_db)) -> RiskScoreResponse:
    """
    Return the latest composite risk score for a single census tract.

    - **geoid**: 11-digit census tract GEOID (e.g. '06037137000')
    """
    row = db.execute(
        select(RiskScore)
        .where(RiskScore.tract_geoid == geoid)
        .order_by(RiskScore.computed_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No risk data found for tract GEOID {geoid}.",
        )

    return RiskScoreResponse.model_validate(row)


@router.get("/risk/state/{state_fips}", response_model=StateRiskResponse)
def get_state_risk(
    state_fips: str,
    top: int = 20,
    db: Session = Depends(get_db),
) -> StateRiskResponse:
    """
    Return aggregate risk statistics for all census tracts in a state.

    - **state_fips**: 2-digit state FIPS code (e.g. '48' for Texas)
    - **top**: number of highest-risk tracts to include (default 20, max 100)
    """
    top = min(top, 100)

    rows = (
        db.execute(select(RiskScore).where(RiskScore.county_fips.startswith(state_fips)))
        .scalars()
        .all()
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No risk data found for state FIPS {state_fips}. "
            "Run ingestion and risk scoring first.",
        )

    scores = [r.composite_score for r in rows]
    county_count = len({r.county_fips for r in rows})
    top_tracts = sorted(rows, key=lambda r: r.composite_score, reverse=True)[:top]

    return StateRiskResponse(
        state_fips=state_fips,
        county_count=county_count,
        tract_count=len(rows),
        avg_composite_score=round(sum(scores) / len(scores), 4),
        max_composite_score=round(max(scores), 4),
        top_tracts=[RiskScoreResponse.model_validate(r) for r in top_tracts],
    )


@router.get("/risk/top", response_model=list[RiskScoreResponse])
def get_highest_risk_tracts(
    limit: int = 20,
    state_fips: str | None = None,
    db: Session = Depends(get_db),
) -> list[RiskScoreResponse]:
    """
    Return the highest-risk census tracts nationally or filtered by state.

    - **limit**: number of tracts to return (default 20, max 100)
    - **state_fips**: optional 2-digit state FIPS to filter results
    """
    limit = min(limit, 100)
    query = select(RiskScore).order_by(RiskScore.composite_score.desc()).limit(limit)

    if state_fips:
        # county_fips starts with state_fips
        query = query.where(RiskScore.county_fips.startswith(state_fips))

    rows = db.execute(query).scalars().all()
    return [RiskScoreResponse.model_validate(r) for r in rows]
