import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.responses import (
    DisasterDeclarationResponse,
    DisasterTrendResponse,
    StateTrendResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/disasters/declarations",
    response_model=list[DisasterDeclarationResponse],
)
def get_declarations(
    state: str | None = Query(None, description="2-letter state code (e.g. CA)"),
    incident_type: str | None = Query(
        None, description="Filter by type: Fire, Flood, Hurricane, etc."
    ),
    since_year: int = Query(2000, ge=1953, le=2030),
    limit: int = Query(200, ge=1, le=5000),
    db: Session = Depends(get_db),
) -> list[DisasterDeclarationResponse]:
    """
    Return FEMA disaster declarations filtered by state, type, and year.
    """
    rows = (
        db.execute(
            text("""
                SELECT disaster_number, fema_id, state, county_fips,
                       declaration_type, incident_type, declaration_title,
                       declaration_date, incident_begin_date, incident_end_date,
                       designated_area
                FROM disaster_declarations
                WHERE declaration_date >= :since
                    AND (:state IS NULL OR state = :state)
                    AND (:itype IS NULL OR incident_type = :itype)
                ORDER BY declaration_date DESC
                LIMIT :limit
            """),
            {
                "since": f"{since_year}-01-01",
                "state": state,
                "itype": incident_type,
                "limit": limit,
            },
        )
        .mappings()
        .all()
    )

    return [DisasterDeclarationResponse(**row) for row in rows]


@router.get("/disasters/trends/yearly", response_model=list[DisasterTrendResponse])
def get_yearly_trends(
    state: str | None = Query(None, description="2-letter state code"),
    incident_type: str | None = Query(None, description="Filter: Fire, Flood, Hurricane, etc."),
    since_year: int = Query(2000, ge=1953, le=2030),
    db: Session = Depends(get_db),
) -> list[DisasterTrendResponse]:
    """
    Return disaster declaration counts per year for trend analysis.
    Counts unique disaster numbers (not individual county declarations).
    """
    rows = (
        db.execute(
            text("""
                SELECT EXTRACT(YEAR FROM declaration_date)::int AS year,
                       COUNT(DISTINCT disaster_number) AS count
                FROM disaster_declarations
                WHERE declaration_date >= :since
                    AND (:state IS NULL OR state = :state)
                    AND (:itype IS NULL OR incident_type = :itype)
                GROUP BY year
                ORDER BY year
            """),
            {
                "since": f"{since_year}-01-01",
                "state": state,
                "itype": incident_type,
            },
        )
        .mappings()
        .all()
    )

    return [DisasterTrendResponse(year=r["year"], count=r["count"]) for r in rows]


@router.get("/disasters/trends/by-type", response_model=list[DisasterTrendResponse])
def get_trends_by_type(
    state: str | None = Query(None, description="2-letter state code"),
    since_year: int = Query(2000, ge=1953, le=2030),
    db: Session = Depends(get_db),
) -> list[DisasterTrendResponse]:
    """
    Return disaster counts grouped by incident type (Fire, Flood, Hurricane, etc.).
    """
    rows = (
        db.execute(
            text("""
                SELECT incident_type,
                       COUNT(DISTINCT disaster_number) AS count
                FROM disaster_declarations
                WHERE declaration_date >= :since
                    AND (:state IS NULL OR state = :state)
                GROUP BY incident_type
                ORDER BY count DESC
            """),
            {
                "since": f"{since_year}-01-01",
                "state": state,
            },
        )
        .mappings()
        .all()
    )

    return [
        DisasterTrendResponse(year=0, count=r["count"], incident_type=r["incident_type"])
        for r in rows
    ]


@router.get("/disasters/trends/states", response_model=list[StateTrendResponse])
def get_state_trends(
    since_year: int = Query(2000, ge=1953, le=2030),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
) -> list[StateTrendResponse]:
    """
    Return top states by total disaster count with yearly breakdown.
    Useful for answering: 'which states are getting worse?'
    """
    # Get top states by total count
    top_states = (
        db.execute(
            text("""
                SELECT state, COUNT(DISTINCT disaster_number) AS total
                FROM disaster_declarations
                WHERE declaration_date >= :since
                GROUP BY state
                ORDER BY total DESC
                LIMIT :limit
            """),
            {"since": f"{since_year}-01-01", "limit": limit},
        )
        .mappings()
        .all()
    )

    results = []
    for s in top_states:
        # Get yearly breakdown for this state
        yearly = (
            db.execute(
                text("""
                    SELECT EXTRACT(YEAR FROM declaration_date)::int AS year,
                           COUNT(DISTINCT disaster_number) AS count
                    FROM disaster_declarations
                    WHERE declaration_date >= :since AND state = :state
                    GROUP BY year
                    ORDER BY year
                """),
                {"since": f"{since_year}-01-01", "state": s["state"]},
            )
            .mappings()
            .all()
        )

        results.append(
            StateTrendResponse(
                state=s["state"],
                total=s["total"],
                trend=[DisasterTrendResponse(year=r["year"], count=r["count"]) for r in yearly],
            )
        )

    return results
