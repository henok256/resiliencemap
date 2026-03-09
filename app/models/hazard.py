"""
ORM models for ResilienceMap.

All geometries stored in EPSG:4326 (WGS84 lat/lon) for interoperability.
Spatial indexes are created automatically by GeoAlchemy2.
"""

from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.session import Base


class CensusTract(Base):
    """US Census tract boundaries (source: Census TIGER/Line)."""

    __tablename__ = "census_tracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    geoid: Mapped[str] = mapped_column(String(11), unique=True, nullable=False, index=True)
    state_fips: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    county_fips: Mapped[str] = mapped_column(String(5), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(100))
    land_area_sqm: Mapped[float | None] = mapped_column(Float)
    geom: Mapped[bytes] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)

    __table_args__ = (Index("idx_census_tracts_geom", "geom", postgresql_using="gist"),)


class FloodZone(Base):
    """FEMA National Flood Hazard Layer — Special Flood Hazard Areas."""

    __tablename__ = "flood_zones"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fld_zone: Mapped[str] = mapped_column(String(17), nullable=False, index=True)
    # A, AE, AH, AO = high risk; X = minimal; V, VE = coastal high risk
    zone_subty: Mapped[str | None] = mapped_column(String(72))
    sfha_tf: Mapped[str | None] = mapped_column(String(1))  # T = in SFHA, F = not
    state_fips: Mapped[str | None] = mapped_column(String(2), index=True)
    geom: Mapped[bytes] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_flood_zones_geom", "geom", postgresql_using="gist"),)


class SeismicHazard(Base):
    """USGS earthquake hazard — recent significant events."""

    __tablename__ = "seismic_hazard"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    usgs_id: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    magnitude: Mapped[float] = mapped_column(Float, nullable=False)
    depth_km: Mapped[float | None] = mapped_column(Float)
    place: Mapped[str | None] = mapped_column(String(255))
    event_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    geom: Mapped[bytes] = mapped_column(Geometry("POINT", srid=4326), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_seismic_geom", "geom", postgresql_using="gist"),)


class StormAlert(Base):
    """NOAA NWS active watches, warnings, and advisories."""

    __tablename__ = "storm_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    noaa_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    event: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    severity: Mapped[str | None] = mapped_column(String(50))  # Extreme, Severe, Moderate, Minor
    certainty: Mapped[str | None] = mapped_column(String(50))
    headline: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    effective: Mapped[datetime | None] = mapped_column(DateTime)
    expires: Mapped[datetime | None] = mapped_column(DateTime, index=True)
    geom: Mapped[bytes | None] = mapped_column(Geometry("MULTIPOLYGON", srid=4326))
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_storm_alerts_geom", "geom", postgresql_using="gist"),)


class WildfireIncident(Base):
    """NIFC active wildfire incidents with fire perimeter boundaries."""

    __tablename__ = "wildfire_incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    irwin_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    incident_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    acres_burned: Mapped[float | None] = mapped_column(Float)
    percent_contained: Mapped[float | None] = mapped_column(Float)
    state_fips: Mapped[str | None] = mapped_column(String(2), index=True)
    fire_cause: Mapped[str | None] = mapped_column(String(100))
    start_date: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime)
    geom: Mapped[bytes] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_wildfire_geom", "geom", postgresql_using="gist"),)


class RiskScore(Base):
    """Computed composite risk score per census tract."""

    __tablename__ = "risk_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tract_geoid: Mapped[str] = mapped_column(String(11), nullable=False, index=True)
    county_fips: Mapped[str] = mapped_column(String(5), nullable=False, index=True)

    # Component scores (0.0 – 1.0)
    flood_score: Mapped[float] = mapped_column(Float, default=0.0)
    seismic_score: Mapped[float] = mapped_column(Float, default=0.0)
    storm_score: Mapped[float] = mapped_column(Float, default=0.0)
    wildfire_score: Mapped[float] = mapped_column(Float, default=0.0)
    social_vulnerability_score: Mapped[float] = mapped_column(Float, default=0.0)

    # Weighted composite (0.0 – 1.0)
    composite_score: Mapped[float] = mapped_column(Float, nullable=False, index=True)

    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
