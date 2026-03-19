from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RiskScoreResponse(BaseModel):
    tract_geoid: str
    county_fips: str
    flood_score: float = Field(ge=0.0, le=1.0)
    seismic_score: float = Field(ge=0.0, le=1.0)
    storm_score: float = Field(ge=0.0, le=1.0)
    wildfire_score: float = Field(ge=0.0, le=1.0)
    social_vulnerability_score: float = Field(ge=0.0, le=1.0)
    composite_score: float = Field(ge=0.0, le=1.0)
    computed_at: datetime

    model_config = {"from_attributes": True}


class CountyRiskResponse(BaseModel):
    county_fips: str
    tract_count: int
    avg_composite_score: float
    max_composite_score: float
    tracts: list[RiskScoreResponse]


class StormAlertResponse(BaseModel):
    noaa_id: str
    event: str
    severity: str | None
    certainty: str | None
    headline: str | None
    effective: datetime | None
    expires: datetime | None

    model_config = {"from_attributes": True}


class InfrastructureResponse(BaseModel):
    hifld_id: str
    facility_type: str
    name: str
    address: str | None
    city: str | None
    state_fips: str | None
    capacity: int | None
    status: str | None
    latitude: float
    longitude: float

    model_config = {"from_attributes": True}


class AtRiskInfrastructureResponse(BaseModel):
    hifld_id: str
    facility_type: str
    name: str
    address: str | None
    city: str | None
    state_fips: str | None
    capacity: int | None
    composite_score: float
    tract_geoid: str


class DisasterDeclarationResponse(BaseModel):
    disaster_number: int
    fema_id: str
    state: str
    county_fips: str | None
    declaration_type: str
    incident_type: str
    declaration_title: str | None
    declaration_date: datetime
    incident_begin_date: datetime | None
    incident_end_date: datetime | None
    designated_area: str | None

    model_config = {"from_attributes": True}


class DisasterTrendResponse(BaseModel):
    year: int
    count: int
    incident_type: str | None = None


class StateTrendResponse(BaseModel):
    state: str
    total: int
    trend: list[DisasterTrendResponse]


class DisasterCostResponse(BaseModel):
    disaster_number: int
    total_ihp_approved: float
    total_ha_approved: float
    total_ona_approved: float
    total_pa_obligated: float
    total_hmgp_obligated: float
    total_cost: float

    model_config = {"from_attributes": True}


class CostTrendResponse(BaseModel):
    year: int
    total_cost: float
    disaster_count: int


class CostByTypeResponse(BaseModel):
    incident_type: str
    total_cost: float
    disaster_count: int


class GeoJSONFeature(BaseModel):
    type: str = "Feature"
    geometry: dict[str, Any]
    properties: dict[str, Any]


class GeoJSONFeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: list[GeoJSONFeature]


class HealthResponse(BaseModel):
    status: str
    database: str
    postgis: str
    version: str = "0.1.0"
