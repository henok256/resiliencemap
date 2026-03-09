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
