import json
import logging

from fastapi import APIRouter, Depends
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.hazard import FloodZone, SeismicHazard, WildfireIncident
from app.schemas.responses import GeoJSONFeature, GeoJSONFeatureCollection

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/hazards/geojson", response_model=GeoJSONFeatureCollection)
def get_hazards_geojson(
    layer: str = "flood",
    state_fips: str | None = None,
    db: Session = Depends(get_db),
) -> GeoJSONFeatureCollection:
    """
    Return hazard data as a GeoJSON FeatureCollection for map rendering.

    - **layer**: 'flood' (FEMA NFHL), 'seismic' (USGS earthquakes), or 'wildfire' (NIFC)
    - **state_fips**: optional 2-digit state FIPS to filter
    """
    features: list[GeoJSONFeature] = []

    if layer == "flood":
        query = select(
            FloodZone.id,
            FloodZone.fld_zone,
            FloodZone.sfha_tf,
            FloodZone.zone_subty,
            text("ST_AsGeoJSON(geom) AS geom_json"),
        ).select_from(FloodZone)

        if state_fips:
            query = query.where(FloodZone.state_fips == state_fips)

        rows = db.execute(query).mappings().all()
        for row in rows:
            features.append(
                GeoJSONFeature(
                    geometry=json.loads(row["geom_json"]),
                    properties={
                        "id": row["id"],
                        "flood_zone": row["fld_zone"],
                        "in_sfha": row["sfha_tf"] == "T",
                        "zone_subtype": row["zone_subty"],
                        "layer": "flood",
                    },
                )
            )

    elif layer == "seismic":
        rows = (
            db.execute(
                select(
                    SeismicHazard.usgs_id,
                    SeismicHazard.magnitude,
                    SeismicHazard.place,
                    SeismicHazard.event_time,
                    text("ST_AsGeoJSON(geom) AS geom_json"),
                )
                .select_from(SeismicHazard)
                .order_by(SeismicHazard.event_time.desc())
                .limit(500)
            )
            .mappings()
            .all()
        )

        for row in rows:
            features.append(
                GeoJSONFeature(
                    geometry=json.loads(row["geom_json"]),
                    properties={
                        "id": row["usgs_id"],
                        "magnitude": row["magnitude"],
                        "place": row["place"],
                        "event_time": row["event_time"].isoformat(),
                        "layer": "seismic",
                    },
                )
            )

    elif layer == "wildfire":
        query = (
            select(
                WildfireIncident.irwin_id,
                WildfireIncident.incident_name,
                WildfireIncident.acres_burned,
                WildfireIncident.percent_contained,
                WildfireIncident.fire_cause,
                WildfireIncident.start_date,
                text("ST_AsGeoJSON(geom) AS geom_json"),
            )
            .select_from(WildfireIncident)
            .order_by(WildfireIncident.acres_burned.desc().nullslast())
            .limit(500)
        )

        if state_fips:
            query = query.where(WildfireIncident.state_fips == state_fips)

        rows = db.execute(query).mappings().all()
        for row in rows:
            features.append(
                GeoJSONFeature(
                    geometry=json.loads(row["geom_json"]),
                    properties={
                        "id": row["irwin_id"],
                        "incident_name": row["incident_name"],
                        "acres_burned": row["acres_burned"],
                        "percent_contained": row["percent_contained"],
                        "fire_cause": row["fire_cause"],
                        "start_date": (
                            row["start_date"].isoformat() if row["start_date"] else None
                        ),
                        "layer": "wildfire",
                    },
                )
            )

    return GeoJSONFeatureCollection(features=features)
