"""
Integration tests for ResilienceMap API routes.

These tests run against a real PostGIS database (provided by the CI service
or a local Docker container). Each test is transaction-isolated via the
conftest.py fixtures — no data persists between tests.
"""

from datetime import UTC, datetime, timedelta

import pytest

from app.models.hazard import (
    DisasterCost,
    DisasterDeclaration,
    RiskScore,
    StormAlert,
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_database_ok(self, client):
        body = client.get("/health").json()
        assert body["database"] == "ok"

    def test_postgis_available(self, client):
        body = client.get("/health").json()
        assert "ok" in body["postgis"]

    def test_overall_status_ok(self, client):
        body = client.get("/health").json()
        assert body["status"] == "ok"


# ---------------------------------------------------------------------------
# Risk scores
# ---------------------------------------------------------------------------

@pytest.fixture()
def risk_row(db):
    row = RiskScore(
        tract_geoid="48201950100",
        county_fips="48201",
        flood_score=0.6,
        seismic_score=0.1,
        storm_score=0.2,
        wildfire_score=0.05,
        social_vulnerability_score=0.75,
        composite_score=0.34,
        computed_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


class TestRiskCounty:
    def test_404_when_no_data(self, client):
        r = client.get("/api/v1/risk/county/99999")
        assert r.status_code == 404

    def test_200_when_data_present(self, client, risk_row):
        r = client.get("/api/v1/risk/county/48201")
        assert r.status_code == 200

    def test_response_shape(self, client, risk_row):
        body = client.get("/api/v1/risk/county/48201").json()
        assert body["county_fips"] == "48201"
        assert body["tract_count"] == 1
        assert "avg_composite_score" in body
        assert "max_composite_score" in body
        assert len(body["tracts"]) == 1

    def test_tract_fields_present(self, client, risk_row):
        tract = client.get("/api/v1/risk/county/48201").json()["tracts"][0]
        for field in ("tract_geoid", "flood_score", "seismic_score",
                      "storm_score", "wildfire_score",
                      "social_vulnerability_score", "composite_score"):
            assert field in tract

    def test_scores_in_range(self, client, risk_row):
        tract = client.get("/api/v1/risk/county/48201").json()["tracts"][0]
        for field in ("flood_score", "seismic_score", "storm_score",
                      "wildfire_score", "social_vulnerability_score", "composite_score"):
            assert 0.0 <= tract[field] <= 1.0


class TestRiskTract:
    def test_404_when_no_data(self, client):
        r = client.get("/api/v1/risk/tract/00000000000")
        assert r.status_code == 404

    def test_200_when_data_present(self, client, risk_row):
        r = client.get("/api/v1/risk/tract/48201950100")
        assert r.status_code == 200

    def test_correct_geoid_returned(self, client, risk_row):
        body = client.get("/api/v1/risk/tract/48201950100").json()
        assert body["tract_geoid"] == "48201950100"


class TestRiskState:
    def test_404_when_no_data(self, client):
        r = client.get("/api/v1/risk/state/99")
        assert r.status_code == 404

    def test_200_when_data_present(self, client, risk_row):
        r = client.get("/api/v1/risk/state/48")
        assert r.status_code == 200

    def test_response_shape(self, client, risk_row):
        body = client.get("/api/v1/risk/state/48").json()
        assert body["state_fips"] == "48"
        assert body["tract_count"] == 1
        assert body["county_count"] == 1
        assert "avg_composite_score" in body
        assert "max_composite_score" in body
        assert isinstance(body["top_tracts"], list)

    def test_top_param_respected(self, client, db):
        for i in range(5):
            db.add(RiskScore(
                tract_geoid=f"3600000{i:04d}",
                county_fips="36000",
                flood_score=0.1,
                seismic_score=0.1,
                storm_score=0.1,
                wildfire_score=0.1,
                social_vulnerability_score=0.1,
                composite_score=round(0.1 + 0.01 * i, 6),
                computed_at=datetime.now(UTC),
            ))
        db.flush()
        body = client.get("/api/v1/risk/state/36?top=2").json()
        assert len(body["top_tracts"]) <= 2


class TestRiskTop:
    def test_returns_200(self, client):
        r = client.get("/api/v1/risk/top")
        assert r.status_code == 200

    def test_returns_list(self, client):
        assert isinstance(client.get("/api/v1/risk/top").json(), list)

    def test_limit_param_respected(self, client, db):
        for i in range(5):
            db.add(RiskScore(
                tract_geoid=f"0600000{i:04d}",
                county_fips="06000",
                flood_score=0.1 * i,
                seismic_score=0.0,
                storm_score=0.0,
                wildfire_score=0.0,
                social_vulnerability_score=0.0,
                composite_score=round(0.1 * i, 6),
                computed_at=datetime.now(UTC),
            ))
        db.flush()
        body = client.get("/api/v1/risk/top?limit=3").json()
        assert len(body) <= 3

    def test_ordered_by_composite_score_desc(self, client, db):
        scores = [0.9, 0.3, 0.6]
        for i, s in enumerate(scores):
            db.add(RiskScore(
                tract_geoid=f"1200000{i:04d}",
                county_fips="12000",
                flood_score=s,
                seismic_score=0.0,
                storm_score=0.0,
                wildfire_score=0.0,
                social_vulnerability_score=0.0,
                composite_score=s,
                computed_at=datetime.now(UTC),
            ))
        db.flush()
        body = client.get("/api/v1/risk/top?state_fips=12").json()
        returned_scores = [t["composite_score"] for t in body]
        assert returned_scores == sorted(returned_scores, reverse=True)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

class TestAlerts:
    def test_returns_200(self, client):
        assert client.get("/api/v1/alerts/active").status_code == 200

    def test_empty_list_when_no_alerts(self, client):
        assert client.get("/api/v1/alerts/active").json() == []

    def test_expired_alert_not_returned(self, client, db):
        db.add(StormAlert(
            noaa_id="test-expired-001",
            event="Tornado Warning",
            severity="Extreme",
            certainty="Observed",
            headline="Test expired alert",
            effective=datetime.now(UTC) - timedelta(hours=4),
            expires=datetime.now(UTC) - timedelta(hours=2),
        ))
        db.flush()
        assert client.get("/api/v1/alerts/active").json() == []

    def test_active_alert_returned(self, client, db):
        db.add(StormAlert(
            noaa_id="test-active-001",
            event="Flash Flood Warning",
            severity="Severe",
            certainty="Likely",
            headline="Test active alert",
            effective=datetime.now(UTC) - timedelta(hours=1),
            expires=datetime.now(UTC) + timedelta(hours=6),
        ))
        db.flush()
        body = client.get("/api/v1/alerts/active").json()
        assert len(body) == 1
        assert body[0]["noaa_id"] == "test-active-001"

    def test_severity_filter(self, client, db):
        for sev, noaa_id in [("Extreme", "alert-ext"), ("Minor", "alert-min")]:
            db.add(StormAlert(
                noaa_id=noaa_id,
                event="Test Event",
                severity=sev,
                certainty="Observed",
                headline="Test",
                effective=datetime.now(UTC) - timedelta(hours=1),
                expires=datetime.now(UTC) + timedelta(hours=6),
            ))
        db.flush()
        body = client.get("/api/v1/alerts/active?severity=Extreme").json()
        assert all(a["severity"] == "Extreme" for a in body)


# ---------------------------------------------------------------------------
# Hazards GeoJSON
# ---------------------------------------------------------------------------

class TestHazardsGeoJSON:
    @pytest.mark.parametrize("layer", ["flood", "seismic", "wildfire", "infrastructure"])
    def test_returns_200_for_all_layers(self, client, layer):
        r = client.get(f"/api/v1/hazards/geojson?layer={layer}")
        assert r.status_code == 200

    def test_response_is_feature_collection(self, client):
        body = client.get("/api/v1/hazards/geojson?layer=flood").json()
        assert body["type"] == "FeatureCollection"
        assert "features" in body
        assert isinstance(body["features"], list)

    def test_unknown_layer_returns_empty_collection(self, client):
        body = client.get("/api/v1/hazards/geojson?layer=unknown").json()
        assert body["type"] == "FeatureCollection"
        assert body["features"] == []


# ---------------------------------------------------------------------------
# Disaster declarations
# ---------------------------------------------------------------------------

@pytest.fixture()
def disaster_row(db):
    row = DisasterDeclaration(
        disaster_number=4001,
        fema_id="DR-4001-TX",
        state="TX",
        state_fips="48",
        county_fips="48201",
        declaration_type="DR",
        incident_type="Flood",
        declaration_title="Flooding in Texas",
        declaration_date=datetime(2022, 6, 15),
        incident_begin_date=datetime(2022, 6, 10),
        incident_end_date=datetime(2022, 6, 14),
        designated_area="Harris County",
        fema_region=6,
    )
    db.add(row)
    db.flush()
    return row


class TestDisasters:
    def test_declarations_returns_200(self, client):
        assert client.get("/api/v1/disasters/declarations").status_code == 200

    def test_declarations_empty_when_no_data(self, client):
        assert client.get("/api/v1/disasters/declarations").json() == []

    def test_declaration_appears_after_seed(self, client, disaster_row):
        body = client.get("/api/v1/disasters/declarations").json()
        assert len(body) == 1
        assert body[0]["fema_id"] == "DR-4001-TX"
        assert body[0]["incident_type"] == "Flood"

    def test_state_filter(self, client, disaster_row):
        body = client.get("/api/v1/disasters/declarations?state=CA").json()
        assert body == []

        body = client.get("/api/v1/disasters/declarations?state=TX").json()
        assert len(body) == 1

    def test_yearly_trends_returns_200(self, client):
        assert client.get("/api/v1/disasters/trends/yearly").status_code == 200

    def test_yearly_trends_empty_when_no_data(self, client):
        assert client.get("/api/v1/disasters/trends/yearly").json() == []

    def test_yearly_trends_after_seed(self, client, disaster_row):
        body = client.get("/api/v1/disasters/trends/yearly").json()
        assert len(body) == 1
        assert body[0]["year"] == 2022
        assert body[0]["count"] == 1

    def test_type_trends_returns_200(self, client):
        assert client.get("/api/v1/disasters/trends/by-type").status_code == 200

    def test_cost_trends_returns_200(self, client):
        assert client.get("/api/v1/disasters/costs/yearly").status_code == 200

    def test_cost_trends_with_joined_data(self, client, db, disaster_row):
        db.add(DisasterCost(
            disaster_number=4001,
            total_ihp_approved=1_000_000.0,
            total_ha_approved=500_000.0,
            total_ona_approved=0.0,
            total_pa_obligated=5_000_000.0,
            total_hmgp_obligated=200_000.0,
            total_cost=6_700_000.0,
        ))
        db.flush()
        body = client.get("/api/v1/disasters/costs/yearly").json()
        assert len(body) == 1
        assert body[0]["year"] == 2022
        assert body[0]["total_cost"] == pytest.approx(6_700_000.0)
