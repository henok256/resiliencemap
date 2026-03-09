"""Unit tests for NIFC wildfire perimeter ingestion."""

from datetime import datetime

from ingestion.nifc.ingest_wildfires import (
    STATE_ABBR_TO_FIPS,
    _parse_epoch_ms,
    _parse_geometry,
)

# ── STATE_ABBR_TO_FIPS ──


def test_state_abbr_mapping_count():
    # 50 states + DC = 51
    assert len(STATE_ABBR_TO_FIPS) == 51


def test_state_abbr_key_states():
    assert STATE_ABBR_TO_FIPS["CA"] == "06"
    assert STATE_ABBR_TO_FIPS["TX"] == "48"
    assert STATE_ABBR_TO_FIPS["OR"] == "41"
    assert STATE_ABBR_TO_FIPS["MT"] == "30"


# ── _parse_epoch_ms ──


def test_parse_epoch_ms_valid():
    # 2024-01-01T00:00:00Z = 1704067200000 ms
    result = _parse_epoch_ms(1704067200000)
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1


def test_parse_epoch_ms_none():
    assert _parse_epoch_ms(None) is None
    assert _parse_epoch_ms(0) is None


def test_parse_epoch_ms_invalid():
    assert _parse_epoch_ms("not a number") is None


# ── _parse_geometry ──


def test_parse_geometry_polygon():
    feature = {
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-120, 38], [-120, 39], [-119, 39], [-119, 38], [-120, 38]]],
        }
    }
    geom = _parse_geometry(feature)
    assert geom is not None
    assert geom.geom_type == "MultiPolygon"


def test_parse_geometry_multipolygon():
    feature = {
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-120, 38], [-120, 39], [-119, 39], [-119, 38], [-120, 38]]],
            ],
        }
    }
    geom = _parse_geometry(feature)
    assert geom is not None
    assert geom.geom_type == "MultiPolygon"


def test_parse_geometry_null():
    assert _parse_geometry({}) is None
    assert _parse_geometry({"geometry": None}) is None


def test_parse_geometry_point_rejected():
    feature = {"geometry": {"type": "Point", "coordinates": [-120, 38]}}
    assert _parse_geometry(feature) is None
