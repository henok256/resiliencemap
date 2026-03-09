"""Unit tests for FEMA disaster declarations ingestion."""

from datetime import datetime

from ingestion.fema.ingest_declarations import (
    _build_county_fips,
    _parse_date,
)

# ── _parse_date ──


def test_parse_date_valid_iso():
    result = _parse_date("2024-01-15T00:00:00.000Z")
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


def test_parse_date_none():
    assert _parse_date(None) is None
    assert _parse_date("") is None


def test_parse_date_invalid():
    assert _parse_date("not-a-date") is None


# ── _build_county_fips ──


def test_build_county_fips_valid():
    assert _build_county_fips("06", "037") == "06037"


def test_build_county_fips_zero_pad():
    assert _build_county_fips("6", "37") == "06037"


def test_build_county_fips_none():
    assert _build_county_fips(None, "037") is None
    assert _build_county_fips("06", None) is None
    assert _build_county_fips(None, None) is None
