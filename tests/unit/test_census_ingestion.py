"""Unit tests for Census TIGER/Line tract ingestion."""

import pytest
from ingestion.census.ingest_tracts import ALL_STATE_FIPS


def test_all_state_fips_count():
    """Should have 50 states + DC = 51 entries."""
    assert len(ALL_STATE_FIPS) == 51


def test_all_state_fips_zero_padded():
    """All FIPS codes should be 2 characters, zero-padded."""
    for fips in ALL_STATE_FIPS:
        assert len(fips) == 2, f"FIPS {fips!r} is not 2 chars"
        assert fips.isdigit(), f"FIPS {fips!r} is not numeric"


def test_key_states_present():
    """Verify a sample of well-known state FIPS codes are present."""
    assert "06" in ALL_STATE_FIPS  # California
    assert "48" in ALL_STATE_FIPS  # Texas
    assert "12" in ALL_STATE_FIPS  # Florida
    assert "36" in ALL_STATE_FIPS  # New York
    assert "11" in ALL_STATE_FIPS  # DC


def test_no_territories():
    """Puerto Rico (72), Guam (66), etc. should not be in the default list."""
    assert "72" not in ALL_STATE_FIPS  # Puerto Rico
    assert "66" not in ALL_STATE_FIPS  # Guam
