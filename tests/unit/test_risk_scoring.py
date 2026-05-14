"""Unit tests for the risk scoring engine."""

from unittest.mock import MagicMock

from processing.score_tracts import (
    WEIGHTS,
    compute_composite_score,
    compute_social_vulnerability_score,
)


def test_weights_sum_to_one():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


def test_weights_include_wildfire():
    assert "wildfire" in WEIGHTS
    assert WEIGHTS["wildfire"] > 0


def test_composite_score_all_zero():
    score = compute_composite_score(0.0, 0.0, 0.0, 0.0, 0.0)
    assert score == 0.0


def test_composite_score_all_one():
    score = compute_composite_score(1.0, 1.0, 1.0, 1.0, 1.0)
    assert score == 1.0


def test_composite_score_clamps_to_range():
    # Even with out-of-range inputs, output must be [0, 1]
    score = compute_composite_score(2.0, 2.0, 2.0, 2.0, 2.0)
    assert 0.0 <= score <= 1.0


def test_composite_score_weighted():
    # Only flood risk = 1.0, others = 0
    score = compute_composite_score(flood=1.0, seismic=0.0, storm=0.0, wildfire=0.0, svi=0.0)
    assert abs(score - WEIGHTS["flood"]) < 1e-9


def test_composite_score_wildfire_only():
    # Only wildfire risk = 1.0, others = 0
    score = compute_composite_score(flood=0.0, seismic=0.0, storm=0.0, wildfire=1.0, svi=0.0)
    assert abs(score - WEIGHTS["wildfire"]) < 1e-9


def test_composite_score_precision():
    score = compute_composite_score(0.5, 0.5, 0.5, 0.5, 0.5)
    # Should be 0.5 since all components equal
    assert abs(score - 0.5) < 1e-4


def _mock_db(scalar_value):
    """Return a mock DB session whose execute().scalar() returns scalar_value."""
    db = MagicMock()
    db.execute.return_value.scalar.return_value = scalar_value
    return db


def test_svi_score_from_db():
    score = compute_social_vulnerability_score("48201950100", _mock_db(0.82))
    assert abs(score - 0.82) < 1e-9


def test_svi_score_fallback_when_missing():
    score = compute_social_vulnerability_score("48201950100", _mock_db(None))
    assert score == 0.5


def test_svi_score_clamps_high():
    score = compute_social_vulnerability_score("48201950100", _mock_db(1.5))
    assert score == 1.0


def test_svi_score_clamps_low():
    score = compute_social_vulnerability_score("48201950100", _mock_db(-0.1))
    assert score == 0.0
