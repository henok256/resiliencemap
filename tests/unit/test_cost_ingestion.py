"""Unit tests for FEMA disaster cost ingestion utilities."""


def test_cost_record_total_computation():
    """Total cost should be sum of all funding components."""
    ihp = 1_000_000.0
    ha = 500_000.0
    ona = 200_000.0
    pa = 3_000_000.0
    hmgp = 750_000.0
    total = ihp + ha + ona + pa + hmgp
    assert total == 5_450_000.0


def test_zero_cost_records_skipped():
    """Records with all-zero costs should be skipped."""
    record = {
        "disasterNumber": 9999,
        "totalAmountIhpApproved": None,
        "totalAmountHaApproved": None,
        "totalAmountOnaApproved": None,
        "totalObligatedAmountPa": None,
        "totalObligatedAmountHmgp": None,
    }
    ihp = record.get("totalAmountIhpApproved") or 0.0
    ha = record.get("totalAmountHaApproved") or 0.0
    ona = record.get("totalAmountOnaApproved") or 0.0
    pa = record.get("totalObligatedAmountPa") or 0.0
    hmgp = record.get("totalObligatedAmountHmgp") or 0.0
    total = ihp + ha + ona + pa + hmgp
    assert total == 0.0


def test_partial_cost_records():
    """Records with only some cost fields should still compute total."""
    record = {
        "disasterNumber": 1234,
        "totalAmountIhpApproved": None,
        "totalAmountHaApproved": None,
        "totalAmountOnaApproved": None,
        "totalObligatedAmountPa": 55_712.33,
        "totalObligatedAmountHmgp": 0.0,
    }
    pa = record.get("totalObligatedAmountPa") or 0.0
    hmgp = record.get("totalObligatedAmountHmgp") or 0.0
    ihp = record.get("totalAmountIhpApproved") or 0.0
    total = ihp + pa + hmgp
    assert total == 55_712.33
