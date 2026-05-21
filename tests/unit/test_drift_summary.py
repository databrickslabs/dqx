from databricks.labs.dqx.anomaly.drift import DriftResult, format_drift_summary


def _drift_result(drift_detected: bool, drifted_columns=None, column_scores=None) -> DriftResult:
    return DriftResult(
        drift_detected=drift_detected,
        drift_score=4.0,
        drifted_columns=drifted_columns or [],
        column_scores=column_scores or {},
        recommendation="",
        sample_size=10_000,
    )


def test_format_drift_summary_returns_none_when_no_result():
    assert format_drift_summary(None) == "none"


def test_format_drift_summary_returns_none_when_drift_not_detected():
    assert format_drift_summary(_drift_result(drift_detected=False)) == "none"


def test_format_drift_summary_lists_drifted_columns_with_scores():
    result = _drift_result(
        drift_detected=True,
        drifted_columns=["amount", "quantity"],
        column_scores={"amount": 4.12, "quantity": 3.55},
    )
    assert format_drift_summary(result) == "drift detected: amount=4.12; quantity=3.55"


def test_format_drift_summary_excludes_redacted_columns():
    result = _drift_result(
        drift_detected=True,
        drifted_columns=["salary", "amount"],
        column_scores={"salary": 4.12, "amount": 3.55},
    )

    summary = format_drift_summary(result, redact_columns=["salary"])

    assert "salary" not in summary
    assert summary == "drift detected: amount=3.55"


def test_format_drift_summary_collapses_to_count_when_all_redacted():
    result = _drift_result(
        drift_detected=True,
        drifted_columns=["salary", "ssn"],
        column_scores={"salary": 4.12, "ssn": 3.55},
    )

    summary = format_drift_summary(result, redact_columns=["salary", "ssn"])

    assert "salary" not in summary
    assert "ssn" not in summary
    assert summary == "drift detected (2 features)"


def test_format_drift_summary_handles_empty_redact_iterable():
    result = _drift_result(
        drift_detected=True,
        drifted_columns=["amount"],
        column_scores={"amount": 4.12},
    )
    assert format_drift_summary(result, redact_columns=[]) == "drift detected: amount=4.12"


def test_format_drift_summary_accepts_arbitrary_iterable():
    result = _drift_result(
        drift_detected=True,
        drifted_columns=["salary", "amount"],
        column_scores={"salary": 4.12, "amount": 3.55},
    )

    summary = format_drift_summary(result, redact_columns=(c for c in ("salary",)))

    assert "salary" not in summary
    assert summary == "drift detected: amount=3.55"
