"""Public driver scoring contracts across supported detectors and attribution flags.

Each case trains once and reuses the persisted model with contributions off and on.
Connect workers do not have the required anomaly packages in the integration environment;
distributed scoring is exercised by the packaged E2E suite. AI is disabled here.
"""

from collections import Counter
import json
import math
from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.utils import quote_column_name


def _original_rows(rows: list[Row], columns: list[str]) -> Counter[str]:
    return Counter(
        json.dumps({name: row.asDict(recursive=True)[name] for name in columns}, sort_keys=True, default=str)
        for row in rows
    )


@pytest.mark.parametrize("separator", [".", "`"])
def test_public_reload_preserves_literal_names_nested_values_and_duplicates(
    ws, spark, quick_model_factory, separator: str
) -> None:
    metric, group, time = (
        separator.join(parts) for parts in (("net", "amount"), ("store", "region"), ("event", "time"))
    )
    schema = (
        f"{quote_column_name(metric)} double, {quote_column_name(group)} string, {quote_column_name(time)} timestamp"
    )
    start = datetime(2025, 1, 1)
    model, registry, _ = quick_model_factory(
        spark,
        columns=[metric],
        baseline_by=[group],
        baseline_over_time=time,
        train_schema=schema,
        train_data=[(10.0 + i * 0.1, "known", start + timedelta(hours=i)) for i in range(100)],
        params=AnomalyParams(sample_fraction=1.0, algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42)),
    )
    # This unrelated payload deliberately resembles a generated feature name.
    source = spark.createDataFrame(
        [(500.0, "known", start + timedelta(hours=50))] * 2 + [(20.0, "unseen", start)], schema
    ).withColumn(f"{metric}_rel_baseline", F.struct(F.lit("preserved").alias("nested")))
    original = source.collect()
    result = DQEngine(ws, spark).apply_checks_by_metadata(
        source,
        [
            {
                "check": {
                    "function": "has_no_row_anomalies",
                    "arguments": {
                        "model_name": model,
                        "registry_table": registry,
                        "driver_only": True,
                        "enable_contributions": True,
                        "enable_ai_explanation": False,
                        "threshold": 0.0,
                    },
                }
            }
        ],
    )
    observed = result.collect()
    _assert_original_rows(source, original, result, observed)
    for row in observed:
        info = row["_dq_info"][0]["anomaly"]
        if row[group] == "unseen":
            assert info["score"] is None and info["is_anomaly"] is False
        else:
            assert math.isfinite(info["score"]) and info["is_anomaly"] is True
            assert info["contributions"] is not None and set(info["contributions"]) == {metric}


def _assert_original_rows(source: DataFrame, expected: list[Row], result: DataFrame, rows: list[Row]) -> None:
    assert len(rows) == len(expected)
    assert _original_rows(rows, source.columns) == _original_rows(expected, source.columns)
    assert set(result.columns) == {*source.columns, "_errors", "_warnings", "_dq_info"}
    assert [name for name in result.columns if name in source.columns] == source.columns
    assert [result.schema[name] for name in source.columns] == list(source.schema)


def _assert_parity(expected: list[Row], observed: list[Row]) -> None:
    first = sorted(expected, key=lambda row: row.probe_id)
    second = sorted(observed, key=lambda row: row.probe_id)
    assert len(first) == len(second)
    for left, right in zip(first, second, strict=True):
        assert left.probe_id == right.probe_id
        expected_info = left["_dq_info"][0]["anomaly"]
        observed_info = right["_dq_info"][0]["anomaly"]
        for name in ("score", "severity_percentile", "confidence_std"):
            if expected_info[name] is None:
                assert observed_info[name] is None
            else:
                assert observed_info[name] == pytest.approx(expected_info[name], rel=1e-9, abs=1e-9)
        for name in ("is_anomaly", "is_new_baseline", "is_stale_baseline"):
            assert observed_info[name] == expected_info[name]


def _assert_contribution_parity(expected: list[Row], observed: list[Row]) -> None:
    for left, right in zip(
        sorted(expected, key=lambda r: r.probe_id), sorted(observed, key=lambda r: r.probe_id), strict=True
    ):
        for field in ("contributions", "basis_contributions"):
            expected_info = left["_dq_info"][0]["anomaly"][field]
            observed_info = right["_dq_info"][0]["anomaly"][field]
            if expected_info is None:
                assert observed_info is None
                continue
            assert observed_info is not None and set(expected_info) == set(observed_info)
            for name, value in expected_info.items():
                assert (
                    observed_info[name] is None
                    if value is None
                    else observed_info[name] == pytest.approx(value, abs=0.1)
                )


def _assert_contributions(info: Row, columns: list[str], profile: str) -> None:
    shares = info["contributions"]
    assert shares is not None and set(shares) == set(columns)
    values = [value for value in shares.values() if value is not None]
    assert all(math.isfinite(value) and value >= 0.0 for value in values)
    if values:
        assert sum(values) == pytest.approx(100.0, abs=0.5)
    if profile == "correlation":
        assert info["basis_contributions"] is None


def _assert_public_info(rows: list[Row], columns: list[str], context: str, profile: str, contributions: bool) -> None:
    for row in rows:
        assert len(row["_dq_info"]) == 1
        info = row["_dq_info"][0]["anomaly"]
        unknown = context == "combined" and row.probe_id in {4, 8}
        assert info["is_new_baseline"] is unknown
        assert info["ai_explanation"] is None
        expected_stale = row.probe_id == 6 if context == "combined" else None
        assert info["is_stale_baseline"] is expected_stale
        if unknown:
            assert info["score"] is None and info["severity_percentile"] is None
            assert info["contributions"] is None and info["basis_contributions"] is None
            assert not row["_warnings"]
            continue
        assert info["score"] is not None and math.isfinite(info["score"])
        assert info["severity_percentile"] is not None and info["is_anomaly"] is True
        assert len(row["_warnings"]) == 1
        if contributions:
            _assert_contributions(info, columns, profile)
            if row.probe_id == 2:
                # This row deliberately perturbs both numeric inputs far outside
                # training support. Its enabled attribution must contain evidence
                # for a changed input, not merely a map whose values are all null.
                shares = info["contributions"]
                assert any(shares[name] is not None and shares[name] > 0.0 for name in ("amount", "quantity"))
        else:
            assert info["contributions"] is None and info["basis_contributions"] is None


def _assert_partitioned_filtered_checks(
    engine: DQEngine, source: DataFrame, originals: list[Row], model: str, registry: str, baseline: list[Row]
) -> None:
    """Changing the physical plan and applying two filtered checks preserves the same contract."""
    variant = source.select(*reversed(source.columns)).repartition(2, "probe_id").orderBy(F.desc("probe_id"))
    rules = [
        {
            "name": f"repeat_{index}",
            "filter": "probe_id != 7",
            "criticality": "warn",
            "check": {
                "function": "has_no_row_anomalies",
                "arguments": {
                    "model_name": model,
                    "registry_table": registry,
                    "threshold": 0.0,
                    "driver_only": True,
                    "enable_contributions": True,
                    "enable_confidence_std": True,
                    "enable_ai_explanation": False,
                },
            },
        }
        for index in range(2)
    ]
    result = engine.apply_checks_by_metadata(variant, rules)
    rows = result.collect()
    _assert_original_rows(variant, originals, result, rows)
    for row in rows:
        if row.probe_id == 7:
            # Dataset reporting preserves one array slot per check, including
            # null results for rows excluded by the pushed-down filter.
            assert row["_dq_info"] == [None, None]
            assert not row["_warnings"]
        else:
            assert len(row["_dq_info"]) == 2
            assert row["_dq_info"][0]["anomaly"] == row["_dq_info"][1]["anomaly"]
            expected_warnings = 0 if row.probe_id in {4, 8} else 2
            assert len(row["_warnings"] or []) == expected_warnings
        assert not row["_errors"]
    included = [row for row in rows if row.probe_id != 7]
    expected = [row for row in baseline if row.probe_id != 7]
    _assert_parity(expected, included)
    _assert_contribution_parity(expected, included)


@pytest.mark.parametrize(
    "profile,ensemble_size",
    [("distribution", None), ("distribution", 3), ("correlation", None)],
    ids=["single", "ensemble", "correlation"],
)
@pytest.mark.parametrize("context", ["plain", "combined"])
def test_public_driver_scoring_contract(
    ws, spark, quick_model_factory, scoring_contract_data, profile: str, ensemble_size: int | None, context: str
) -> None:
    training, source, columns, baseline_by, baseline_over_time = scoring_contract_data(context)
    model, registry, _ = quick_model_factory(
        spark,
        train_data=[tuple(row) for row in training.collect()],
        train_schema=training.schema.simpleString(),
        columns=columns,
        baseline_by=baseline_by,
        baseline_over_time=baseline_over_time,
        profile=profile,
        params=AnomalyParams(
            sample_fraction=1.0,
            ensemble_size=ensemble_size,
            algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42),
        ),
    )
    engine = DQEngine(ws, spark)
    originals = source.collect()
    snapshots: dict[bool, list[Row]] = {}
    for contributions in (False, True):
        checks = [
            {
                "name": "contract",
                "criticality": "warn",
                "check": {
                    "function": "has_no_row_anomalies",
                    "arguments": {
                        "model_name": model,
                        "registry_table": registry,
                        "threshold": 0.0,
                        "driver_only": True,
                        "enable_contributions": contributions,
                        "enable_confidence_std": True,
                        "enable_ai_explanation": False,
                    },
                },
            }
        ]
        result = engine.apply_checks_by_metadata(source, checks)
        rows = result.collect()
        _assert_original_rows(source, originals, result, rows)
        _assert_public_info(rows, columns, context, profile, contributions)
        snapshots[contributions] = rows
        # Round-robin repartitioning must not change which source row receives
        # a score when the driver collects features and returns Spark results.
        result = engine.apply_checks_by_metadata(source.repartition(2), checks)
        rows = result.collect()
        _assert_original_rows(source, originals, result, rows)
        _assert_public_info(rows, columns, context, profile, contributions)
        _assert_parity(snapshots[contributions], rows)
        _assert_contribution_parity(snapshots[contributions], rows)
    for rows in snapshots.values():
        _assert_parity(snapshots[False], rows)

    if context == "plain" and profile == "distribution" and ensemble_size is None:
        # Public engine calls reject existing reporting columns. Callers explicitly
        # project their input columns to apply the persisted model a second time.
        checked_schema = result.schema
        with pytest.raises(InvalidParameterError, match="reserved DQX result columns"):
            engine.apply_checks_by_metadata(result, checks)
        assert result.schema == checked_schema
        result = engine.apply_checks_by_metadata(result.select(*source.columns), checks)
        repeated_rows = result.collect()
        _assert_original_rows(source, originals, result, repeated_rows)
        _assert_public_info(repeated_rows, columns, context, profile, True)
        _assert_parity(snapshots[True], repeated_rows)
        _assert_contribution_parity(snapshots[True], repeated_rows)

    if context == "combined":
        _assert_partitioned_filtered_checks(engine, source, originals, model, registry, snapshots[True])
