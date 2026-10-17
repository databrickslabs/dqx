"""Persisted temporal models keep public results across scoring session time zones."""

from collections import Counter
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
import json
import math

import pytest
from pyspark.sql import Row, SparkSession

from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine
from tests.integration_anomaly.conftest import create_anomaly_check_rule


@pytest.fixture
def timezone_spark(spark: SparkSession) -> Iterator[SparkSession]:
    original = spark.conf.get("spark.sql.session.timeZone", "UTC") or "UTC"
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        yield spark
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)


def _assert_public_parity(reference: list[Row], rows: list[Row], *, calendar_time: bool) -> None:
    for before, after in zip(reference, rows, strict=True):
        assert before.probe_id == after.probe_id
        expected, observed = before["_dq_info"][0]["anomaly"], after["_dq_info"][0]["anomaly"]
        for field in ("score", "severity_percentile"):
            assert observed[field] == pytest.approx(expected[field], rel=1e-10, abs=1e-10)
        for field in ("is_anomaly", "is_new_baseline", "is_stale_baseline"):
            assert observed[field] == expected[field]
        for name, value in expected["contributions"].items():
            if value is None:
                assert observed["contributions"][name] is None
            else:
                assert observed["contributions"][name] == pytest.approx(value, abs=0.01)
        if calendar_time:
            assert observed["stale_baseline_horizon"] == expected["stale_baseline_horizon"]


@pytest.mark.parametrize("time_type", ["date", "timestamp_ntz", "timestamp"])
def test_registered_temporal_model_keeps_public_results_across_timezones(
    ws, timezone_spark: SparkSession, quick_model_factory, time_type: str
) -> None:
    spark = timezone_spark
    start = datetime(2025, 3, 7, tzinfo=timezone.utc if time_type == "timestamp" else None)

    def event_time(day: int) -> date | datetime:
        value = start + timedelta(days=day)
        return value.date() if time_type == "date" else value

    schema = f"amount double, quantity double, `event.time` {time_type}"
    training = [
        (100.0 + 2.0 * day + 0.3 * math.sin(day * 0.19), 10.0 + 0.01 * day + math.cos(day * 0.13), event_time(day))
        for day in range(160)
    ]
    model, registry, _ = quick_model_factory(
        spark,
        columns=["amount", "quantity"],
        train_data=training,
        train_schema=schema,
        baseline_by=[],
        baseline_over_time="event.time",
        profile="correlation",
        params=AnomalyParams(sample_fraction=1.0),
    )
    # Include an ordinary row, a deliberate perturbation twice, a null time, and
    # a point well past the training horizon. Every call reloads the persisted model.
    source = spark.createDataFrame(
        [
            (0, *training[20]),
            (1, 9000.0, 1.0, event_time(40)),
            (1, 9000.0, 1.0, event_time(40)),
            (2, 150.0, 11.0, None),
            (3, 1500.0, 12.0, event_time(700)),
        ],
        "probe_id int, " + schema,
    )
    originals = Counter(json.dumps(row.asDict(), sort_keys=True, default=str) for row in source.collect())
    rules = [create_anomaly_check_rule(model, registry, threshold=0.0, criticality="warn", enable_contributions=True)]
    reference = None
    for zone in ("UTC", "America/New_York", "Asia/Kolkata"):
        spark.conf.set("spark.sql.session.timeZone", zone)
        result = DQEngine(ws, spark).apply_checks(source.repartition(2), rules)
        rows = sorted(result.collect(), key=lambda row: row.probe_id)
        assert len(rows) == 5
        assert result.select("probe_id", "amount", "quantity", "`event.time`").schema == source.schema
        assert set(result.columns) == {*source.columns, "_errors", "_warnings", "_dq_info"}
        assert [name for name in result.columns if name in source.columns] == source.columns
        assert (
            Counter(
                json.dumps({name: row[name] for name in source.columns}, sort_keys=True, default=str) for row in rows
            )
            == originals
        )
        for row in rows:
            assert len(row["_dq_info"]) == 1
            info = row["_dq_info"][0]["anomaly"]
            assert math.isfinite(info["score"]) and math.isfinite(info["severity_percentile"])
            assert info["is_anomaly"] is (info["severity_percentile"] >= info["threshold"])
            assert info["is_anomaly"] is True and len(row["_warnings"]) == 1
            assert info["is_stale_baseline"] is (row.probe_id == 3)
            assert info["ai_explanation"] is None and info["basis_contributions"] is None
            assert info["contributions"] is not None and set(info["contributions"]) == {"amount", "quantity"}
        if reference is not None:
            _assert_public_parity(reference, rows, calendar_time=time_type != "timestamp")
        else:
            reference = rows
