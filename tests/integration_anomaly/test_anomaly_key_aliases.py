"""Public regressions for sources whose names collide with baseline-key aliases."""

from collections import Counter
import math

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.engine import DQEngine


@pytest.mark.parametrize(
    ("role", "collision_name"),
    [("metric", "__dqx_baseline_key"), ("group", "__DQX_BASELINE_KEY")],
    ids=["canonical-key-is-metric", "case-variant-key-is-group"],
)
def test_public_grouped_model_preserves_baseline_key_named_sources(
    ws,
    spark: SparkSession,
    anomaly_engine,
    anomaly_registry_prefix: str,
    make_random,
    role: str,
    collision_name: str,
) -> None:
    group = collision_name if role == "group" else "region"
    metric = collision_name if role == "metric" else "amount"
    source_schema = f"""
        row_id int,
        {group} string,
        {metric} double,
        __dqx_baseline_key_0 double,
        __DQX_BASELINE_KEY_1 double
    """
    training_rows = [
        (index, region, float(index % 11), float(index), float(index + 1))
        for region in ("north", "south")
        for index in range(100)
    ]
    training = spark.createDataFrame(training_rows, source_schema)
    suffix = make_random(6).lower()
    model_name = f"{anomaly_registry_prefix}.model_{suffix}"
    registry_table = f"{anomaly_registry_prefix}.registry_{suffix}"

    anomaly_engine.train(
        training,
        columns=[metric],
        model_name=model_name,
        registry_table=registry_table,
        baseline_by=[group],
        params=AnomalyParams(
            sample_fraction=1.0,
            ensemble_size=1,
            algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42),
        ),
    )

    source = spark.createDataFrame(
        [
            (1, "north", 1.0, 10.0, 11.0),
            (1, "north", 1.0, 10.0, 11.0),
            (2, "south", 2.0, 20.0, 21.0),
            (3, "unseen", 3.0, 30.0, 31.0),
        ],
        source_schema,
    )
    result = DQEngine(ws, spark).apply_checks_by_metadata(
        source,
        [
            {
                "criticality": "warn",
                "check": {
                    "function": "has_no_row_anomalies",
                    "arguments": {
                        "model_name": model_name,
                        "registry_table": registry_table,
                        "threshold": 0.0,
                        "driver_only": True,
                        "enable_contributions": False,
                        "enable_ai_explanation": False,
                    },
                },
            }
        ],
    )
    rows = result.collect()

    assert set(result.columns) == {*source.columns, "_errors", "_warnings", "_dq_info"}
    assert [name for name in result.columns if name in source.columns] == source.columns
    assert [result.schema[name] for name in source.columns] == list(source.schema)
    assert Counter(tuple(row[name] for name in source.columns) for row in rows) == Counter(
        tuple(row[name] for name in source.columns) for row in source.collect()
    )
    assert len(rows) == 4

    for row in rows:
        assert not row["_errors"]
        assert len(row["_dq_info"]) == 1
        anomaly = row["_dq_info"][0]["anomaly"]
        assert anomaly["contributions"] is None
        assert anomaly["basis_contributions"] is None
        assert anomaly["ai_explanation"] is None
        if row[group] == "unseen":
            assert anomaly["is_new_baseline"] is True
            assert anomaly["score"] is None
            assert anomaly["severity_percentile"] is None
            assert anomaly["is_anomaly"] is False
            assert not row["_warnings"]
            continue

        assert anomaly["is_new_baseline"] is False
        assert math.isfinite(anomaly["score"])
        assert math.isfinite(anomaly["severity_percentile"])
        assert anomaly["is_anomaly"] == (anomaly["severity_percentile"] >= 0.0)
        assert len(row["_warnings"]) == int(anomaly["is_anomaly"])
