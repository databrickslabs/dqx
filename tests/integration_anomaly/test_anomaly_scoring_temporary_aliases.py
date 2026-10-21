"""Public regressions for temporary anomaly-scoring aliases and feature-name collisions."""

from collections import Counter
from collections.abc import Iterator
import math

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidParameterError


@pytest.fixture
def restored_case_sensitive_spark(spark: SparkSession) -> Iterator[SparkSession]:
    """Restore Spark's session-wide case resolver after the collision regression."""
    try:
        original = spark.conf.get("spark.sql.caseSensitive", "false") or "false"
    except AnalysisException as exc:
        if exc.getErrorClass() == "CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION":
            pytest.skip(
                "Serverless Spark does not permit reading spark.sql.caseSensitive; "
                "this regression must change and restore that session setting."
            )
        raise
    try:
        yield spark
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_public_frequency_model_preserves_selected_and_payload_alias_collisions(
    ws,
    spark: SparkSession,
    anomaly_engine,
    anomaly_registry_prefix: str,
    make_random,
) -> None:
    source_schema = """
        row_id int,
        category string,
        __dqx_category_frequency double,
        __dqx_category double,
        __dqx_category_0 double,
        __DQX_FREQUENCY double,
        payload string
    """
    training = spark.createDataFrame(
        [
            (
                index,
                f"category_{index % 25}",
                float(index % 7),
                float(index),
                float(index + 1),
                float(index + 2),
                f"payload_{index}",
            )
            for index in range(125)
        ],
        source_schema,
    )
    suffix = make_random(6).lower()
    model_name = f"{anomaly_registry_prefix}.model_{suffix}"
    registry_table = f"{anomaly_registry_prefix}.registry_{suffix}"

    anomaly_engine.train(
        training,
        columns=["category", "__dqx_category_frequency"],
        model_name=model_name,
        registry_table=registry_table,
        params=AnomalyParams(
            sample_fraction=1.0,
            ensemble_size=1,
            algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42),
        ),
    )

    source = spark.createDataFrame(
        [
            (1, "category_1", 1.0, 10.0, 11.0, 12.0, "preserve-me"),
            (1, "category_1", 1.0, 10.0, 11.0, 12.0, "preserve-me"),
            (2, "category_24", 2.0, 20.0, 21.0, 22.0, "also-preserved"),
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
    for row in rows:
        assert not row["_errors"]
        assert len(row["_dq_info"]) == 1
        anomaly = row["_dq_info"][0]["anomaly"]
        assert math.isfinite(anomaly["score"])
        assert math.isfinite(anomaly["severity_percentile"])
        assert anomaly["is_anomaly"] == (anomaly["severity_percentile"] >= 0.0)
        assert anomaly["contributions"] is None
        assert anomaly["basis_contributions"] is None
        assert anomaly["ai_explanation"] is None
        assert len(row["_warnings"]) == int(anomaly["is_anomaly"])


def test_public_training_respects_case_sensitive_generated_feature_collisions(
    ws,
    restored_case_sensitive_spark: SparkSession,
    anomaly_engine,
    anomaly_registry_prefix: str,
    make_random,
) -> None:
    spark = restored_case_sensitive_spark
    source_schema = "amount double, AMOUNT_REL_BASELINE double, region string"
    training = spark.createDataFrame(
        [(float(index), float(index + 1), "north" if index % 2 else "south") for index in range(100)], source_schema
    )
    params = AnomalyParams(
        sample_fraction=1.0,
        ensemble_size=1,
        algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42),
    )
    spark.conf.set("spark.sql.caseSensitive", "false")
    with pytest.raises(InvalidParameterError, match="overwrite existing columns"):
        anomaly_engine.train(
            training,
            columns=["amount", "AMOUNT_REL_BASELINE"],
            model_name=f"{anomaly_registry_prefix}.rejected_{make_random(6).lower()}",
            registry_table=f"{anomaly_registry_prefix}.rejected_registry_{make_random(6).lower()}",
            baseline_by=["region"],
            params=params,
        )
    spark.conf.set("spark.sql.caseSensitive", "true")
    suffix = make_random(6).lower()
    model_name = f"{anomaly_registry_prefix}.model_{suffix}"
    registry_table = f"{anomaly_registry_prefix}.registry_{suffix}"
    anomaly_engine.train(
        training,
        columns=["amount", "AMOUNT_REL_BASELINE"],
        model_name=model_name,
        registry_table=registry_table,
        baseline_by=["region"],
        params=params,
    )
    source = spark.createDataFrame([(1.0, 2.0, "north")], source_schema)
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
    row = result.collect()[0]

    assert set(result.columns) == {*source.columns, "_errors", "_warnings", "_dq_info"}
    assert [name for name in result.columns if name in source.columns] == source.columns
    assert [row[name] for name in source.columns] == [1.0, 2.0, "north"]
    assert not row["_errors"]
    assert len(row["_dq_info"]) == 1
    anomaly = row["_dq_info"][0]["anomaly"]
    assert math.isfinite(anomaly["score"])
    assert math.isfinite(anomaly["severity_percentile"])
    assert anomaly["is_anomaly"] == (anomaly["severity_percentile"] >= 0.0)
    assert anomaly["contributions"] is None
    assert anomaly["basis_contributions"] is None
    assert anomaly["ai_explanation"] is None
    assert len(row["_warnings"]) == int(anomaly["is_anomaly"])
