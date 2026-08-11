from unittest.mock import Mock, create_autospec

from pyspark.sql import SparkSession

from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.llm.llm_engine import DQLLMEngine
from databricks.labs.dqx.llm.llm_pk_engine import DQLLMPrimaryKeyEngine


def test_rule_generation_engine_requires_no_spark_session():
    """Rule generation works from table metadata alone, so it must not depend on Spark."""
    engine = DQLLMEngine(model_config=LLMModelConfig(model_name="test-model"))

    assert not hasattr(engine, "spark")
    assert not hasattr(engine, "detect_primary_keys_with_llm")


def test_primary_key_engine_uses_provided_spark_session():
    """The primary key engine is the only LLM engine that holds a Spark session."""
    spark = create_autospec(SparkSession)

    engine = DQLLMPrimaryKeyEngine(model_config=LLMModelConfig(model_name="test-model"), spark=spark)

    assert engine.spark is spark


def test_primary_key_engine_delegates_to_detector():
    """Detection results are returned unchanged from the underlying detector."""
    expected = {"table": "catalog.schema.orders", "success": True, "primary_key_columns": ["order_id"]}
    detector = Mock()
    detector.detect_primary_keys_with_llm.return_value = expected

    spark = create_autospec(SparkSession)
    engine = DQLLMPrimaryKeyEngine(
        model_config=LLMModelConfig(model_name="test-model"),
        spark=spark,
        detector=detector,
    )

    result = engine.detect_primary_keys_with_llm("catalog.schema.orders")

    assert result == expected
    detector.detect_primary_keys_with_llm.assert_called_once_with(table="catalog.schema.orders")


def test_primary_key_engine_reports_metadata_failure_without_raising():
    """An unusable Spark session surfaces as a failed result, not an exception."""
    engine = DQLLMPrimaryKeyEngine(model_config=LLMModelConfig(model_name="test-model"), spark=Mock())

    result = engine.detect_primary_keys_with_llm("catalog.schema.orders")

    assert result["success"] is False
    assert result["table"] == "catalog.schema.orders"
