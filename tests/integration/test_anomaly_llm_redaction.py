"""Live-Spark round-trip for anomaly redaction escaping (_sql_string_literal / pattern_spark_expr).

The unit tests only pin the Python string that _sql_string_literal emits; they cannot confirm that
Spark's parser actually matches the escaped literal. This test closes that gap for the redaction
path: a redact_columns entry whose name contains a single quote or backslash must be filtered out of
the SHAP-contribution pattern by ``not array_contains(array('<escaped>'), e.key)``.

Spark runs with ``spark.sql.parser.escapedStringLiterals`` false, where a doubled ``''`` pair is
dropped rather than unescaped. A regression to ANSI ``''`` escaping would parse ``cust'id`` as
``custid``, fail to match the real key, leave it in the pattern, and leak that column's (potentially
PII) values to the LLM. Kept in tests/integration (not integration_anomaly) because it needs only a
SparkSession — no MLflow/UC — and mirrors the metrics_observer escaping round-trip in
test_summary_metrics.py, so it runs on every integration CI rather than nightly-only.
"""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly.anomaly_llm_explainer import pattern_spark_expr


def _pattern(spark: SparkSession, contributions: dict[str, float], redact: set[str]) -> str:
    df = spark.createDataFrame([(contributions,)], "contributions map<string,double>")
    expr = pattern_spark_expr("contributions", frozenset(redact))
    return df.select(expr.alias("pattern")).collect()[0]["pattern"]


@pytest.mark.parametrize(
    "name", ["cust'id", "a\\b", "it's\\weird"], ids=["single_quote", "backslash", "quote_and_backslash"]
)
def test_pattern_spark_expr_redacts_escaped_key(spark: SparkSession, name: str):
    # The redacted key has the largest |value|; if the escaped literal failed to match it in
    # array_contains it would surface in the top-2 pattern. Correct escaping filters it out, leaving
    # only the non-sensitive feature.
    pattern = _pattern(spark, {name: 10.0, "safe_col": 5.0}, {name})
    assert pattern == "safe_col"


def test_pattern_spark_expr_keeps_unredacted_quoted_key(spark: SparkSession):
    # Escaping must not over-match: a quoted key that is not in redact_columns stays in the pattern.
    pattern = _pattern(spark, {"cust'id": 10.0, "safe_col": 5.0}, {"other"})
    assert "cust'id" in pattern
