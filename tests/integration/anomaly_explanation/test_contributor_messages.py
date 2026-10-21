"""Contributor messages must evaluate correctly, rather than merely construct a Spark column."""

import pytest

from databricks.labs.dqx.anomaly.explainability import add_top_contributors_to_message


@pytest.mark.parametrize("severity_source", ["column", "info", "both"])
def test_contributor_messages_use_public_severity_and_respect_threshold(spark, severity_source: str) -> None:
    rows = [
        (
            1,
            59.0,
            [{"anomaly": {"severity_percentile": 99.0 if severity_source == "both" else 59.0}}],
            {"a": 80.0, "b": 20.0},
        ),
        (
            2,
            60.0,
            [{"anomaly": {"severity_percentile": 1.0 if severity_source == "both" else 60.0}}],
            {"a": 80.0, "b": 20.0},
        ),
        (3, 99.0, [{"anomaly": {"severity_percentile": 99.0}}], None),
        (4, 99.0, [{"anomaly": {"severity_percentile": 99.0}}], {"a": 0.0, "b": None}),
    ]
    source = spark.createDataFrame(
        rows,
        "row_id int, severity_percentile double, _dq_info array<struct<anomaly:struct<severity_percentile:double>>>, anomaly_contributions map<string,double>",
    )
    if severity_source == "column":
        source = source.drop("_dq_info")
    elif severity_source == "info":
        source = source.drop("severity_percentile")
    result = add_top_contributors_to_message(source, threshold=60.0, top_n=1)
    observed = result.orderBy("row_id").collect()
    assert [row["_top_contributors"] for row in observed] == ["", "a (80%)", "", ""]
    assert result.select(*source.columns).orderBy("row_id").collect() == source.orderBy("row_id").collect()
