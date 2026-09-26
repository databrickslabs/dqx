"""The basis split, end to end on real Spark, in the configuration a customer reported it missing from.

One metric checked (*units*), grouped by two columns, and compared along a date. That is the shape where
``contributions`` can only say ``{"units": 100}``, because attribution is keyed by source column and the
three engineered views of *units* are summed into it. The unit tests pin the arithmetic; this pins that the
field survives training, scoring, the struct cast and publication, which no unit test can see.
"""

import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.conftest import create_anomaly_check_rule, qualify_model_name

_START = datetime.datetime(2024, 1, 1)


def _published_basis():
    """The published map, read the way the guide documents."""
    return F.element_at(F.col("_dq_info"), 1).getField("anomaly").getField("basis_contributions")


def test_the_published_basis_split_names_the_time_comparison(
    ws,
    spark: SparkSession,
    anomaly_engine,
    make_schema,
    make_random,
):
    """A row ordinary in level but wrong for its date must say so in the published struct.

    Training data trends gently upward per group, so a late row holding an early value is unremarkable
    against the table and against its own group's overall spread, and clearly away from what its history
    expects for that date. That is the customer's situation: sales that look plausible until you ask what
    was expected that week.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    rows = [
        (
            _START + datetime.timedelta(days=day),
            brand,
            nation,
            float(100.0 + 0.4 * day + offset),
        )
        for day in range(240)
        for brand, nation, offset in (("LOKELMA", "UK", 0.0), ("LOKELMA", "DE", 12.0), ("OTHER", "UK", -8.0))
    ]
    table = f"{TEST_CATALOG}.{schema.name}.sales_{suffix}"
    spark.createDataFrame(rows, "date timestamp, brand string, nation string, units double").write.saveAsTable(table)

    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    model_name = f"test_basis_{suffix}"
    anomaly_engine.train(
        df=spark.table(table),
        columns=["units"],
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        baseline_by=["brand", "nation"],
        baseline_over_time="date",
    )

    # Day 239 would expect roughly 100 + 0.4*239 = ~196 for this group. 104 is an ordinary *value* for the
    # table (it occurs early in the window) and wrong for when it arrived.
    probe = spark.createDataFrame(
        [(_START + datetime.timedelta(days=239), "LOKELMA", "UK", 104.0)],
        "date timestamp, brand string, nation string, units double",
    )
    # enable_contributions explicitly: the shared helper defaults it OFF to keep SHAP out of most
    # tests, and the basis split is derived from that attribution, so without it both maps are null.
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=90.0,
            enable_contributions=True,
        )
    ]
    result_df = DQEngine(ws, spark).apply_checks(probe, checks)

    published = dict(result_df.select(_published_basis().alias("basis")).dtypes)
    assert published["basis"] == "map<string,double>", published

    basis = result_df.select(_published_basis().alias("basis")).collect()[0]["basis"]
    assert basis is not None, "the basis split must be published for the tabular detector"
    assert "units vs its expected level at that time" in basis, f"the time comparison must be named: {basis}"
    total = sum(value for value in basis.values() if value is not None)
    assert 99.0 <= total <= 101.0, f"one column's entries should total 100, got {total} in {basis}"


def test_the_correlation_detector_publishes_no_basis_split(
    ws,
    spark: SparkSession,
    anomaly_engine,
    make_schema,
    make_random,
):
    """Null rather than a fabricated split, asserted on the published struct.

    Its attribution marginalises a whole column at once; per-view numbers from it would each measure almost
    nothing and normalising them names the wrong comparison. The contract is that the field is null and the
    column map is unaffected.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    rows = [
        (_START + datetime.timedelta(days=day), float(100.0 + day % 7), float(50.0 + (day % 7) * 2))
        for day in range(200)
    ]
    table = f"{TEST_CATALOG}.{schema.name}.corr_{suffix}"
    spark.createDataFrame(rows, "date timestamp, metric_a double, metric_b double").write.saveAsTable(table)

    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    model_name = f"test_corr_basis_{suffix}"
    anomaly_engine.train(
        df=spark.table(table),
        columns=["metric_a", "metric_b"],
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        baseline_by=[],
        baseline_over_time="date",
        profile="correlation",
    )

    # The relationship breaks: metric_b stays put while metric_a moves.
    probe = spark.createDataFrame(
        [(_START + datetime.timedelta(days=201), 106.0, 50.0)],
        "date timestamp, metric_a double, metric_b double",
    )
    # enable_contributions explicitly: the shared helper defaults it OFF to keep SHAP out of most
    # tests, and the basis split is derived from that attribution, so without it both maps are null.
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=90.0,
            enable_contributions=True,
        )
    ]
    result_df = DQEngine(ws, spark).apply_checks(probe, checks)

    row = result_df.select(
        _published_basis().alias("basis"),
        F.element_at(F.col("_dq_info"), 1).getField("anomaly").getField("contributions").alias("contributions"),
    ).collect()[0]

    assert row["basis"] is None, f"no split should be published for this detector, got {row['basis']}"
    assert row["contributions"] is not None, "the column map is still produced"
