"""Unit tests for *CollectLineageAction* — pure-logic scope only.

System-table read paths and recursive-CTE walks are exercised by the integration suite
(*tests/integration/test_lineage_action.py*).
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, create_autospec

import pytest
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.actions import lineage as lineage_mod
from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.lineage import (
    CollectLineageAction,
    LineageActionConfig,
    LineageSearchConfig,
)
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.errors import InvalidActionError


@pytest.mark.parametrize("field", ["depth", "lookback_days", "max_nodes"])
def test_lineage_search_config_rejects_out_of_range(field: str) -> None:
    """*depth*, *lookback_days*, and *max_nodes* each require ``>= 1`` — sub-minimum values raise."""
    with pytest.raises(InvalidActionError):
        LineageSearchConfig.model_validate({field: 0})


def _make_context(input_location: str | None) -> ActionContext:
    return ActionContext(
        metrics={"error_row_count": 5},
        run_id="run-lineage-001",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        input_location=input_location,
    )


def _make_services(spark: SparkSession | None) -> ActionServices:
    services = create_autospec(ActionServices, instance=True)
    services.spark = spark
    services.ws = None
    return services


def test_execute_returns_healthy_with_none_extras_when_input_location_missing() -> None:
    """*context.input_location=None* short-circuits before touching Spark."""
    spark = create_autospec(SparkSession, instance=True)
    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))

    result = action.execute(_make_context(input_location=None), _make_services(spark=spark))

    assert result.status == ActionStatus.HEALTHY
    assert result.extras is None
    spark.sql.assert_not_called()


def test_execute_returns_config_error_when_services_spark_is_none() -> None:
    """No SparkSession on services → CONFIG_ERROR, extras=None (guard clause)."""
    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))

    result = action.execute(_make_context(input_location="cat.sch.tbl"), _make_services(spark=None))

    assert result.status == ActionStatus.CONFIG_ERROR
    assert result.extras is None


def _mock_empty_dataframe() -> MagicMock:
    """A DataFrame mock whose relational ops stay chainable and whose ``.collect()`` is empty."""
    df = MagicMock(spec=DataFrame)
    df.unionByName.return_value = df
    df.where.return_value = df
    df.select.return_value = df
    df.collect.return_value = []
    return df


def test_execute_returns_config_error_when_all_enabled_branches_fail(monkeypatch) -> None:
    """Every enabled lineage read failing → CONFIG_ERROR + extras=None + no write.

    Writing an empty frame in overwrite mode would clobber prior lineage, so the action must
    abort the write (and not report HEALTHY) when every direction it tried failed to read.
    *save_dataframe_as_table* and the Delta-history lookup are patched at their boundaries
    because they need a real Spark backend; the behaviour we assert is independent of them.
    """
    writes: list = []
    monkeypatch.setattr(
        lineage_mod, "save_dataframe_as_table", lambda df, cfg: writes.append((df, cfg))
    )
    monkeypatch.setattr(lineage_mod, "_document_table", lambda **_: None)
    monkeypatch.setattr(lineage_mod, "_resolve_target_delta_versions", lambda _spark, df: df)

    spark = create_autospec(SparkSession, instance=True)
    spark.createDataFrame.return_value = _mock_empty_dataframe()
    spark.sql.side_effect = RuntimeError("lineage read denied")

    action = CollectLineageAction(
        output_config=OutputConfig(location="cat.sch.lineage"),
        config=LineageActionConfig(column_upstream=None, column_downstream=None),
    )

    result = action.execute(_make_context(input_location="cat.sch.src"), _make_services(spark=spark))

    assert result.status == ActionStatus.CONFIG_ERROR
    assert result.extras is None
    assert not writes
