"""Unit tests for *CollectLineageAction* — pure-logic scope only.

System-table read paths, recursive-CTE walks, and modifier joins are exercised by the
integration suite (see *tests/integration/actions/test_lineage_action.py*). Anything that
would require a live Spark session or fabricated *spark.sql* results has been moved there.
"""

from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.lineage import (
    CollectLineageAction,
    LineageEntitySearchConfig,
    LineageSearchConfig,
)
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.errors import InvalidActionError


# ---------------------------------------------------------------------------
# Config validators — pure Pydantic, no Spark
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["depth", "lookback_days", "max_nodes"])
def test_lineage_search_config_rejects_out_of_range(field: str) -> None:
    """Each numeric bound on *LineageSearchConfig* rejects sub-minimum values with InvalidActionError."""
    invalid = -1 if field == "depth" else 0
    with pytest.raises(InvalidActionError):
        LineageSearchConfig.model_validate({field: invalid})


@pytest.mark.parametrize("field", ["lookback_days", "max_last_runs"])
def test_lineage_entity_search_config_rejects_out_of_range(field: str) -> None:
    """Each numeric bound on *LineageEntitySearchConfig* rejects sub-minimum values."""
    with pytest.raises(InvalidActionError):
        LineageEntitySearchConfig.model_validate({field: 0})


# ---------------------------------------------------------------------------
# execute() guard clauses — no query issued, no Spark session required
# ---------------------------------------------------------------------------


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
    spark.sql.assert_not_called()  # type: ignore[attr-defined]


def test_execute_returns_config_error_when_services_spark_is_none() -> None:
    """No SparkSession on services → CONFIG_ERROR, extras=None (guard clause)."""
    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))

    result = action.execute(_make_context(input_location="cat.sch.tbl"), _make_services(spark=None))

    assert result.status == ActionStatus.CONFIG_ERROR
    assert result.extras is None
