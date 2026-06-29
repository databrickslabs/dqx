import re

import pytest
from pyspark.sql import Column
import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError
from databricks.labs.dqx.rule import DQRowRule, register_rule, requires_dbr_version


@requires_dbr_version("1.0")
@register_rule("row")
def _check_requires_dbr_v1(column: str) -> Column:
    return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")


def _row_check_requiring(version: str):
    """Build a row check decorated with a dynamic minimum DBR requirement."""

    @requires_dbr_version(version)
    @register_rule("row")
    def _check(column: str) -> Column:
        return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")

    return _check


def _current_dbr_version(spark) -> tuple[int, int] | None:
    """Resolve the current runtime as a (major, minor) tuple, or None when undetermined (e.g. serverless)."""
    rows = spark.sql("select current_version().dbr_version as dbr_version").collect()
    version_str = rows[0]["dbr_version"] if rows else None
    if not version_str or not version_str.strip():
        return None
    match = re.match(r"\s*(\d+)\.(\d+)", version_str)
    return (int(match.group(1)), int(match.group(2))) if match else None


def test_apply_checks_passes_when_dbr_version_requirement_satisfied(ws, spark):
    engine = DQEngine(ws)
    df = spark.createDataFrame([("value",)], "a: string")
    engine.apply_checks(df, [DQRowRule(check_func=_check_requires_dbr_v1, column="a")])


def test_apply_checks_passes_on_serverless(ws, spark, skip_if_classic_compute):
    # Serverless reports a suffixed version such as "18.2.x-photon-scala2.13"; the 17.1 requirement carried by
    # the geo checks must parse the leading major.minor and pass, not be blocked by the suffix.
    engine = DQEngine(ws)
    df = spark.createDataFrame([("value",)], "a: string")
    engine.apply_checks(df, [DQRowRule(check_func=_row_check_requiring("17.1"), column="a")])


def test_apply_checks_dbr_version_boundary_is_minor_version_aware(ws, spark):
    current = _current_dbr_version(spark)
    if current is None:
        pytest.skip("Requires a resolvable DBR version; the current runtime returned none")

    major, minor = current
    engine = DQEngine(ws)
    df = spark.createDataFrame([("value",)], "a: string")

    # Exactly the current version must pass - guards against a '<' -> '<=' regression.
    engine.apply_checks(df, [DQRowRule(check_func=_row_check_requiring(f"{major}.{minor}"), column="a")])

    # One minor above the current version must fail - guards against a major-only comparison that would
    # wrongly let an equal major with a lower minor through.
    with pytest.raises(InvalidCheckError, match="require Databricks Runtime"):
        engine.apply_checks(df, [DQRowRule(check_func=_row_check_requiring(f"{major}.{minor + 1}"), column="a")])
