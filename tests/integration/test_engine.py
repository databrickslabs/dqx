import sys

import pytest
from pyspark.sql import Column
import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError
from databricks.labs.dqx.rule import DQRowRule, register_rule, requires_dbr_version


@requires_dbr_version(1)
@register_rule("row")
def _check_requires_dbr_v1(column: str) -> Column:
    return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")


@requires_dbr_version(sys.maxsize)
@register_rule("row")
def _check_requires_dbr_max(column: str) -> Column:
    return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")


def test_apply_checks_passes_when_dbr_version_requirement_satisfied(ws, spark):
    engine = DQEngine(ws)
    df = spark.createDataFrame([("value",)], "a: string")
    engine.apply_checks(df, [DQRowRule(check_func=_check_requires_dbr_v1, column="a")])


def test_apply_checks_raises_when_dbr_version_requirement_not_satisfied(ws, spark):
    engine = DQEngine(ws)
    df = spark.createDataFrame([("value",)], "a: string")
    with pytest.raises(InvalidCheckError, match=r"Check functions \[.*\] require Databricks Runtime"):
        engine.apply_checks(df, [DQRowRule(check_func=_check_requires_dbr_max, column="a")])
