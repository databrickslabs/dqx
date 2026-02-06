# based on checks from dqx/checks/custom/SKILL.md:64-101
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.rule import register_rule

@register_rule("row")
def not_ends_with(column: str, suffix: str) -> Column:
    """Check that column value does not end with the given suffix."""
    col_expr = F.col(column)
    return make_condition(
        col_expr.endswith(suffix),
        f"Column {column} ends with {suffix}",
        f"{column}_ends_with_{suffix}",
    )

df = spark.createDataFrame(
    [("alice@company.com",), ("bob@test.com",), ("carol@company.com",)],
    ["email"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: not_ends_with
    arguments:
      column: email
      suffix: "@test.com"
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks, custom_check_functions={"not_ends_with": not_ends_with}).show(truncate=False)
