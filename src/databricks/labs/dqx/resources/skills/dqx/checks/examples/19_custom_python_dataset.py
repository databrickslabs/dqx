# based on checks from dqx/checks/custom/SKILL.md:119-167
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import uuid
import yaml
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.rule import register_rule

@register_rule("dataset")
def max_group_size(column: str, group_by: str, max_size: int) -> tuple[Column, callable]:
    """Check that no group exceeds max_size rows."""
    condition_col = f"_condition_{uuid.uuid4().hex[:8]}"

    def apply(df: DataFrame, spark: SparkSession) -> DataFrame:
        aggr = df.groupBy(group_by).agg(
            (F.count(column) > max_size).alias(condition_col)
        )
        return df.join(aggr, on=group_by, how="left")

    return (
        make_condition(
            F.col(condition_col),
            f"Group {group_by} has more than {max_size} rows",
            f"{group_by}_max_group_size",
        ),
        apply,
    )

df = spark.createDataFrame(
    [("c1", 1), ("c1", 2), ("c1", 3), ("c2", 4)],
    ["customer_id", "order_id"],
)

checks = yaml.safe_load("""
- criticality: warn
  check:
    function: max_group_size
    arguments:
      column: order_id
      group_by: customer_id
      max_size: 2
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks, custom_check_functions={"max_group_size": max_group_size}).show(truncate=False)
