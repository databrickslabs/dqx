# based on checks from dqx/checks/row-level/SKILL.md:47-54
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(25, 10.0, 5.0), (200, 3.0, 1.0), (-1, 50.0, 10.0)],
    ["age", "sale_price", "cost_price"],
)

checks = yaml.safe_load("""
- criticality: warn
  check:
    function: is_in_range
    arguments:
      column: age
      min_limit: 0
      max_limit: 150

- criticality: error
  check:
    function: is_in_range
    arguments:
      column: sale_price
      min_limit: cost_price
      max_limit: cost_price * 3
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
