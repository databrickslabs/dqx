# based on checks from dqx/checks/row-level/SKILL.md:70-108
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(5, "active", 10.0, 25.0), (-1, "unknown", 20.0, 15.0), (0, "active", 5.0, 8.0)],
    ["quantity", "status", "discount", "price"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: quantity
      limit: 0

- criticality: warn
  check:
    function: is_not_equal_to
    arguments:
      column: status
      value: "'unknown'"

- criticality: warn
  check:
    function: is_not_greater_than
    arguments:
      column: discount
      limit: price * 0.5
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
