# based on checks from dqx/checks/row-level/SKILL.md:91-95
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("2024-01-01", "2024-06-01"), ("2024-06-01", "2024-01-01"), ("2024-03-01", "2024-03-01")],
    ["start_date", "end_date"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: "end_date >= start_date"
      msg: "end_date is before start_date"
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
