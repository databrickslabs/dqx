# based on checks from dqx/checks/dataset-level/SKILL.md:22-35
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("A", 10.0), ("A", 20.0), ("B", -5.0), ("B", 6000.0)],
    ["category", "price"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_aggr_not_less_than
    arguments:
      column: '*'
      aggr_type: count
      limit: 2

- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: price
      aggr_type: avg
      limit: 5000

- criticality: error
  check:
    function: is_aggr_equal
    arguments:
      column: price
      aggr_type: count
      limit: 0
      row_filter: "price < 0"
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
