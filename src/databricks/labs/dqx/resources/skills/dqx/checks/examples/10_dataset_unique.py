# based on checks from dqx/checks/dataset-level/SKILL.md:12-42
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(1, "alice@x.com"), (2, "bob@x.com"), (3, "alice@x.com"), (4, None), (5, None)],
    ["order_id", "email"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
        - order_id

- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
        - email
      nulls_distinct: false
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
