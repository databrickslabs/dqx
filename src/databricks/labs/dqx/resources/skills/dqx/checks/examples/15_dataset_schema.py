# based on checks from dqx/checks/dataset-level/SKILL.md:188-207
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(1, "Alice", 9.99), (2, "Bob", 19.99)],
    ["id", "name", "price"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: has_valid_schema
    arguments:
      expected_schema: "id LONG, name STRING, price DOUBLE"
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
