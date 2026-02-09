# based on checks from dqx/checks/dataset-level/SKILL.md:69-73
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("c1", 50000), ("c1", 60000), ("c2", 1000)],
    ["customer_id", "amount"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: sql_query
    arguments:
      query: >
        SELECT customer_id, SUM(amount) > 100000 AS condition
        FROM {{ input }}
        GROUP BY customer_id
      input_placeholder: input
      merge_columns:
        - customer_id
      condition_column: condition
      msg: "customer total amount exceeds 100k"
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
