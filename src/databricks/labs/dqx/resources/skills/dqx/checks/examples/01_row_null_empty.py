# based on checks from dqx/checks/row-level/SKILL.md:16-26
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("1", "Alice", "alice@x.com", [1, 2]), ("2", None, "", []), (None, "Bob", " ", None)],
    ["customer_id", "first_name", "email", "tags"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: customer_id

- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
      - first_name
      - email

- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: email
      trim_strings: true

- criticality: error
  check:
    function: is_not_null_and_not_empty_array
    arguments:
      column: tags
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
