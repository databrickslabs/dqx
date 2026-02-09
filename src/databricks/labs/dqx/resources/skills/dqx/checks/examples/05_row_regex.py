# based on checks from dqx/checks/row-level/SKILL.md:56-60
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("alice@example.com", "Alice"), ("bad-email", "TEST_Bob"), ("x@y.z", "Carol")],
    ["email", "name"],
)

checks = yaml.safe_load("""
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: email
      regex: '^[^@]+@[^@]+\\.[^@]+$'

- criticality: warn
  check:
    function: regex_match
    arguments:
      column: name
      regex: '^TEST_'
      negate: true
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
