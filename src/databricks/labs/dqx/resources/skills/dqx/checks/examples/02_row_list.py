# based on checks from dqx/checks/row-level/SKILL.md:47-68
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("active", "US"), ("deleted", "GB"), (None, "XX"), ("pending", None)],
    ["status", "country_code"],
)

checks = yaml.safe_load("""
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: status
      allowed: [active, inactive, pending]

- criticality: error
  check:
    function: is_not_null_and_is_in_list
    arguments:
      column: country_code
      allowed: [US, GB, DE, FR, JP]
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
