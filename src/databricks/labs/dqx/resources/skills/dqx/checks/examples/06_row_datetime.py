# based on checks from dqx/checks/row-level/SKILL.md:168-212
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("2024-01-15", "2024-01-10", "2024-01-20"),
     ("not-a-date", "2024-06-01", "2024-06-02"),
     ("2099-12-31", "2024-03-01", "2024-03-01")],
    ["birth_date", "start_date", "end_date"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_valid_date
    arguments:
      column: birth_date

- criticality: error
  check:
    function: is_not_in_future
    arguments:
      column: birth_date
      offset: 86400

- criticality: warn
  check:
    function: is_older_than_col2_for_n_days
    arguments:
      column1: start_date
      column2: end_date
      days: 1
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
