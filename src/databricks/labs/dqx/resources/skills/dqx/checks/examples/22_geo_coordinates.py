# based on checks from dqx/checks/geospatial/SKILL.md:16-21
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(40.7128, -74.0060), (91.0, -74.0060), (40.7128, -200.0), (None, None)],
    ["lat", "lon"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_latitude
    arguments:
      column: lat

- criticality: error
  check:
    function: is_longitude
    arguments:
      column: lon
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
