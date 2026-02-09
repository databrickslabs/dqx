# based on checks from dqx/checks/custom/SKILL.md:35-49
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("s1", 50), ("s1", 150), ("s2", 30), ("s2", 40)],
    ["sensor_id", "reading_value"],
)

checks = yaml.safe_load("""
- criticality: error
  name: sensor_reading_exceeded
  check:
    function: sql_expression
    arguments:
      expression: "MAX(reading_value) OVER (PARTITION BY sensor_id) > 100"
      msg: "sensor has reading above threshold"
      negate: true
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
