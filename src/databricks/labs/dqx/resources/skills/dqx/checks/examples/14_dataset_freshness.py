# based on checks from dqx/checks/dataset-level/SKILL.md:51-55
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

from datetime import datetime, timedelta
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

now = datetime.now()
df = spark.createDataFrame(
    [(1, now - timedelta(minutes=1)),
     (2, now - timedelta(minutes=3)),
     (3, now - timedelta(minutes=12))],
    ["id", "event_timestamp"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_data_fresh_per_time_window
    arguments:
      column: event_timestamp
      window_minutes: 5
      min_records_per_window: 1
      lookback_windows: 3
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
