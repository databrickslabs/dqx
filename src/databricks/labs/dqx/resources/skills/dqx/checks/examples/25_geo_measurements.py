# based on checks from dqx/checks/geospatial/SKILL.md:45-73
# Requires Databricks serverless compute or runtime 17.1+.
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [
        ("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",),
        ("LINESTRING (0 0, 1 1, 2 2)",),
        ("POINT (200 100)",),
    ],
    ["geom"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: has_x_coordinate_between
    arguments:
      column: geom
      min_value: -180.0
      max_value: 180.0

- criticality: error
  check:
    function: has_y_coordinate_between
    arguments:
      column: geom
      min_value: -90.0
      max_value: 90.0

- criticality: warn
  check:
    function: is_num_points_not_less_than
    arguments:
      column: geom
      value: 3
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
