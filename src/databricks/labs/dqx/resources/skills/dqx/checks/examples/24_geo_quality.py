# based on checks from dqx/checks/geospatial/SKILL.md:37-43
# Requires Databricks serverless compute or runtime 17.1+.
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [
        ("POINT (1 2)",),
        ("POINT (0 0)",),
        ("GEOMETRYCOLLECTION EMPTY",),
        ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",),
    ],
    ["geom"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_ogc_valid
    arguments:
      column: geom

- criticality: warn
  check:
    function: is_non_empty_geometry
    arguments:
      column: geom

- criticality: warn
  check:
    function: is_not_null_island
    arguments:
      column: geom
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
