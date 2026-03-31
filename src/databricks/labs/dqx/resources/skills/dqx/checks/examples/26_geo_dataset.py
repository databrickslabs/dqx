# based on checks from dqx/checks/geospatial/SKILL.md
# Requires Databricks serverless compute or runtime 17.1+.
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [
        ("A", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),
        ("B", "POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))"),   # overlaps A
        ("C", "POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))"),  # disjoint
        ("D", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),   # exact duplicate of A
    ],
    ["id", "boundary"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: are_polygons_mutually_disjoint
    arguments:
      column: boundary
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
