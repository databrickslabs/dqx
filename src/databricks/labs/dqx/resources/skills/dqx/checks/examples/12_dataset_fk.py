# based on checks from dqx/checks/dataset-level/SKILL.md:102-146
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(1, "US"), (2, "GB"), (3, "XX")],
    ["id", "country_code"],
)
ref_countries = spark.createDataFrame([("US",), ("GB",), ("DE",)], ["code"])

checks = yaml.safe_load("""
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [country_code]
      ref_columns: [code]
      ref_df_name: countries
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks, ref_dfs={"countries": ref_countries}).show(truncate=False)
