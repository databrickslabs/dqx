# based on checks from dqx/checks/row-level/SKILL.md:321-337
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql.types import StructType, StructField, StringType, MapType

schema = StructType([
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType()),
    ])),
    StructField("metadata", MapType(StringType(), StringType())),
])

df = spark.createDataFrame(
    [({"city": "NYC", "zip": "10001"}, {"source": "web"}),
     ({"city": None, "zip": "20002"}, {"source": "api"}),
     ({"city": "LA", "zip": "90001"}, None)],
    schema,
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: address.city

- criticality: error
  check:
    function: is_not_null
    arguments:
      column: "try_element_at(metadata, 'source')"
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
