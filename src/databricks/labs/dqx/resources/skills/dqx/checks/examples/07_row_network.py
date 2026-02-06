# based on checks from dqx/checks/row-level/SKILL.md:214-231
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [("10.0.1.5",), ("192.168.1.1",), ("999.999.999.999",)],
    ["ip_address"],
)

checks = yaml.safe_load("""
- criticality: warn
  check:
    function: is_valid_ipv4_address
    arguments:
      column: ip_address

- criticality: error
  check:
    function: is_ipv4_address_in_cidr
    arguments:
      column: ip_address
      cidr_block: '10.0.0.0/8'
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks).show(truncate=False)
