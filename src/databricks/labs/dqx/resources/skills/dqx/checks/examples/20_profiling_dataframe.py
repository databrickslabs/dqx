# based on checks from dqx/profiling/SKILL.md:12-57
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler

df = spark.table("samples.tpch.orders").limit(500)

profiler = DQProfiler(WorkspaceClient())
summary_stats, profiles = profiler.profile(df)

print("=== Summary Stats ===")
for col, stats in summary_stats.items():
    print(f"  {col}: {stats}")

print("\n=== Generated Quality Rules ===")
for p in profiles:
    print(f"  {p.column}: {p.name} {p.parameters or ''}")
