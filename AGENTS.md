# Agent Instructions

## Databricks Connectivity

Before running any code that requires a Databricks workspace client or Spark session,
set up the environment:

```bash
# Only if .databricks/.databricks.env exists
if [ -f .databricks/.databricks.env ]; then
  set -a && source .databricks/.databricks.env && set +a
fi
source .venv/bin/activate
```

If `.databricks/.databricks.env` does **not** exist, ask the user how to configure the
Databricks workspace client and Spark session before proceeding.
