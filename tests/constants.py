import os

# Unity Catalog catalog used by integration tests. Defaults to "dqx" (the dedicated CI
# workspace catalog) but can be overridden via the DQX_TEST_CATALOG environment variable so
# the suite can run against a workspace that exposes a different catalog (e.g. "main").
TEST_CATALOG = os.environ.get("DQX_TEST_CATALOG", "dqx")
