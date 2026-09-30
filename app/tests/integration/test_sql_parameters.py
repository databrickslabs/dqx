"""Live parameter binding checks for the Lakebase executor."""

from collections.abc import Callable

from databricks.sdk import WorkspaceClient

from tests.integration.conftest import LakebaseProject

from databricks_labs_dqx_app.backend.pg_executor import build_pg_executor_from_connection
from databricks_labs_dqx_app.backend.setup.resources import LakebaseConnection


def test_lakebase_query_binds_untrusted_text(
    ws: WorkspaceClient,
    make_lakebase_project: Callable[[], LakebaseProject],
) -> None:
    """Quoted SQL text remains a value through both query result shapes."""
    project = make_lakebase_project()
    connection = LakebaseConnection(
        endpoint=project.endpoint,
        host=None,
        port=5432,
        database="databricks_postgres",
        username=None,
        password=None,
        schema="public",
    )
    executor = build_pg_executor_from_connection(ws, connection)
    payload = "quote' backslash\\ %(marker)s OR 1=1 --"
    try:
        assert executor.query("SELECT %(payload)s AS payload", parameters={"payload": payload}) == [[payload]]
        assert executor.query_dicts("SELECT %(payload)s AS payload", parameters={"payload": payload}) == [
            {"payload": payload}
        ]
    finally:
        executor.close()
