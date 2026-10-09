"""Live round trip of oversized run configs through the Delta *dq_run_configs* table."""

from collections.abc import Callable, Generator
import json
import os
from pathlib import Path
import sys

import pytest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from databricks_labs_dqx_app.backend.config import conf
from databricks_labs_dqx_app.backend.migrations import MigrationRunner
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS, PgMigrationRunner
from databricks_labs_dqx_app.backend.pg_executor import PgExecutor, build_pg_executor
from databricks_labs_dqx_app.backend.run_config_store import (
    RUN_CONFIGS_TABLE,
    build_manifest_config_payload,
    delete_staged_config,
    prepare_config_json,
    stage_config_to_table,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_TASKS_SRC = Path(__file__).resolve().parents[2] / "tasks" / "src"
if str(_TASKS_SRC) not in sys.path:
    sys.path.insert(0, str(_TASKS_SRC))


@pytest.fixture
def runner():
    import dqx_task_runner.runner as r

    return r


@pytest.fixture
def staging_sql(
    ws: WorkspaceClient,
    live_warehouse: object,
    make_setup_schema: Callable[..., str],
    make_random: Callable[[int], str],
) -> SqlExecutor:
    """Provide a Delta executor on a fresh schema with the app's Delta migrations applied."""
    catalog = os.environ.get("DQX_TEST_CATALOG", "").strip()
    if not catalog:
        pytest.skip("Set DQX_TEST_CATALOG to a catalog the test profile can create schemas in.")
    schema = f"dqx_run_configs_{make_random(10).lower()}"
    make_setup_schema(catalog=catalog, schema=schema)
    warehouse_id = getattr(getattr(live_warehouse, "response", None), "id", None)
    if not isinstance(warehouse_id, str) or not warehouse_id:
        raise RuntimeError("The test warehouse did not return an ID.")
    sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=catalog, schema=schema)
    MigrationRunner(sql).run_all()
    return sql


@pytest.fixture
def spark(databricks_profile: str) -> SparkSession:
    """Provide a serverless Spark session, the compute the task runner uses."""
    return DatabricksSession.builder.profile(databricks_profile).serverless(True).getOrCreate()


@pytest.fixture
def lakebase_pg(ws: WorkspaceClient, make_random: Callable[[int], str]) -> Generator[PgExecutor, None, None]:
    """Provide a Postgres executor on a throwaway schema of an existing Lakebase endpoint."""
    endpoint = os.environ.get("DQX_LAKEBASE_ENDPOINT", "").strip()
    if not endpoint:
        pytest.skip("Set DQX_LAKEBASE_ENDPOINT to an endpoint the test profile can create schemas on.")
    pg = build_pg_executor(
        ws,
        endpoint=endpoint,
        database=conf.lakebase_database_name,
        schema=f"dqx_test_{make_random(8).lower()}",
    )
    try:
        yield pg
    finally:
        try:
            pg.execute_no_schema(f"DROP SCHEMA IF EXISTS {pg.q(pg.schema)} CASCADE")
        finally:
            pg.close()


def _oversized_config() -> dict:
    checks = [
        {
            "name": f"rule_{i}_it's",
            "criticality": "error",
            "check": {
                "function": "regex_match",
                "arguments": {"column": "code", "regex": r"^\d{3}-[A-Z]\\w+$"},
            },
            "filter": "status = 'confirmed'\nAND region IN ('EU', 'US')",
            "user_metadata": {"note": "naïve – ünïcode ✓"},
        }
        for i in range(120)
    ]
    return {"checks": checks, "sample_size": 1000, "source_table_fqn": "main.sales.orders"}


def _base_params(sql: SqlExecutor, run_id: str) -> dict[str, str]:
    return {
        "task_type": "dryrun",
        "view_fqn": f"{sql.catalog}.{sql.schema}_tmp.tmp_view_{run_id}",
        "result_catalog": sql.catalog,
        "result_schema": sql.schema,
        "run_id": run_id,
        "requesting_user": "it@example.com",
        "warehouse_id": sql.warehouse_id,
    }


def _staged_run_ids(sql: SqlExecutor) -> list[str]:
    return [row[0] for row in sql.query(f"SELECT run_id FROM {sql.fqn(RUN_CONFIGS_TABLE)} ORDER BY run_id")]


def test_oversized_config_round_trips_from_app_to_runner(
    staging_sql: SqlExecutor, spark: SparkSession, ws: WorkspaceClient, runner
) -> None:
    """The app stages an oversized config; the runner reads it back intact and deletes it."""
    run_id = "it_round_trip"
    config = _oversized_config()

    config_json = prepare_config_json(
        staging_sql, run_id=run_id, config=config, job_parameters_without_config=_base_params(staging_sql, run_id)
    )

    assert config_json == build_manifest_config_payload()
    table = runner._run_configs_table(staging_sql.catalog, staging_sql.schema)
    resolved, cleanup = runner._resolve_run_config(spark, ws, json.loads(config_json), table, run_id)
    assert resolved == config

    runner._cleanup_run_config(spark, ws, cleanup, run_id)
    assert _staged_run_ids(staging_sql) == []


def test_restaging_a_run_replaces_its_row(staging_sql: SqlExecutor, spark: SparkSession, runner) -> None:
    """A resubmit of the same run leaves exactly one row holding the latest config."""
    run_id = "it_restage"
    stage_config_to_table(staging_sql, run_id, {"checks": [], "version": 1})
    stage_config_to_table(staging_sql, run_id, {"checks": [], "version": 2})

    assert _staged_run_ids(staging_sql) == [run_id]
    table = runner._run_configs_table(staging_sql.catalog, staging_sql.schema)
    assert runner._read_manifest_config(spark, table, run_id) == {"checks": [], "version": 2}


def test_submit_failure_cleanup_deletes_only_its_row(staging_sql: SqlExecutor) -> None:
    """The app's submit-failure cleanup removes the failed run's row and leaves others."""
    stage_config_to_table(staging_sql, "it_failed", {"checks": []})
    stage_config_to_table(staging_sql, "it_other", {"checks": []})

    delete_staged_config(staging_sql, "it_failed")

    assert _staged_run_ids(staging_sql) == ["it_other"]


def test_postgres_migrations_drop_the_legacy_run_configs_table(lakebase_pg: PgExecutor) -> None:
    """An install that has the old Lakebase *dq_run_configs* table loses it on upgrade."""
    PgMigrationRunner(lakebase_pg, migrations=[m for m in PG_MIGRATIONS if m.version < 3]).run_all()
    lakebase_pg.execute(
        f"CREATE TABLE {lakebase_pg.fqn(RUN_CONFIGS_TABLE)} "
        "(run_id TEXT PRIMARY KEY, config TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL)"
    )

    PgMigrationRunner(lakebase_pg).run_all()

    assert (
        lakebase_pg.query(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{lakebase_pg.schema}' AND table_name = '{RUN_CONFIGS_TABLE}'"
        )
        == []
    )
