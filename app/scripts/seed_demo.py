#!/usr/bin/env python
"""Standalone CLI for seeding the DQX Studio e-commerce demo.

Assembles the same service graph as :func:`backend.dependencies.get_demo_seed_service`
but from a standalone :class:`~databricks.sdk.WorkspaceClient` constructed via
a named CLI profile rather than FastAPI dependency injection, then calls
:meth:`~backend.demo.seed_service.DemoSeedService.run`.

Intended for DEV ITERATION — a developer runs this from their machine against a
named Databricks profile to drive the ~1-hour seed outside the app process.

Environment prerequisites
--------------------------
The app's ``DQX_*`` environment variables must be set before running this script
(exactly as the Databricks App reads them):

* ``DATABRICKS_WAREHOUSE_ID`` or ``DATABRICKS_SQL_WAREHOUSE_ID`` — the SQL
  warehouse the app uses (overridden by ``--warehouse-id`` if supplied).
* ``DQX_CATALOG`` (default ``dqx``) — the Unity Catalog catalog where the app's
  ``dqx_studio`` schema lives.
* ``DQX_JOB_ID`` — the task-runner Databricks Job ID (required for binding runs
  during the weekly history phase; not needed for ``--weeks 0`` build-only runs).

Lakebase / OLTP note
---------------------
For a full Lakebase deployment the ``DQX_LAKEBASE_ENDPOINT`` env var must be set
to the app's ``projects/<project>/branches/<branch>/endpoints/primary`` path AND
``psycopg`` / ``psycopg[pool]`` must be installed in the Python environment (they
are declared as app extras, not the root DQX library's deps).

When Lakebase is **not** configured (no ``DQX_LAKEBASE_ENDPOINT``) this script
falls back to the Delta-OLTP path automatically — the same fallback the app uses.
The simplest dev setup is therefore a Delta-fallback deployment (no
``DQX_LAKEBASE_ENDPOINT`` required).

Usage
-----
    uv run --group dev python scripts/seed_demo.py [options]

Options
-------
    --profile          Databricks config profile (default: env DATABRICKS_CONFIG_PROFILE
                       or "DEFAULT").
    --warehouse-id     SQL warehouse ID; resolved automatically if omitted.
    --catalog          Unity Catalog catalog for the app schema (default: "dqx").
    --weeks            Number of weekly history batches to generate (default: 9).
                       Pass 0 for a build-only run (rules + bindings + products, no runs).
    --wipe-first       Drop and re-seed all demo governed objects before starting.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from databricks.sdk import WorkspaceClient


def _pick_warehouse(ws: WorkspaceClient, explicit_id: str | None) -> str:
    """Return a running warehouse ID.

    Uses *explicit_id* when supplied; otherwise picks the first RUNNING
    warehouse from the workspace listing, starting it when none are running.

    Args:
        ws: authenticated :class:`~databricks.sdk.WorkspaceClient`.
        explicit_id: warehouse ID from ``--warehouse-id`` (may be empty string).

    Returns:
        A valid SQL warehouse ID string.
    """
    if explicit_id:
        return explicit_id

    whs = None
    for attempt in range(8):
        try:
            whs = list(ws.warehouses.list())
            break
        except Exception as exc:  # noqa: BLE001
            s = str(exc).lower()
            transient = any(k in s for k in ("ip acl", "503", "502", "500", "connection", "timeout"))
            if transient and attempt < 7:
                print(f"  (transient listing warehouses, retrying: {str(exc)[:120]})")
                time.sleep(min(5 * (attempt + 1), 30))
                continue
            raise

    if not whs:
        raise RuntimeError("No SQL warehouses found in this workspace.")

    for wh in whs:
        if str(wh.state) in ("State.RUNNING", "RUNNING"):
            print(f"Using RUNNING warehouse: {wh.name} ({wh.id})")
            return str(wh.id)

    wh = whs[0]
    print(f"No RUNNING warehouse; starting {wh.name} ({wh.id}) ...")
    ws.warehouses.start(str(wh.id)).result(callback=lambda _: None)
    return str(wh.id)


def main() -> int:
    """CLI entry point.

    Returns:
        0 on success, 1 on failure.
    """
    # Line-buffer stdout so progress is visible when piped to a log or monitor.
    try:
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass

    default_profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")

    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--profile",
        default=default_profile,
        help=f"Databricks config profile (default: {default_profile!r})",
    )
    ap.add_argument(
        "--warehouse-id",
        default=None,
        help="SQL warehouse ID; auto-resolved from workspace if omitted",
    )
    ap.add_argument(
        "--catalog",
        default=os.environ.get("DQX_CATALOG", "dqx"),
        help="Unity Catalog catalog for the app schema (default: dqx)",
    )
    ap.add_argument(
        "--weeks",
        type=int,
        default=9,
        help="Number of weekly history batches (default: 9; 0 = build-only, no runs)",
    )
    ap.add_argument(
        "--wipe-first",
        action="store_true",
        help="Drop and re-seed all demo governed objects before starting",
    )
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Bootstrap WorkspaceClient from profile
    # ------------------------------------------------------------------
    # NOTE: In a Databricks App the WorkspaceClient() uses service-principal
    # env vars injected by the platform.  For this CLI we use a named profile
    # from the local ~/.databrickscfg so the developer's own identity drives
    # the calls — equivalent to ``databricks auth login --profile <profile>``.
    ws = WorkspaceClient(profile=args.profile)

    # Resolve current user for run attribution.
    me = ws.current_user.me()
    user_email: str = me.user_name or me.display_name or "demo-cli"
    print(f"Authenticated as: {user_email} (profile={args.profile!r})")

    warehouse_id = _pick_warehouse(ws, args.warehouse_id)

    # ------------------------------------------------------------------
    # Wire the DI graph that get_demo_seed_service assembles in FastAPI
    # ------------------------------------------------------------------
    # Import deferred so heavy app deps (psycopg, etc.) are only loaded
    # if present; an ImportError is surfaced with a clear message.
    try:
        from databricks_labs_dqx_app.backend.config import AppConfig
        from databricks_labs_dqx_app.backend.demo.manifest import SOURCE_SCHEMA as DEMO_SOURCE_SCHEMA
        from databricks_labs_dqx_app.backend.demo.seed_service import DemoSeedService
        from databricks_labs_dqx_app.backend.demo.status import DemoStatusStore
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
        from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
        from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
        from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
        from databricks_labs_dqx_app.backend.services.database_reset_service import DatabaseResetService
        from databricks_labs_dqx_app.backend.services.job_service import JobService
        from databricks_labs_dqx_app.backend.services.materializer import Materializer
        from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
        from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
        from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
        from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
        from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
        from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
        from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
        from databricks_labs_dqx_app.backend.services.view_service import ViewService
        from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, SqlExecutor
    except ImportError as exc:
        print(f"ERROR: Failed to import app modules: {exc}")
        print("Ensure you are running from within the app/ Python environment.")
        return 1

    catalog = args.catalog

    # Override config catalog from CLI arg so the entire graph uses the
    # same catalog the developer passed in.
    os.environ.setdefault("DQX_CATALOG", catalog)
    # Re-read config after the env override so the singleton picks it up.
    conf = AppConfig()

    # SP executor — main app schema (dqx_studio), same as get_sp_sql_executor.
    sp_sql = SqlExecutor(
        ws=ws,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=conf.schema_name,
    )

    # Demo source executor — bound to the demo source schema
    # (dqx_studio_demo), same as the demo_sql in get_demo_seed_service.
    demo_sql = SqlExecutor(
        ws=ws,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=DEMO_SOURCE_SCHEMA,
    )

    # OLTP executor — prefer Lakebase when configured, else Delta fallback.
    # For the CLI we use the same detection logic as the app's lifespan:
    # when DQX_LAKEBASE_ENDPOINT is set, attempt to construct PgExecutor;
    # on ImportError (psycopg missing) or missing env, fall back to sp_sql.
    oltp: OltpExecutorProtocol = sp_sql  # Delta fallback (default)
    lakebase_endpoint = os.environ.get("DQX_LAKEBASE_ENDPOINT", "")
    if lakebase_endpoint:
        try:
            from databricks_labs_dqx_app.backend.pg_executor import build_pg_executor

            lakebase_schema = os.environ.get("DQX_LAKEBASE_SCHEMA", "dqx_studio")
            lakebase_db = os.environ.get("DQX_LAKEBASE_DB", "databricks_postgres")
            pg = build_pg_executor(
                ws,
                endpoint=lakebase_endpoint,
                database=lakebase_db,
                schema=lakebase_schema,
            )
            oltp = pg
            print(f"Lakebase OLTP executor configured (endpoint={lakebase_endpoint!r})")
        except (ImportError, AttributeError) as exc:
            print(
                f"WARNING: Lakebase endpoint set but PgExecutor unavailable ({exc}); "
                f"falling back to Delta OLTP executor."
            )
    else:
        print("DQX_LAKEBASE_ENDPOINT not set — using Delta OLTP fallback.")

    # Build sub-services (mirrors get_demo_seed_service in dependencies.py).
    app_settings = AppSettingsService(sql=oltp)
    registry = RegistryService(sql=oltp)
    embeddings = RuleEmbeddingsService(sql=oltp, sp_ws=ws, app_settings=app_settings)
    monitored_tables = MonitoredTableService(sql=oltp, profiling_sql=sp_sql)
    apply_rules = ApplyRulesService(sql=oltp, registry=registry, app_settings=app_settings)
    materializer = Materializer(
        sql=oltp,
        registry=registry,
        monitored_tables=monitored_tables,
        app_settings=app_settings,
    )
    rules_catalog = RulesCatalogService(sql=oltp)
    version_service = MonitoredTableVersionService(
        sql=oltp,
        monitored_tables=monitored_tables,
        rules_catalog=rules_catalog,
    )
    run_set_service = RunSetService(oltp_sql=oltp, validation_sql=sp_sql)

    # For the standalone CLI, ViewService uses the SP executor for BOTH
    # slots (same as get_demo_seed_service) — no OBO token available.
    sp_view = ViewService(sql=sp_sql, sp_sql=sp_sql)

    job_service = JobService(
        ws=ws,
        job_id=conf.job_id,
        sql=sp_sql,
        warehouse_id=warehouse_id,
    )
    binding_run = BindingRunService(
        monitored_tables=monitored_tables,
        version_service=version_service,
        materializer=materializer,
        view_service=sp_view,
        job_service=job_service,
        run_set_service=run_set_service,
        settings_service=app_settings,
        runs_table=sp_sql.fqn("dq_validation_runs"),
    )
    score_cache = ScoreCacheService(oltp=oltp, warehouse_sql=sp_sql, genie_schema=conf.genie_schema_name)
    status = DemoStatusStore(app_settings)
    reset_service: DatabaseResetService | None = DatabaseResetService(delta_sql=sp_sql, oltp_sql=oltp)

    data_products = DataProductService(
        sql=oltp,
        monitored_tables=monitored_tables,
        run_set_service=run_set_service,
        binding_run_service=binding_run,
        version_service=version_service,
        app_settings=app_settings,
        materializer=materializer,
    )

    svc = DemoSeedService(
        demo_sql=demo_sql,
        app_sql=sp_sql,
        oltp=oltp,
        sp_ws=ws,
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=apply_rules,
        materializer=materializer,
        rules_catalog=rules_catalog,
        version_service=version_service,
        data_products=data_products,
        binding_run=binding_run,
        score_cache=score_cache,
        status=status,
        reset_service=reset_service,
        embeddings=embeddings,
        job_service=job_service,
        profiler_view=sp_view,
        catalog=catalog,
    )

    # ------------------------------------------------------------------
    # Run the seed
    # ------------------------------------------------------------------
    weeks = max(0, args.weeks)
    print(f"\nStarting demo seed: catalog={catalog!r} weeks={weeks} wipe_first={args.wipe_first}")
    result = svc.run(user_email=user_email, wipe_first=args.wipe_first, weeks=weeks)
    print(
        f"\nDone: rules={result.rules} tables={result.tables} products={result.products} "
        f"weeks={result.weeks} trend_points={result.trend_points}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
