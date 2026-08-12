"""Run Lakebase Postgres migrations as the bundle deployer.

Databricks Apps authenticate to Lakebase as the app service principal, but
OLTP tables are often created under the *deployer's* Postgres role (local
``make app-start-dev``, ``seed_demo.py``, or an earlier manual session against
the same project). Postgres requires table ownership for ``ALTER TABLE`` — the
app SP's ``DATABRICKS_SUPERUSER`` membership grants broad DML but does not
let a non-owner add columns.

This script runs :class:`PgMigrationRunner` once per ``make app-deploy``,
*before* ``bundle run`` starts the app, using the deployer's OAuth credential
(the same identity that ran ``databricks bundle deploy``). When the deployer
owns the schema objects, pending migrations apply cleanly; the subsequent app
startup ``run_all()`` is then a no-op.

Usage (from ``app/``):

    uv run python scripts/post_deploy_lakebase_migrations.py -p <profile> -t <target>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


def _bundle_json(profile: str, target: str, extra_args: list[str]) -> dict:
    cmd = [
        "databricks",
        "-p",
        profile,
        "bundle",
        "validate",
        "-t",
        target,
        "-o",
        "json",
        *extra_args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        print("ERROR: databricks bundle validate failed:", file=sys.stderr)
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(1)
    return json.loads(proc.stdout)


def _workspace_host(profile: str) -> str:
    proc = subprocess.run(
        ["databricks", "-p", profile, "auth", "describe", "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print("ERROR: databricks auth describe failed:", file=sys.stderr)
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(1)
    details = json.loads(proc.stdout).get("details", {})
    host = details.get("host") or details.get("configuration", {}).get("host", {}).get("value")
    if not host:
        print("ERROR: could not resolve workspace host from databricks auth describe.", file=sys.stderr)
        raise SystemExit(1)
    return host


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply Lakebase migrations as the deployer")
    parser.add_argument("-p", "--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("-t", "--target", required=True, help="Bundle target name")
    parser.add_argument(
        "bundle_extra",
        nargs="*",
        help="Extra arguments forwarded to bundle validate (after --)",
    )
    args = parser.parse_args()

    bundle = _bundle_json(args.profile, args.target, args.bundle_extra)
    variables = bundle.get("variables", {})

    def _var(name: str, default: str = "") -> str:
        node = variables.get(name, {})
        return str(node.get("value") or node.get("default") or default)

    endpoint = _var("lakebase_endpoint")
    if not endpoint or endpoint == "-":
        print("==> Skipping Lakebase migrations (lakebase_endpoint unset or Delta-only mode).")
        return

    database = _var("lakebase_database_name", "databricks_postgres")
    schema = _var("lakebase_schema_name", "dqx_studio")

    # Import after arg parse so ``--help`` works without the full app graph when possible.
    from databricks.sdk import WorkspaceClient

    from databricks_labs_dqx_app.backend.migrations.postgres import PgMigrationRunner
    from databricks_labs_dqx_app.backend.pg_executor import build_pg_executor

    ws = WorkspaceClient(profile=args.profile, host=_workspace_host(args.profile))
    pg = build_pg_executor(
        ws,
        endpoint=endpoint,
        database=database,
        schema=schema,
    )
    applied = PgMigrationRunner(pg).run_all()
    pg.close()
    if applied:
        print(f"==> Applied {applied} Lakebase migration(s) as deployer (schema={schema}).")
    else:
        print(f"==> Lakebase schema up to date (schema={schema}).")


if __name__ == "__main__":
    main()
