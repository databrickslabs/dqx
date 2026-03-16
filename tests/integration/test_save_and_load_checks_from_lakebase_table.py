import dataclasses
import os
import re
import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.schema import CreateSchema
from databricks.labs.dqx.checks_serializer import compute_rule_set_fingerprint
from databricks.labs.dqx.checks_storage import LakebaseChecksStorageHandler
from databricks.labs.dqx.config import InstallationChecksStorageConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound

from tests.conftest import compare_checks
from tests.integration.test_save_and_load_checks_from_table import EXPECTED_CHECKS as TEST_CHECKS


def test_remove_orphaned_lakebase_instances(ws):
    """
    Make sure all orphaned / leftover lakeabse instances are removed.
    Orphaned instances are created when github action is cancelled and fixtures clean up process is not run.
    """
    run_id = os.getenv("GITHUB_RUN_ID")

    if not run_id:
        return  # only applicable when run in CI

    # must match pattern from make_lakebase_instance fixture
    current_run_pattern = re.compile(rf"^dqx-test-{run_id}-[A-Za-z0-9]+$")
    pattern = re.compile(r"^dqx-test-\d+-[A-Za-z0-9]{10}$")

    grace_period = datetime.now(timezone.utc) - timedelta(hours=2)  # aligned with tests timeout
    instances = []
    for instance in ws.database.list_database_instances():
        if current_run_pattern.match(instance.name):
            continue  # skip as it belongs to the current run
        if pattern.match(instance.name):
            creation_time = datetime.fromisoformat(instance.creation_time)
            # if database was created within the last 2h it maybe actively used by another test execution
            if creation_time < grace_period:
                instances.append(instance.name)

    for instance in instances:
        ws.database.delete_database_instance(name=instance)


def test_load_checks_when_lakebase_table_does_not_exist(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=lakebase_location, client_id=lakebase_client_id, instance_name=instance.name
    )

    with pytest.raises(NotFound, match=f"Table '{config.location}' does not exist in the Lakebase instance"):
        dq_engine.load_checks(config=config)


def test_save_and_load_checks_from_lakebase_table_with_client_id(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=lakebase_location, client_id=lakebase_client_id, instance_name=instance.name
    )

    dq_engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table(ws, spark, make_lakebase_instance, make_random):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(location=lakebase_location, instance_name=instance.name)

    dq_engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table_with_run_config(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    # test the first run config
    config = LakebaseChecksStorageConfig(
        location=lakebase_location,
        client_id=lakebase_client_id,
        instance_name=instance.name,
        run_config_name="workflow_001",
    )
    dq_engine.save_checks(TEST_CHECKS[:1], config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS[:1])

    # test the second run config
    second_config = LakebaseChecksStorageConfig(
        location=lakebase_location,
        client_id=lakebase_client_id,
        instance_name=instance.name,
        run_config_name="workflow_002",
    )
    dq_engine.save_checks(TEST_CHECKS[1:], config=second_config)
    checks = dq_engine.load_checks(config=second_config)

    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_from_lakebase_table_with_output_modes(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    """Save to different run_configs with append and overwrite modes."""
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    run_config_name = "workflow_003"
    dq_engine.save_checks(
        TEST_CHECKS[:1],
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            client_id=lakebase_client_id,
            instance_name=instance.name,
            run_config_name=run_config_name,
            mode="append",
        ),
    )

    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            client_id=lakebase_client_id,
            instance_name=instance.name,
            run_config_name=run_config_name,
        )
    )
    compare_checks(checks, TEST_CHECKS[:1])

    run_config_name = "workflow_004"
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            client_id=lakebase_client_id,
            instance_name=instance.name,
            run_config_name=run_config_name,
            mode="overwrite",
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            client_id=lakebase_client_id,
            instance_name=instance.name,
            run_config_name=run_config_name,
        )
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_from_lakebase_table_with_user_installation(
    ws, spark, installation_ctx, make_lakebase_instance, lakebase_client_id, make_random
):
    instance = make_lakebase_instance()

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = _create_lakebase_location(instance.database_name, make_random)
    installation_ctx.installation.save(config)
    product_name = installation_ctx.product_info.product_name()

    config = InstallationChecksStorageConfig(
        instance_name=instance.name,
        client_id=lakebase_client_id,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    dq_engine = DQEngine(ws, spark)

    dq_engine.save_checks(TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS)


def test_profiler_workflow_save_to_lakebase(
    ws, spark, setup_workflows, make_lakebase_instance, lakebase_client_id, make_random
):
    installation_ctx, run_config = setup_workflows()

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = lakebase_location
    run_config.lakebase_client_id = lakebase_client_id
    run_config.lakebase_instance_name = instance.name
    run_config.lakebase_port = "5432"

    installation_ctx.installation.save(config)

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            instance_name=instance.name,
            client_id=lakebase_client_id,
            run_config_name=run_config.name,
        )
    )
    assert checks, "Checks are missing"


def _create_lakebase_location(database_name, make_random):
    table_name = f"checks_{make_random(10).lower()}"
    location = f"{database_name}.config.{table_name}"
    return location


def test_save_and_load_checks_from_lakebase_without_rule_set_fingerprint(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=lakebase_location, client_id=lakebase_client_id, instance_name=instance.name
    )

    dq_engine.save_checks(checks=TEST_CHECKS[0:2], config=config)
    dq_engine.save_checks(checks=TEST_CHECKS[2:], config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS[2:])


def test_save_and_load_checks_from_lakebase_with_rule_set_fingerprint(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config_save = LakebaseChecksStorageConfig(
        location=lakebase_location, client_id=lakebase_client_id, instance_name=instance.name
    )

    dq_engine.save_checks(checks=TEST_CHECKS[0:2], config=config_save)

    dq_engine.save_checks(checks=TEST_CHECKS[2:], config=config_save)
    config_load = LakebaseChecksStorageConfig(
        location=lakebase_location,
        client_id=lakebase_client_id,
        instance_name=instance.name,
        rule_set_fingerprint=compute_rule_set_fingerprint(TEST_CHECKS[0:2]),
    )
    checks = dq_engine.load_checks(config=config_load)

    compare_checks(checks, TEST_CHECKS[0:2])


def test_save_and_load_checks_from_lakebase_rule_set_fingerprint_already_exists(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    dq_engine = DQEngine(ws, spark)

    instance = make_lakebase_instance()
    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=lakebase_location, client_id=lakebase_client_id, instance_name=instance.name
    )

    dq_engine.save_checks(checks=TEST_CHECKS[0:2], config=config)
    dq_engine.save_checks(checks=TEST_CHECKS[0:2], config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS[0:2])


def test_save_checks_for_each_column_and_expanded_have_same_rule_set_fingerprint(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    """A for_each_column check and its manually-expanded equivalents produce the same rule_set_fingerprint.

    Verified via idempotency: if both forms hash identically, the second save is a no-op, so loading
    returns exactly 2 rows (from the first save only, not 4).
    """
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(location=location, client_id=lakebase_client_id, instance_name=instance.name)

    unexpanded = [{"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"]}}]
    expanded = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "col1"}}},
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "col2"}}},
    ]

    dq_engine.save_checks(checks=unexpanded, config=config)
    # Expanded form has the same rule_set_fingerprint — idempotency guard should skip this save
    dq_engine.save_checks(checks=expanded, config=config)

    checks = dq_engine.load_checks(config=config)
    assert (
        len(checks) == 2
    ), f"Expected 2 checks (idempotency guard should have skipped the second save), got {len(checks)}"


def _create_legacy_lakebase_table(ws, spark, config: LakebaseChecksStorageConfig, rows: list[dict]) -> None:
    """Create a Lakebase table with the legacy schema (no versioning columns) and insert rows."""
    import json as _json

    handler = LakebaseChecksStorageHandler(ws, spark)
    engine = handler._get_engine(config)
    tbl = f'"{config.schema_name}"."{config.table_name}"'
    with engine.begin() as conn:
        if not conn.dialect.has_schema(conn, config.schema_name):
            conn.execute(CreateSchema(config.schema_name))
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                name VARCHAR(255),
                criticality VARCHAR(50) DEFAULT 'error',
                "check" JSONB,
                filter TEXT,
                run_config_name VARCHAR(255) DEFAULT 'default',
                user_metadata JSONB
            )
        """
            )
        )
        for row in rows:
            check_json = _json.dumps(row.get("check", {}))
            conn.execute(
                text(
                    f'INSERT INTO {tbl} (name, criticality, "check", filter, run_config_name) '
                    "VALUES (:n, :c, CAST(:check_json AS jsonb), :f, :r)"
                ),
                {
                    "n": row.get("name"),
                    "c": row.get("criticality", "error"),
                    "check_json": check_json,
                    "f": row.get("filter"),
                    "r": row.get("run_config_name", "default"),
                },
            )


def test_save_to_legacy_lakebase_table_adds_versioning_columns(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    """Saving to an existing Lakebase table without versioning columns triggers ALTER TABLE and succeeds."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(location=location, client_id=lakebase_client_id, instance_name=instance.name)

    _create_legacy_lakebase_table(
        ws, spark, config, [{"name": "legacy_check", "criticality": "error", "check": {"function": "is_not_null"}}]
    )

    dq_engine.save_checks(checks=TEST_CHECKS[:1], config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS[:1])


def test_load_from_legacy_lakebase_table_adds_versioning_columns(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    """Loading from a Lakebase table without versioning columns triggers ALTER TABLE and returns all rows for run_config."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(location=location, client_id=lakebase_client_id, instance_name=instance.name)

    _create_legacy_lakebase_table(
        ws,
        spark,
        config,
        [
            {
                "name": "col1_is_null",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
                "run_config_name": "default",
            },
            {
                "name": "col2_is_null",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
                "run_config_name": "default",
            },
            {
                "name": "other_config_check",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "col3"}},
                "run_config_name": "other",
            },
        ],
    )

    checks = dq_engine.load_checks(config=config)

    # Only "default" run_config rows returned; versioning columns added (nullable, so load still works)
    assert len(checks) == 2
    assert all(c.get("check", {}).get("function") == "is_not_null" for c in checks)


def test_save_overwrite_replaces_existing_records_with_different_fingerprint(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
):
    """Overwrite replaces all rows for run_config when fingerprint differs."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=location, client_id=lakebase_client_id, instance_name=instance.name, mode="overwrite"
    )

    dq_engine.save_checks(checks=TEST_CHECKS[:1], config=config)
    dq_engine.save_checks(checks=TEST_CHECKS[1:], config=config)
    checks = dq_engine.load_checks(config=config)

    # Second save overwrote the first; only TEST_CHECKS[1:] should be present
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_idempotency_overwrite_mode(ws, spark, make_lakebase_instance, lakebase_client_id, make_random):
    """Same fingerprint: mode=overwrite is skipped (idempotency); no duplicate write."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=location, client_id=lakebase_client_id, instance_name=instance.name, mode="overwrite"
    )

    dq_engine.save_checks(checks=TEST_CHECKS[:1], config=config)
    # Second save with same checks and overwrite mode — idempotency guard must skip it
    dq_engine.save_checks(checks=TEST_CHECKS[:1], config=config)

    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS[:1])


def test_save_append_then_overwrite_same_run_config(ws, spark, make_lakebase_instance, lakebase_client_id, make_random):
    """Append then overwrite for same run_config: overwrite replaces all rows for that run_config."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(location=location, client_id=lakebase_client_id, instance_name=instance.name)

    dq_engine.save_checks(checks=TEST_CHECKS[:1], config=dataclasses.replace(config, mode="append"))
    dq_engine.save_checks(checks=TEST_CHECKS[1:], config=dataclasses.replace(config, mode="overwrite"))

    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_append_accumulates_multiple_versions(ws, spark, make_lakebase_instance, lakebase_client_id, make_random):
    """Append twice with different fingerprints; load returns latest version."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(
        location=location, client_id=lakebase_client_id, instance_name=instance.name, mode="append"
    )

    dq_engine.save_checks(checks=TEST_CHECKS[:1], config=config)
    time.sleep(1)  # ensures second append has later created_at
    dq_engine.save_checks(checks=TEST_CHECKS[1:], config=config)

    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS[1:])
