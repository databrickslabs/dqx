import dataclasses
import os
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from sqlalchemy import insert
from sqlalchemy.schema import CreateSchema

from databricks.labs.dqx.checks_serializer import ChecksNormalizer
from databricks.labs.dqx.rule_fingerprint import compute_rule_set_fingerprint_by_metadata
from databricks.labs.dqx.config import InstallationChecksStorageConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.checks_storage import LakebaseChecksStorageHandler
from databricks.sdk.errors import NotFound

from tests.conftest import compare_checks


TEST_CHECKS = [
    {
        "name": "col1_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "col2_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1.01}},
        "filter": "Col_3 >1",
        "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
    },
    {
        "name": "column_in_list",
        "criticality": "warn",
        "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
    },
    {
        "name": "col_3_is_in_range",
        "criticality": "warn",
        "check": {
            "function": "is_in_range",
            "arguments": {
                "column": "col_3",
                "min_limit": Decimal("0.01"),
                "max_limit": Decimal("999.99"),
            },
        },
    },
]


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
    schema_name = f"dqx_checks_{make_random(10).lower()}"
    table_name = f"checks_{make_random(10).lower()}"
    location = f"{database_name}.{schema_name}.{table_name}"
    return location


def _create_legacy_lakebase_table(ws, spark, config: LakebaseChecksStorageConfig, checks: list[dict]) -> None:
    """Create a Lakebase table with legacy schema (no versioning columns) and insert checks."""
    handler = LakebaseChecksStorageHandler(ws=ws, spark=spark)
    engine = handler.get_engine(config)
    legacy_table = LakebaseChecksStorageHandler.get_table_definition(
        config.schema_name, config.table_name, versioning=False
    )
    with engine.begin() as conn:
        if not conn.dialect.has_schema(conn, config.schema_name):
            conn.execute(CreateSchema(config.schema_name))

    with engine.begin() as conn:
        legacy_table.create(engine, checkfirst=True)

    normalized = ChecksNormalizer.normalize(checks)
    rows = [
        {
            "name": c.get("name"),
            "criticality": c.get("criticality", "error"),
            "check": c.get("check") or {},
            "filter": c.get("filter"),
            "run_config_name": c.get("run_config_name", "default"),
            "user_metadata": c.get("user_metadata"),
        }
        for c in normalized
    ]
    with engine.begin() as conn:
        conn.execute(insert(legacy_table), rows)


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
        rule_set_fingerprint=compute_rule_set_fingerprint_by_metadata(TEST_CHECKS[0:2]),
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


def test_save_checks_for_each_column_idempotency(ws, spark, make_lakebase_instance, lakebase_client_id, make_random):
    """A for_each_column check produces deterministic fingerprint; saving twice is idempotent (second save skipped)."""
    dq_engine = DQEngine(ws, spark)
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)

    config = LakebaseChecksStorageConfig(location=location, client_id=lakebase_client_id, instance_name=instance.name)

    compact = [{"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"]}}]

    dq_engine.save_checks(checks=compact, config=config)
    dq_engine.save_checks(checks=compact, config=config)  # same fingerprint — idempotency guard skips

    checks = dq_engine.load_checks(config=config)
    assert len(checks) == 1, f"Expected 1 compact check (idempotency), got {len(checks)}"


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
