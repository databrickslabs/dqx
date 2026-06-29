import dataclasses
import logging
import os
import re
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import partial

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

logger = logging.getLogger(__name__)

# Matches the resource names produced by the make_lakebase_instance fixture: dqx-test-<run_id>-<10 alnum>.
_LAKEBASE_RESOURCE_PATTERN = re.compile(r"^dqx-test-\d+-[A-Za-z0-9]{10}$")


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


def _safe_delete(delete: Callable[[], object], name: str, kind: str) -> None:
    """Best-effort delete: a single failure is logged and never blocks the rest of the sweep."""
    try:
        delete()
    except NotFound:
        pass
    except Exception as e:  # noqa: BLE001 — best-effort sweep must not block the test run
        logger.warning(f"Failed to delete orphaned Lakebase {kind} {name}: {e}")


def _is_current_or_foreign(name: str, current_run_pattern: re.Pattern[str]) -> bool:
    """True if the resource belongs to the current run or does not match the test fixture naming."""
    return bool(current_run_pattern.match(name)) or not _LAKEBASE_RESOURCE_PATTERN.match(name)


def _sweep_orphaned_catalogs(ws, current_run_pattern: re.Pattern[str], grace_period: datetime) -> None:
    """Delete orphaned test catalogs. A catalog with no ``created_at`` is treated as orphaned and
    deleted (it cannot be age-gated); the count is logged so we can see whether that path is hit."""
    listed = matched = missing_created_at = attempted = 0
    for catalog in ws.catalogs.list():
        listed += 1
        name = catalog.name or ""
        if _is_current_or_foreign(name, current_run_pattern):
            continue
        matched += 1
        created = datetime.fromtimestamp(catalog.created_at / 1000, tz=timezone.utc) if catalog.created_at else None
        if created is None:
            missing_created_at += 1
        elif created >= grace_period:
            continue
        # catalogs.delete removes the UC catalog object (the metastore slot). These orphans have no
        # surviving instance, so the database-catalog API cannot be used; force=True handles non-empty.
        _safe_delete(partial(ws.catalogs.delete, name=name, force=True), name, "catalog")
        attempted += 1
    logger.info(
        f"Lakebase catalog sweep: listed={listed} matched_naming={matched} "
        f"missing_created_at={missing_created_at} delete_attempts={attempted}"
    )


def _sweep_orphaned_instances(ws, current_run_pattern: re.Pattern[str], grace_period: datetime) -> None:
    """Delete orphaned test database instances older than the grace period."""
    matched = attempted = 0
    for instance in ws.database.list_database_instances():
        name = instance.name or ""
        if _is_current_or_foreign(name, current_run_pattern):
            continue
        matched += 1
        if datetime.fromisoformat(instance.creation_time) >= grace_period:
            continue
        _safe_delete(partial(ws.database.delete_database_instance, name=name), name, "instance")
        attempted += 1
    logger.info(f"Lakebase instance sweep: matched_naming={matched} delete_attempts={attempted}")


def _remove_orphaned_lakebase_resources(ws) -> None:
    """Delete orphaned Lakebase catalogs and instances left behind by cancelled CI runs.

    Orphaned resources accumulate when a github action is cancelled and the fixture cleanup process
    does not run. Database catalogs count toward the per-metastore catalog limit (1000) and are NOT
    removed when their instance is deleted, so they are swept explicitly. Catalogs are enumerated via
    the Unity Catalog API (``ws.catalogs.list`` finds orphans whose instance is already gone, which
    the per-instance database listing cannot) and deleted via the database catalog API, matching the
    make_lakebase_instance fixture teardown.

    Runs only in CI. Resources for the current run, those not matching the fixture naming, and those
    younger than the 2h grace period (possibly in use by a concurrent run) are skipped.
    """
    run_id = os.getenv("GITHUB_RUN_ID")
    if not run_id:
        return  # only applicable when run in CI

    current_run_pattern = re.compile(rf"^dqx-test-{run_id}-[A-Za-z0-9]+$")
    grace_period = datetime.now(timezone.utc) - timedelta(hours=2)  # aligned with tests timeout

    # Sweep orphaned catalogs first — these exhaust the metastore catalog limit and block new runs.
    _sweep_orphaned_catalogs(ws, current_run_pattern, grace_period)
    _sweep_orphaned_instances(ws, current_run_pattern, grace_period)


def test_remove_orphaned_lakebase_resources(ws):
    """Maintenance sweep: remove orphaned Lakebase catalogs and instances left by cancelled CI runs.

    Mirrors the established instance-cleanup test. Database catalogs count toward the per-metastore
    catalog limit (1000) and are NOT removed when their instance is deleted, so they are swept
    explicitly. Runs only in CI; resources for the current run, those not matching the fixture
    naming, and those younger than the 2h grace period are skipped.
    """
    _remove_orphaned_lakebase_resources(ws)


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
