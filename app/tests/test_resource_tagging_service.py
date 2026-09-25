from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import EntityTagAssignment

from databricks_labs_dqx_app.backend.demo.manifest import SOURCE_SCHEMA
from databricks_labs_dqx_app.backend.migrations import ANALYTICAL_TABLE_NAMES
from databricks_labs_dqx_app.backend.services import resource_tagging_service
from databricks_labs_dqx_app.backend.services.entitlement_service import (
    ENTITLEMENTS_TABLE_NAME,
    FAILING_ROWS_VIEW_NAME,
)
from databricks_labs_dqx_app.backend.services.metadata_dim_service import (
    DIM_MONITORED_TABLES_TABLE_NAME,
    DIM_RULES_TABLE_NAME,
)
from databricks_labs_dqx_app.backend.services.score_view_service import (
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
)
from databricks_labs_dqx_app.backend.services.resource_tagging_service import (
    ResourceTaggingService,
    TagTarget,
    demo_tag_targets,
    startup_tag_targets,
)
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation


def _resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation("main", "studio", "wheels", "/Volumes/main/studio/wheels"),
        lakebase=LakebaseConnection("endpoint", None, 5432, "db", None, None, "public"),
        warehouse_id="warehouse",
        job_id=None,
        tmp_schema="studio_tmp",
        genie_schema="genie",
    )


def test_reconcile_creates_missing_assignment() -> None:
    workspace = MagicMock()
    workspace.entity_tag_assignments.get.side_effect = NotFound("missing")
    service = ResourceTaggingService(workspace)

    service.reconcile((TagTarget("schemas", "main.dqx_studio"),))

    assignment = workspace.entity_tag_assignments.create.call_args.args[0]
    assert assignment == EntityTagAssignment(
        entity_type="schemas", entity_name="main.dqx_studio", tag_key="app", tag_value="dqx-studio"
    )


def test_reconcile_keeps_matching_assignment() -> None:
    workspace = MagicMock()
    workspace.entity_tag_assignments.get.return_value = EntityTagAssignment(
        entity_type="tables", entity_name="main.dqx_studio.dq_metrics", tag_key="app", tag_value="dqx-studio"
    )

    ResourceTaggingService(workspace).reconcile((TagTarget("tables", "main.dqx_studio.dq_metrics"),))

    workspace.entity_tag_assignments.create.assert_not_called()
    workspace.entity_tag_assignments.update.assert_not_called()


def test_reconcile_corrects_only_app_assignment() -> None:
    workspace = MagicMock()
    workspace.entity_tag_assignments.get.return_value = EntityTagAssignment(
        entity_type="volumes", entity_name="main.dqx_studio.wheels", tag_key="app", tag_value="legacy"
    )

    ResourceTaggingService(workspace).reconcile((TagTarget("volumes", "main.dqx_studio.wheels"),))

    workspace.entity_tag_assignments.update.assert_called_once()
    kwargs = workspace.entity_tag_assignments.update.call_args.kwargs
    assert kwargs["entity_type"] == "volumes"
    assert kwargs["entity_name"] == "main.dqx_studio.wheels"
    assert kwargs["tag_key"] == "app"
    assert kwargs["update_mask"] == "tag_value"
    assert kwargs["tag_assignment"].tag_value == "dqx-studio"


def test_reconcile_continues_after_one_target_fails() -> None:
    workspace = MagicMock()
    workspace.entity_tag_assignments.get.side_effect = [RuntimeError("denied"), NotFound("missing")]
    targets = (TagTarget("schemas", "main.first"), TagTarget("schemas", "main.second"))

    ResourceTaggingService(workspace).reconcile(targets)

    assert workspace.entity_tag_assignments.get.call_count == 2
    assert workspace.entity_tag_assignments.create.call_count == 1


def test_reconcile_warning_identifies_target_without_logging_control_characters(
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace = MagicMock()
    workspace.entity_tag_assignments.get.side_effect = RuntimeError("sensitive API detail")

    ResourceTaggingService(workspace).reconcile((TagTarget("schemas", "main.bad\nschema"),))

    assert len(caplog.records) == 1
    message = caplog.records[0].message
    assert "main.bad\\nschema" in message
    assert "RuntimeError" in message
    assert "\n" not in message
    assert "sensitive API detail" not in message


def test_startup_targets_respect_bundle_boundary() -> None:
    resources = _resources()
    marketplace_targets = startup_tag_targets(resources, include_bundle_resources=False)
    bundle_targets = startup_tag_targets(resources, include_bundle_resources=True)

    assert TagTarget("schemas", "main.studio_tmp") in marketplace_targets
    assert TagTarget("schemas", "main.genie") in marketplace_targets
    assert TagTarget("tables", "main.studio.dq_metrics") in marketplace_targets
    assert TagTarget("schemas", "main.studio") not in marketplace_targets
    assert TagTarget("volumes", "main.studio.wheels") not in marketplace_targets
    assert TagTarget("schemas", "main.studio") in bundle_targets
    assert TagTarget("schemas", "main.dqx_studio_demo") in bundle_targets
    assert TagTarget("volumes", "main.studio.wheels") in bundle_targets

    expected_main_tables = set(ANALYTICAL_TABLE_NAMES) | {"dq_migrations", ENTITLEMENTS_TABLE_NAME}
    assert {
        target.entity_name.rsplit(".", 1)[-1] for target in marketplace_targets if target.entity_type == "tables"
    } >= expected_main_tables
    assert {
        target.entity_name.rsplit(".", 1)[-1] for target in marketplace_targets if target.entity_type == "tables"
    } >= {
        DIM_RULES_TABLE_NAME,
        DIM_MONITORED_TABLES_TABLE_NAME,
        ATTRIBUTION_VIEW_NAME,
        SHAPING_VIEW_NAME,
        ASOF_VIEW_NAME,
        METRIC_VIEW_NAME,
        FAILING_ROWS_VIEW_NAME,
    }


def test_metadata_dimension_targets_include_only_replaced_dimension_tables() -> None:
    targets = resource_tagging_service.metadata_dimension_tag_targets("main", "genie")

    assert targets == (
        TagTarget("tables", "main.genie.dim_dq_monitored_tables"),
        TagTarget("tables", "main.genie.dim_dq_rules"),
    )


def test_demo_targets_include_source_schema_tables() -> None:
    targets = demo_tag_targets("main", SOURCE_SCHEMA, ("orders", "customers"))
    assert targets == (
        TagTarget("schemas", "main.dqx_studio_demo"),
        TagTarget("tables", "main.dqx_studio_demo.customers"),
        TagTarget("tables", "main.dqx_studio_demo.orders"),
    )
