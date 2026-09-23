"""Best-effort reconciliation of the DQX Studio Unity Catalog ownership tag."""

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import EntityTagAssignment

from databricks_labs_dqx_app.backend.demo.manifest import SOURCE_SCHEMA
from databricks_labs_dqx_app.backend.migrations import ANALYTICAL_TABLE_NAMES
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
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources

logger = logging.getLogger(__name__)

TAG_KEY = "app"
TAG_VALUE = "dqx-studio"
SUPPORTED_ENTITY_TYPES = frozenset({"schemas", "tables", "volumes"})


@dataclass(frozen=True)
class TagTarget:
    """A Unity Catalog object to receive the DQX Studio ownership tag."""

    entity_type: str
    entity_name: str

    def __post_init__(self) -> None:
        if self.entity_type not in SUPPORTED_ENTITY_TYPES:
            raise ValueError(f"Unsupported Unity Catalog tag entity type: {self.entity_type}")
        if not self.entity_name.strip():
            raise ValueError("Unity Catalog tag entity name is required")


class ResourceTaggingService:
    """Apply the DQX Studio ownership tag to Unity Catalog objects."""

    def __init__(self, workspace: WorkspaceClient) -> None:
        self._workspace = workspace

    def reconcile(self, targets: Iterable[TagTarget]) -> None:
        """Create or update the ownership tag for each target.

        A failure for one target is isolated so a missing permission or object
        does not prevent the remaining resources from being reconciled.
        """
        for target in targets:
            try:
                self._reconcile_target(target)
            except Exception:
                logger.warning(
                    "Could not apply the DQX Studio ownership tag to a Unity Catalog %s resource",
                    target.entity_type,
                )

    def _reconcile_target(self, target: TagTarget) -> None:
        api = self._workspace.entity_tag_assignments
        try:
            existing = api.get(target.entity_type, target.entity_name, TAG_KEY)
        except NotFound:
            api.create(
                EntityTagAssignment(
                    entity_type=target.entity_type,
                    entity_name=target.entity_name,
                    tag_key=TAG_KEY,
                    tag_value=TAG_VALUE,
                )
            )
            return
        if existing.tag_value == TAG_VALUE:
            return
        api.update(
            entity_type=target.entity_type,
            entity_name=target.entity_name,
            tag_key=TAG_KEY,
            tag_assignment=EntityTagAssignment(
                entity_type=target.entity_type,
                entity_name=target.entity_name,
                tag_key=TAG_KEY,
                tag_value=TAG_VALUE,
            ),
            update_mask="tag_value",
        )


def startup_tag_targets(resources: ActiveResources, include_bundle_resources: bool) -> tuple[TagTarget, ...]:
    """Build deterministic startup targets from the active installation resources."""
    catalog = resources.volume.catalog
    schema = resources.volume.schema
    main_tables = (*ANALYTICAL_TABLE_NAMES, "dq_migrations", ENTITLEMENTS_TABLE_NAME)
    genie_tables = (
        ATTRIBUTION_VIEW_NAME,
        SHAPING_VIEW_NAME,
        ASOF_VIEW_NAME,
        METRIC_VIEW_NAME,
        FAILING_ROWS_VIEW_NAME,
    )
    targets: set[TagTarget] = {
        TagTarget("schemas", f"{catalog}.{resources.tmp_schema}"),
        TagTarget("schemas", f"{catalog}.{resources.genie_schema}"),
        *(TagTarget("tables", f"{catalog}.{schema}.{name}") for name in main_tables),
        *(TagTarget("tables", f"{catalog}.{resources.genie_schema}.{name}") for name in genie_tables),
        *metadata_dimension_tag_targets(catalog, resources.genie_schema),
    }
    if include_bundle_resources:
        targets.update(
            {
                TagTarget("schemas", f"{catalog}.{schema}"),
                TagTarget("schemas", f"{catalog}.{SOURCE_SCHEMA}"),
                TagTarget("volumes", f"{catalog}.{schema}.{resources.volume.volume}"),
            }
        )
    return tuple(sorted(targets, key=lambda target: (target.entity_type, target.entity_name)))


def metadata_dimension_tag_targets(catalog: str, genie_schema: str) -> tuple[TagTarget, ...]:
    """Build deterministic targets for the replace-on-refresh Genie dimensions."""
    return tuple(
        sorted(
            (
                TagTarget("tables", f"{catalog}.{genie_schema}.{DIM_RULES_TABLE_NAME}"),
                TagTarget("tables", f"{catalog}.{genie_schema}.{DIM_MONITORED_TABLES_TABLE_NAME}"),
            ),
            key=lambda target: target.entity_name,
        )
    )


def demo_tag_targets(catalog: str, schema: str, table_names: tuple[str, ...]) -> tuple[TagTarget, ...]:
    """Build deterministic targets for the demo source schema and tables."""
    targets = {
        TagTarget("schemas", f"{catalog}.{schema}"),
        *(TagTarget("tables", f"{catalog}.{schema}.{name}") for name in table_names),
    }
    return tuple(sorted(targets, key=lambda target: (target.entity_type, target.entity_name)))
