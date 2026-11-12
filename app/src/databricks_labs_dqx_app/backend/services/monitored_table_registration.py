"""Shared single-table registration for monitored tables.

Used by the register route and by rule policies that register matching tables,
so both follow exactly the same steps: default the owner to the table's Unity
Catalog owner (read on-behalf-of the caller), then register the binding as a
draft (which seeds the default grants with the caller as owner).
"""

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.registry_models import MonitoredTable
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.policy_evaluation_cache import bump_policy_generation

REGISTER_TABLE_ROLES: tuple[UserRole, ...] = (UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR)


def can_register_tables(role: UserRole) -> bool:
    """Whether *role* may register tables for monitoring (same roles as the register route)."""
    return role in REGISTER_TABLE_ROLES


def register_table(
    table_fqn: str,
    user_email: str,
    *,
    monitored_tables: MonitoredTableService,
    discovery: DiscoveryService,
    owner: str | None = None,
    owner_display_name: str | None = None,
) -> MonitoredTable:
    """Register *table_fqn* as a draft monitored table on behalf of *user_email*.

    Args:
        table_fqn: The table to register (catalog.schema.table).
        user_email: The acting user; recorded as creator and seeded as owner of the grants.
        monitored_tables: Service owning the bindings.
        discovery: OBO discovery service used to read the table's Unity Catalog owner.
        owner: Explicit owner; when omitted the Unity Catalog owner is used (falling back to the creator).
        owner_display_name: Optional display name for *owner*.

    Returns:
        The new binding.

    Raises:
        DuplicateMonitoredTableError: the table is already monitored.
        ValueError: *table_fqn* is not a valid three-part name.
    """
    resolved_owner = owner or discovery.get_table_owner(table_fqn)
    table = monitored_tables.register(
        table_fqn,
        user_email,
        owner=resolved_owner,
        owner_display_name=owner_display_name,
    )
    bump_policy_generation()  # a new binding is a new table rule policies can reach
    return table
