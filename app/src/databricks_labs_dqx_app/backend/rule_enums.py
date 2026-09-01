"""Lifecycle enums for the per-table rules catalog (``dq_quality_rules``).

Kept in a leaf module so migrations and ``RulesCatalogService`` can import them
without pulling in ``models`` (which itself imports services that import the
catalog — a circular dependency on the Studio branch).
"""

from enum import Enum


class RuleSource(Enum):
    """Source (e.g. 'ui', 'profiler') where the rule was created."""

    ui = "ui"
    sql = "sql"
    profiler = "profiler"
    user_import = "import"
    ai = "ai"
    # Materialized from a published Rules Registry rule (Studio).
    registry = "registry"

    @classmethod
    def sql_in_list(cls) -> str:
        """Renders the members as a SQL-safe list for 'IN' expressions."""
        return ", ".join(f"'{member.value}'" for member in cls)


class RuleStatus(Enum):
    """Lifecycle status of a rule in the catalog."""

    draft = "draft"
    pending_approval = "pending_approval"
    approved = "approved"
    rejected = "rejected"

    @classmethod
    def sql_in_list(cls) -> str:
        """Renders the members as a SQL-safe list for 'IN' expressions."""
        return ", ".join(f"'{member.value}'" for member in cls)
