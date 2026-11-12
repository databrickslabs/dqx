"""Rule policy storage service.

CRUD over ``dq_rule_policies`` (Lakebase Postgres). A policy belongs to one
registry rule; names are unique per rule (case-insensitive, trimmed).
"""

import json
import logging
from datetime import UTC, datetime
from uuid import uuid4

from databricks_labs_dqx_app.backend.policy_models import RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.sanitization import safe_log_value
from databricks_labs_dqx_app.backend.services.policy_evaluation_cache import bump_policy_generation
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

_TRUE_TEXT = frozenset({"true", "t", "1"})
_UNIQUE_VIOLATION_SQLSTATE = "23505"


def _is_unique_violation(exc: Exception) -> bool:
    """Whether *exc* is a database unique-constraint violation (psycopg and SQL errors expose *sqlstate*)."""
    return getattr(exc, "sqlstate", None) == _UNIQUE_VIOLATION_SQLSTATE


class RulePolicyNotFoundError(LookupError):
    """Raised when a policy id does not exist."""


class DuplicatePolicyNameError(ValueError):
    """Raised when a rule already has a policy with the same name."""


class RulePolicyService:
    """Manages ``dq_rule_policies``."""

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._table = sql.fqn("dq_rule_policies")
        self._select = (
            "SELECT policy_id, rule_id, name, description, "
            f"{sql.select_json_text('scope')} AS scope, "
            f"{sql.select_json_text('table_condition')} AS table_condition, "
            f"{sql.select_json_text('column_conditions')} AS column_conditions, "
            "register_tables, created_by, "
            f"{sql.ts_text('created_at')} AS created_at, updated_by, "
            f"{sql.ts_text('updated_at')} AS updated_at "
            f"FROM {self._table}"
        )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_for_rule(self, rule_id: str) -> list[RulePolicy]:
        """List every policy attached to *rule_id*, oldest first."""
        rows = self._sql.query_dicts(
            f"{self._select} WHERE rule_id = '{escape_sql_string(rule_id)}' ORDER BY created_at, policy_id"
        )
        return [self._row_to_policy(row) for row in rows]

    def list_all(self) -> list[RulePolicy]:
        """List every policy across all rules."""
        rows = self._sql.query_dicts(f"{self._select} ORDER BY created_at, policy_id")
        return [self._row_to_policy(row) for row in rows]

    def get(self, policy_id: str) -> RulePolicy | None:
        """Return one policy by id, or None."""
        rows = self._sql.query_dicts(f"{self._select} WHERE policy_id = '{escape_sql_string(policy_id)}'")
        return self._row_to_policy(rows[0]) if rows else None

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def create(self, rule_id: str, body: RulePolicyIn, user_email: str) -> RulePolicy:
        """Create a policy under *rule_id*.

        Raises:
            DuplicatePolicyNameError: another policy of this rule has the same name.
        """
        name = body.name.strip()
        self._ensure_name_unique(rule_id, name)
        policy_id = str(uuid4())
        now = datetime.now(UTC).isoformat()
        try:
            self._insert(policy_id, rule_id, name, body, user_email)
        except Exception as exc:
            # A concurrent save of the same name slips past the pre-check; the unique index catches it.
            if _is_unique_violation(exc):
                raise DuplicatePolicyNameError(f"This rule already has a policy named '{name}'.") from exc
            raise
        bump_policy_generation()
        logger.info(f"Created rule policy {safe_log_value(policy_id)} for rule {safe_log_value(rule_id)}")
        return RulePolicy(
            **body.model_dump(),
            policy_id=policy_id,
            rule_id=rule_id,
            created_by=user_email,
            created_at=now,
            updated_by=user_email,
            updated_at=now,
        ).model_copy(update={"name": name})

    def _insert(self, policy_id: str, rule_id: str, name: str, body: RulePolicyIn, user_email: str) -> None:
        self._sql.insert(
            self._table,
            values={
                "policy_id": policy_id,
                "rule_id": rule_id,
                "name": name,
                "description": body.description,
                **self._json_values(body),
                "register_tables": body.register_tables,
                "created_by": user_email,
                "created_at": RawSql("current_timestamp()"),
                "updated_by": user_email,
                "updated_at": RawSql("current_timestamp()"),
            },
        )

    def update(self, policy_id: str, body: RulePolicyIn, user_email: str) -> RulePolicy:
        """Replace a policy's definition; created_* fields are preserved.

        Raises:
            RulePolicyNotFoundError: unknown *policy_id*.
            DuplicatePolicyNameError: another policy of the same rule has this name.
        """
        existing = self.get(policy_id)
        if existing is None:
            raise RulePolicyNotFoundError(f"Rule policy '{policy_id}' not found.")
        name = body.name.strip()
        self._ensure_name_unique(existing.rule_id, name, exclude_policy_id=policy_id)
        try:
            self._sql.update(
                self._table,
                updates={
                    "name": name,
                    "description": body.description,
                    **self._json_values(body),
                    "register_tables": body.register_tables,
                    "updated_by": user_email,
                    "updated_at": RawSql("current_timestamp()"),
                },
                where={"policy_id": policy_id},
            )
        except Exception as exc:
            if _is_unique_violation(exc):
                raise DuplicatePolicyNameError(f"This rule already has a policy named '{name}'.") from exc
            raise
        bump_policy_generation()
        logger.info(f"Updated rule policy {safe_log_value(policy_id)}")
        refreshed = self.get(policy_id)
        if refreshed is None:
            raise RulePolicyNotFoundError(f"Rule policy '{policy_id}' not found.")
        return refreshed

    def delete(self, policy_id: str) -> None:
        """Delete one policy.

        Raises:
            RulePolicyNotFoundError: unknown *policy_id*.
        """
        if self._sql.count(self._table, where={"policy_id": policy_id}) == 0:
            raise RulePolicyNotFoundError(f"Rule policy '{policy_id}' not found.")
        self._sql.delete(self._table, where={"policy_id": policy_id})
        bump_policy_generation()
        logger.info(f"Deleted rule policy {safe_log_value(policy_id)}")

    def delete_for_rule(self, rule_id: str) -> int:
        """Delete every policy of *rule_id*; return how many were removed."""
        count = self._sql.count(self._table, where={"rule_id": rule_id})
        if count:
            self._sql.delete(self._table, where={"rule_id": rule_id})
            bump_policy_generation()
            logger.info(f"Deleted {count} rule policies for rule {safe_log_value(rule_id)}")
        return count

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_name_unique(self, rule_id: str, name: str, *, exclude_policy_id: str | None = None) -> None:
        sql = (
            f"SELECT policy_id FROM {self._table} "
            f"WHERE rule_id = '{escape_sql_string(rule_id)}' "
            f"AND lower(name) = '{escape_sql_string(name.lower())}'"
        )
        if exclude_policy_id is not None:
            sql += f" AND policy_id <> '{escape_sql_string(exclude_policy_id)}'"
        if self._sql.query(sql):
            raise DuplicatePolicyNameError(f"This rule already has a policy named '{name}'.")

    def _json_values(self, body: RulePolicyIn) -> dict[str, RawSql]:
        dumped = body.model_dump(mode="json")
        return {
            column: RawSql(self._sql.json_literal_expr(json.dumps(dumped[column])))
            for column in ("scope", "table_condition", "column_conditions")
        }

    @staticmethod
    def _row_to_policy(row: dict[str, str | None]) -> RulePolicy:
        return RulePolicy.model_validate(
            {
                "policy_id": row["policy_id"],
                "rule_id": row["rule_id"],
                "name": row["name"],
                "description": row.get("description"),
                "scope": json.loads(row["scope"] or "{}"),
                "table_condition": json.loads(row["table_condition"] or "{}"),
                "column_conditions": json.loads(row["column_conditions"] or "{}"),
                "register_tables": str(row.get("register_tables") or "").lower() in _TRUE_TEXT,
                "created_by": row.get("created_by"),
                "created_at": row.get("created_at"),
                "updated_by": row.get("updated_by"),
                "updated_at": row.get("updated_at"),
            }
        )
