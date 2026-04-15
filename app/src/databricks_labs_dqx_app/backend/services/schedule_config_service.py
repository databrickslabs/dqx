"""Service for managing schedule configurations in their own Delta table.

Each schedule is stored as a separate row in ``dq_schedule_configs`` with its
config serialized as JSON.  Every mutation is recorded in
``dq_schedule_configs_history`` for auditability.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


@dataclass
class ScheduleConfigEntry:
    schedule_name: str
    config: dict[str, Any]
    version: int = 1
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None


class ScheduleConfigService:
    """CRUD for per-schedule configuration rows in ``dq_schedule_configs``."""

    def __init__(
        self,
        ws: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        schema: str,
    ) -> None:
        self._ws = ws
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._table = f"{catalog}.{schema}.dq_schedule_configs"
        self._history_table = f"{catalog}.{schema}.dq_schedule_configs_history"

    def list_schedules(self) -> list[ScheduleConfigEntry]:
        sql = (
            f"SELECT schedule_name, config_json, version, created_by, "
            f"CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING) "
            f"FROM {self._table} ORDER BY schedule_name"
        )
        rows = self._query(sql)
        return [self._row_to_entry(row) for row in rows]

    def get(self, name: str) -> ScheduleConfigEntry | None:
        escaped = self._esc(name)
        sql = (
            f"SELECT schedule_name, config_json, version, created_by, "
            f"CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING) "
            f"FROM {self._table} WHERE schedule_name = '{escaped}'"
        )
        rows = self._query(sql)
        if not rows:
            return None
        return self._row_to_entry(rows[0])

    def save(
        self,
        name: str,
        config: dict[str, Any],
        user_email: str,
    ) -> ScheduleConfigEntry:
        escaped_name = self._esc(name)
        config_json = json.dumps(config)
        escaped_json = self._esc(config_json)
        e_email = self._esc(user_email)
        now = datetime.now(timezone.utc).isoformat()

        sql = (
            f"MERGE INTO {self._table} AS target "
            f"USING (SELECT '{escaped_name}' AS schedule_name) AS source "
            "ON target.schedule_name = source.schedule_name "
            "WHEN MATCHED THEN UPDATE SET "
            f"  config_json = '{escaped_json}', "
            "  version = target.version + 1, "
            f"  updated_by = '{e_email}', "
            f"  updated_at = '{now}' "
            "WHEN NOT MATCHED THEN INSERT "
            "(schedule_name, config_json, version, created_by, created_at, updated_by, updated_at) "
            f"VALUES ('{escaped_name}', '{escaped_json}', 1, '{e_email}', '{now}', '{e_email}', '{now}')"
        )
        self._execute(sql)
        self._record_history(name, config_json, user_email, "save")
        logger.info("Saved schedule config: %s (user=%s)", name, user_email)

        entry = self.get(name)
        if entry is None:
            return ScheduleConfigEntry(
                schedule_name=name,
                config=config,
                version=1,
                created_by=user_email,
                created_at=now,
                updated_by=user_email,
                updated_at=now,
            )
        return entry

    def delete(self, name: str, user_email: str) -> None:
        existing = self.get(name)
        if existing:
            self._record_history(name, json.dumps(existing.config), user_email, "delete")
        escaped_name = self._esc(name)
        sql = f"DELETE FROM {self._table} WHERE schedule_name = '{escaped_name}'"
        self._execute(sql)
        logger.info("Deleted schedule config: %s (user=%s)", name, user_email)

    def get_history(self, name: str) -> list[dict[str, Any]]:
        escaped = self._esc(name)
        sql = (
            f"SELECT schedule_name, config_json, version, action, changed_by, "
            f"CAST(changed_at AS STRING) "
            f"FROM {self._history_table} "
            f"WHERE schedule_name = '{escaped}' "
            "ORDER BY changed_at DESC"
        )
        rows = self._query(sql)
        result = []
        for row in rows:
            try:
                cfg = json.loads(row[1]) if row[1] else {}
            except json.JSONDecodeError:
                cfg = {}
            result.append(
                {
                    "schedule_name": row[0],
                    "config": cfg,
                    "version": int(row[2]) if row[2] else 0,
                    "action": row[3],
                    "changed_by": row[4],
                    "changed_at": row[5],
                }
            )
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_history(
        self,
        name: str,
        config_json: str,
        user_email: str,
        action: str,
    ) -> None:
        try:
            existing = self.get(name)
            version = existing.version if existing else 0
            escaped_name = self._esc(name)
            escaped_json = self._esc(config_json)
            e_email = self._esc(user_email)
            now = datetime.now(timezone.utc).isoformat()
            sql = (
                f"INSERT INTO {self._history_table} "
                "(schedule_name, config_json, version, action, changed_by, changed_at) "
                f"VALUES ('{escaped_name}', '{escaped_json}', {int(version)}, "
                f"'{action}', '{e_email}', '{now}')"
            )
            self._execute(sql)
        except Exception:
            logger.warning("Failed to record history for %s (non-fatal)", name, exc_info=True)

    @staticmethod
    def _esc(value: str) -> str:
        return escape_sql_string(value)

    def _row_to_entry(self, row: list[str]) -> ScheduleConfigEntry:
        try:
            config = json.loads(row[1]) if row[1] else {}
        except json.JSONDecodeError:
            config = {}
        return ScheduleConfigEntry(
            schedule_name=row[0],
            config=config,
            version=int(row[2]) if row[2] else 1,
            created_by=row[3],
            created_at=row[4],
            updated_by=row[5],
            updated_at=row[6],
        )

    def _execute(self, sql: str) -> None:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}")

    def _query(self, sql: str) -> list[list[str]]:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}")
        if resp.result and resp.result.data_array:
            return resp.result.data_array
        return []
