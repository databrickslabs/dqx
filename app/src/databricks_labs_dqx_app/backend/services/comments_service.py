from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


class Comment:
    def __init__(
        self,
        comment_id: str,
        entity_type: str,
        entity_id: str,
        user_email: str,
        comment: str,
        created_at: str | None = None,
    ) -> None:
        self.comment_id = comment_id
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.user_email = user_email
        self.comment = comment
        self.created_at = created_at


class CommentsService:
    VALID_ENTITY_TYPES = {"run", "rule"}

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        self._table = f"{sql.catalog}.{sql.schema}.dq_comments"

    def add_comment(self, entity_type: str, entity_id: str, user_email: str, comment: str) -> Comment:
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_entity_type

        validate_entity_type(entity_type, self.VALID_ENTITY_TYPES)

        comment_id = uuid4().hex[:16]
        now = datetime.now(timezone.utc).isoformat()
        e_comment = escape_sql_string(comment)
        e_email = escape_sql_string(user_email)
        e_entity_id = escape_sql_string(entity_id)
        e_type = escape_sql_string(entity_type)

        sql = (
            f"INSERT INTO {self._table} (comment_id, entity_type, entity_id, user_email, comment, created_at) "
            f"VALUES ('{comment_id}', '{e_type}', '{e_entity_id}', "
            f"'{e_email}', '{e_comment}', '{now}')"
        )
        self._sql.execute(sql)
        logger.info("Added comment %s on %s/%s by %s", comment_id, entity_type, entity_id, user_email)

        return Comment(
            comment_id=comment_id,
            entity_type=entity_type,
            entity_id=entity_id,
            user_email=user_email,
            comment=comment,
            created_at=now,
        )

    def list_comments(self, entity_type: str, entity_id: str) -> list[Comment]:
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_entity_type

        validate_entity_type(entity_type, self.VALID_ENTITY_TYPES)
        e_type = escape_sql_string(entity_type)
        e_entity_id = escape_sql_string(entity_id)
        sql = (
            f"SELECT comment_id, entity_type, entity_id, user_email, comment, "
            f"CAST(created_at AS STRING) "
            f"FROM {self._table} "
            f"WHERE entity_type = '{e_type}' AND entity_id = '{e_entity_id}' "
            f"ORDER BY created_at ASC LIMIT 200"
        )
        rows = self._sql.query(sql)
        return [
            Comment(
                comment_id=row[0],
                entity_type=row[1],
                entity_id=row[2],
                user_email=row[3],
                comment=row[4],
                created_at=row[5],
            )
            for row in rows
        ]

    def delete_comment(self, comment_id: str, user_email: str) -> None:
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        e_id = escape_sql_string(comment_id)
        e_email = escape_sql_string(user_email)
        sql = f"DELETE FROM {self._table} " f"WHERE comment_id = '{e_id}' AND user_email = '{e_email}'"
        self._sql.execute(sql)
        logger.info("Deleted comment %s by %s", comment_id, user_email)
