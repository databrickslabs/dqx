"""Saved views: named presets of a page's column/filter layout and filter values.

Views are either org-wide (admin-managed) or personal (owner-only). Stars and
personal defaults are per-user state. The org default is a flag on a single org
view per page; resolution tolerates zero or several flagged rows by falling
back to the built-in Default view.
"""

import logging
import re
from uuid import uuid4

from pydantic import ValidationError

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.saved_views_models import (
    SavedViewCreateIn,
    SavedViewListOut,
    SavedViewOut,
    SavedViewPage,
    SavedViewPayload,
    SavedViewScope,
)
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

MAX_NAME_LENGTH = 80
BUILTIN_VIEW_NAME = "Default"
_UNIQUE_VIOLATION_SQLSTATE = "23505"


class SavedViewNotFoundError(Exception):
    """The view does not exist or is not visible to the caller."""


class SavedViewForbiddenError(Exception):
    """The caller may see the view but is not allowed to perform the action."""


class SavedViewConflictError(Exception):
    """A view with the same name already exists in the same scope."""


class SavedViewValidationError(Exception):
    """The request is well-formed but violates a saved-view rule."""


def normalize_view_name(raw: str) -> str:
    """Sanitise a user-supplied view name.

    Args:
        raw: Name as typed by the user.

    Returns:
        The name with control characters replaced, whitespace collapsed and
        the ends trimmed.

    Raises:
        SavedViewValidationError: If the cleaned name is not 1 to 80 characters.
    """
    cleaned = re.sub(r"\s+", " ", replace_control_characters(raw)).strip()
    if not 1 <= len(cleaned) <= MAX_NAME_LENGTH:
        raise SavedViewValidationError(f"View name must be between 1 and {MAX_NAME_LENGTH} characters")
    return cleaned


def _builtin_id(page: SavedViewPage) -> str:
    return f"builtin:{page.value}"


def _is_true(raw: str | None) -> bool:
    return (raw or "").strip().lower() == "true"


def _is_unique_violation(error: Exception) -> bool:
    return getattr(error, "sqlstate", None) == _UNIQUE_VIOLATION_SQLSTATE


class SavedViewsService:
    """Reads and writes saved views in the OLTP store."""

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._views = sql.fqn("dq_saved_views")
        self._state = sql.fqn("dq_saved_view_user_state")
        self._defaults = sql.fqn("dq_saved_view_defaults")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list_views(self, page: SavedViewPage, *, user: str, is_admin: bool) -> SavedViewListOut:
        """List the views visible to *user* on *page* with the effective default.

        Args:
            page: Page whose views to list.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            Visible views (org views plus the caller's own) and the id of the view to load by default.
        """
        self._ensure_builtin(page)
        rows = self._select_views(
            f"page_key = '{escape_sql_string(page.value)}' AND (scope = 'org' OR owner = '{escape_sql_string(user)}')"
        )
        stars = self._star_map(user)
        my_default = self._my_default_id(user, page)
        org_default = self._org_default_id(rows, page)
        visible_ids = {row["id"] for row in rows}
        effective = my_default if my_default in visible_ids and my_default is not None else org_default
        views = [
            self._to_out(row, user=user, is_admin=is_admin, stars=stars, my_default=my_default, org_default=org_default)
            for row in rows
        ]
        return SavedViewListOut(views=views, effective_default_id=effective)

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def create_view(self, body: SavedViewCreateIn, *, user: str, is_admin: bool) -> SavedViewOut:
        """Create a personal or org view.

        Args:
            body: Create request.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The created view.

        Raises:
            SavedViewForbiddenError: Org scope or org default requested by a non-admin.
            SavedViewValidationError: Invalid name, or org default requested for a personal view.
            SavedViewConflictError: A view with this name already exists.
        """
        self._ensure_builtin(body.page)
        name = normalize_view_name(body.name)
        if (body.scope == SavedViewScope.ORG or body.make_org_default) and not is_admin:
            raise SavedViewForbiddenError("Only admins can create org views or set the org default")
        if body.make_org_default and body.scope != SavedViewScope.ORG:
            raise SavedViewValidationError("Only an org view can be the org default")
        owner = user if body.scope == SavedViewScope.USER else None
        self._ensure_name_free(body.page, body.scope, owner, name, exclude_id=None)

        view_id = uuid4().hex
        try:
            self._sql.insert(
                self._views,
                values={
                    "id": view_id,
                    "page_key": body.page.value,
                    "name": name,
                    "scope": body.scope.value,
                    "owner": owner,
                    "payload": RawSql(self._sql.json_literal_expr(body.payload.model_dump_json())),
                    "is_builtin": False,
                    "is_org_default": False,
                    "created_by": user,
                    "updated_by": user,
                },
            )
        except Exception as error:
            if _is_unique_violation(error):
                raise SavedViewConflictError("A view with this name already exists") from error
            raise
        try:
            if body.make_my_default:
                self._write_my_default(user, body.page, view_id)
            if body.make_org_default:
                self._flag_org_default(body.page, view_id)
        except Exception:
            # The OLTP protocol spans Delta and Lakebase, so there is no portable
            # transaction: undo the insert instead (the delete cascades any default
            # pointer already written), so a failed create leaves nothing behind and
            # a retry can't hit a name conflict.
            try:
                self._sql.delete(self._views, where={"id": view_id})
            except Exception:
                logger.warning(f"Could not remove saved view {view_id} after a failed default write", exc_info=True)
            raise
        logger.info(f"Created saved view {view_id} on {body.page.value}")
        return self._out_for(view_id, user=user, is_admin=is_admin)

    def update_view(self, view_id: str, payload: SavedViewPayload, *, user: str, is_admin: bool) -> SavedViewOut:
        """Replace a view's payload.

        Args:
            view_id: View to update.
            payload: New payload.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The updated view.

        Raises:
            SavedViewNotFoundError: The view is missing or not visible.
            SavedViewForbiddenError: The caller may not edit this view.
            SavedViewValidationError: The view is built in.
        """
        row = self._get_visible(view_id, user)
        if _is_true(row["is_builtin"]):
            raise SavedViewValidationError("The Default view cannot be saved over; use Save as new")
        self._require_edit(row, user, is_admin)
        self._sql.update(
            self._views,
            updates={
                "payload": RawSql(self._sql.json_literal_expr(payload.model_dump_json())),
                "updated_by": user,
                "updated_at": RawSql("current_timestamp()"),
            },
            where={"id": view_id},
        )
        return self._out_for(view_id, user=user, is_admin=is_admin)

    def rename_view(self, view_id: str, name: str, *, user: str, is_admin: bool) -> SavedViewOut:
        """Rename a view.

        Args:
            view_id: View to rename.
            name: New name (sanitised).
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The renamed view.

        Raises:
            SavedViewNotFoundError: The view is missing or not visible.
            SavedViewForbiddenError: The caller may not edit this view.
            SavedViewValidationError: The view is built in, or the name is invalid.
            SavedViewConflictError: Another view already uses this name.
        """
        row = self._get_visible(view_id, user)
        if _is_true(row["is_builtin"]):
            raise SavedViewValidationError("The Default view cannot be renamed")
        self._require_edit(row, user, is_admin)
        clean = normalize_view_name(name)
        page = SavedViewPage(row["page_key"])
        self._ensure_builtin(page)
        self._ensure_name_free(page, SavedViewScope(row["scope"]), row["owner"], clean, exclude_id=view_id)
        try:
            self._sql.update(
                self._views,
                updates={"name": clean, "updated_by": user, "updated_at": RawSql("current_timestamp()")},
                where={"id": view_id},
            )
        except Exception as error:
            if _is_unique_violation(error):
                raise SavedViewConflictError("A view with this name already exists") from error
            raise
        return self._out_for(view_id, user=user, is_admin=is_admin)

    def delete_view(self, view_id: str, *, user: str, is_admin: bool) -> None:
        """Delete a view (stars and personal defaults cascade).

        Args:
            view_id: View to delete.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Raises:
            SavedViewNotFoundError: The view is missing or not visible.
            SavedViewForbiddenError: The caller may not edit this view.
            SavedViewValidationError: The view is built in.
        """
        row = self._get_visible(view_id, user)
        if _is_true(row["is_builtin"]):
            raise SavedViewValidationError("The Default view cannot be deleted")
        self._require_edit(row, user, is_admin)
        self._sql.delete(self._views, where={"id": view_id})
        logger.info(f"Deleted saved view {view_id}")

    def share_view(self, view_id: str, *, user: str, is_admin: bool) -> SavedViewOut:
        """Move an admin's own personal view into the org scope.

        The view keeps its id, so stars and personal defaults pointing at it keep working.

        Args:
            view_id: Personal view to share.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The view, now org-scoped and without an owner.

        Raises:
            SavedViewForbiddenError: The caller is not an admin.
            SavedViewNotFoundError: The view is missing or not visible (including other users' personal views).
            SavedViewValidationError: The view is built in or already an org view.
            SavedViewConflictError: An org view on the page already uses this name.
        """
        if not is_admin:
            raise SavedViewForbiddenError("Only admins can share views with the organisation")
        row = self._get_visible(view_id, user)
        if _is_true(row["is_builtin"]) or row["scope"] != SavedViewScope.USER.value:
            raise SavedViewValidationError("Only a personal view can be shared")
        if row["owner"] != user:
            raise SavedViewNotFoundError("Saved view not found")
        page = SavedViewPage(str(row["page_key"]))
        self._ensure_builtin(page)
        self._ensure_name_free(page, SavedViewScope.ORG, None, str(row["name"]), exclude_id=view_id)
        try:
            self._sql.update(
                self._views,
                updates={
                    "scope": SavedViewScope.ORG.value,
                    "owner": None,
                    "updated_by": user,
                    "updated_at": RawSql("current_timestamp()"),
                },
                where={"id": view_id},
            )
        except Exception as error:
            if _is_unique_violation(error):
                raise SavedViewConflictError("An org view with this name already exists") from error
            raise
        logger.info(f"Shared saved view {view_id} with the organisation")
        return self._out_for(view_id, user=user, is_admin=is_admin)

    def set_starred(self, view_id: str, starred: bool, *, user: str, is_admin: bool) -> SavedViewOut:
        """Star or unstar a visible view for the caller.

        Args:
            view_id: View to (un)star.
            starred: Desired state.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The view with the new star state.

        Raises:
            SavedViewNotFoundError: The view is missing or not visible.
        """
        self._get_visible(view_id, user)
        self._sql.upsert(
            self._state,
            key_cols={"user_email": user, "view_id": view_id},
            value_cols={"starred": starred},
        )
        return self._out_for(view_id, user=user, is_admin=is_admin)

    def set_my_default(
        self, page: SavedViewPage, view_id: str | None, *, user: str, is_admin: bool
    ) -> SavedViewListOut:
        """Set or clear the caller's personal default for *page*.

        Args:
            page: Page the default applies to.
            view_id: Visible view on that page, or None to clear.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The refreshed view list.

        Raises:
            SavedViewNotFoundError: The view is missing or not visible.
            SavedViewValidationError: The view belongs to a different page.
        """
        if view_id is None:
            self._sql.delete(self._defaults, where={"user_email": user, "page_key": page.value})
        else:
            row = self._get_visible(view_id, user)
            if row["page_key"] != page.value:
                raise SavedViewValidationError("View belongs to a different page")
            self._write_my_default(user, page, view_id)
        return self.list_views(page, user=user, is_admin=is_admin)

    def set_org_default(self, page: SavedViewPage, view_id: str, *, user: str, is_admin: bool) -> SavedViewListOut:
        """Make *view_id* the org default for *page* in a single statement.

        Args:
            page: Page the default applies to.
            view_id: Org view on that page.
            user: Caller email.
            is_admin: Whether the caller is an admin.

        Returns:
            The refreshed view list.

        Raises:
            SavedViewForbiddenError: The caller is not an admin.
            SavedViewNotFoundError: The view is missing or not visible.
            SavedViewValidationError: The view is personal or on a different page.
        """
        if not is_admin:
            raise SavedViewForbiddenError("Only admins can set the org default")
        row = self._get_visible(view_id, user)
        if row["page_key"] != page.value:
            raise SavedViewValidationError("View belongs to a different page")
        if row["scope"] != SavedViewScope.ORG.value:
            raise SavedViewValidationError("Only an org view can be the org default")
        self._flag_org_default(page, view_id)
        return self.list_views(page, user=user, is_admin=is_admin)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _select_views(self, where: str) -> list[dict[str, str | None]]:
        sql = (
            "SELECT id, page_key, name, scope, owner, "
            f"{self._sql.select_json_text('payload')} AS payload, is_builtin, is_org_default "
            f"FROM {self._views} WHERE {where} "
            "ORDER BY is_builtin DESC, lower(name) ASC"
        )
        return self._sql.query_dicts(sql)

    def _ensure_builtin(self, page: SavedViewPage) -> None:
        """Re-create the built-in Default row if a database reset removed it.

        The row is inserted unflagged: with zero flagged org views, resolution
        already falls back to the built-in, and it never overrides an admin's choice.
        Any conflict (id or the per-scope name constraint) is ignored so that an
        org view already named "Default" can never break listing.
        """
        self._sql.execute(
            f"INSERT INTO {self._views} (id, page_key, name, scope, owner, payload, is_builtin, "
            "is_org_default, created_by, updated_by) "
            f"VALUES ('{escape_sql_string(_builtin_id(page))}', '{escape_sql_string(page.value)}', "
            f"'{escape_sql_string(BUILTIN_VIEW_NAME)}', 'org', NULL, NULL, TRUE, FALSE, 'system', 'system') "
            "ON CONFLICT DO NOTHING"
        )

    def _get_visible(self, view_id: str, user: str) -> dict[str, str | None]:
        prefix = "builtin:"
        if view_id.startswith(prefix):
            try:
                self._ensure_builtin(SavedViewPage(view_id[len(prefix) :]))
            except ValueError:
                pass
        rows = self._select_views(
            f"id = '{escape_sql_string(view_id)}' AND (scope = 'org' OR owner = '{escape_sql_string(user)}')"
        )
        if not rows:
            raise SavedViewNotFoundError("Saved view not found")
        return rows[0]

    def _star_map(self, user: str) -> dict[str, bool]:
        rows = self._sql.select_dicts(self._state, ["view_id", "starred"], where={"user_email": user})
        return {str(row["view_id"]): _is_true(row["starred"]) for row in rows}

    def _my_default_id(self, user: str, page: SavedViewPage) -> str | None:
        rows = self._sql.select_dicts(self._defaults, ["view_id"], where={"user_email": user, "page_key": page.value})
        return rows[0]["view_id"] if rows else None

    @staticmethod
    def _org_default_id(rows: list[dict[str, str | None]], page: SavedViewPage) -> str:
        flagged = [row["id"] for row in rows if _is_true(row["is_org_default"]) and row["scope"] == "org"]
        if len(flagged) == 1 and flagged[0] is not None:
            return flagged[0]
        return _builtin_id(page)

    def _to_out(
        self,
        row: dict[str, str | None],
        *,
        user: str,
        is_admin: bool,
        stars: dict[str, bool],
        my_default: str | None,
        org_default: str,
    ) -> SavedViewOut:
        view_id = str(row["id"])
        is_builtin = _is_true(row["is_builtin"])
        scope = SavedViewScope(str(row["scope"]))
        return SavedViewOut(
            id=view_id,
            page=SavedViewPage(str(row["page_key"])),
            name=str(row["name"]),
            scope=scope,
            payload=self._parse_payload(view_id, row["payload"]),
            is_builtin=is_builtin,
            is_org_default=view_id == org_default,
            is_my_default=view_id == my_default,
            starred=stars.get(view_id, is_builtin),
            can_edit=self._can_edit(row, user, is_admin),
            can_set_org_default=is_admin and scope == SavedViewScope.ORG,
        )

    def _out_for(self, view_id: str, *, user: str, is_admin: bool) -> SavedViewOut:
        row = self._get_visible(view_id, user)
        page = SavedViewPage(str(row["page_key"]))
        org_rows = self._select_views(f"page_key = '{escape_sql_string(page.value)}' AND scope = 'org'")
        return self._to_out(
            row,
            user=user,
            is_admin=is_admin,
            stars=self._star_map(user),
            my_default=self._my_default_id(user, page),
            org_default=self._org_default_id(org_rows, page),
        )

    @staticmethod
    def _parse_payload(view_id: str, raw: str | None) -> SavedViewPayload | None:
        if raw is None:
            return None
        try:
            return SavedViewPayload.model_validate_json(raw)
        except ValidationError:
            logger.warning(f"Saved view {view_id} has an invalid stored payload; returning it without a payload")
            return None

    @staticmethod
    def _can_edit(row: dict[str, str | None], user: str, is_admin: bool) -> bool:
        if _is_true(row["is_builtin"]):
            return False
        if row["scope"] == SavedViewScope.ORG.value:
            return is_admin
        return row["owner"] == user

    def _require_edit(self, row: dict[str, str | None], user: str, is_admin: bool) -> None:
        if not self._can_edit(row, user, is_admin):
            raise SavedViewForbiddenError("You do not have permission to edit this view")

    def _ensure_name_free(
        self,
        page: SavedViewPage,
        scope: SavedViewScope,
        owner: str | None,
        name: str,
        *,
        exclude_id: str | None,
    ) -> None:
        owner_clause = "owner IS NULL" if owner is None else f"owner = '{escape_sql_string(owner)}'"
        exclude_clause = f" AND id <> '{escape_sql_string(exclude_id)}'" if exclude_id else ""
        rows = self._sql.query_dicts(
            f"SELECT id FROM {self._views} WHERE page_key = '{escape_sql_string(page.value)}' "
            f"AND scope = '{escape_sql_string(scope.value)}' AND {owner_clause} "
            f"AND lower(name) = lower('{escape_sql_string(name)}'){exclude_clause} LIMIT 1"
        )
        if rows:
            raise SavedViewConflictError("A view with this name already exists")

    def _write_my_default(self, user: str, page: SavedViewPage, view_id: str) -> None:
        self._sql.upsert(
            self._defaults,
            key_cols={"user_email": user, "page_key": page.value},
            value_cols={"view_id": view_id},
        )

    def _flag_org_default(self, page: SavedViewPage, view_id: str) -> None:
        self._sql.execute(
            f"UPDATE {self._views} SET is_org_default = (id = '{escape_sql_string(view_id)}') "
            f"WHERE page_key = '{escape_sql_string(page.value)}' AND scope = 'org'"
        )
