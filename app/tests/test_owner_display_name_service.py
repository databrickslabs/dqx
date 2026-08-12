"""Unit tests for owner_display_name_service — SCIM resolver (batch + single)."""

from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User

from databricks_labs_dqx_app.backend.services import owner_display_name_service
from databricks_labs_dqx_app.backend.services.owner_display_name_service import (
    resolve_emails_to_display_names,
    resolve_owner_display_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(user_name: str, display_name: str | None) -> User:
    u = User()
    u.user_name = user_name
    u.display_name = display_name
    return u


def _make_sp_ws(users: list[User]) -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.users.list.return_value = iter(users)
    return ws


# ---------------------------------------------------------------------------
# resolve_emails_to_display_names
# ---------------------------------------------------------------------------


class TestResolveEmailsToDisplayNames:
    def test_resolves_matched_emails(self) -> None:
        sp_ws = _make_sp_ws(
            [
                _make_user("alice@example.com", "Alice Smith"),
                _make_user("bob@example.com", "Bob Jones"),
            ]
        )
        result = resolve_emails_to_display_names(["alice@example.com", "bob@example.com"], sp_ws)
        assert result == {"alice@example.com": "Alice Smith", "bob@example.com": "Bob Jones"}

    def test_returns_empty_for_no_emails(self) -> None:
        sp_ws = _make_sp_ws([])
        result = resolve_emails_to_display_names([], sp_ws)
        assert result == {}

    def test_unmatched_emails_absent_from_result(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        result = resolve_emails_to_display_names(["alice@example.com", "unknown@example.com"], sp_ws)
        assert "unknown@example.com" not in result
        assert "alice@example.com" in result

    def test_scim_failure_returns_empty(self) -> None:
        sp_ws = create_autospec(WorkspaceClient, instance=True)
        sp_ws.users.list.side_effect = RuntimeError("SCIM unavailable")
        result = resolve_emails_to_display_names(["alice@example.com"], sp_ws)
        assert result == {}

    def test_user_without_display_name_excluded(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", None)])
        result = resolve_emails_to_display_names(["alice@example.com"], sp_ws)
        assert "alice@example.com" not in result


# ---------------------------------------------------------------------------
# resolve_owner_display_name (single, cached, best-effort)
# ---------------------------------------------------------------------------


class TestResolveOwnerDisplayName:
    def setup_method(self) -> None:
        # Isolate the module-level TTL cache between tests.
        owner_display_name_service._resolve_cache.clear()

    def test_resolves_single_email(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        assert resolve_owner_display_name("alice@example.com", sp_ws) == "Alice Smith"

    def test_none_when_no_client(self) -> None:
        assert resolve_owner_display_name("alice@example.com", None) is None

    def test_none_for_empty_owner(self) -> None:
        sp_ws = _make_sp_ws([])
        assert resolve_owner_display_name("", sp_ws) is None
        assert resolve_owner_display_name(None, sp_ws) is None

    def test_group_or_unresolvable_returns_none(self) -> None:
        # A group name has no SCIM user match → None (frontend shows the name).
        sp_ws = _make_sp_ws([])
        assert resolve_owner_display_name("data-stewards", sp_ws) is None

    def test_scim_failure_returns_none(self) -> None:
        sp_ws = create_autospec(WorkspaceClient, instance=True)
        sp_ws.users.list.side_effect = RuntimeError("SCIM down")
        assert resolve_owner_display_name("alice@example.com", sp_ws) is None

    def test_result_is_cached(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        assert resolve_owner_display_name("alice@example.com", sp_ws) == "Alice Smith"
        # Second call must hit the cache, not SCIM again (the iterator is spent).
        assert resolve_owner_display_name("alice@example.com", sp_ws) == "Alice Smith"
        assert sp_ws.users.list.call_count == 1
