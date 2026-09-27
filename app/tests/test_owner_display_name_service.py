"""Unit tests for owner_display_name_service — SCIM resolver (batch + single)."""

from dataclasses import dataclass
from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest
from databricks.sdk.service.iam import ComplexValue, Group, User

from databricks_labs_dqx_app.backend.services import owner_display_name_service
from databricks_labs_dqx_app.backend.services.owner_display_name_service import (
    ResolvedOwner,
    canonicalize_owner,
    fill_missing_owner_display_names,
    lookup_owners,
    resolve_emails_to_display_names,
    resolve_owner_display_name,
    resolve_owners_cached,
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


# ---------------------------------------------------------------------------
# resolve_owners_cached (batched read-time enrichment)
# ---------------------------------------------------------------------------


class TestResolveOwnersCached:
    def setup_method(self) -> None:
        owner_display_name_service._resolve_cache.clear()

    def test_batches_misses_into_one_scim_call(self) -> None:
        sp_ws = _make_sp_ws(
            [
                _make_user("alice@example.com", "Alice Smith"),
                _make_user("bob@example.com", "Bob Jones"),
            ]
        )
        result = resolve_owners_cached(["alice@example.com", "bob@example.com"], sp_ws)
        assert result == {"alice@example.com": "Alice Smith", "bob@example.com": "Bob Jones"}
        # A single batched SCIM call covers both misses.
        assert sp_ws.users.list.call_count == 1

    def test_dedupes_and_omits_unresolvable(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        result = resolve_owners_cached(["alice@example.com", "alice@example.com", "data-stewards"], sp_ws)
        assert result == {"alice@example.com": "Alice Smith"}
        assert "data-stewards" not in result

    def test_serves_from_cache_without_rehitting_scim(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        assert resolve_owners_cached(["alice@example.com"], sp_ws) == {"alice@example.com": "Alice Smith"}
        # Second call is served from the TTL cache; the spent iterator is untouched.
        assert resolve_owners_cached(["alice@example.com"], sp_ws) == {"alice@example.com": "Alice Smith"}
        assert sp_ws.users.list.call_count == 1

    def test_empty_or_no_client_returns_empty(self) -> None:
        sp_ws = _make_sp_ws([])
        assert resolve_owners_cached([], sp_ws) == {}
        assert resolve_owners_cached(["alice@example.com"], None) == {}


# ---------------------------------------------------------------------------
# lookup_owners / canonicalize_owner (principal matching for imported owners)
# ---------------------------------------------------------------------------


class TestLookupOwners:
    def setup_method(self) -> None:
        owner_display_name_service._resolve_cache.clear()

    def test_requests_display_name_attribute(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        lookup_owners(["alice@example.com"], sp_ws)
        assert "displayName" in sp_ws.users.list.call_args.kwargs["attributes"]

    def test_matches_mis_cased_email_to_canonical_user(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        result = lookup_owners(["Alice@Example.com"], sp_ws)
        assert result == {"Alice@Example.com": ResolvedOwner("alice@example.com", "Alice Smith", "user")}

    def test_matches_secondary_email(self) -> None:
        user = _make_user("asmith", "Alice Smith")
        user.emails = [ComplexValue(value="alice@example.com")]
        sp_ws = _make_sp_ws([user])
        result = lookup_owners(["alice@example.com"], sp_ws)
        assert result["alice@example.com"] == ResolvedOwner("asmith", "Alice Smith", "user")

    def test_confirmed_miss_is_none(self) -> None:
        sp_ws = _make_sp_ws([])
        assert lookup_owners(["jhon.doe@example.com"], sp_ws) == {"jhon.doe@example.com": None}

    def test_scim_failure_is_unknown_not_missing(self) -> None:
        sp_ws = create_autospec(WorkspaceClient, instance=True)
        sp_ws.users.list.side_effect = RuntimeError("SCIM down")
        assert lookup_owners(["alice@example.com"], sp_ws) == {}
        # Failures are not cached, so a later call retries SCIM.
        lookup_owners(["alice@example.com"], sp_ws)
        assert sp_ws.users.list.call_count == 2

    def test_group_name_is_verified(self) -> None:
        sp_ws = _make_sp_ws([])
        group = Group(display_name="data-stewards")
        sp_ws.groups.list.return_value = iter([group])
        result = lookup_owners(["data-stewards"], sp_ws)
        assert result["data-stewards"] == ResolvedOwner("data-stewards", "data-stewards", "group")

    def test_emails_are_not_looked_up_as_groups(self) -> None:
        sp_ws = _make_sp_ws([])
        lookup_owners(["jhon.doe@example.com"], sp_ws)
        sp_ws.groups.list.assert_not_called()


class TestCanonicalizeOwner:
    def setup_method(self) -> None:
        owner_display_name_service._resolve_cache.clear()

    def test_replaces_owner_with_canonical_user(self) -> None:
        sp_ws = _make_sp_ws([_make_user("alice@example.com", "Alice Smith")])
        assert canonicalize_owner(" ALICE@example.com ", sp_ws) == ("alice@example.com", "Alice Smith")

    def test_keeps_unmatched_owner_verbatim(self) -> None:
        sp_ws = _make_sp_ws([])
        assert canonicalize_owner("jhon.doe@example.com", sp_ws) == ("jhon.doe@example.com", None)

    def test_group_owner_has_no_display_name(self) -> None:
        sp_ws = _make_sp_ws([])
        sp_ws.groups.list.return_value = iter([Group(display_name="data-stewards")])
        assert canonicalize_owner("data-stewards", sp_ws) == ("data-stewards", None)


# ---------------------------------------------------------------------------
# fill_missing_owner_display_names (read-time backfill for list pages)
# ---------------------------------------------------------------------------


@dataclass
class _Owned:
    owner: str | None
    owner_display_name: str | None = None


class TestFillMissingOwnerDisplayNames:
    def setup_method(self) -> None:
        owner_display_name_service._resolve_cache.clear()

    def test_fills_missing_names_and_persists_them(self) -> None:
        sp_ws = _make_sp_ws([_make_user("tasha@example.com", "Tasha Yang")])
        sql = MagicMock()
        rows = [_Owned("tasha@example.com"), _Owned("mina@example.com", "Mina A")]
        fill_missing_owner_display_names(rows, sp_ws, sql, "cat.sch.dq_data_products")
        assert [r.owner_display_name for r in rows] == ["Tasha Yang", "Mina A"]
        [stmt] = [c.args[0] for c in sql.execute.call_args_list]
        assert "UPDATE cat.sch.dq_data_products SET owner_display_name = 'Tasha Yang'" in stmt
        assert "WHERE owner = 'tasha@example.com'" in stmt
        assert "owner_display_name IS NULL" in stmt

    def test_no_scim_call_when_every_row_has_a_name(self) -> None:
        sp_ws = _make_sp_ws([])
        sql = MagicMock()
        fill_missing_owner_display_names([_Owned("a@example.com", "A"), _Owned(None)], sp_ws, sql, "t")
        sp_ws.users.list.assert_not_called()
        sql.execute.assert_not_called()

    def test_unresolved_owner_stays_raw(self) -> None:
        sp_ws = _make_sp_ws([])
        sql = MagicMock()
        row = _Owned("ghost@example.com")
        fill_missing_owner_display_names([row], sp_ws, sql, "t")
        assert row.owner_display_name is None
        sql.execute.assert_not_called()

    def test_write_back_failure_does_not_raise(self) -> None:
        sp_ws = _make_sp_ws([_make_user("tasha@example.com", "Tasha Yang")])
        sql = MagicMock()
        sql.execute.side_effect = RuntimeError("db down")
        row = _Owned("tasha@example.com")
        fill_missing_owner_display_names([row], sp_ws, sql, "t")
        assert row.owner_display_name == "Tasha Yang"


class TestEmailFilterFallback:
    def setup_method(self) -> None:
        owner_display_name_service._resolve_cache.clear()

    def test_retries_with_username_only_filter_when_emails_filter_rejected(self) -> None:
        ws = create_autospec(WorkspaceClient, instance=True)
        user = _make_user("tasha@example.com", "Tasha Yang")

        def _list(*, filter: str, **_kwargs):  # noqa: A002
            if "emails.value" in filter:
                raise BadRequest("Attribute emails.value is not supported")
            return iter([user])

        ws.users.list.side_effect = _list
        assert resolve_owners_cached(["tasha@example.com"], ws) == {"tasha@example.com": "Tasha Yang"}
        assert ws.users.list.call_count == 2
