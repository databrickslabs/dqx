"""Tests for SavedViewsService, its models and the v5 migration."""

from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS
from databricks_labs_dqx_app.backend.saved_views_models import (
    SavedViewCreateIn,
    SavedViewLayout,
    SavedViewPage,
    SavedViewPayload,
    SavedViewScope,
)
from databricks_labs_dqx_app.backend.services.saved_views_service import (
    SavedViewConflictError,
    SavedViewForbiddenError,
    SavedViewNotFoundError,
    SavedViewsService,
    SavedViewValidationError,
    normalize_view_name,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

PAGE = SavedViewPage.MONITORED_TABLES
BUILTIN = {
    "id": "builtin:monitored_tables",
    "page_key": "monitored_tables",
    "name": "Default",
    "scope": "org",
    "owner": None,
    "payload": None,
    "is_builtin": "true",
    "is_org_default": "true",
}


def view_row(view_id, *, scope="org", owner=None, name=None, flagged=False, payload='{"v":1}'):
    return {
        "id": view_id,
        "page_key": "monitored_tables",
        "name": name or view_id,
        "scope": scope,
        "owner": owner,
        "payload": payload,
        "is_builtin": "false",
        "is_org_default": "true" if flagged else "false",
    }


class Store:
    """Rows the fake executor returns, keyed by the SQL shape."""

    def __init__(self, views, stars=None, my_default=None, name_matches=False):
        self.views = views
        self.stars = stars or []
        self.my_default = my_default
        self.name_matches = name_matches

    def _org_name_taken(self, name, exclude_id=None):
        return any(
            r["scope"] == "org" and r["name"].lower() == name.lower() and r["id"] != exclude_id for r in self.views
        )

    def ensure_builtin(self, sql):
        # Mirrors Postgres: a bare ON CONFLICT DO NOTHING skips the insert on an id
        # clash or on a clash with uq_dq_saved_views_name (org view named "Default").
        if "INSERT INTO" in sql and "ON CONFLICT DO NOTHING" in sql:
            if any(r["id"] == BUILTIN["id"] for r in self.views) or self._org_name_taken(BUILTIN["name"]):
                return
            self.views.append(dict(BUILTIN, is_org_default="false"))

    def query_dicts(self, sql, **_kwargs):
        if "lower(name) = lower(" in sql:
            if self.name_matches:
                return [{"id": "dup"}]
            if "scope = 'org'" not in sql:
                return []
            name = sql.split("lower(name) = lower('")[1].split("')")[0]
            exclude = sql.split("AND id <> '")[1].split("'")[0] if "AND id <> '" in sql else None
            return [{"id": "dup"}] if self._org_name_taken(name, exclude) else []
        rows = self.views
        if "WHERE id = '" in sql:
            wanted = sql.split("WHERE id = '")[1].split("'")[0]
            rows = [r for r in rows if r["id"] == wanted]
        if "scope = 'org' OR owner = '" in sql:
            user = sql.split("owner = '")[1].split("'")[0]
            rows = [r for r in rows if r["scope"] == "org" or r["owner"] == user]
        elif "AND scope = 'org'" in sql:
            rows = [r for r in rows if r["scope"] == "org"]
        return rows

    def select_dicts(self, table, _columns, *, where=None, **_kwargs):
        if table.endswith("dq_saved_view_user_state"):
            return self.stars
        return [{"view_id": self.my_default}] if self.my_default else []


@pytest.fixture
def sql():
    mock = create_autospec(SqlExecutor, instance=True)
    mock.fqn.side_effect = lambda t: f"cat.sch.{t}"
    mock.select_json_text.side_effect = lambda c: c
    mock.json_literal_expr.side_effect = lambda j: f"'{j}'::jsonb"
    return mock


def make_service(sql, store):
    sql.query_dicts.side_effect = store.query_dicts
    sql.select_dicts.side_effect = store.select_dicts
    sql.execute.side_effect = store.ensure_builtin
    return SavedViewsService(sql=sql)


def payload():
    return SavedViewPayload()


# ---------------------------------------------------------------- names


def test_normalize_collapses_whitespace_and_strips():
    assert normalize_view_name("  Sales  q3 ") == "Sales q3"


def test_normalize_replaces_control_characters():
    assert normalize_view_name("a\nb") == "a b"


@pytest.mark.parametrize("raw", ["", "   ", "x" * 81])
def test_normalize_rejects_bad_length(raw):
    with pytest.raises(SavedViewValidationError):
        normalize_view_name(raw)


# -------------------------------------------------------------- payload


def test_payload_minimal_is_valid():
    assert SavedViewPayload().v == 1


def test_payload_over_16kb_rejected():
    values = {f"k{i}": "x" * 512 for i in range(40)}
    with pytest.raises(ValidationError):
        SavedViewPayload(values=values)


def test_layout_over_64_visibility_keys_rejected():
    with pytest.raises(ValidationError):
        SavedViewLayout(visibility={f"c{i}": True for i in range(65)})


def test_payload_list_over_200_items_rejected():
    with pytest.raises(ValidationError):
        SavedViewPayload(values={"catalog": ["a"] * 201})


def test_payload_long_string_rejected():
    with pytest.raises(ValidationError):
        SavedViewPayload(values={"search": "x" * 513})


# ----------------------------------------------------------------- list


def test_list_query_filters_to_org_or_own_views(sql):
    svc = make_service(sql, Store([BUILTIN]))
    svc.list_views(PAGE, user="o'brien@x", is_admin=False)
    stmt = sql.query_dicts.call_args_list[0][0][0]
    assert "scope = 'org' OR owner = 'o''brien@x'" in stmt


def test_list_hides_other_users_personal_views(sql):
    store = Store([BUILTIN, view_row("mine", scope="user", owner="a@x"), view_row("theirs", scope="user", owner="b@x")])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=True)
    assert {v.id for v in out.views} == {"builtin:monitored_tables", "mine"}


def test_star_defaults_to_is_builtin_without_state_row(sql):
    out = make_service(sql, Store([BUILTIN, view_row("v1")])).list_views(PAGE, user="a@x", is_admin=False)
    starred = {v.id: v.starred for v in out.views}
    assert starred == {"builtin:monitored_tables": True, "v1": False}


def test_star_state_row_overrides_default(sql):
    store = Store([BUILTIN, view_row("v1")], stars=[{"view_id": "v1", "starred": "true"}])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    assert {v.id: v.starred for v in out.views}["v1"] is True


def test_effective_default_is_my_default_when_visible(sql):
    store = Store([BUILTIN, view_row("v1", flagged=True), view_row("v2")], my_default="v2")
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    assert out.effective_default_id == "v2"
    assert [v.id for v in out.views if v.is_my_default] == ["v2"]


def test_effective_default_is_flagged_org_view_without_personal_default(sql):
    store = Store([dict(BUILTIN, is_org_default="false"), view_row("v1", flagged=True)])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    assert out.effective_default_id == "v1"
    assert [v.id for v in out.views if v.is_org_default] == ["v1"]


def test_effective_default_is_builtin_when_personal_default_missing_and_none_flagged(sql):
    store = Store([dict(BUILTIN, is_org_default="false"), view_row("v1")], my_default="gone")
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    assert out.effective_default_id == "builtin:monitored_tables"


def test_effective_default_is_builtin_when_two_org_views_flagged(sql):
    store = Store([dict(BUILTIN, is_org_default="false"), view_row("v1", flagged=True), view_row("v2", flagged=True)])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    assert out.effective_default_id == "builtin:monitored_tables"


def test_invalid_stored_payload_is_returned_as_none(sql):
    store = Store([BUILTIN, view_row("bad", payload='{"v": 2}')])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    assert {v.id: v.payload for v in out.views}["bad"] is None


def test_permission_flags(sql):
    store = Store([BUILTIN, view_row("org1"), view_row("mine", scope="user", owner="a@x")])
    user_view = {v.id: v for v in make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False).views}
    assert (user_view["org1"].can_edit, user_view["org1"].can_set_org_default) == (False, False)
    assert user_view["mine"].can_edit is True
    admin_view = {v.id: v for v in make_service(sql, store).list_views(PAGE, user="a@x", is_admin=True).views}
    assert (admin_view["org1"].can_edit, admin_view["org1"].can_set_org_default) == (True, True)
    assert admin_view["mine"].can_set_org_default is False


# ---------------------------------------------------------- permissions


def create_body(**kwargs):
    return SavedViewCreateIn(page=PAGE, name="Mine", payload=payload(), **kwargs)


def test_non_admin_cannot_create_org_view(sql):
    svc = make_service(sql, Store([BUILTIN]))
    with pytest.raises(SavedViewForbiddenError):
        svc.create_view(create_body(scope=SavedViewScope.ORG), user="a@x", is_admin=False)
    sql.insert.assert_not_called()


def test_non_admin_cannot_make_org_default(sql):
    svc = make_service(sql, Store([BUILTIN]))
    with pytest.raises(SavedViewForbiddenError):
        svc.create_view(create_body(make_org_default=True), user="a@x", is_admin=False)


def test_admin_make_org_default_requires_org_scope(sql):
    svc = make_service(sql, Store([BUILTIN]))
    with pytest.raises(SavedViewValidationError):
        svc.create_view(create_body(make_org_default=True), user="a@x", is_admin=True)


def test_admin_creates_org_view(sql):
    store = Store([BUILTIN])
    svc = make_service(sql, store)
    sql.insert.side_effect = lambda _t, *, values: store.views.append(view_row(values["id"], name=values["name"]))
    body = create_body(scope=SavedViewScope.ORG, make_org_default=True, make_my_default=True)
    out = svc.create_view(body, user="admin@x", is_admin=True)
    values = sql.insert.call_args.kwargs["values"]
    assert values["scope"] == "org" and values["owner"] is None
    assert values["payload"].expr.endswith("::jsonb")
    assert out.name == "Mine"
    updates = [c[0][0] for c in sql.execute.call_args_list if c[0][0].startswith("UPDATE")]
    assert len(updates) == 1 and "SET is_org_default" in updates[0]
    sql.upsert.assert_called_once()


def test_user_creates_personal_view_owned_by_caller(sql):
    store = Store([BUILTIN])
    svc = make_service(sql, store)
    sql.insert.side_effect = lambda _t, *, values: store.views.append(
        view_row(values["id"], scope="user", owner=values["owner"], name=values["name"])
    )
    out = svc.create_view(create_body(), user="a@x", is_admin=False)
    assert sql.insert.call_args.kwargs["values"]["owner"] == "a@x"
    assert out.scope == SavedViewScope.USER and out.can_edit is True


def test_non_admin_cannot_update_org_view(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("org1")]))
    with pytest.raises(SavedViewForbiddenError):
        svc.update_view("org1", payload(), user="a@x", is_admin=False)
    sql.update.assert_not_called()


def test_update_other_users_personal_view_is_not_found(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("theirs", scope="user", owner="b@x")]))
    with pytest.raises(SavedViewNotFoundError):
        svc.update_view("theirs", payload(), user="a@x", is_admin=True)


def test_owner_updates_own_view(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("mine", scope="user", owner="a@x")]))
    svc.update_view("mine", payload(), user="a@x", is_admin=False)
    assert sql.update.call_args.kwargs["where"] == {"id": "mine"}


def test_admin_update_builtin_is_validation_error(sql):
    svc = make_service(sql, Store([BUILTIN]))
    with pytest.raises(SavedViewValidationError):
        svc.update_view("builtin:monitored_tables", payload(), user="admin@x", is_admin=True)
    sql.update.assert_not_called()


@pytest.mark.parametrize("is_admin", [True, False])
def test_builtin_can_edit_is_false_for_everyone(sql, is_admin):
    svc = make_service(sql, Store([BUILTIN]))
    out = svc.list_views(PAGE, user="u@x", is_admin=is_admin)
    assert next(v for v in out.views if v.is_builtin).can_edit is False


def test_rename_builtin_is_validation_error(sql):
    svc = make_service(sql, Store([BUILTIN]))
    with pytest.raises(SavedViewValidationError):
        svc.rename_view("builtin:monitored_tables", "New", user="admin@x", is_admin=True)


def test_delete_builtin_is_validation_error(sql):
    svc = make_service(sql, Store([BUILTIN]))
    with pytest.raises(SavedViewValidationError):
        svc.delete_view("builtin:monitored_tables", user="admin@x", is_admin=True)
    sql.delete.assert_not_called()


def test_owner_deletes_own_view(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("mine", scope="user", owner="a@x")]))
    svc.delete_view("mine", user="a@x", is_admin=False)
    sql.delete.assert_called_once()


def test_rename_sanitises_name(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("mine", scope="user", owner="a@x")]))
    svc.rename_view("mine", "  New\n name ", user="a@x", is_admin=False)
    assert sql.update.call_args.kwargs["updates"]["name"] == "New name"


# ------------------------------------------------------------ conflicts


def test_create_duplicate_name_is_conflict(sql):
    svc = make_service(sql, Store([BUILTIN], name_matches=True))
    with pytest.raises(SavedViewConflictError):
        svc.create_view(create_body(), user="a@x", is_admin=False)
    sql.insert.assert_not_called()


def test_rename_duplicate_name_is_conflict(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("mine", scope="user", owner="a@x")], name_matches=True))
    with pytest.raises(SavedViewConflictError):
        svc.rename_view("mine", "Other", user="a@x", is_admin=False)


def test_unique_violation_on_insert_maps_to_conflict(sql):
    class UniqueViolation(Exception):
        sqlstate = "23505"

    svc = make_service(sql, Store([BUILTIN]))
    sql.insert.side_effect = UniqueViolation("duplicate key")
    with pytest.raises(SavedViewConflictError):
        svc.create_view(create_body(), user="a@x", is_admin=False)


# ------------------------------------------------------------- defaults


def test_set_org_default_on_personal_view_is_validation_error(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("mine", scope="user", owner="admin@x")]))
    with pytest.raises(SavedViewValidationError):
        svc.set_org_default(PAGE, "mine", user="admin@x", is_admin=True)
    sql.execute.assert_not_called()


def test_set_org_default_requires_admin(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("org1")]))
    with pytest.raises(SavedViewForbiddenError):
        svc.set_org_default(PAGE, "org1", user="a@x", is_admin=False)


def test_set_org_default_issues_single_update(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("org1")]))
    svc.set_org_default(PAGE, "org1", user="admin@x", is_admin=True)
    updates = [c[0][0] for c in sql.execute.call_args_list if c[0][0].startswith("UPDATE")]
    assert len(updates) == 1
    stmt = updates[0]
    assert "is_org_default = (id = 'org1')" in stmt
    assert "page_key = 'monitored_tables' AND scope = 'org'" in stmt


def test_set_my_default_upserts_and_none_deletes(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("org1")]))
    svc.set_my_default(PAGE, "org1", user="a@x", is_admin=False)
    assert sql.upsert.call_args.kwargs["value_cols"] == {"view_id": "org1"}
    svc.set_my_default(PAGE, None, user="a@x", is_admin=False)
    assert sql.delete.call_args.kwargs["where"] == {"user_email": "a@x", "page_key": "monitored_tables"}


def test_set_my_default_on_invisible_view_is_not_found(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("theirs", scope="user", owner="b@x")]))
    with pytest.raises(SavedViewNotFoundError):
        svc.set_my_default(PAGE, "theirs", user="a@x", is_admin=False)


def test_set_starred_upserts_state(sql):
    svc = make_service(sql, Store([BUILTIN, view_row("org1")]))
    svc.set_starred("org1", True, user="a@x", is_admin=False)
    assert sql.upsert.call_args.kwargs["value_cols"] == {"starred": True}


# ------------------------------------------------------------ migration


def test_migration_v5_creates_tables_and_seeds_every_page():
    migration = next(m for m in PG_MIGRATIONS if m.version == 5)
    flat = " ".join(migration.sql.split())
    for table in ("dq_saved_views", "dq_saved_view_user_state", "dq_saved_view_defaults"):
        assert f"CREATE TABLE IF NOT EXISTS {{schema}}.{table}" in flat
    assert "uq_dq_saved_views_name" in flat and "idx_dq_saved_views_page" in flat
    assert "UNIQUE INDEX" in flat and "is_org_default" in flat
    assert "(is_org_default)" not in flat
    for page in SavedViewPage:
        assert f"'builtin:{page.value}'" in flat
    assert "ON CONFLICT (id) DO NOTHING" in flat


# ------------------------------------------------------- builtin healing


def test_list_recreates_missing_builtin_after_reset(sql):
    store = Store([])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    stmt = sql.execute.call_args_list[0][0][0]
    assert stmt.startswith("INSERT INTO cat.sch.dq_saved_views")
    assert "'builtin:monitored_tables'" in stmt and "ON CONFLICT DO NOTHING" in stmt
    assert out.effective_default_id == "builtin:monitored_tables"
    assert [v.id for v in out.views] == ["builtin:monitored_tables"]


def test_set_org_default_on_builtin_heals_missing_row(sql):
    store = Store([])
    make_service(sql, store).set_org_default(PAGE, "builtin:monitored_tables", user="admin@x", is_admin=True)
    assert any("ON CONFLICT DO NOTHING" in c[0][0] for c in sql.execute.call_args_list)
    assert any("is_org_default = (id = 'builtin:monitored_tables')" in c[0][0] for c in sql.execute.call_args_list)


def test_create_after_reset_heals_builtin_before_name_check(sql):
    store = Store([])
    svc = make_service(sql, store)
    body = SavedViewCreateIn(page=PAGE, name="default", scope=SavedViewScope.ORG, payload=payload())
    with pytest.raises(SavedViewConflictError):
        svc.create_view(body, user="admin@x", is_admin=True)
    assert sql.execute.call_args_list[0][0][0].startswith("INSERT INTO cat.sch.dq_saved_views")
    sql.insert.assert_not_called()


def test_rename_after_reset_heals_builtin_before_name_check(sql):
    store = Store([view_row("v1", name="Sales")])
    svc = make_service(sql, store)
    with pytest.raises(SavedViewConflictError):
        svc.rename_view("v1", "Default", user="admin@x", is_admin=True)
    sql.update.assert_not_called()


def test_list_survives_org_view_named_default_with_builtin_missing(sql):
    store = Store([view_row("v1", name="Default")])
    out = make_service(sql, store).list_views(PAGE, user="a@x", is_admin=False)
    heal = sql.execute.call_args_list[0][0][0]
    assert heal.endswith("ON CONFLICT DO NOTHING")
    assert "ON CONFLICT (id)" not in heal
    assert [v.id for v in out.views] == ["v1"]
