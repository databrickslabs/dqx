"""Tests for the ``/rule-policies`` routes.

Follows ``test_dq_score_route.py``'s TestClient convention: a minimal FastAPI
app with only the router under test, every service replaced through
``app.dependency_overrides`` by a spec-bound ``create_autospec`` mock. The
services themselves are covered by ``test_rule_policy_service.py`` and
``test_policy_apply_service.py``; these tests pin the HTTP contract
(status codes, permission checks, atomic multi-rule create, best-effort runs).
"""

from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.dependencies import (
    get_apply_rules_service,
    get_current_principal_ids,
    get_permissions_service,
    get_policy_apply_service,
    get_registry_service,
    get_rule_policy_service,
    get_user_role,
)
from databricks_labs_dqx_app.backend.policy_models import ColumnCondition, PolicyScope, RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.policy_apply_service import (
    ActingUser,
    ApplyResult,
    PolicyApplyService,
    PolicyPreview,
    PreviewTable,
    RuleNotFoundError,
)
from databricks_labs_dqx_app.backend.services.policy_engine import PolicyMatch
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import (
    DuplicatePolicyNameError,
    RulePolicyNotFoundError,
    RulePolicyService,
)

BASE = "/api/v1/rule-policies"
USER = "alice@example.com"
PRINCIPALS = frozenset({"u1", "data-eng"})


def _rule(rule_id: str = "r1", slots: tuple[str, ...] = ("column",)) -> RegistryRule:
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {s: "{{" + s + "}}" for s in slots}},
            "slots": [{"name": s, "family": "any", "position": i, "cardinality": "one"} for i, s in enumerate(slots)],
            "parameters": [],
        }
    )
    return RegistryRule(rule_id=rule_id, mode="dqx_native", status="approved", version=1, definition=definition)


def _policy_body(name: str = "PII columns", slots: tuple[str, ...] = ("column",)) -> dict:
    return {
        "name": name,
        "scope": {"catalogs": ["main"], "schemas": "*", "tables": "*"},
        "table_condition": {"kind": "none"},
        "column_conditions": {s: {"kind": "tags", "tags": ["class=pii"]} for s in slots},
        "register_tables": False,
    }


def _policy(policy_id: str = "p1", rule_id: str = "r1", name: str = "PII columns") -> RulePolicy:
    body = RulePolicyIn(
        name=name,
        scope=PolicyScope(catalogs=["main"]),
        column_conditions={"column": ColumnCondition(kind="tags", tags=["class=pii"])},
    )
    return RulePolicy(**body.model_dump(), policy_id=policy_id, rule_id=rule_id, created_by=USER)


@pytest.fixture
def policies() -> MagicMock:
    mock = create_autospec(RulePolicyService, instance=True)
    mock.get.return_value = _policy()
    mock.list_for_rule.return_value = []
    created: list[str] = []

    def _create(rule_id: str, body: RulePolicyIn, user_email: str) -> RulePolicy:
        policy_id = f"p-{rule_id}"
        created.append(policy_id)
        return RulePolicy(**body.model_dump(), policy_id=policy_id, rule_id=rule_id, created_by=user_email)

    mock.create.side_effect = _create
    mock.update.side_effect = lambda policy_id, body, user_email: RulePolicy(
        **body.model_dump(), policy_id=policy_id, rule_id="r1", updated_by=user_email
    )
    return mock


@pytest.fixture
def apply() -> MagicMock:
    mock = create_autospec(PolicyApplyService, instance=True)
    mock.run_policy.return_value = ApplyResult(attached=3, registered=1, ambiguous=[], registration_capped=True)
    return mock


@pytest.fixture
def apply_rules() -> MagicMock:
    mock = create_autospec(ApplyRulesService, instance=True)
    mock.count_bindings_by_policy.side_effect = lambda ids: {i: 2 for i in ids}
    return mock


@pytest.fixture
def registry() -> MagicMock:
    mock = create_autospec(RegistryService, instance=True)
    mock.get_rule.side_effect = lambda rule_id: _rule(rule_id)
    return mock


@pytest.fixture
def perms() -> MagicMock:
    return create_autospec(PermissionsService, instance=True)


@pytest.fixture
def role() -> list[UserRole]:
    return [UserRole.RULE_AUTHOR]


@pytest.fixture
def client(policies, apply, apply_rules, registry, perms, role) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.rule_policies import router

    app = FastAPI()
    app.include_router(router, prefix=BASE)
    app.dependency_overrides[get_rule_policy_service] = lambda: policies
    app.dependency_overrides[get_policy_apply_service] = lambda: apply
    app.dependency_overrides[get_apply_rules_service] = lambda: apply_rules
    app.dependency_overrides[get_registry_service] = lambda: registry
    app.dependency_overrides[get_permissions_service] = lambda: perms
    app.dependency_overrides[get_user_role] = lambda: role[0]
    app.dependency_overrides[get_user_email] = lambda: USER
    app.dependency_overrides[get_current_principal_ids] = lambda: PRINCIPALS
    return TestClient(app)


def _deny_modify_on(rule_id: str):
    def _require(object_type, object_id, privilege, **_kwargs):
        if object_id == rule_id and privilege == Privilege.MODIFY:
            raise HTTPException(status_code=403, detail="Forbidden")

    return _require


def _acting_user() -> ActingUser:
    return ActingUser(email=USER, role=UserRole.RULE_AUTHOR, principal_ids=PRINCIPALS)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


class TestRead:
    def test_list_for_rule_checks_select_and_counts_tables(self, client, policies, perms, apply_rules):
        policies.list_for_rule.return_value = [_policy("p1"), _policy("p2", name="Other")]

        response = client.get(BASE, params={"rule_id": "r1"})

        assert response.status_code == 200
        body = response.json()
        assert [p["policy_id"] for p in body] == ["p1", "p2"]
        assert all(p["tables_matched"] == 2 for p in body)
        perms.require_object.assert_called_once_with(
            ObjectType.REGISTRY_RULE.value,
            "r1",
            Privilege.SELECT,
            role=UserRole.RULE_AUTHOR,
            principal_ids=set(PRINCIPALS),
            principal_email=USER,
        )
        apply_rules.count_bindings_by_policy.assert_called_once_with(["p1", "p2"])

    def test_list_requires_rule_id(self, client):
        assert client.get(BASE).status_code == 422

    def test_list_forbidden_without_select(self, client, perms, policies):
        perms.require_object.side_effect = HTTPException(status_code=403, detail="Forbidden")
        assert client.get(BASE, params={"rule_id": "r1"}).status_code == 403
        policies.list_for_rule.assert_not_called()

    def test_viewer_can_read(self, client, role):
        role[0] = UserRole.VIEWER
        assert client.get(BASE, params={"rule_id": "r1"}).status_code == 200

    def test_get_returns_policy_with_count(self, client, perms):
        response = client.get(f"{BASE}/p1")

        assert response.status_code == 200
        assert response.json()["policy_id"] == "p1"
        assert response.json()["tables_matched"] == 2
        assert perms.require_object.call_args.args[1:3] == ("r1", Privilege.SELECT)

    def test_get_missing_is_404(self, client, policies):
        policies.get.return_value = None
        assert client.get(f"{BASE}/nope").status_code == 404

    def test_list_failure_is_generic_500(self, client, policies):
        policies.list_for_rule.side_effect = RuntimeError("secret connection string")

        response = client.get(BASE, params={"rule_id": "r1"})

        assert response.status_code == 500
        assert "secret" not in response.text


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


class TestCreate:
    def test_creates_one_policy_per_rule_and_runs_each(self, client, policies, apply, perms):
        response = client.post(BASE, json={"rule_ids": ["r1", "r2"], "policy": _policy_body()})

        assert response.status_code == 200
        body = response.json()
        assert [p["rule_id"] for p in body] == ["r1", "r2"]
        assert [p["tables_matched"] for p in body] == [2, 2]
        assert policies.create.call_count == 2
        modify_checks = [c.args[1] for c in perms.require_object.call_args_list if c.args[2] == Privilege.MODIFY]
        assert modify_checks == ["r1", "r2"]
        ran = [c.args for c in apply.run_policy.call_args_list]
        assert ran == [("p-r1", _acting_user()), ("p-r2", _acting_user())]

    def test_per_rule_column_conditions_override_policy(self, client, policies, registry):
        registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, ("a", "b") if rule_id == "r2" else ("column",))
        response = client.post(
            BASE,
            json={
                "rule_ids": ["r1", "r2"],
                "policy": _policy_body(),
                "column_conditions_by_rule": {
                    "r2": {
                        "a": {"kind": "tags", "tags": ["x"]},
                        "b": {"kind": "tags", "tags": ["y"]},
                    }
                },
            },
        )

        assert response.status_code == 200
        r2_body = policies.create.call_args_list[1].args[1]
        assert set(r2_body.column_conditions) == {"a", "b"}
        r1_body = policies.create.call_args_list[0].args[1]
        assert set(r1_body.column_conditions) == {"column"}

    def test_forbidden_on_one_rule_creates_nothing(self, client, perms, policies, apply):
        perms.require_object.side_effect = _deny_modify_on("r2")

        response = client.post(BASE, json={"rule_ids": ["r1", "r2"], "policy": _policy_body()})

        assert response.status_code == 403
        policies.create.assert_not_called()
        apply.run_policy.assert_not_called()

    def test_viewer_cannot_create(self, client, role, policies):
        role[0] = UserRole.VIEWER
        assert client.post(BASE, json={"rule_ids": ["r1"], "policy": _policy_body()}).status_code == 403
        policies.create.assert_not_called()

    def test_conditions_for_unknown_slot_is_422(self, client, policies):
        body = _policy_body()
        body["column_conditions"] = {"nope": {"kind": "tags", "tags": ["x"]}}

        response = client.post(BASE, json={"rule_ids": ["r1"], "policy": body})

        assert response.status_code == 422
        policies.create.assert_not_called()

    def test_missing_slot_condition_is_422(self, client, policies, registry):
        registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, ("a", "b"))
        body = _policy_body(slots=("a",))

        response = client.post(BASE, json={"rule_ids": ["r1"], "policy": body})

        assert response.status_code == 422
        policies.create.assert_not_called()

    def test_conditions_for_rule_not_in_request_is_422(self, client, policies):
        response = client.post(
            BASE,
            json={
                "rule_ids": ["r1"],
                "policy": _policy_body(),
                "column_conditions_by_rule": {"r9": {"column": {"kind": "tags", "tags": ["x"]}}},
            },
        )
        assert response.status_code == 422
        policies.create.assert_not_called()

    def test_invalid_condition_body_is_422(self, client, policies):
        body = _policy_body()
        body["column_conditions"] = {"column": {"kind": "tags", "tags": []}}
        assert client.post(BASE, json={"rule_ids": ["r1"], "policy": body}).status_code == 422
        policies.create.assert_not_called()

    def test_too_many_rules_is_422(self, client):
        rule_ids = [f"r{i}" for i in range(51)]
        assert client.post(BASE, json={"rule_ids": rule_ids, "policy": _policy_body()}).status_code == 422

    def test_unknown_rule_is_404_and_creates_nothing(self, client, registry, policies):
        registry.get_rule.side_effect = lambda rule_id: None if rule_id == "r2" else _rule(rule_id)

        response = client.post(BASE, json={"rule_ids": ["r1", "r2"], "policy": _policy_body()})

        assert response.status_code == 404
        policies.create.assert_not_called()

    def test_duplicate_name_is_409_and_rolls_back(self, client, policies, apply):
        def _create(rule_id, body, user_email):
            if rule_id == "r2":
                raise DuplicatePolicyNameError("This rule already has a policy named 'PII columns'.")
            return RulePolicy(**body.model_dump(), policy_id=f"p-{rule_id}", rule_id=rule_id)

        policies.create.side_effect = _create

        response = client.post(BASE, json={"rule_ids": ["r1", "r2"], "policy": _policy_body()})

        assert response.status_code == 409
        assert "PII columns" not in response.json()["detail"]
        policies.delete.assert_called_once_with("p-r1")
        apply.run_policy.assert_not_called()

    def test_unexpected_create_failure_is_generic_500_and_rolls_back(self, client, policies):
        def _create(rule_id, body, user_email):
            if rule_id == "r2":
                raise RuntimeError("db password leaked")
            return RulePolicy(**body.model_dump(), policy_id=f"p-{rule_id}", rule_id=rule_id)

        policies.create.side_effect = _create

        response = client.post(BASE, json={"rule_ids": ["r1", "r2"], "policy": _policy_body()})

        assert response.status_code == 500
        assert "password" not in response.text
        policies.delete.assert_called_once_with("p-r1")

    def test_run_failure_never_fails_the_save(self, client, apply):
        apply.run_policy.side_effect = RuntimeError("boom")

        response = client.post(BASE, json={"rule_ids": ["r1"], "policy": _policy_body()})

        assert response.status_code == 200
        assert response.json()[0]["policy_id"] == "p-r1"


# ---------------------------------------------------------------------------
# Update / delete
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_update_checks_modify_saves_and_runs(self, client, policies, apply, perms):
        response = client.put(f"{BASE}/p1", json=_policy_body(name="Renamed"))

        assert response.status_code == 200
        assert response.json()["name"] == "Renamed"
        assert response.json()["tables_matched"] == 2
        assert perms.require_object.call_args.args[1:3] == ("r1", Privilege.MODIFY)
        policies.update.assert_called_once()
        apply.run_policy.assert_called_once_with("p1", _acting_user())

    def test_update_missing_is_404(self, client, policies):
        policies.get.return_value = None
        assert client.put(f"{BASE}/p1", json=_policy_body()).status_code == 404
        policies.update.assert_not_called()

    def test_update_forbidden(self, client, perms, policies):
        perms.require_object.side_effect = _deny_modify_on("r1")
        assert client.put(f"{BASE}/p1", json=_policy_body()).status_code == 403
        policies.update.assert_not_called()

    def test_update_duplicate_name_is_409(self, client, policies, apply):
        policies.update.side_effect = DuplicatePolicyNameError("This rule already has a policy named 'X'.")
        response = client.put(f"{BASE}/p1", json=_policy_body(name="X"))
        assert response.status_code == 409
        assert "'X'" not in response.json()["detail"]
        apply.run_policy.assert_not_called()

    def test_update_slot_mismatch_is_422(self, client, policies):
        assert client.put(f"{BASE}/p1", json=_policy_body(slots=("other",))).status_code == 422
        policies.update.assert_not_called()

    def test_update_run_failure_never_fails_the_save(self, client, apply):
        apply.run_policy.side_effect = RuntimeError("boom")
        assert client.put(f"{BASE}/p1", json=_policy_body()).status_code == 200


class TestDelete:
    def test_delete_checks_modify_and_returns_204(self, client, policies, perms):
        response = client.delete(f"{BASE}/p1")

        assert response.status_code == 204
        policies.delete.assert_called_once_with("p1")
        assert perms.require_object.call_args.args[1:3] == ("r1", Privilege.MODIFY)

    def test_delete_missing_is_404(self, client, policies):
        policies.get.return_value = None
        assert client.delete(f"{BASE}/p1").status_code == 404

    def test_delete_race_is_404(self, client, policies):
        policies.delete.side_effect = RulePolicyNotFoundError("gone")
        assert client.delete(f"{BASE}/p1").status_code == 404

    def test_delete_forbidden(self, client, perms, policies):
        perms.require_object.side_effect = _deny_modify_on("r1")
        assert client.delete(f"{BASE}/p1").status_code == 403
        policies.delete.assert_not_called()


# ---------------------------------------------------------------------------
# Preview / rescan
# ---------------------------------------------------------------------------


class TestPreview:
    def test_preview_maps_tables_and_mappings(self, client, apply, perms):
        match = PolicyMatch(
            policy_id="",
            rule_id="r1",
            table_fqn="main.s.t",
            column_mapping={"column": "email"},
            status="auto",
            reason="tag class=pii",
        )
        apply.preview.return_value = PolicyPreview(
            tables=[PreviewTable("main.s.t", monitored=False, matches=[match])],
            tables_matched=1,
            columns_matched=1,
            new_tables=1,
            truncated=False,
            unreadable_catalogs=["secret_cat"],
        )

        response = client.post(f"{BASE}/preview", json={"rule_id": "r1", "policy": _policy_body()})

        assert response.status_code == 200
        body = response.json()
        assert body["tables_matched"] == 1
        assert body["columns_matched"] == 1
        assert body["new_tables"] == 1
        assert body["truncated"] is False
        assert body["unreadable_catalogs"] == ["secret_cat"]
        assert body["tables"] == [
            {
                "table_fqn": "main.s.t",
                "monitored": False,
                "mappings": [{"column_mapping": {"column": "email"}, "reason": "tag class=pii"}],
            }
        ]
        assert perms.require_object.call_args.args[1:3] == ("r1", Privilege.SELECT)
        assert apply.preview.call_args.args[0] == "r1"
        assert apply.preview.call_args.args[2] == _acting_user()

    def test_preview_unknown_rule_is_404(self, client, apply):
        apply.preview.side_effect = RuleNotFoundError("Registry rule 'r1' not found.")
        assert client.post(f"{BASE}/preview", json={"rule_id": "r1", "policy": _policy_body()}).status_code == 404

    def test_preview_failure_is_generic_500(self, client, apply):
        apply.preview.side_effect = RuntimeError("warehouse token abc")
        response = client.post(f"{BASE}/preview", json={"rule_id": "r1", "policy": _policy_body()})
        assert response.status_code == 500
        assert "token" not in response.text

    def test_preview_forbidden_without_select(self, client, perms, apply):
        perms.require_object.side_effect = HTTPException(status_code=403, detail="Forbidden")
        assert client.post(f"{BASE}/preview", json={"rule_id": "r1", "policy": _policy_body()}).status_code == 403
        apply.preview.assert_not_called()


class TestRescan:
    def test_rescan_returns_counts(self, client, apply, perms):
        response = client.post(f"{BASE}/p1/rescan")

        assert response.status_code == 200
        assert response.json() == {"attached": 3, "registered": 1, "registration_capped": True}
        assert perms.require_object.call_args.args[1:3] == ("r1", Privilege.MODIFY)
        apply.run_policy.assert_called_once_with("p1", _acting_user())

    def test_rescan_missing_is_404(self, client, policies):
        policies.get.return_value = None
        assert client.post(f"{BASE}/p1/rescan").status_code == 404

    def test_rescan_forbidden(self, client, perms, apply):
        perms.require_object.side_effect = _deny_modify_on("r1")
        assert client.post(f"{BASE}/p1/rescan").status_code == 403
        apply.run_policy.assert_not_called()

    def test_viewer_cannot_rescan(self, client, role, apply):
        role[0] = UserRole.VIEWER
        assert client.post(f"{BASE}/p1/rescan").status_code == 403
        apply.run_policy.assert_not_called()
