"""Tests for the ``/registry-rules`` route handlers.

Follows ``test_rules_routes.py``'s convention: call the route functions
directly with mocked dependencies (RegistryService, OBO WorkspaceClient)
rather than spinning up a FastAPI TestClient — the routes themselves are
thin adapters over ``RegistryService``, whose behaviour is already covered
by ``test_registry_service.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.models import CreateRegistryRuleIn, UpdateRegistryRuleIn
from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.routes.v1.registry_rules import (
    approve_registry_rule,
    create_registry_rule,
    delete_registry_rule,
    deprecate_registry_rule,
    get_registry_rule,
    list_registry_rules,
    reject_registry_rule,
    submit_registry_rule,
    undeprecate_registry_rule,
    update_registry_rule,
)


def _definition() -> RuleDefinition:
    return RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            "slots": [{"name": "column", "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )


def _rule(rule_id: str = "r1", status: str = "draft", version: int = 0) -> RegistryRule:
    return RegistryRule(rule_id=rule_id, mode="dqx_native", status=status, version=version, definition=_definition())


def _mock_obo_ws(user_email: str = "alice@x") -> MagicMock:
    obo = MagicMock()
    me = MagicMock()
    me.user_name = user_email
    obo.current_user.me.return_value = me
    return obo


class TestListAndGet:
    def test_list_maps_domain_rules_to_dto(self):
        svc = MagicMock()
        svc.list_rules.return_value = [_rule()]
        result = list_registry_rules(svc=svc, status="draft")
        assert len(result) == 1
        assert result[0].rule_id == "r1"
        svc.list_rules.assert_called_once_with(status="draft", dimension=None, severity=None, steward=None, tag=None)

    def test_get_returns_detail_with_no_version_when_unpublished(self):
        svc = MagicMock()
        svc.get_rule_with_version.return_value = (_rule(), None)
        result = get_registry_rule("r1", svc=svc)
        assert result.rule.rule_id == "r1"
        assert result.current_version is None

    def test_get_missing_rule_raises_404(self):
        svc = MagicMock()
        svc.get_rule_with_version.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            get_registry_rule("missing", svc=svc)
        assert excinfo.value.status_code == 404


class TestCreateAndUpdate:
    def test_create_returns_rule_and_warning(self):
        svc = MagicMock()
        svc.create_rule.return_value = (_rule(), "possible duplicate")
        body = CreateRegistryRuleIn(mode="dqx_native", definition=_definition())
        result = create_registry_rule(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert result.rule.rule_id == "r1"
        assert result.dedup_warning == "possible duplicate"

    def test_update_rejects_non_draft_with_400(self):
        svc = MagicMock()
        svc.update_draft.side_effect = ValueError("only draft rules can be edited")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"})
        with pytest.raises(HTTPException) as excinfo:
            update_registry_rule("r1", body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 400

    def test_update_missing_rule_raises_404(self):
        svc = MagicMock()
        svc.update_draft.side_effect = RuntimeError("Registry rule not found: r1")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"})
        with pytest.raises(HTTPException) as excinfo:
            update_registry_rule("r1", body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404


class TestDelete:
    def test_delete_success(self):
        svc = MagicMock()
        result = delete_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert result == {"status": "deleted", "rule_id": "r1"}
        svc.delete.assert_called_once_with("r1", "alice@x")

    def test_delete_missing_rule_raises_404(self):
        svc = MagicMock()
        svc.delete.side_effect = RuntimeError("Registry rule not found: r1")
        with pytest.raises(HTTPException) as excinfo:
            delete_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404


class TestLifecycleRoutes:
    def test_submit_success(self):
        svc = MagicMock()
        svc.submit.return_value = _rule(status="pending_approval")
        result = submit_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "pending_approval"

    def test_submit_invalid_transition_raises_400(self):
        svc = MagicMock()
        svc.submit.side_effect = ValueError("Cannot transition from 'approved' to 'pending_approval'")
        with pytest.raises(HTTPException) as excinfo:
            submit_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 400

    def test_approve_success(self):
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=1)
        result = approve_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "approved"
        assert result.version == 1

    def test_reject_success(self):
        svc = MagicMock()
        svc.reject.return_value = _rule(status="rejected")
        result = reject_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "rejected"

    def test_deprecate_success(self):
        svc = MagicMock()
        svc.deprecate.return_value = _rule(status="deprecated", version=1)
        result = deprecate_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "deprecated"

    def test_undeprecate_success(self):
        svc = MagicMock()
        svc.undeprecate.return_value = _rule(status="approved", version=1)
        result = undeprecate_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "approved"

    def test_approve_missing_rule_raises_404(self):
        svc = MagicMock()
        svc.approve.side_effect = RuntimeError("Registry rule not found: r1")
        with pytest.raises(HTTPException) as excinfo:
            approve_registry_rule("r1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404
