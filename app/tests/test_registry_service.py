"""Tests for ``RegistryService`` — the Rules Registry CRUD + approval gate.

Follows the same testing shape as ``test_rules_catalog_service.py``: a
spec-bound ``create_autospec(SqlExecutor)`` mock with dialect-helper
side effects wired to their real Delta-flavoured behaviour, so assertions
read real SQL/JSON rather than MagicMock reprs.
"""

from __future__ import annotations

import json

import pytest

from databricks_labs_dqx_app.backend.registry_models import RuleDefinition
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


@pytest.fixture
def sql(sql_executor_mock):
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    return sql_executor_mock


@pytest.fixture
def svc(sql):
    return RegistryService(sql=sql)


def _native_definition(column: str = "id") -> RuleDefinition:
    return RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            "slots": [{"name": "column", "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )


def _row_for(rule, *, status: str | None = None, version: int | None = None) -> list[str]:
    """Build a SELECT row matching ``RegistryService._build_select_cols`` order."""
    return [
        rule.rule_id,
        rule.mode,
        status if status is not None else rule.status,
        str(version if version is not None else rule.version),
        rule.polarity,
        rule.author_kind,
        json.dumps(rule.definition.model_dump(mode="json")),
        json.dumps(rule.user_metadata),
        rule.fingerprint,
        rule.steward,
        "true" if rule.is_builtin else "false",
        rule.source,
        rule.created_by,
        "2026-07-02T00:00:00+00:00",
        rule.updated_by,
        "2026-07-02T00:00:00+00:00",
    ]


# ---------------------------------------------------------------------------
# create_rule
# ---------------------------------------------------------------------------


class TestCreateRule:
    def test_creates_draft_rule_with_fingerprint(self, svc, sql):
        rule, warning = svc.create_rule(
            mode="dqx_native",
            definition=_native_definition(),
            user_email="alice@x",
        )
        assert rule.status == "draft"
        assert rule.version == 0
        assert rule.fingerprint
        assert warning is None
        inserted_sql = sql.execute.call_args_list[0].args[0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_rules" in inserted_sql

    def test_records_history_on_create(self, svc, sql):
        svc.create_rule(mode="dqx_native", definition=_native_definition(), user_email="alice@x")
        # execute called twice: once for dq_rules insert, once for history
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("dq_rules_history" in c for c in calls)

    def test_dedup_warning_when_published_rule_shares_fingerprint(self, svc, sql):
        existing_definition = _native_definition()
        published = None

        def fake_query(query_sql):
            nonlocal published
            if "fingerprint" in query_sql and "approved" in query_sql:
                assert published is not None
                return [_row_for(published)]
            return []

        # First create a rule to compute its real fingerprint shape, then
        # simulate that an *existing* published rule shares it.
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule
        from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint

        published = RegistryRule(
            rule_id="existing1",
            mode="dqx_native",
            status="approved",
            version=1,
            definition=existing_definition,
            user_metadata={"name": "Existing Rule"},
        )
        published.fingerprint = compute_registry_rule_fingerprint(published)

        sql.query.side_effect = fake_query
        rule, warning = svc.create_rule(mode="dqx_native", definition=existing_definition, user_email="bob@x")
        assert warning is not None
        assert "existing1" in warning
        # Creation still succeeds despite the warning.
        assert rule.status == "draft"


# ---------------------------------------------------------------------------
# update_draft
# ---------------------------------------------------------------------------


class TestUpdateDraft:
    def test_updates_draft_rule(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="r1", mode="dqx_native", status="draft", version=0, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(draft)]
        updated = svc.update_draft(
            "r1", user_email="alice@x", user_metadata={"name": "Renamed"}
        )
        assert updated.user_metadata["name"] == "Renamed"

    def test_rejects_editing_non_draft_rule(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        approved = RegistryRule(
            rule_id="r1", mode="dqx_native", status="approved", version=1, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(approved)]
        with pytest.raises(ValueError, match="only draft rules"):
            svc.update_draft("r1", user_email="alice@x", user_metadata={"name": "x"})

    def test_missing_rule_raises(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError, match="not found"):
            svc.update_draft("missing", user_email="alice@x")


# ---------------------------------------------------------------------------
# Lifecycle transitions
# ---------------------------------------------------------------------------


class TestLifecycle:
    def _rule(self, status: str, version: int = 0):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        return RegistryRule(rule_id="r1", mode="dqx_native", status=status, version=version, definition=_native_definition())

    def test_submit_draft_to_pending(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("draft"))]
        updated = svc.submit("r1", "alice@x")
        assert updated.status == "pending_approval"

    def test_submit_rejects_invalid_transition(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("approved", version=1))]
        with pytest.raises(ValueError, match="Cannot transition"):
            svc.submit("r1", "alice@x")

    def test_approve_bumps_version_and_writes_snapshot(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("pending_approval"))]
        updated = svc.approve("r1", "approver@x")
        assert updated.status == "approved"
        assert updated.version == 1
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("dq_rule_versions" in c for c in calls)

    def test_reapprove_after_second_submit_bumps_to_v2(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("pending_approval", version=1))]
        updated = svc.approve("r1", "approver@x")
        assert updated.version == 2

    def test_reject_pending(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("pending_approval"))]
        updated = svc.reject("r1", "approver@x")
        assert updated.status == "rejected"

    def test_deprecate_approved(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("approved", version=1))]
        updated = svc.deprecate("r1", "approver@x")
        assert updated.status == "deprecated"

    def test_undeprecate_does_not_bump_version(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("deprecated", version=2))]
        updated = svc.undeprecate("r1", "approver@x")
        assert updated.status == "approved"
        assert updated.version == 2

    def test_cannot_deprecate_draft(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("draft"))]
        with pytest.raises(ValueError):
            svc.deprecate("r1", "approver@x")

    def test_transition_missing_rule_raises(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError, match="not found"):
            svc.submit("missing", "alice@x")


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_removes_row_and_records_history(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        rule = RegistryRule(rule_id="r1", mode="dqx_native", status="draft", version=0, definition=_native_definition())
        sql.query.return_value = [_row_for(rule)]
        svc.delete("r1", "alice@x")
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any(c.startswith("DELETE FROM") and "dq_rules" in c and "dq_rules_history" not in c for c in calls)
        assert any("dq_rules_history" in c for c in calls)

    def test_delete_missing_rule_raises(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError, match="not found"):
            svc.delete("missing", "alice@x")


# ---------------------------------------------------------------------------
# list_rules / get_rule
# ---------------------------------------------------------------------------


class TestListAndGet:
    def test_list_filters_by_status_in_sql(self, svc, sql):
        sql.query.return_value = []
        svc.list_rules(status="approved")
        called_sql = sql.query.call_args[0][0]
        assert "status = 'approved'" in called_sql

    def test_list_filters_by_dimension_in_python(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        a = RegistryRule(
            rule_id="a", mode="dqx_native", status="approved", version=1,
            definition=_native_definition(), user_metadata={"dimension": "Validity"},
        )
        b = RegistryRule(
            rule_id="b", mode="dqx_native", status="approved", version=1,
            definition=_native_definition(), user_metadata={"dimension": "Completeness"},
        )
        sql.query.return_value = [_row_for(a), _row_for(b)]
        result = svc.list_rules(dimension="Validity")
        assert [r.rule_id for r in result] == ["a"]

    def test_get_rule_returns_none_when_missing(self, svc, sql):
        sql.query.return_value = []
        assert svc.get_rule("missing") is None

    def test_get_rule_with_version_none_for_unpublished(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(rule_id="r1", mode="dqx_native", status="draft", version=0, definition=_native_definition())
        sql.query.return_value = [_row_for(draft)]
        result = svc.get_rule_with_version("r1")
        assert result is not None
        rule, version = result
        assert version is None

    def test_get_rule_by_fingerprint_found(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule
        from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint

        rule = RegistryRule(rule_id="r1", mode="dqx_native", status="approved", version=1, definition=_native_definition())
        rule.fingerprint = compute_registry_rule_fingerprint(rule)
        sql.query.return_value = [_row_for(rule)]
        found = svc.get_rule_by_fingerprint(rule.fingerprint)
        assert found is not None
        assert found.rule_id == "r1"
        called_sql = sql.query.call_args[0][0]
        assert "fingerprint = " in called_sql

    def test_get_rule_by_fingerprint_not_found(self, svc, sql):
        sql.query.return_value = []
        assert svc.get_rule_by_fingerprint("deadbeef") is None


# ---------------------------------------------------------------------------
# seed_builtin_rule (Phase 2C)
# ---------------------------------------------------------------------------


class TestSeedBuiltinRule:
    def test_creates_approved_v1_rule_with_snapshot(self, svc, sql):
        rule = svc.seed_builtin_rule(
            definition=_native_definition(),
            user_metadata={"name": "Is not null", "dimension": "Completeness", "severity": "Medium"},
        )
        assert rule.status == "approved"
        assert rule.version == 1
        assert rule.is_builtin is True
        assert rule.source == "builtin"
        assert rule.fingerprint
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("INSERT INTO dqx_test.dqx_app_test.dq_rules " in c for c in calls)
        assert any("dq_rule_versions" in c for c in calls)
        assert any("dq_rules_history" in c for c in calls)

    def test_uses_provided_steward_and_user_email(self, svc, sql):
        rule = svc.seed_builtin_rule(
            definition=_native_definition(),
            user_metadata={},
            user_email="system",
            steward="system",
        )
        assert rule.created_by == "system"
        assert rule.steward == "system"
