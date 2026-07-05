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

    def test_error_message_persists_through_create(self, svc, sql):
        """Phase 7C-a: optional custom failure message threads through create
        as part of the ``definition`` jsonb blob (no separate column)."""
        definition = _native_definition()
        definition.error_message = "Column {{column}} must not be null"
        rule, _ = svc.create_rule(mode="dqx_native", definition=definition, user_email="alice@x")
        assert rule.definition.error_message == "Column {{column}} must not be null"
        inserted_sql = sql.execute.call_args_list[0].args[0]
        start = inserted_sql.index("parse_json('") + len("parse_json('")
        end = inserted_sql.index("')", start)
        stored_definition = json.loads(inserted_sql[start:end])
        assert stored_definition["error_message"] == "Column {{column}} must not be null"

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

    def test_clones_non_draft_rule_into_editable_draft(self, svc, sql):
        """Regression test for the "can't edit an existing rule" bug: an
        approved (or pending/rejected/deprecated) rule can't be edited
        in-place via ``update_draft`` (see
        ``TestUpdateDraft.test_rejects_editing_non_draft_rule``), so the
        Studio UI's "Edit as new draft" action instead clones the rule's
        mode/definition/user_metadata/steward/author_kind through
        ``create_rule`` — exactly like authoring a brand-new rule. This
        must always succeed (dedup warning aside) and produce an
        independent, freshly-editable draft rather than mutating the
        frozen original."""
        approved_definition = _native_definition()
        approved_metadata = {"name": "Is not null", "dimension": "Completeness", "severity": "Medium"}

        clone, warning = svc.create_rule(
            mode="dqx_native",
            definition=approved_definition,
            user_email="alice@x",
            polarity=None,
            author_kind="human",
            user_metadata=approved_metadata,
            steward="system",
        )

        # Creation always succeeds — a dedup warning may fire (the clone
        # shares its fingerprint with the rule it was cloned from) but must
        # never block it, and it must never touch the frozen original.
        assert clone.status == "draft"
        assert clone.version == 0
        assert clone.definition == approved_definition
        assert clone.user_metadata == approved_metadata
        assert clone.steward == "system"

    def test_rejects_unsafe_sql_predicate(self, svc, sql):
        """The 'save as new draft' clone path for editing a non-draft rule
        goes through ``create_rule``, not ``update_draft`` — so the same
        SQL-safety gate must apply here too, or unsafe SQL could be
        persisted via that path. Mirrors
        ``TestUpdateDraft.test_rejects_unsafe_sql_predicate``."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        unsafe_definition = RuleDefinition.model_validate(
            {"body": {"predicate": "1=1; DROP TABLE users"}, "slots": [], "parameters": []}
        )
        with pytest.raises(UnsafeSqlQueryError):
            svc.create_rule(mode="sql", definition=unsafe_definition, user_email="alice@x")

    def test_rejects_unsafe_sql_query_in_native_mode(self, svc, sql):
        """Mirrors ``TestUpdateDraft.test_rejects_unsafe_sql_query_in_native_mode``:
        a ``dqx_native`` check routed through ``sql_query`` must have its
        ``query`` argument validated on create too."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        unsafe_definition = RuleDefinition.model_validate(
            {
                "body": {"function": "sql_query", "arguments": {"query": "DROP TABLE users", "negate": False}},
                "slots": [],
                "parameters": [],
            }
        )
        with pytest.raises(UnsafeSqlQueryError):
            svc.create_rule(mode="dqx_native", definition=unsafe_definition, user_email="alice@x")

    def test_accepts_safe_sql_predicate(self, svc, sql):
        safe_definition = RuleDefinition.model_validate(
            {"body": {"predicate": "{{column}} IS NOT NULL"}, "slots": [], "parameters": []}
        )
        rule, _ = svc.create_rule(mode="sql", definition=safe_definition, user_email="alice@x")
        assert rule.definition.body["predicate"] == "{{column}} IS NOT NULL"


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

    def test_author_kind_is_updated_and_persisted(self, svc, sql):
        """AI provenance stamped during an edit-in-place session (e.g. a
        human accepts an AI-suggested field on an otherwise human-authored
        draft) must persist on update — both on the returned domain object
        and in the ``UPDATE`` SQL sent to the store."""
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="r1",
            mode="dqx_native",
            status="draft",
            version=0,
            author_kind="human",
            definition=_native_definition(),
        )
        sql.query.return_value = [_row_for(draft)]
        updated = svc.update_draft("r1", user_email="alice@x", author_kind="ai_assisted")

        assert updated.author_kind == "ai_assisted"
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "author_kind = 'ai_assisted'" in update_sql

    def test_author_kind_untouched_when_not_provided(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="r1",
            mode="dqx_native",
            status="draft",
            version=0,
            author_kind="human",
            definition=_native_definition(),
        )
        sql.query.return_value = [_row_for(draft)]
        updated = svc.update_draft("r1", user_email="alice@x", user_metadata={"name": "x"})
        assert updated.author_kind == "human"

    def test_rejects_unsafe_sql_predicate(self, svc, sql):
        """A 'sql' mode definition whose predicate fails ``is_sql_query_safe``
        must be rejected at save time (mirrors the check
        ``materializer.render_check`` already applies at materialization),
        not persisted."""
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="r1", mode="sql", status="draft", version=0, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(draft)]
        unsafe_definition = RuleDefinition.model_validate(
            {"body": {"predicate": "1=1; DROP TABLE users"}, "slots": [], "parameters": []}
        )
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        with pytest.raises(UnsafeSqlQueryError):
            svc.update_draft("r1", user_email="alice@x", mode="sql", definition=unsafe_definition)

    def test_rejects_unsafe_sql_query_in_native_mode(self, svc, sql):
        """A ``dqx_native`` check routed through the ``sql_query`` function
        must have its ``query`` argument validated too, not just genuine
        'sql'/'lowcode' mode bodies."""
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        draft = RegistryRule(
            rule_id="r1", mode="dqx_native", status="draft", version=0, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(draft)]
        unsafe_definition = RuleDefinition.model_validate(
            {
                "body": {"function": "sql_query", "arguments": {"query": "DROP TABLE users", "negate": False}},
                "slots": [],
                "parameters": [],
            }
        )
        with pytest.raises(UnsafeSqlQueryError):
            svc.update_draft("r1", user_email="alice@x", definition=unsafe_definition)

    def test_accepts_safe_sql_predicate(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="r1", mode="sql", status="draft", version=0, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(draft)]
        safe_definition = RuleDefinition.model_validate(
            {"body": {"predicate": "{{column}} IS NOT NULL"}, "slots": [], "parameters": []}
        )
        updated = svc.update_draft("r1", user_email="alice@x", mode="sql", definition=safe_definition)
        assert updated.definition.body["predicate"] == "{{column}} IS NOT NULL"


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


class TestDeleteBuiltinRules:
    def test_deletes_builtin_rows_and_versions_returns_count(self, svc, sql):
        sql.query.return_value = [["b1"], ["b2"]]
        deleted = svc.delete_builtin_rules()
        assert deleted == 2
        calls = [c.args[0] for c in sql.execute.call_args_list]
        versions_delete = next(c for c in calls if c.startswith("DELETE FROM") and "dq_rule_versions" in c)
        assert "'b1'" in versions_delete and "'b2'" in versions_delete
        rules_delete = next(c for c in calls if c.startswith("DELETE FROM") and "dq_rules " in c and "dq_rule_versions" not in c)
        assert "is_builtin = TRUE" in rules_delete

    def test_no_builtins_is_noop(self, svc, sql):
        sql.query.return_value = []
        deleted = svc.delete_builtin_rules()
        assert deleted == 0
        sql.execute.assert_not_called()


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
        # Regression guard: seeded built-in (OOTB) rules must always be
        # dqx_native — never lowcode/sql — per "Don't auto-create OOTB DQX
        # low-code rules."
        assert rule.mode == "dqx_native"
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
