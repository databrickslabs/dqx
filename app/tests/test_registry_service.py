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

    def test_defaults_steward_to_creator_when_unset(self, svc):
        # No steward supplied -> the creator becomes the accountable steward.
        rule, _ = svc.create_rule(mode="dqx_native", definition=_native_definition(), user_email="alice@x")
        assert rule.steward == "alice@x"

    def test_explicit_steward_wins_over_creator(self, svc):
        rule, _ = svc.create_rule(
            mode="dqx_native", definition=_native_definition(), user_email="alice@x", steward="bob@x"
        )
        assert rule.steward == "bob@x"

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

    def test_updates_draft_missing_dimension_and_severity_tags(self, svc, sql):
        """A draft created before ``dimension``/``severity`` became required
        registry tags (e.g. imported from plain DQX YAML, which has no such
        concept) must still be editable in place — the completeness gate
        that later blocks the *frontend* Save buttons until those tags are
        filled in is purely a UI concern; the backend never required them
        and must accept the update once the caller supplies them."""
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="r1",
            mode="dqx_native",
            status="draft",
            version=0,
            definition=_native_definition(),
            user_metadata={"name": "Legacy import"},
        )
        sql.query.return_value = [_row_for(draft)]
        updated = svc.update_draft(
            "r1",
            user_email="alice@x",
            user_metadata={"name": "Legacy import", "dimension": "Completeness", "severity": "High"},
        )
        assert updated.user_metadata["dimension"] == "Completeness"
        assert updated.user_metadata["severity"] == "High"

    def test_metadata_only_edit_persists_on_incomplete_draft(self, svc, sql):
        """P17-A regression: a draft that is INCOMPLETE by the frontend's
        submit-completeness standards — required function parameters still
        null, no ``dimension``/``severity`` tags — must accept a plain
        metadata edit (e.g. changing the description) without requiring the
        caller to complete the rule first. Drafts are allowed to be
        incomplete; completeness is enforced at submit/approve time, not at
        save time. The incomplete definition must round-trip unchanged."""
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        incomplete_definition = RuleDefinition.model_validate(
            {
                "body": {"function": "has_no_aggr_outliers", "arguments": {"column": "{{column}}"}},
                "slots": [
                    {"name": "column", "family": "any", "position": 0, "cardinality": "one", "arg_key": "column"}
                ],
                # ``time_column`` is a required parameter of the function —
                # still unset on this draft, exactly like a rule created
                # before required-parameter gating existed.
                "parameters": [{"name": "time_column", "type": "string", "value": None}],
            }
        )
        draft = RegistryRule(
            rule_id="r1",
            mode="dqx_native",
            status="draft",
            version=0,
            definition=incomplete_definition,
            user_metadata={"name": "test"},  # no dimension / severity
        )
        sql.query.return_value = [_row_for(draft)]

        updated = svc.update_draft(
            "r1",
            user_email="alice@x",
            definition=incomplete_definition,
            user_metadata={"name": "test", "description": "edited description"},
        )

        assert updated.user_metadata["description"] == "edited description"
        assert updated.definition.parameters[0].value is None
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "edited description" in update_sql
        assert '"value": null' in update_sql

    def test_edits_approved_rule_in_place_without_bumping_version(self, svc, sql):
        """Edit-in-place revision path: an ``approved`` rule is editable, its
        live definition/tags change, but its ``version`` stays N (no new
        snapshot) until the revision is re-submitted and re-approved."""
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        approved = RegistryRule(
            rule_id="r1", mode="dqx_native", status="approved", version=1, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(approved)]
        updated = svc.update_draft("r1", user_email="alice@x", user_metadata={"name": "renamed"})
        assert updated.status == "approved"
        assert updated.version == 1
        assert updated.user_metadata == {"name": "renamed"}
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "dq_rule_versions" not in update_sql

    @pytest.mark.parametrize("status", ["pending_approval", "rejected", "deprecated"])
    def test_rejects_editing_non_editable_status(self, svc, sql, status):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        rule = RegistryRule(
            rule_id="r1", mode="dqx_native", status=status, version=1, definition=_native_definition()
        )
        sql.query.return_value = [_row_for(rule)]
        with pytest.raises(ValueError, match="can be"):
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

    def test_submit_approved_revision_to_pending_keeps_version(self, svc, sql):
        """Re-submitting an EDITED approved rule sends it back through the
        gate (approved -> pending_approval) WITHOUT bumping version — vN keeps
        serving until the revision is approved as vN+1."""
        published_def = RuleDefinition.model_validate(
            {"body": {"function": "is_not_empty", "arguments": {"column": "{{column}}"}}, "slots": [], "parameters": []}
        )
        sql.query.side_effect = [
            [_row_for(self._rule("approved", version=1))],  # submit._get (live, differs)
            [_version_row("r1", 1, published_def, {})],  # submit._get_version (snapshot differs -> modified)
            [_row_for(self._rule("approved", version=1))],  # _transition._get
        ]
        updated = svc.submit("r1", "alice@x")
        assert updated.status == "pending_approval"
        assert updated.version == 1

    def test_submit_unchanged_approved_raises_no_changes(self, svc, sql):
        """Submitting an approved rule with NO unpublished edits is rejected
        (would only mint an identical, empty vN+1)."""
        live = self._rule("approved", version=1)
        sql.query.side_effect = [
            [_row_for(live)],  # submit._get
            [_version_row("r1", 1, live.definition, {})],  # submit._get_version (snapshot matches live)
        ]
        with pytest.raises(ValueError, match="No changes to submit"):
            svc.submit("r1", "alice@x")

    def test_submit_rejects_invalid_transition(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("deprecated", version=1))]
        with pytest.raises(ValueError, match="Cannot transition"):
            svc.submit("r1", "alice@x")

    def test_reject_first_draft_is_terminal(self, svc, sql):
        """Rejecting a first-time draft's review (version 0) is terminal."""
        sql.query.return_value = [_row_for(self._rule("pending_approval", version=0))]
        updated = svc.reject("r1", "approver@x")
        assert updated.status == "rejected"

    def test_reject_published_revision_returns_to_approved(self, svc, sql):
        """Rejecting a REVISION of an already-published rule (version >= 1)
        returns it to approved-at-vN with the live edits retained."""
        sql.query.return_value = [_row_for(self._rule("pending_approval", version=1))]
        updated = svc.reject("r1", "approver@x")
        assert updated.status == "approved"
        assert updated.version == 1

    def test_approve_bumps_version_and_writes_snapshot(self, svc, sql):
        sql.query.return_value = [_row_for(self._rule("pending_approval"))]
        updated = svc.approve("r1", "approver@x")
        assert updated.status == "approved"
        assert updated.version == 1
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("dq_rule_versions" in c for c in calls)

    def test_approve_freezes_mode_into_snapshot(self, svc, sql):
        """Publishing freezes the rule's authoring mode into the
        dq_rule_versions snapshot so a later in-place mode switch can't corrupt
        the served vN's rendering."""
        rule = self._rule("pending_approval")
        rule.mode = "sql"
        sql.query.return_value = [_row_for(rule)]
        svc.approve("r1", "approver@x")
        snapshot_insert = next(
            c.args[0]
            for c in sql.execute.call_args_list
            if "INSERT INTO" in c.args[0] and "dq_rule_versions" in c.args[0]
        )
        assert "mode" in snapshot_insert
        assert "'sql'" in snapshot_insert

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
        # First query: the list itself; second: the modified-snapshot batch
        # for the filtered (published) rules — empty here (no modified state
        # under test).
        sql.query.side_effect = [[_row_for(a), _row_for(b)], []]
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


# ---------------------------------------------------------------------------
# Modified-since-publish detection + version history
# ---------------------------------------------------------------------------


def _version_row(rule_id, version, definition, user_metadata, polarity=None, mode="dqx_native"):
    """Build a SELECT row matching ``RegistryService._row_to_version`` order."""
    return [
        rule_id,
        str(version),
        json.dumps(definition.model_dump(mode="json")),
        polarity,
        json.dumps(user_metadata),
        "author@x",
        "2026-07-02T00:00:00+00:00",
        mode,
    ]


class TestModifiedSincePublish:
    def _approved(self, definition, metadata):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        return RegistryRule(
            rule_id="r1", mode="dqx_native", status="approved", version=1,
            definition=definition, user_metadata=metadata,
        )

    def test_not_modified_when_live_matches_snapshot(self, svc, sql):
        definition = _native_definition()
        metadata = {"name": "n"}
        rule = self._approved(definition, metadata)
        sql.query.side_effect = [
            [_row_for(rule)],  # _get
            [_version_row("r1", 1, definition, metadata)],  # _get_version
        ]
        result = svc.get_rule_with_version("r1")
        assert result is not None
        got_rule, _ = result
        assert got_rule.modified_since_publish is False

    def test_modified_when_definition_differs(self, svc, sql):
        live_def = RuleDefinition.model_validate(
            {"body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}}, "slots": [], "parameters": []}
        )
        published_def = RuleDefinition.model_validate(
            {"body": {"function": "is_not_empty", "arguments": {"column": "{{column}}"}}, "slots": [], "parameters": []}
        )
        rule = self._approved(live_def, {"name": "n"})
        sql.query.side_effect = [
            [_row_for(rule)],
            [_version_row("r1", 1, published_def, {"name": "n"})],
        ]
        got_rule, _ = svc.get_rule_with_version("r1")
        assert got_rule.modified_since_publish is True

    def test_modified_when_metadata_differs(self, svc, sql):
        definition = _native_definition()
        rule = self._approved(definition, {"name": "new", "severity": "High"})
        sql.query.side_effect = [
            [_row_for(rule)],
            [_version_row("r1", 1, definition, {"name": "old", "severity": "High"})],
        ]
        got_rule, _ = svc.get_rule_with_version("r1")
        assert got_rule.modified_since_publish is True

    def test_list_attaches_modified_flag(self, svc, sql):
        definition = _native_definition()
        rule = self._approved(definition, {"name": "live"})
        # ``_attach_modified`` selects a 6-column batch row
        # (rule_id, version, definition, polarity, user_metadata, mode).
        attach_row = ["r1", "1", json.dumps(definition.model_dump(mode="json")), None,
                      json.dumps({"name": "published"}), "dqx_native"]
        sql.query.side_effect = [
            [_row_for(rule)],  # list query
            [attach_row],  # _attach_modified batch
        ]
        rules = svc.list_rules()
        assert len(rules) == 1
        assert rules[0].modified_since_publish is True

    def test_modified_when_mode_differs(self, svc, sql):
        """A mode switch on the live approved rule (e.g. native -> sql) is
        flagged as modified even when the definition/tags are unchanged."""
        definition = _native_definition()
        rule = self._approved(definition, {"name": "n"})
        rule.mode = "sql"  # live edited in place
        sql.query.side_effect = [
            [_row_for(rule)],
            [_version_row("r1", 1, definition, {"name": "n"}, mode="dqx_native")],
        ]
        got_rule, _ = svc.get_rule_with_version("r1")
        assert got_rule.modified_since_publish is True

    def test_list_no_snapshot_query_when_all_unpublished(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(rule_id="d1", mode="dqx_native", status="draft", version=0, definition=_native_definition())
        sql.query.return_value = [_row_for(draft)]
        rules = svc.list_rules()
        assert rules[0].modified_since_publish is False
        # Only the list query itself runs — no snapshot batch for unpublished rules.
        assert sql.query.call_count == 1


class TestListVersions:
    def test_lists_versions_newest_first(self, svc, sql):
        definition = _native_definition()
        sql.query.return_value = [
            _version_row("r1", 2, definition, {"name": "v2"}),
            _version_row("r1", 1, definition, {"name": "v1"}),
        ]
        versions = svc.list_versions("r1")
        assert [v.version for v in versions] == [2, 1]
        called_sql = sql.query.call_args[0][0]
        assert "ORDER BY version DESC" in called_sql

    def test_empty_when_no_versions(self, svc, sql):
        sql.query.return_value = []
        assert svc.list_versions("r1") == []


class TestMatchOrCreateApprovedRule:
    """The profiling match-or-create-and-approve primitive (idempotent by fingerprint)."""

    def _approved(self):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        rule = RegistryRule(
            rule_id="approved1",
            mode="dqx_native",
            status="approved",
            version=1,
            definition=_native_definition(),
            user_metadata={"name": "Is not null"},
        )
        rule.fingerprint = "fp-existing"
        return rule

    def test_reuses_existing_approved_rule_without_creating(self, svc, sql):
        approved = self._approved()
        sql.query.return_value = [_row_for(approved)]

        rule, created = svc.match_or_create_approved_rule(_native_definition(), {"name": "x"}, "alice@x")

        assert created is False
        assert rule is not None
        assert rule.rule_id == "approved1"
        # No INSERT into dq_rules — the existing approved rule was reused.
        assert not any("INSERT INTO dqx_test.dqx_app_test.dq_rules " in c.args[0] for c in sql.execute.call_args_list)

    def test_idempotent_on_rerun(self, svc, sql):
        # A second suggest run finds the rule the first run created (same
        # fingerprint) and reuses it — never a duplicate.
        approved = self._approved()
        sql.query.return_value = [_row_for(approved)]

        first, first_created = svc.match_or_create_approved_rule(_native_definition(), {}, "alice@x")
        second, second_created = svc.match_or_create_approved_rule(_native_definition(), {}, "alice@x")

        assert first_created is False and second_created is False
        assert first.rule_id == second.rule_id == "approved1"

    def test_skips_when_non_approved_duplicate_exists(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        draft = RegistryRule(
            rule_id="draft1", mode="dqx_native", status="draft", version=0, definition=_native_definition()
        )
        # get_approved -> none; get_rule_by_fingerprint -> a draft (foreign work).
        sql.query.side_effect = [[], [_row_for(draft)]]

        rule, created = svc.match_or_create_approved_rule(_native_definition(), {}, "alice@x")

        assert rule is None
        assert created is False
        # Neither duplicated nor auto-approved someone else's draft.
        assert not sql.execute.called

    def test_creates_and_approves_when_absent(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        # Query order: get_approved([]), get_rule_by_fingerprint([]),
        # create_rule._dedup_warning([]), submit._get(draft), _transition._get(draft),
        # approve._get(pending).
        def draft_row():
            r = RegistryRule(
                rule_id="new1", mode="dqx_native", status="draft", version=0, definition=_native_definition()
            )
            return _row_for(r)

        def pending_row():
            r = RegistryRule(
                rule_id="new1", mode="dqx_native", status="pending_approval", version=0, definition=_native_definition()
            )
            return _row_for(r)

        sql.query.side_effect = [[], [], [], [draft_row()], [draft_row()], [pending_row()]]

        rule, created = svc.match_or_create_approved_rule(
            _native_definition(), {"name": "Is not null", "dimension": "Completeness"}, "alice@x"
        )

        assert created is True
        assert rule is not None
        assert rule.status == "approved"
        assert rule.version == 1
        executed = [c.args[0] for c in sql.execute.call_args_list]
        # A new dq_rules row was inserted and a frozen version snapshot written
        # (i.e. it was published/approved, not left as a draft).
        assert any("INSERT INTO dqx_test.dqx_app_test.dq_rules " in s for s in executed)
        assert any("INSERT INTO dqx_test.dqx_app_test.dq_rule_versions" in s for s in executed)

    def test_new_rule_attributed_to_profiling(self, svc, sql):
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        def draft_row():
            r = RegistryRule(
                rule_id="new1", mode="dqx_native", status="draft", version=0, definition=_native_definition()
            )
            return _row_for(r)

        def pending_row():
            r = RegistryRule(
                rule_id="new1", mode="dqx_native", status="pending_approval", version=0, definition=_native_definition()
            )
            return _row_for(r)

        sql.query.side_effect = [[], [], [], [draft_row()], [draft_row()], [pending_row()]]

        svc.match_or_create_approved_rule(_native_definition(), {}, "alice@x")

        insert = next(
            c.args[0] for c in sql.execute.call_args_list if "INSERT INTO dqx_test.dqx_app_test.dq_rules " in c.args[0]
        )
        # source='profiling' + author_kind='ai_generated' make the auto-created rule auditable.
        assert "'profiling'" in insert
        assert "'ai_generated'" in insert
