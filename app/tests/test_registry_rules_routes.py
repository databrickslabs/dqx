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
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.models import (
    BATCH_IMPORT_MAX_RULES,
    BatchImportRegistryRulesIn,
    CreateRegistryRuleIn,
    UpdateRegistryRuleIn,
)
from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.routes.v1.registry_rules import (
    approve_registry_rule,
    backfill_rule_embeddings,
    batch_import_registry_rules,
    create_registry_rule,
    delete_registry_rule,
    deprecate_registry_rule,
    get_registry_rule,
    list_registry_rule_versions,
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


def _no_auto_upgrade() -> MagicMock:
    """App-settings mock with auto-upgrade OFF (the default posture).

    Keeps the Data Products Task 2 re-freeze hook dormant for the base
    approve-registry-rule cases so they assert the pre-existing publish
    behaviour unchanged; the auto-upgrade ON path has its own test.
    """
    app_settings = MagicMock()
    app_settings.get_auto_upgrade_without_approval.return_value = False
    return app_settings


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

    def test_get_surfaces_modified_display_status(self):
        svc = MagicMock()
        rule = _rule(status="approved", version=1)
        rule.modified_since_publish = True
        svc.get_rule_with_version.return_value = (rule, None)
        result = get_registry_rule("r1", svc=svc)
        assert result.rule.modified_since_publish is True
        assert result.rule.display_status == "modified"

    def test_list_versions_maps_snapshots(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleVersion

        svc = MagicMock()
        svc.list_versions.return_value = [
            RuleVersion(rule_id="r1", version=2, definition=_definition()),
            RuleVersion(rule_id="r1", version=1, definition=_definition()),
        ]
        result = list_registry_rule_versions("r1", svc=svc)
        assert [v.version for v in result] == [2, 1]


class TestCreateAndUpdate:
    def test_create_returns_rule_and_warning(self):
        svc = MagicMock()
        svc.create_rule.return_value = (_rule(), "possible duplicate")
        body = CreateRegistryRuleIn(mode="dqx_native", definition=_definition())
        result = create_registry_rule(body=body, svc=svc, user_email="alice@x")
        assert result.rule.rule_id == "r1"
        assert result.dedup_warning == "possible duplicate"

    def test_create_uses_injected_user_email_without_scim_call(self):
        """Author identity comes from the header-derived ``get_user_email``
        dependency (``CurrentUser``), NOT a per-request SCIM
        ``current_user.me()`` round-trip — that redundant call added
        ~200-500ms to every registry save/action (P19 bug #1). The route now
        takes ``user_email`` directly, so it can only forward what it is
        given and cannot issue a network call to resolve identity."""
        svc = MagicMock()
        svc.create_rule.return_value = (_rule(), None)
        body = CreateRegistryRuleIn(mode="dqx_native", definition=_definition())
        create_registry_rule(body=body, svc=svc, user_email="carol@x")
        assert svc.create_rule.call_args.kwargs["user_email"] == "carol@x"

    def test_update_forwards_injected_user_email(self):
        svc = MagicMock()
        svc.update_draft.return_value = _rule(status="draft")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"})
        update_registry_rule(
            "r1",
            body=body,
            svc=svc,
            user_email="carol@x",
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
        )
        assert svc.update_draft.call_args.kwargs["user_email"] == "carol@x"

    def test_create_rejects_unsafe_sql_with_400(self):
        """``RegistryService.create_rule`` also validates SQL safety (the
        "save as new draft" clone path for editing a non-draft rule goes
        through ``create_rule``, not ``update_draft``) — the route must
        surface that as a 400, not a 500. Mirrors
        ``test_update_rejects_unsafe_sql_with_400``."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        svc = MagicMock()
        svc.create_rule.side_effect = UnsafeSqlQueryError("The rule's SQL contains prohibited statements")
        body = CreateRegistryRuleIn(
            mode="sql",
            definition=RuleDefinition.model_validate(
                {"body": {"predicate": "1=1; DROP TABLE users"}, "slots": [], "parameters": []}
            ),
        )
        with pytest.raises(HTTPException) as excinfo:
            create_registry_rule(body=body, svc=svc, user_email="alice@x")
        assert excinfo.value.status_code == 400


class TestBatchImport:
    def test_batch_import_creates_all_rules(self):
        svc = MagicMock()
        svc.create_rule.side_effect = [(_rule("r1"), None), (_rule("r2"), None)]
        body = BatchImportRegistryRulesIn(
            rules=[
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
            ],
        )
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert result.saved == 2
        assert len(result.created) == 2
        assert svc.create_rule.call_count == 2
        assert svc.create_rule.call_args_list[0].kwargs["source"] == "import"

    def test_batch_import_submits_when_requested(self):
        svc = MagicMock()
        svc.create_rule.return_value = (_rule("r1"), None)
        body = BatchImportRegistryRulesIn(
            rules=[CreateRegistryRuleIn(mode="dqx_native", definition=_definition())],
            also_submit=True,
        )
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert result.submitted == 1
        svc.submit.assert_called_once_with("r1", "alice@x")

    def test_batch_import_continues_after_failure(self):
        svc = MagicMock()
        svc.create_rule.side_effect = [ValueError("bad"), (_rule("r2"), None)]
        body = BatchImportRegistryRulesIn(
            rules=[
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
            ],
        )
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert result.saved == 1
        assert len(result.failed) == 1
        assert result.failed[0].index == 0

    def test_batch_import_generic_exception_returns_sanitized_message(self):
        # CWE-209: a DB/driver exception must NOT be echoed verbatim to the
        # caller; the response carries a generic message while the details are
        # logged server-side.
        svc = MagicMock()
        leaky = RuntimeError('psycopg: relation "dq_rules" does not exist; host=db.internal port=5432')
        svc.create_rule.side_effect = leaky
        body = BatchImportRegistryRulesIn(rules=[CreateRegistryRuleIn(mode="dqx_native", definition=_definition())])
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert result.saved == 0
        assert len(result.failed) == 1
        assert "dq_rules" not in result.failed[0].error
        assert "db.internal" not in result.failed[0].error
        assert result.failed[0].error == "Failed to import this rule due to an internal error."

    def test_batch_import_validation_error_stays_user_facing(self):
        # ValueError is app-raised validation — its message is safe + actionable
        # and must still reach the importer.
        svc = MagicMock()
        svc.create_rule.side_effect = ValueError("definition is missing required slot 'column'")
        body = BatchImportRegistryRulesIn(rules=[CreateRegistryRuleIn(mode="dqx_native", definition=_definition())])
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert result.failed[0].error == "definition is missing required slot 'column'"

    def test_batch_import_rejects_empty_payload(self):
        with pytest.raises(ValidationError):
            BatchImportRegistryRulesIn(rules=[])

    def test_batch_import_rejects_payload_over_cap(self):
        # DoS guard: the per-rule synchronous DB loop must never be handed an
        # unbounded payload — validation rejects it before any DB work.
        one = CreateRegistryRuleIn(mode="dqx_native", definition=_definition())
        with pytest.raises(ValidationError):
            BatchImportRegistryRulesIn(rules=[one] * (BATCH_IMPORT_MAX_RULES + 1))
        # The cap itself is accepted.
        assert len(BatchImportRegistryRulesIn(rules=[one] * BATCH_IMPORT_MAX_RULES).rules) == BATCH_IMPORT_MAX_RULES

    def test_skip_duplicates_reuses_existing_active_rule(self):
        # A re-import of a rule that already exists (any active status) must
        # reuse it, not mint a copy — keeps re-importing a contract idempotent.
        svc = MagicMock()
        svc.compute_definition_fingerprint.return_value = "fp1"
        svc.get_active_rule_by_fingerprint.return_value = _rule("existing", status="approved", version=1)
        body = BatchImportRegistryRulesIn(
            rules=[CreateRegistryRuleIn(mode="dqx_native", definition=_definition())],
            skip_duplicates=True,
        )
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert result.saved == 0
        assert result.created == []
        assert len(result.reused) == 1
        assert result.reused[0].rule.rule_id == "existing"
        svc.create_rule.assert_not_called()

    def test_skip_duplicates_dedupes_within_the_batch(self):
        # Two structurally-identical rules in one payload collapse to a single
        # create; the second is reported as reused with no extra DB write.
        svc = MagicMock()
        svc.compute_definition_fingerprint.return_value = "fp1"
        svc.get_active_rule_by_fingerprint.return_value = None
        svc.create_rule.return_value = (_rule("r1"), None)
        body = BatchImportRegistryRulesIn(
            rules=[
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
            ],
            skip_duplicates=True,
        )
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert len(result.created) == 1
        assert len(result.reused) == 1
        assert result.reused[0].rule.rule_id == "r1"
        svc.create_rule.assert_called_once()

    def test_skip_duplicates_off_never_dedupes(self):
        # Default posture: no fingerprinting, both identical rules are created.
        svc = MagicMock()
        svc.create_rule.side_effect = [(_rule("r1"), None), (_rule("r2"), None)]
        body = BatchImportRegistryRulesIn(
            rules=[
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
                CreateRegistryRuleIn(mode="dqx_native", definition=_definition()),
            ],
        )
        result = batch_import_registry_rules(body=body, svc=svc, user_email="alice@x")
        assert len(result.created) == 2
        assert result.reused == []
        svc.compute_definition_fingerprint.assert_not_called()
        svc.get_active_rule_by_fingerprint.assert_not_called()


class TestCreateAndUpdateContinued:
    def test_update_rejects_non_draft_with_400(self):
        svc = MagicMock()
        svc.update_draft.side_effect = ValueError("only draft rules can be edited")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"})
        with pytest.raises(HTTPException) as excinfo:
            update_registry_rule(
                "r1",
                body=body,
                svc=svc,
                user_email="alice@x",
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
            )
        assert excinfo.value.status_code == 400

    def test_update_missing_rule_raises_404(self):
        svc = MagicMock()
        svc.update_draft.side_effect = RuntimeError("Registry rule not found: r1")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"})
        with pytest.raises(HTTPException) as excinfo:
            update_registry_rule(
                "r1",
                body=body,
                svc=svc,
                user_email="alice@x",
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
            )
        assert excinfo.value.status_code == 404

    def test_update_passes_author_kind_through_to_service(self):
        svc = MagicMock()
        svc.update_draft.return_value = _rule(status="draft")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"}, author_kind="ai_assisted")
        update_registry_rule(
            "r1",
            body=body,
            svc=svc,
            user_email="alice@x",
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
        )
        assert svc.update_draft.call_args.kwargs["author_kind"] == "ai_assisted"

    def test_update_without_author_kind_passes_none(self):
        svc = MagicMock()
        svc.update_draft.return_value = _rule(status="draft")
        body = UpdateRegistryRuleIn(user_metadata={"name": "x"})
        update_registry_rule(
            "r1",
            body=body,
            svc=svc,
            user_email="alice@x",
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
        )
        assert svc.update_draft.call_args.kwargs["author_kind"] is None

    def test_update_rejects_unsafe_sql_with_400(self):
        """``RegistryService.update_draft`` raises ``UnsafeSqlQueryError`` for
        an unsafe SQL/lowcode body or ``sql_query``/``sql_expression``
        native check (see ``test_registry_service.py``); the route must
        surface that as a 400, not a 500, so the JSON-edit dialog can show
        an inline error."""
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        svc = MagicMock()
        svc.update_draft.side_effect = UnsafeSqlQueryError("The rule's SQL contains prohibited statements")
        body = UpdateRegistryRuleIn(
            mode="sql",
            definition=RuleDefinition.model_validate(
                {"body": {"predicate": "1=1; DROP TABLE users"}, "slots": [], "parameters": []}
            ),
        )
        with pytest.raises(HTTPException) as excinfo:
            update_registry_rule(
                "r1",
                body=body,
                svc=svc,
                user_email="alice@x",
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
            )
        assert excinfo.value.status_code == 400


class TestDelete:
    def test_delete_success(self):
        svc = MagicMock()
        apply_rules = MagicMock()
        apply_rules.count_applications_for_rule.return_value = 0
        result = delete_registry_rule(
            "r1",
            svc=svc,
            apply_rules=apply_rules,
            user_email="alice@x",
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
        )
        assert result == {"status": "deleted", "rule_id": "r1"}
        svc.delete.assert_called_once_with("r1", "alice@x")

    def test_delete_missing_rule_raises_404(self):
        svc = MagicMock()
        apply_rules = MagicMock()
        apply_rules.count_applications_for_rule.return_value = 0
        svc.delete.side_effect = RuntimeError("Registry rule not found: r1")
        with pytest.raises(HTTPException) as excinfo:
            delete_registry_rule(
                "r1",
                svc=svc,
                apply_rules=apply_rules,
                user_email="alice@x",
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
            )
        assert excinfo.value.status_code == 404

    def test_delete_applied_rule_raises_409(self):
        svc = MagicMock()
        apply_rules = MagicMock()
        apply_rules.count_applications_for_rule.return_value = 2
        with pytest.raises(HTTPException) as excinfo:
            delete_registry_rule(
                "r1",
                svc=svc,
                apply_rules=apply_rules,
                user_email="alice@x",
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
            )
        assert excinfo.value.status_code == 409
        svc.delete.assert_not_called()


class TestLifecycleRoutes:
    @staticmethod
    def _pending_stub() -> MagicMock:
        """A PendingApplicationService stub with no staged rows (activation no-ops)."""
        pending = MagicMock()
        pending.list_for_rule.return_value = []
        return pending

    @classmethod
    def _submit_deps(cls) -> dict:
        """Common submit-route deps with approvals mode = ``enabled`` (no auto-approve)."""
        app_settings = _no_auto_upgrade()
        app_settings.get_approvals_mode.return_value = "enabled"
        return {
            "embeddings": MagicMock(),
            "materializer": MagicMock(),
            "version_svc": MagicMock(),
            "monitored_tables": MagicMock(),
            "app_settings": app_settings,
            "apply_rules": MagicMock(),
            "pending": cls._pending_stub(),
            "perms": MagicMock(),
            "role": UserRole.RULE_AUTHOR,
            "principal_ids": frozenset(),
        }

    def test_submit_success(self):
        svc = MagicMock()
        svc.submit.return_value = _rule(status="pending_approval")
        result = submit_registry_rule("r1", svc=svc, user_email="alice@x", **self._submit_deps())
        assert result.status == "pending_approval"
        svc.approve.assert_not_called()

    def test_submit_invalid_transition_raises_400(self):
        svc = MagicMock()
        svc.submit.side_effect = ValueError("Cannot transition from 'approved' to 'pending_approval'")
        with pytest.raises(HTTPException) as excinfo:
            submit_registry_rule("r1", svc=svc, user_email="alice@x", **self._submit_deps())
        assert excinfo.value.status_code == 400

    def test_approve_success(self):
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=1)
        embeddings = MagicMock()
        materializer = MagicMock()
        result = approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=embeddings,
            materializer=materializer,
            version_svc=MagicMock(),
            app_settings=_no_auto_upgrade(),
            monitored_tables=MagicMock(),
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=MagicMock(),
            user_email="alice@x",
        )
        assert result.status == "approved"
        assert result.version == 1

    def test_approve_embeds_the_published_rule(self):
        svc = MagicMock()
        published = _rule(status="approved", version=1)
        svc.approve.return_value = published
        embeddings = MagicMock()
        materializer = MagicMock()
        approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=embeddings,
            materializer=materializer,
            version_svc=MagicMock(),
            app_settings=_no_auto_upgrade(),
            monitored_tables=MagicMock(),
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=MagicMock(),
            user_email="alice@x",
        )
        embeddings.embed_and_store.assert_called_once_with(published)

    def test_approve_propagates_unexpected_embedding_errors_as_500(self):
        # In production RuleEmbeddingsService.embed_and_store never raises (it swallows
        # its own errors) — this documents the route's fallback behaviour if that
        # invariant were ever broken, rather than silently hiding a bug.
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=1)
        embeddings = MagicMock()
        embeddings.embed_and_store.side_effect = TypeError("boom")
        materializer = MagicMock()
        with pytest.raises(HTTPException) as excinfo:
            approve_registry_rule(
                "r1",
                svc=svc,
                embeddings=embeddings,
                materializer=materializer,
                version_svc=MagicMock(),
                app_settings=_no_auto_upgrade(),
                monitored_tables=MagicMock(),
                apply_rules=MagicMock(),
                pending=self._pending_stub(),
                tag_reconcile=MagicMock(),
                user_email="alice@x",
            )
        assert excinfo.value.status_code == 500

    def test_approve_rematerializes_following_applications(self):
        """Publishing a registry rule must propagate to every FOLLOWING
        (unpinned) application so their materialized ``dq_quality_rules``
        rows pick up the new version (design spec §5)."""
        svc = MagicMock()
        published = _rule(status="approved", version=2)
        svc.approve.return_value = published
        embeddings = MagicMock()
        materializer = MagicMock()
        approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=embeddings,
            materializer=materializer,
            version_svc=MagicMock(),
            app_settings=_no_auto_upgrade(),
            monitored_tables=MagicMock(),
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=MagicMock(),
            user_email="alice@x",
        )
        materializer.rematerialize_for_rule.assert_called_once_with("r1")

    def test_approve_propagates_unexpected_materializer_errors_as_500(self):
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=1)
        embeddings = MagicMock()
        materializer = MagicMock()
        materializer.rematerialize_for_rule.side_effect = TypeError("boom")
        with pytest.raises(HTTPException) as excinfo:
            approve_registry_rule(
                "r1",
                svc=svc,
                embeddings=embeddings,
                materializer=materializer,
                version_svc=MagicMock(),
                app_settings=_no_auto_upgrade(),
                monitored_tables=MagicMock(),
                apply_rules=MagicMock(),
                pending=self._pending_stub(),
                tag_reconcile=MagicMock(),
                user_email="alice@x",
            )
        assert excinfo.value.status_code == 500

    def test_approve_auto_upgrade_off_rolls_up_follower_bindings(self):
        """P23 item 1 regression: with auto-upgrade OFF, a republish drops
        changed follower checks to ``pending_approval``. The binding must be
        rolled up (not silently left ``approved``) so the table-level approve
        path is unblocked and the run stops serving the stale snapshot.
        Re-freeze must NOT fire (the new content isn't approved yet)."""
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=2)
        embeddings = MagicMock()
        materializer = MagicMock()
        materializer.rematerialize_for_rule.return_value = ["b1", "b2"]
        version_svc = MagicMock()
        monitored_tables = MagicMock()
        approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=embeddings,
            materializer=materializer,
            version_svc=version_svc,
            monitored_tables=monitored_tables,
            app_settings=_no_auto_upgrade(),
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=MagicMock(),
            user_email="alice@x",
        )
        assert monitored_tables.rollup_status.call_count == 2
        monitored_tables.rollup_status.assert_any_call("b1", "alice@x")
        monitored_tables.rollup_status.assert_any_call("b2", "alice@x")
        version_svc.refreeze_current.assert_not_called()

    def test_approve_auto_upgrade_on_refreezes_and_does_not_rollup(self):
        """With auto-upgrade ON, follower checks stay approved and the frozen
        snapshot is re-frozen in place; the binding status is untouched."""
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=2)
        embeddings = MagicMock()
        materializer = MagicMock()
        materializer.rematerialize_for_rule.return_value = ["b1"]
        version_svc = MagicMock()
        monitored_tables = MagicMock()
        app_settings = MagicMock()
        app_settings.get_auto_upgrade_without_approval.return_value = True
        approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=embeddings,
            materializer=materializer,
            version_svc=version_svc,
            monitored_tables=monitored_tables,
            app_settings=app_settings,
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=MagicMock(),
            user_email="alice@x",
        )
        version_svc.refreeze_current.assert_called_once_with("b1")
        monitored_tables.rollup_status.assert_not_called()

    def test_reject_success(self):
        svc = MagicMock()
        svc.reject.return_value = _rule(status="rejected")
        result = reject_registry_rule("r1", svc=svc, user_email="alice@x")
        assert result.status == "rejected"

    def test_revoke_success(self):
        from databricks_labs_dqx_app.backend.routes.v1.registry_rules import revoke_registry_rule

        svc = MagicMock()
        rule = _rule(status="pending_approval")
        rule.created_by = "alice@x"
        svc.get_rule.return_value = rule
        svc.revoke.return_value = _rule(status="draft")
        result = revoke_registry_rule(
            "r1",
            svc=svc,
            user_email="alice@x",
            role=UserRole.RULE_AUTHOR,
        )
        assert result.status == "draft"

    def test_revoke_forbidden_for_other_author(self):
        from databricks_labs_dqx_app.backend.routes.v1.registry_rules import revoke_registry_rule

        svc = MagicMock()
        rule = _rule(status="pending_approval")
        rule.created_by = "other@x"
        svc.get_rule.return_value = rule
        with pytest.raises(HTTPException) as exc:
            revoke_registry_rule(
                "r1",
                svc=svc,
                user_email="alice@x",
                role=UserRole.RULE_AUTHOR,
            )
        assert exc.value.status_code == 403

    def test_deprecate_success(self):
        svc = MagicMock()
        svc.deprecate.return_value = _rule(status="deprecated", version=1)
        result = deprecate_registry_rule("r1", svc=svc, user_email="alice@x")
        assert result.status == "deprecated"

    def test_undeprecate_success(self):
        svc = MagicMock()
        svc.undeprecate.return_value = _rule(status="approved", version=1)
        result = undeprecate_registry_rule("r1", svc=svc, user_email="alice@x")
        assert result.status == "approved"

    def test_approve_missing_rule_raises_404(self):
        svc = MagicMock()
        svc.approve.side_effect = RuntimeError("Registry rule not found: r1")
        embeddings = MagicMock()
        materializer = MagicMock()
        with pytest.raises(HTTPException) as excinfo:
            approve_registry_rule(
                "r1",
                svc=svc,
                embeddings=embeddings,
                materializer=materializer,
                version_svc=MagicMock(),
                app_settings=_no_auto_upgrade(),
                monitored_tables=MagicMock(),
                apply_rules=MagicMock(),
                pending=self._pending_stub(),
                tag_reconcile=MagicMock(),
                user_email="alice@x",
            )
        assert excinfo.value.status_code == 404

    def test_approve_triggers_tag_reconcile(self):
        """Apply-on-tag (Task 7): a successful approve reconciles the rule once."""
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=1)
        tag_reconcile = MagicMock()
        approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=MagicMock(),
            materializer=MagicMock(),
            version_svc=MagicMock(),
            app_settings=_no_auto_upgrade(),
            monitored_tables=MagicMock(),
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=tag_reconcile,
            user_email="alice@x",
        )
        tag_reconcile.reconcile_rule.assert_called_once_with("r1", "alice@x")

    def test_approve_tag_reconcile_failure_is_non_fatal(self):
        """A raising tag-reconcile must NOT turn a successful approve into a 500."""
        svc = MagicMock()
        svc.approve.return_value = _rule(status="approved", version=1)
        tag_reconcile = MagicMock()
        tag_reconcile.reconcile_rule.side_effect = RuntimeError("boom")
        result = approve_registry_rule(
            "r1",
            svc=svc,
            embeddings=MagicMock(),
            materializer=MagicMock(),
            version_svc=MagicMock(),
            app_settings=_no_auto_upgrade(),
            monitored_tables=MagicMock(),
            apply_rules=MagicMock(),
            pending=self._pending_stub(),
            tag_reconcile=tag_reconcile,
            user_email="alice@x",
        )
        assert result.status == "approved"


class TestBackfillRuleEmbeddings:
    def test_backfills_every_published_rule(self):
        svc = MagicMock()
        svc.list_rules.return_value = [_rule(status="approved", version=1), _rule("r2", status="approved", version=1)]
        embeddings = MagicMock()
        embeddings.backfill.return_value = 2

        result = backfill_rule_embeddings(svc=svc, embeddings=embeddings)

        svc.list_rules.assert_called_once_with(status="approved")
        embeddings.backfill.assert_called_once_with(svc.list_rules.return_value)
        assert result.total_published == 2
        assert result.embedded == 2

    def test_no_op_when_unconfigured(self):
        svc = MagicMock()
        svc.list_rules.return_value = [_rule(status="approved", version=1)]
        embeddings = MagicMock()
        embeddings.backfill.return_value = 0

        result = backfill_rule_embeddings(svc=svc, embeddings=embeddings)

        assert result.total_published == 1
        assert result.embedded == 0
