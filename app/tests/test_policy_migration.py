"""Tests for the one-off slot_tags -> rule policies data migration."""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.policy_models import ColumnCondition, PolicyScope, RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.registry_models import (
    RESERVED_NAME_KEY,
    RegistryRule,
    RuleDefinition,
    RuleSlot,
    set_reserved_tag,
    set_slot_tags,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.policy_migration import (
    MIGRATED_MARKER_KEY,
    migrate_slot_tags_to_policies,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import (
    DuplicatePolicyNameError,
    RulePolicyService,
)


def _rule(rule_id: str, name: str, slot_tags: dict[str, list[str]] | None, status: str = "approved") -> RegistryRule:
    metadata: dict[str, object] = set_reserved_tag({}, RESERVED_NAME_KEY, name)
    if slot_tags is not None:
        metadata = set_slot_tags(metadata, slot_tags)
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status=status,
        version=1,
        definition=RuleDefinition(
            body={"function": "is_not_null", "arguments": {}},
            slots=[RuleSlot(name="column", family="any", position=0, cardinality="one")],
        ),
        user_metadata=metadata,
    )


class _Deps:
    def __init__(self, settings_rows: dict[str, str]) -> None:
        self.rows = dict(settings_rows)
        self.settings = create_autospec(AppSettingsService, instance=True)
        self.settings.get_setting.side_effect = self.rows.get
        self.settings.save_setting.side_effect = lambda key, value, user_email=None: self.rows.__setitem__(key, value)
        self.settings.delete_setting.side_effect = lambda key: self.rows.pop(key, None)
        self.registry = create_autospec(RegistryService, instance=True)
        self.policies = create_autospec(RulePolicyService, instance=True)
        self.created: list[tuple[str, RulePolicyIn, str]] = []
        self.policies.create.side_effect = self.create_policy
        self.applied = create_autospec(ApplyRulesService, instance=True)
        self.applied.restamp_tag_auto_to_policy.return_value = 1

    def create_policy(self, rule_id: str, body: RulePolicyIn, user_email: str) -> RulePolicy:
        self.created.append((rule_id, body, user_email))
        return RulePolicy(**body.model_dump(), policy_id=f"p-{rule_id}-{len(self.created)}", rule_id=rule_id)

    def run(self) -> int:
        return migrate_slot_tags_to_policies(self.settings, self.registry, self.policies, self.applied)


def test_flag_on_converts_every_tagged_rule_and_restamps_legacy_rows() -> None:
    deps = _Deps({"tag_auto_apply": "true"})
    deps.registry.list_rules.return_value = [
        _rule("r1", "Email not null", {"column": ["class.email_address"]}),
        _rule("r2", "Draft rule", {"column": ["pii", "class.phone=mobile"]}, status="draft"),
        _rule("r3", "Untagged", None),
    ]

    assert deps.run() == 2

    deps.registry.list_rules.assert_called_once_with()
    assert [(rule_id, body.name, user) for rule_id, body, user in deps.created] == [
        ("r1", "Email not null by tag", "system"),
        ("r2", "Draft rule by tag", "system"),
    ]
    body = deps.created[1][1]
    assert body.scope == PolicyScope()
    assert body.column_conditions == {"column": ColumnCondition(kind="tags", tags=["pii", "class.phone=mobile"])}
    assert body.register_tables is False
    assert deps.applied.restamp_tag_auto_to_policy.call_args_list == [
        (("r1", "p-r1-1"),),
        (("r2", "p-r2-2"),),
    ]
    assert "tag_auto_apply" not in deps.rows
    assert deps.rows[MIGRATED_MARKER_KEY] == "true"


@pytest.mark.parametrize("flag", [None, "false", "nonsense"])
def test_flag_off_creates_nothing_but_retires_the_setting(flag: str | None) -> None:
    deps = _Deps({} if flag is None else {"tag_auto_apply": flag})
    deps.registry.list_rules.return_value = [_rule("r1", "Email not null", {"column": ["class.email_address"]})]

    assert deps.run() == 0

    deps.policies.create.assert_not_called()
    deps.applied.restamp_tag_auto_to_policy.assert_not_called()
    assert "tag_auto_apply" not in deps.rows
    assert deps.rows[MIGRATED_MARKER_KEY] == "true"


def test_second_run_is_a_no_op() -> None:
    deps = _Deps({"tag_auto_apply": "true"})
    deps.registry.list_rules.return_value = [_rule("r1", "Email not null", {"column": ["class.email_address"]})]

    assert deps.run() == 1
    deps.rows["tag_auto_apply"] = "true"  # even if the legacy flag reappeared
    assert deps.run() == 0

    assert len(deps.created) == 1
    deps.registry.list_rules.assert_called_once()


def test_name_conflict_retries_with_suffix() -> None:
    deps = _Deps({"tag_auto_apply": "true"})
    deps.registry.list_rules.return_value = [_rule("r1", "Email not null", {"column": ["class.email_address"]})]

    def create(rule_id: str, body: RulePolicyIn, user_email: str) -> RulePolicy:
        if body.name == "Email not null by tag":
            raise DuplicatePolicyNameError("taken")
        return deps.create_policy(rule_id, body, user_email)

    deps.policies.create.side_effect = create

    assert deps.run() == 1
    assert [body.name for _, body, _ in deps.created] == ["Email not null by tag (2)"]


def test_one_failing_rule_does_not_stop_the_others() -> None:
    deps = _Deps({"tag_auto_apply": "true"})
    deps.registry.list_rules.return_value = [
        _rule("r1", "First", {"column": ["a"]}),
        _rule("r2", "Second", {"column": ["b"]}),
    ]
    deps.applied.restamp_tag_auto_to_policy.side_effect = [RuntimeError("boom"), 3]

    assert deps.run() == 1
    assert deps.rows[MIGRATED_MARKER_KEY] == "true"


def test_unreadable_rules_leave_the_marker_unset_for_a_retry() -> None:
    deps = _Deps({"tag_auto_apply": "true"})
    deps.registry.list_rules.side_effect = RuntimeError("db down")

    assert deps.run() == 0

    assert MIGRATED_MARKER_KEY not in deps.rows
    assert deps.rows["tag_auto_apply"] == "true"
