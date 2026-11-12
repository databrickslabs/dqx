"""One-off data migration: legacy tag auto-apply -> rule policies.

Tag auto-apply (the ``tag_auto_apply`` admin setting) attached every approved
rule with ``slot_tags`` wherever its tags matched. Rule policies replace it.
When the legacy setting was on, each rule with ``slot_tags`` (any status) gets a
policy ``"{rule name} by tag"`` scoped to everything, whose column conditions are
the rule's slot tags, and the rule's ``origin=tag_auto`` applied rows are
re-stamped as made by that policy (so removing one still records a suppression
against the policy). Each migrated rule is recorded (``MIGRATED_RULES_KEY``) so a
retry never processes it again — a policy an admin deleted in between stays
deleted. A rule that can never migrate (both policy names taken, legacy tags that
fail validation) is skipped and logged. Once no rule failed transiently, the legacy
setting is deleted and a marker setting is written so the migration runs once;
after a transient failure both are left as they are so the next startup retries. ``slot_tags`` are kept: they still drive tag
suggestions.
"""

import json
import logging

from databricks_labs_dqx_app.backend.policy_models import ColumnCondition, PolicyScope, RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.registry_models import RegistryRule, get_rule_name, get_slot_tags
from databricks_labs_dqx_app.backend.sanitization import safe_log_value
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import DuplicatePolicyNameError, RulePolicyService

logger = logging.getLogger(__name__)

LEGACY_TAG_AUTO_APPLY_KEY = "tag_auto_apply"
MIGRATED_MARKER_KEY = "rule_policies_migrated"
MIGRATED_RULES_KEY = "rule_policies_migrated_rules"  # JSON list of rule ids already migrated (retry progress)
_MARKER_VALUE = "true"
_SYSTEM_USER = "system"
_NAME_SUFFIX = " by tag"
_MAX_RULE_NAME_LEN = 180  # leaves room for the suffix and " (2)" within the 200-character policy name limit


def migrate_slot_tags_to_policies(
    settings: AppSettingsService,
    registry: RegistryService,
    policies: RulePolicyService,
    applied: ApplyRulesService,
) -> int:
    """Turn legacy tag auto-apply into rule policies once; return the number of rules migrated.

    Idempotent (a marker setting skips later runs; a re-run after a partial run reuses
    the policies it already created instead of adding copies) and never raises: a failing rule
    is logged and skipped. When the setting or the rules cannot be read, or any rule failed
    transiently, nothing is marked so the next startup retries (skipping rules already
    migrated). Permanent failures (``ValueError``: name collisions, invalid legacy tags) are
    logged and do not block completion.
    """
    try:
        if settings.get_setting(MIGRATED_MARKER_KEY) == _MARKER_VALUE:
            return 0
        flag = settings.get_setting(LEGACY_TAG_AUTO_APPLY_KEY)
        rules = registry.list_rules() if flag is not None and flag.strip().lower() == "true" else []
        done = _read_progress(settings.get_setting(MIGRATED_RULES_KEY)) if rules else set()
    except Exception:
        logger.warning("Rule policy migration could not read settings or rules; will retry", exc_info=True)
        return 0
    created = 0
    failed = 0
    for rule in rules:
        slot_tags = {slot: tags for slot, tags in get_slot_tags(rule.user_metadata).items() if tags}
        if not slot_tags or rule.rule_id in done:
            continue
        try:
            policy = _create_policy(policies, rule, slot_tags)
            applied.restamp_tag_auto_to_policy(rule.rule_id, policy.policy_id)
            done.add(rule.rule_id)
            settings.save_setting(MIGRATED_RULES_KEY, json.dumps(sorted(done)), user_email=_SYSTEM_USER)
            created += 1
        except ValueError:  # permanent (validation, both names taken): retrying cannot help
            logger.warning(
                f"Rule policy migration cannot migrate rule {safe_log_value(rule.rule_id)}; skipped", exc_info=True
            )
        except Exception:
            failed += 1
            logger.warning(f"Rule policy migration skipped rule {safe_log_value(rule.rule_id)}", exc_info=True)
    if failed:
        logger.warning(f"Rule policy migration left {failed} rule(s) unmigrated; will retry on the next startup")
        return created
    try:
        settings.delete_setting(LEGACY_TAG_AUTO_APPLY_KEY)
        settings.save_setting(MIGRATED_MARKER_KEY, _MARKER_VALUE, user_email=_SYSTEM_USER)
    except Exception:
        logger.warning("Rule policy migration could not record completion", exc_info=True)
    if created:
        logger.info(f"Migrated {created} tag auto-apply rule(s) to rule policies")
    return created


def _create_policy(policies: RulePolicyService, rule: RegistryRule, slot_tags: dict[str, list[str]]) -> RulePolicy:
    base = (get_rule_name(rule.user_metadata) or rule.rule_id)[:_MAX_RULE_NAME_LEN].strip() + _NAME_SUFFIX
    conditions = {slot: ColumnCondition(kind="tags", tags=tags) for slot, tags in slot_tags.items()}
    body = RulePolicyIn(name=base, scope=PolicyScope(), column_conditions=conditions)
    suffixed = f"{base} (2)"
    # A re-run after a partial migration finds its own earlier policy: reuse it.
    for existing in policies.list_for_rule(rule.rule_id):
        if existing.created_by == _SYSTEM_USER and existing.name in (base, suffixed):
            return existing
    try:
        return policies.create(rule.rule_id, body, _SYSTEM_USER)
    except DuplicatePolicyNameError:
        # The name is taken by a user's policy.
        return policies.create(rule.rule_id, body.model_copy(update={"name": suffixed}), _SYSTEM_USER)


def _read_progress(raw: str | None) -> set[str]:
    """Rule ids recorded by an earlier, partial run; an unreadable value counts as none."""
    if not raw:
        return set()
    try:
        value = json.loads(raw)
    except ValueError:
        logger.warning("Rule policy migration progress is unreadable; starting over")
        return set()
    return {item for item in value if isinstance(item, str)} if isinstance(value, list) else set()
