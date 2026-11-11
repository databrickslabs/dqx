"""Unit tests for :class:`TagSuggestionService` (tag-match suggestions).

A monitored table's tag-matched rules are always surfaced as accept-to-attach
SUGGESTIONS (automatic attachment is the job of rule policies). Uses the REAL pure resolver
(``tag_mapping_service.resolve``) so the wiring is exercised end-to-end;
``create_autospec`` fakes stand in for every I/O collaborator and a plain
callable fakes the SP/OBO-authed column-tag reader.
"""

from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
    RuleSlot,
    compute_mapping_hash,
    set_reserved_tag,
    set_slot_tags,
    RESERVED_DIMENSION_KEY,
    RESERVED_NAME_KEY,
    RESERVED_SEVERITY_KEY,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.tag_mapping_service import ColumnInfo
from databricks_labs_dqx_app.backend.services.tag_suggestion_service import (
    TagRuleSuggestion,
    TagSuggestionService,
)


def _rule(
    rule_id: str = "r1",
    *,
    status: str = "approved",
    slot_tags: dict[str, list[str]] | None = None,
    name: str | None = None,
    dimension: str | None = None,
    severity: str | None = None,
) -> RegistryRule:
    slots = [RuleSlot(name="c1", family="any", position=0, cardinality="one")]
    user_metadata: dict[str, object] = {}
    if slot_tags is not None:
        user_metadata = set_slot_tags(user_metadata, slot_tags)
    if name is not None:
        user_metadata = set_reserved_tag(user_metadata, RESERVED_NAME_KEY, name)
    if dimension is not None:
        user_metadata = set_reserved_tag(user_metadata, RESERVED_DIMENSION_KEY, dimension)
    if severity is not None:
        user_metadata = set_reserved_tag(user_metadata, RESERVED_SEVERITY_KEY, severity)
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status=status,
        version=1,
        definition=RuleDefinition(body={"function": "is_not_null", "arguments": {}}, slots=slots),
        user_metadata=user_metadata,
    )


def _detail(binding_id: str, table_fqn: str) -> MonitoredTableDetail:
    return MonitoredTableDetail(table=MonitoredTable(binding_id=binding_id, table_fqn=table_fqn))


def _make_service(
    *,
    columns_by_fqn: dict[str, list[ColumnInfo]],
    detail: MonitoredTableDetail | None = _detail("b1", "cat.sch.t1"),
    read_raises: bool = False,
) -> tuple[TagSuggestionService, dict[str, object]]:
    registry = create_autospec(RegistryService, instance=True)
    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    apply_rules = create_autospec(ApplyRulesService, instance=True)
    monitored_tables.get.return_value = detail
    apply_rules.list_applied.return_value = []

    def read_columns(table_fqn: str) -> list[ColumnInfo]:
        if read_raises:
            raise RuntimeError("boom")
        return columns_by_fqn.get(table_fqn, [])

    svc = TagSuggestionService(
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=apply_rules,
        read_columns=read_columns,
    )
    return svc, {
        "registry": registry,
        "monitored_tables": monitored_tables,
        "apply_rules": apply_rules,
    }


def test_matching_tag_mapped_rule_yields_one_suggestion() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [
        _rule(slot_tags={"c1": ["class.pii"]}, name="No null email", dimension="Completeness", severity="High")
    ]

    result = svc.suggest("b1")

    assert len(result) == 1
    suggestion = result[0]
    assert isinstance(suggestion, TagRuleSuggestion)
    assert suggestion.rule_id == "r1"
    assert suggestion.rule_name == "No null email"
    assert suggestion.dimension == "Completeness"
    assert suggestion.severity == "High"
    assert suggestion.column_mapping == {"c1": "email"}
    assert suggestion.explanation == "Matched tag class.pii"
    deps["registry"].list_rules.assert_called_once_with(status="approved")


def test_already_applied_mapping_is_excluded() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]
    deps["apply_rules"].list_applied.return_value = [
        AppliedRule(binding_id="b1", rule_id="r1", column_mapping=[{"c1": "email"}])
    ]

    assert svc.suggest("b1") == []


def test_rule_without_slot_tags_is_ignored() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [_rule(slot_tags=None)]

    assert svc.suggest("b1") == []


def test_read_columns_failure_returns_empty_no_raise() -> None:
    svc, deps = _make_service(columns_by_fqn={}, read_raises=True)
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]

    assert svc.suggest("b1") == []


def test_unknown_binding_returns_empty() -> None:
    svc, deps = _make_service(columns_by_fqn={}, detail=None)
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]

    assert svc.suggest("does-not-exist") == []


def test_cartesian_one_group_per_matching_column() -> None:
    columns = {
        "cat.sch.t1": [
            ColumnInfo("email", "STRING", ["class.pii"]),
            ColumnInfo("ssn", "STRING", ["class.pii"]),
        ]
    }
    svc, deps = _make_service(columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]

    result = svc.suggest("b1")

    # Cartesian product: a 1-column rule matching BOTH pii columns yields one
    # group per matching column (not a single representative).
    mappings = [s.column_mapping for s in result]
    assert len(result) == 2
    assert {"c1": "email"} in mappings
    assert {"c1": "ssn"} in mappings


def test_excludes_only_the_applied_group_not_the_whole_rule() -> None:
    # email already applied; a fresh table with a different pii column still suggests.
    columns = {"cat.sch.t1": [ColumnInfo("ssn", "STRING", ["class.pii"])]}
    svc, deps = _make_service(columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]
    deps["apply_rules"].list_applied.return_value = [
        AppliedRule(binding_id="b1", rule_id="r1", column_mapping=[{"c1": "email"}])
    ]

    result = svc.suggest("b1")

    assert len(result) == 1
    assert result[0].column_mapping == {"c1": "ssn"}
    # sanity: the applied hash is for a different group.
    assert compute_mapping_hash([{"c1": "email"}]) != compute_mapping_hash([{"c1": "ssn"}])


# ---------------------------------------------------------------------------
# Always on: no setting gates suggestions, and nothing is ever attached
# ---------------------------------------------------------------------------


def test_suggestions_are_always_returned_and_nothing_is_attached() -> None:
    """Suggestions do not depend on any setting, and suggesting never writes an applied rule."""
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]

    suggestions = svc.suggest("b1")

    assert [s.column_mapping for s in suggestions] == [{"c1": "email"}]
    deps["apply_rules"].attach_policy_mapping.assert_not_called()
