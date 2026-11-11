"""Tests for rule policy domain models and their validation."""

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.policy_models import (
    ColumnCondition,
    ConditionExpr,
    ConditionRow,
    PolicyScope,
    RulePolicyIn,
    TableCondition,
)


def _row(obj, predicate, value, combinator=None) -> dict:
    return {"combinator": combinator, "object": obj, "predicate": predicate, "value": value}


def test_valid_table_builder_condition():
    cond = TableCondition(
        kind="builder",
        expr={
            "rows": [
                _row("table", "has_tag", "class=pii"),
                _row("schema", "has_owner", "data-eng", "AND"),
            ]
        },
    )
    assert cond.expr is not None
    assert [r.combinator for r in cond.expr.rows] == [None, "AND"]


def test_column_condition_rejects_has_owner():
    with pytest.raises(ValidationError):
        ColumnCondition(kind="builder", expr={"rows": [_row("column", "has_owner", "bob")]})


def test_table_condition_rejects_column_object():
    with pytest.raises(ValidationError):
        TableCondition(kind="builder", expr={"rows": [_row("column", "has_name", "id")]})


def test_column_condition_requires_column_object_rows():
    with pytest.raises(ValidationError):
        ColumnCondition(kind="builder", expr={"rows": [_row("table", "has_tag", "x")]})


def test_first_row_combinator_rejected():
    with pytest.raises(ValidationError):
        ConditionExpr(rows=[ConditionRow(combinator="AND", object="table", predicate="has_tag", value="x")])


def test_later_row_requires_combinator():
    with pytest.raises(ValidationError):
        ConditionExpr(rows=[_row("table", "has_tag", "a"), _row("table", "has_tag", "b")])


def test_tags_kind_requires_tags():
    with pytest.raises(ValidationError):
        TableCondition(kind="tags", tags=[])
    with pytest.raises(ValidationError):
        ColumnCondition(kind="tags", tags=[])


def test_builder_kind_requires_expr():
    with pytest.raises(ValidationError):
        TableCondition(kind="builder")


def test_none_kind_needs_nothing():
    assert TableCondition().kind == "none"


@pytest.mark.parametrize("tag", ["\nkey", "", "=value", "k\x00ey", "a" * 256])
def test_invalid_tags_rejected(tag):
    with pytest.raises(ValidationError):
        TableCondition(kind="tags", tags=[tag])


@pytest.mark.parametrize("tag", ["class", "class=pii", "class.x=a=b"])
def test_valid_tags_accepted(tag):
    assert TableCondition(kind="tags", tags=[tag]).tags == [tag]


def test_value_control_chars_rejected():
    with pytest.raises(ValidationError):
        ConditionRow(object="table", predicate="has_name", value="a\nb")


def test_scope_defaults_to_all():
    scope = PolicyScope()
    assert scope.catalogs == "*"
    assert scope.schemas == "*"
    assert scope.tables == "*"


def test_schema_requires_two_parts():
    with pytest.raises(ValidationError):
        PolicyScope(schemas=["cat"])
    assert PolicyScope(schemas=["cat.sch"]).schemas == ["cat.sch"]


def test_table_requires_three_parts():
    with pytest.raises(ValidationError):
        PolicyScope(tables=["cat.sch"])
    assert PolicyScope(tables=["cat.sch.tbl"]).tables == ["cat.sch.tbl"]


def test_scope_lists_must_be_non_empty():
    with pytest.raises(ValidationError):
        PolicyScope(catalogs=[])


def test_scope_rejects_backtick_identifiers():
    with pytest.raises(ValidationError):
        PolicyScope(catalogs=["bad`name"])
    with pytest.raises(ValidationError):
        PolicyScope(schemas=["cat.sc`h"])


def test_policy_in_defaults():
    body = RulePolicyIn(name="p")
    assert body.register_tables is False
    assert body.column_conditions == {}
    assert body.table_condition.kind == "none"
