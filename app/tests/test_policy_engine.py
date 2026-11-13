from collections.abc import Iterable

from databricks_labs_dqx_app.backend.policy_models import (
    ColumnCondition,
    ConditionExpr,
    ConditionRow,
    PolicyScope,
    RulePolicy,
    TableCondition,
)
from databricks_labs_dqx_app.backend.registry_models import RuleSlot
from databricks_labs_dqx_app.backend.services.policy_engine import (
    ColumnSnapshot,
    TableSnapshot,
    column_condition_holds,
    evaluate,
    ALL_NEEDS,
    SnapshotNeeds,
    in_scope,
    like,
    needs_of,
    table_condition_holds,
)


def snap(
    columns: Iterable[ColumnSnapshot] = (),
    table_tags: Iterable[str] = (),
    owner: str | None = None,
    catalog_owner: str | None = None,
    schema_owner: str | None = None,
    catalog_tags: Iterable[str] = (),
    fqn: str = "c.s.t",
) -> TableSnapshot:
    return TableSnapshot(
        fqn, owner, catalog_owner, schema_owner, tuple(catalog_tags), (), tuple(table_tags), tuple(columns)
    )


def col(name: str, type_name: str = "STRING", *tags: str) -> ColumnSnapshot:
    return ColumnSnapshot(name, type_name, tuple(tags))


def row(obj: str, pred: str, value: str, comb: str | None = None) -> ConditionRow:
    return ConditionRow(combinator=comb, object=obj, predicate=pred, value=value)


def tcond(*rows: ConditionRow) -> TableCondition:
    return TableCondition(kind="builder", expr=ConditionExpr(rows=list(rows)))


def policy(
    column_conditions: dict[str, ColumnCondition] | None = None,
    scope: PolicyScope | None = None,
    table_condition: TableCondition | None = None,
) -> RulePolicy:
    return RulePolicy(
        name="p",
        policy_id="p1",
        rule_id="r1",
        scope=scope or PolicyScope(),
        table_condition=table_condition or TableCondition(),
        column_conditions=column_conditions or {},
    )


def tag_cond(*tags: str) -> ColumnCondition:
    return ColumnCondition(kind="tags", tags=list(tags))


def slot(name: str, position: int = 0, family: str = "any", cardinality: str = "one") -> RuleSlot:
    return RuleSlot(name=name, family=family, position=position, cardinality=cardinality)


def test_out_of_scope_no_match():
    p = policy(scope=PolicyScope(catalogs=["other"]))
    assert evaluate(p, [], snap()) == []


def test_scope_star_and_lists_case_insensitive():
    assert in_scope(PolicyScope(), "c.s.t")
    assert in_scope(PolicyScope(catalogs=["C"], schemas=["c.S"], tables=["C.S.T"]), "c.s.t")
    assert not in_scope(PolicyScope(schemas=["c.x"]), "c.s.t")


def test_table_condition_tags_any_overlap_bare_key_matches_any_value():
    cond = TableCondition(kind="tags", tags=["domain", "tier=gold"])
    assert table_condition_holds(cond, snap(table_tags=["domain=sales"]))
    assert table_condition_holds(cond, snap(table_tags=["tier=gold"]))
    assert not table_condition_holds(cond, snap(table_tags=["tier=silver"]))


def test_table_builder_and_binds_tighter_than_or():
    # A OR B AND C
    cond = tcond(
        row("table", "has_tag", "a"),
        row("table", "has_tag", "b", "OR"),
        row("table", "has_tag", "c", "AND"),
    )
    assert table_condition_holds(cond, snap(table_tags=["a"]))
    assert table_condition_holds(cond, snap(table_tags=["b", "c"]))
    assert not table_condition_holds(cond, snap(table_tags=["b"]))


def test_table_builder_catalog_schema_table_objects():
    cond = tcond(
        row("schema", "has_owner", "Alice"),
        row("catalog", "has_name", "C", "AND"),
        row("table", "has_tag", "pii", "AND"),
    )
    assert table_condition_holds(cond, snap(schema_owner="alice", table_tags=["pii"]))
    assert not table_condition_holds(cond, snap(schema_owner="bob", table_tags=["pii"]))
    assert not table_condition_holds(cond, snap(schema_owner="alice"))


def test_not_has_tag():
    cond = tcond(row("table", "not_has_tag", "skip"))
    assert table_condition_holds(cond, snap())
    assert not table_condition_holds(cond, snap(table_tags=["skip"]))
    ccond = ColumnCondition(kind="builder", expr=ConditionExpr(rows=[row("column", "name_like", "%_id")]))
    assert column_condition_holds(ccond, col("user_id"))


def test_like_is_literal_for_regex_metachars():
    assert not like("aXb_c", "a.b%")
    assert like("a.b_c", "a.b%")
    assert like("A.B", "a.b")
    assert like("ab", "a_")


def test_table_level_rule_one_match():
    out = evaluate(policy(), [], snap())
    assert len(out) == 1
    assert out[0].column_mapping == {} and out[0].status == "auto" and out[0].reason == "table-level"


def test_single_slot_one_match_per_candidate_family_filtered():
    p = policy({"c": tag_cond("class=email")})
    s = snap([col("e1", "STRING", "class=email"), col("n", "INT", "class=email"), col("e2", "STRING", "class=email")])
    out = evaluate(p, [slot("c", family="text")], s)
    assert [m.column_mapping for m in out] == [{"c": "e1"}, {"c": "e2"}]
    assert all(m.status == "auto" and m.reason == "matched class=email" for m in out)


def test_cardinality_many_binds_all():
    p = policy({"c": tag_cond("k")})
    out = evaluate(p, [slot("c", cardinality="many")], snap([col("b", "STRING", "k"), col("a", "STRING", "k")]))
    assert len(out) == 1 and out[0].column_mapping == {"c": "a,b"}


def test_missing_slot_condition_no_match():
    assert evaluate(policy(), [slot("c")], snap([col("a")])) == []


def test_partial_when_a_slot_has_no_candidates():
    p = policy({"a": tag_cond("x"), "b": tag_cond("y")})
    out = evaluate(p, [slot("a"), slot("b", 1)], snap([col("c1", "STRING", "x")]))
    assert len(out) == 1
    assert out[0].status == "partial" and out[0].column_mapping == {"a": "c1"}
    assert out[0].reason == "no candidate for b"


def test_two_slots_unique_candidates_auto():
    p = policy({"a": tag_cond("x"), "b": tag_cond("y")})
    out = evaluate(p, [slot("a"), slot("b", 1)], snap([col("p", "STRING", "x"), col("q", "STRING", "y")]))
    assert len(out) == 1 and out[0].status == "auto"
    assert out[0].column_mapping == {"a": "p", "b": "q"} and out[0].reason == "single candidate per input"


def test_two_slots_pairs_by_name_stem():
    p = policy({"a": tag_cond("s"), "b": tag_cond("e")})
    s = snap(
        [
            col("order_start_date", "DATE", "s"),
            col("ship_start_date", "DATE", "s"),
            col("order_end_date", "DATE", "e"),
            col("ship_end_date", "DATE", "e"),
        ]
    )
    out = evaluate(p, [slot("a"), slot("b", 1)], s)
    assert len(out) == 2 and all(m.status == "auto" for m in out)
    assert {m.reason for m in out} == {"paired by name order_*_date", "paired by name ship_*_date"}
    assert {m.column_mapping["a"]: m.column_mapping["b"] for m in out} == {
        "order_start_date": "order_end_date",
        "ship_start_date": "ship_end_date",
    }


def test_two_slots_tie_is_ambiguous():
    p = policy({"a": tag_cond("s"), "b": tag_cond("e")})
    s = snap([col("start_ts", "TIMESTAMP", "s"), col("end_a", "TIMESTAMP", "e"), col("end_b", "TIMESTAMP", "e")])
    out = evaluate(p, [slot("a"), slot("b", 1)], s)
    assert all(m.status == "ambiguous" and m.reason == "several possible columns" for m in out)
    assert sorted(m.column_mapping["b"] for m in out) == ["end_a", "end_b"]
    assert {m.column_mapping["a"] for m in out} == {"start_ts"}


def test_two_by_two_all_tie_yields_no_auto():
    p = policy({"a": tag_cond("s"), "b": tag_cond("e")})
    s = snap(
        [
            col("order_start", "DATE", "s"),
            col("order_begin", "DATE", "s"),
            col("order_end", "DATE", "e"),
            col("order_stop", "DATE", "e"),
        ]
    )
    out = evaluate(p, [slot("a"), slot("b", 1)], s)
    assert len(out) == 4
    assert all(m.status == "ambiguous" for m in out)


def test_column_never_fills_two_slots():
    p = policy({"a": tag_cond("k"), "b": tag_cond("k")})
    out = evaluate(p, [slot("a"), slot("b", 1)], snap([col("only", "STRING", "k")]))
    assert len(out) == 1
    assert out[0].status == "partial"
    assert out[0].column_mapping == {"a": "only"}
    assert out[0].reason == "no candidate for b"


def test_two_many_slots_do_not_share_columns():
    p = policy({"a": tag_cond("k"), "b": tag_cond("k")})
    s = snap([col("x", "STRING", "k"), col("y", "STRING", "k")])
    out = evaluate(p, [slot("a", cardinality="many"), slot("b", 1, cardinality="many")], s)
    assert len(out) == 1
    assert out[0].column_mapping == {"a": "x,y"}
    assert out[0].status == "partial"


def test_one_slot_plus_many_slot_binds_remaining():
    p = policy({"a": tag_cond("one"), "b": tag_cond("k")})
    s = snap([col("x", "STRING", "one", "k"), col("y", "STRING", "k"), col("z", "STRING", "k")])
    out = evaluate(p, [slot("a"), slot("b", 1, cardinality="many")], s)
    assert len(out) == 1 and out[0].status == "auto"
    assert out[0].column_mapping == {"a": "x", "b": "y,z"}


def test_one_slot_with_several_candidates_plus_many_slot_one_match_each():
    p = policy({"a": tag_cond("one"), "b": tag_cond("m")})
    s = snap(
        [col("x1", "STRING", "one"), col("x2", "STRING", "one"), col("m1", "STRING", "m"), col("m2", "STRING", "m")]
    )
    out = evaluate(p, [slot("a"), slot("b", 1, cardinality="many")], s)
    assert [m.column_mapping for m in out] == [{"a": "x1", "b": "m1,m2"}, {"a": "x2", "b": "m1,m2"}]
    assert all(m.status == "auto" for m in out)


def test_too_many_combinations_is_single_ambiguous():
    p = policy({"a": tag_cond("k"), "b": tag_cond("k")})
    cols = [col(f"c{i:03d}", "STRING", "k") for i in range(101)]
    out = evaluate(p, [slot("a"), slot("b", 1)], snap(cols))
    assert len(out) == 1 and out[0].status == "ambiguous" and out[0].column_mapping == {}
    assert out[0].reason == "too many possible column combinations"


# ---------------------------------------------------------------------------
# Facts a policy needs from a snapshot (need-driven reads)
# ---------------------------------------------------------------------------


def _needs_policy(table: TableCondition | None = None, **columns: ColumnCondition) -> RulePolicy:
    return RulePolicy(
        name="p",
        policy_id="p1",
        rule_id="r1",
        table_condition=table or TableCondition(),
        column_conditions=columns,
    )


def _rows(*rows: tuple[str, str]) -> ConditionExpr:
    return ConditionExpr(
        rows=[
            ConditionRow(combinator=None if i == 0 else "AND", object=obj, predicate=pred, value="x")
            for i, (obj, pred) in enumerate(rows)
        ]
    )


def test_needs_of_column_tags_only():
    needs = needs_of([_needs_policy(column=ColumnCondition(kind="tags", tags=["class=pii"]))])
    assert needs == SnapshotNeeds(column_tags=True)


def test_needs_of_column_name_rows_need_no_tags():
    cond = ColumnCondition(kind="builder", expr=_rows(("column", "name_like"), ("column", "has_name")))
    assert needs_of([_needs_policy(column=cond)]) == SnapshotNeeds()


def test_needs_of_table_builder_rows_pick_their_level():
    table = TableCondition(
        kind="builder",
        expr=_rows(("schema", "has_tag"), ("catalog", "has_owner"), ("table", "not_has_tag"), ("schema", "has_owner")),
    )
    assert needs_of([_needs_policy(table)]) == SnapshotNeeds(
        table_tags=True, schema_tags=True, schema_owner=True, catalog_owner=True
    )


def test_needs_of_unions_policies_and_table_tags_condition():
    first = _needs_policy(TableCondition(kind="tags", tags=["domain"]))
    second = _needs_policy(TableCondition(kind="builder", expr=_rows(("catalog", "has_tag"))))
    assert needs_of([first, second]) == SnapshotNeeds(table_tags=True, catalog_tags=True)
    assert ALL_NEEDS.covers(needs_of([first, second])) and not SnapshotNeeds().covers(ALL_NEEDS)
