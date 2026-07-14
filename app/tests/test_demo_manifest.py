from databricks_labs_dqx_app.backend.demo import manifest as m


def test_every_rule_has_contract_compliant_name_and_description():
    assert m.RULES, "expected a non-empty rule set"
    for r in m.RULES:
        assert r.name and len(r.name) <= 80, f"{r.key}: name empty or >80 chars"
        # minimal phrasing: no leading article
        first = r.name.split()[0].lower()
        assert first not in {"a", "an", "the"}, f"{r.key}: name starts with an article: {r.name!r}"
        assert r.description.endswith("."), f"{r.key}: description not one sentence: {r.description!r}"
        assert r.description.count(".") == 1, f"{r.key}: description has >1 sentence: {r.description!r}"
        dfirst = r.description.split()[0].lower()
        assert dfirst not in {"a", "an", "the"}, f"{r.key}: description starts with an article"
        assert r.dimension in {"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"}
        assert r.severity in {"Low", "Medium", "High", "Critical"}
        assert r.mode in {"dqx_native", "lowcode", "sql"}


def test_all_six_dimensions_and_all_severities_are_covered():
    dims = {r.dimension for r in m.RULES}
    sevs = {r.severity for r in m.RULES}
    assert dims == {"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"}
    assert sevs == {"Low", "Medium", "High", "Critical"}


def test_binding_mappings_reference_real_rules_and_fill_declared_slots():
    for b in m.BINDINGS:
        table = next(t for t in m.TABLES if t.name == b.table)
        for rule_key, groups in b.mappings.items():
            rule = m.RULES_BY_KEY[rule_key]  # KeyError => bad manifest
            slot_names = {s.name for s in rule.slots}
            for group in groups:
                assert set(group.keys()) == slot_names, f"{b.table}/{rule_key}: group {group} != slots {slot_names}"
                for col in group.values():
                    assert col in table.columns, f"{b.table}/{rule_key}: column {col} not in {table.name}"


def test_slot_tags_are_class_namespaced_and_reference_real_slots():
    for r in m.RULES:
        for slot_name, tags in r.slot_tags.items():
            assert slot_name in {s.name for s in r.slots}, f"{r.key}: slot_tags names unknown slot {slot_name}"
            for tag in tags:
                assert tag.startswith("class."), f"{r.key}: slot tag not class.*: {tag}"


def test_column_tags_target_real_columns():
    for ct in m.COLUMN_TAGS:
        table = next(t for t in m.TABLES if t.name == ct.table)
        assert ct.column in table.columns
        assert ct.tag.startswith("class.")


def test_data_products_reference_real_tables():
    names = {t.name for t in m.TABLES}
    for dp in m.DATA_PRODUCTS:
        assert dp.members, f"{dp.name}: no members"
        assert set(dp.members) <= names


def test_active_mapping_honours_lifecycle_windows():
    ship = next(b for b in m.BINDINGS if b.table == "shipments")
    # min_len is added at week 5 per the story
    assert ("shipments", "min_len") in m.RULE_LIFECYCLE
    start, end = m.RULE_LIFECYCLE[("shipments", "min_len")]
    assert "min_len" not in m.active_mapping(ship, start - 1)
    assert "min_len" in m.active_mapping(ship, start)
