from databricks_labs_dqx_app.backend.registry_models import RuleSlot
from databricks_labs_dqx_app.backend.services.tag_mapping_service import (
    ColumnInfo,
    family_for_type,
    parse_tag,
    resolve,
)


def _slot(name, family="any", position=0):
    return RuleSlot(name=name, family=family, position=position, cardinality="one")


def test_family_for_type():
    assert family_for_type("INT") == "numeric"
    assert family_for_type("DECIMAL(10,2)") == "numeric"
    assert family_for_type("STRING") == "text"
    assert family_for_type("TIMESTAMP") == "temporal"
    assert family_for_type("BOOLEAN") == "boolean"
    assert family_for_type("ARRAY<INT>") == "array"
    assert family_for_type("MAP<STRING,INT>") == "any"


def test_parse_tag():
    assert parse_tag("class.pii") == ("class.pii", None)
    assert parse_tag("class.name=given") == ("class.name", "given")


def test_single_slot_any_overlap():
    slots = [_slot("c1", "any")]
    slot_tags = {"c1": ["class.pii", "class.sensitive"]}
    cols = [
        ColumnInfo("email", "STRING", ["class.pii"]),
        ColumnInfo("age", "INT", ["class.other"]),
        ColumnInfo("ssn", "STRING", ["class.sensitive", "class.pii"]),
    ]
    groups = resolve(slots, slot_tags, cols)
    # email + ssn match (any overlap); age has no overlapping class tag
    assert groups == [{"c1": "email"}, {"c1": "ssn"}]


def test_family_still_enforced():
    slots = [_slot("c1", "text")]
    slot_tags = {"c1": ["class.pii"]}
    cols = [
        ColumnInfo("email", "STRING", ["class.pii"]),   # text OK
        ColumnInfo("pii_id", "INT", ["class.pii"]),      # tag matches but family text != numeric
    ]
    assert resolve(slots, slot_tags, cols) == [{"c1": "email"}]


def test_value_optional_vs_exact():
    slots = [_slot("c1", "any")]
    cols = [
        ColumnInfo("a", "STRING", ["class.name=given"]),
        ColumnInfo("b", "STRING", ["class.name=family"]),
    ]
    # bare key matches any value
    assert resolve(slots, {"c1": ["class.name"]}, cols) == [{"c1": "a"}, {"c1": "b"}]
    # key=value requires exact
    assert resolve(slots, {"c1": ["class.name=given"]}, cols) == [{"c1": "a"}]


def test_any_tag_matches():
    # Governed tags of any namespace match — a bare "pii" slot tag matches a
    # column tagged "pii", and a "owner=finance" key=value matches exactly.
    slots = [_slot("c1", "any")]
    cols = [ColumnInfo("a", "STRING", ["pii", "owner=finance"])]
    assert resolve(slots, {"c1": ["pii"]}, cols) == [{"c1": "a"}]
    assert resolve(slots, {"c1": ["owner=finance"]}, cols) == [{"c1": "a"}]
    assert resolve(slots, {"c1": ["owner=marketing"]}, cols) == []


def test_multi_slot_cartesian_and_all_filled():
    slots = [_slot("c1", "any", 0), _slot("c2", "any", 1)]
    slot_tags = {"c1": ["class.a"], "c2": ["class.b"]}
    cols = [
        ColumnInfo("x1", "STRING", ["class.a"]),
        ColumnInfo("x2", "STRING", ["class.a"]),
        ColumnInfo("y1", "STRING", ["class.b"]),
    ]
    groups = resolve(slots, slot_tags, cols)
    assert groups == [{"c1": "x1", "c2": "y1"}, {"c1": "x2", "c2": "y1"}]


def test_multi_slot_missing_slot_yields_nothing():
    slots = [_slot("c1", "any", 0), _slot("c2", "any", 1)]
    slot_tags = {"c1": ["class.a"], "c2": ["class.b"]}
    cols = [ColumnInfo("x1", "STRING", ["class.a"])]  # nothing matches c2
    assert resolve(slots, slot_tags, cols) == []


def test_no_column_used_twice_in_a_group():
    slots = [_slot("c1", "any", 0), _slot("c2", "any", 1)]
    slot_tags = {"c1": ["class.a"], "c2": ["class.a"]}
    cols = [ColumnInfo("x1", "STRING", ["class.a"]), ColumnInfo("x2", "STRING", ["class.a"])]
    groups = resolve(slots, slot_tags, cols)
    # (x1,x1) and (x2,x2) excluded; only cross pairings
    assert {"c1": "x1", "c2": "x2"} in groups
    assert {"c1": "x1", "c2": "x1"} not in groups


def test_single_flag_returns_one_group():
    slots = [_slot("c1", "any")]
    cols = [ColumnInfo("a", "STRING", ["class.pii"]), ColumnInfo("b", "STRING", ["class.pii"])]
    assert resolve(slots, {"c1": ["class.pii"]}, cols, single=True) == [{"c1": "a"}]


def test_slot_with_no_declared_tags_yields_nothing():
    slots = [_slot("c1", "any")]
    cols = [ColumnInfo("a", "STRING", ["class.pii"])]
    assert resolve(slots, {}, cols) == []
