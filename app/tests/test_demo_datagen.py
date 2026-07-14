from databricks_labs_dqx_app.backend.demo import datagen as d

CAT, SCH = "dqx", "dqx_studio_demo"


def test_create_table_sql_is_fully_qualified_and_deterministic():
    sql = d.build_create_table_sql("customers", CAT, SCH)
    assert "CREATE OR REPLACE TABLE" in sql
    assert f"{CAT}.{SCH}.customers" in sql
    assert "range(" in sql  # server-side generation
    assert "pmod(hash(" in sql  # deterministic seeded issues


def test_mutation_sql_is_a_list_of_full_column_overwrites():
    stmts = d.build_mutation_sql("orders", week=4, weeks=9, catalog=CAT, schema=SCH)
    assert isinstance(stmts, list) and stmts
    for s in stmts:
        assert s.strip().upper().startswith("UPDATE")
        assert f"{CAT}.{SCH}.orders" in s


def test_baseline_reset_covers_every_table():
    stmts = d.build_baseline_reset_sql(CAT, SCH)
    joined = "\n".join(stmts)
    for t in ("customers", "orders", "payments", "products", "shipments"):
        assert f"{CAT}.{SCH}.{t}" in joined


def test_set_column_tag_sql_escapes_and_quotes():
    from databricks_labs_dqx_app.backend.demo.manifest import ColumnTagSpec

    sql = d.build_set_column_tag_sql(ColumnTagSpec("customers", "first_name", "class.name"), CAT, SCH)
    assert "ALTER TABLE" in sql and "ALTER COLUMN" in sql and "SET TAGS" in sql
    assert "class.name" in sql


def test_set_column_tag_rejects_non_class_namespace():
    from databricks_labs_dqx_app.backend.demo.manifest import ColumnTagSpec
    import pytest

    with pytest.raises(ValueError):
        d.build_set_column_tag_sql(ColumnTagSpec("customers", "first_name", "pii.name"), CAT, SCH)


def test_identifiers_are_validated_no_injection():
    import pytest

    with pytest.raises(ValueError):
        d.build_create_table_sql("customers; DROP TABLE x", CAT, SCH)
