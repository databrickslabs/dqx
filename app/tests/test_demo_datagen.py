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


def test_identifiers_are_validated_no_injection():
    import pytest

    with pytest.raises(ValueError):
        d.build_create_table_sql("customers; DROP TABLE x", CAT, SCH)


def test_build_column_comment_sql_quotes_column_identifier():
    from databricks_labs_dqx_app.backend.sql_utils import quote_ident

    sql = d.build_column_comment_sql("customers", "card_last4", "last 4 digits", CAT, SCH)
    assert quote_ident("card_last4") in sql
    # bare unquoted form must NOT appear between ALTER COLUMN and COMMENT
    import re
    assert not re.search(r"ALTER COLUMN\s+card_last4\s+COMMENT", sql)
