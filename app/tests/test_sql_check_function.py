"""Unit tests for the shared sql_query / sql_expression decision (materializer + Test)."""

from databricks_labs_dqx_app.backend.sql_check_function import sql_check_function


def test_body_with_sql_query_key_materializes_to_sql_query():
    assert sql_check_function({"sql_query": "SELECT (x) AS condition FROM {{input_view}}"}) == "sql_query"


def test_body_with_predicate_key_materializes_to_sql_expression():
    assert sql_check_function({"predicate": "{{amount}} > 0"}) == "sql_expression"


def test_sql_query_key_wins_over_a_predicate_key():
    body = {"predicate": "{{amount}} > 0", "sql_query": "SELECT (x) AS condition FROM {{input_view}}"}
    assert sql_check_function(body) == "sql_query"


def test_query_text_under_a_predicate_key_is_still_a_row_expression():
    # The decision is the stored key, never the text: a SELECT saved as `predicate`
    # materializes as a row expression, so the Test run must treat it as one too.
    assert sql_check_function({"predicate": "SELECT (x) AS condition FROM {{input_view}}"}) == "sql_expression"


def test_empty_body_defaults_to_sql_expression():
    assert sql_check_function({}) == "sql_expression"
