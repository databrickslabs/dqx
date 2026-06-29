import json
import logging
from unittest.mock import Mock

from databricks.labs.dqx.llm.llm_core import (
    DspyRuleGeneration,
    DspyRuleGenerationWithSchemaInference,
    DspyRuleUsingDataStats,
    _filter_unsafe_sql_rules,
)


def _make_mock_prediction(quality_rules: str) -> Mock:
    pred = Mock()
    pred.quality_rules = quality_rules
    return pred


# --- _filter_unsafe_sql_rules (pure-function tests — accepts pre-parsed list) ---


def test_filter_unsafe_sql_rules_safe_query_kept():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "SELECT id, condition FROM {{ orders }}"}}}]
    result = _filter_unsafe_sql_rules(rules)
    assert len(result) == 1
    assert result[0]["check"]["arguments"]["query"] == "SELECT id, condition FROM {{ orders }}"


def test_filter_unsafe_sql_rules_drops_drop_table():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}]
    assert _filter_unsafe_sql_rules(rules) == []


def test_filter_unsafe_sql_rules_drops_delete():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "DELETE FROM orders WHERE 1=1"}}}]
    assert _filter_unsafe_sql_rules(rules) == []


def test_filter_unsafe_sql_rules_drops_insert():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "INSERT INTO bad VALUES (1)"}}}]
    assert _filter_unsafe_sql_rules(rules) == []


def test_filter_unsafe_sql_rules_keeps_non_sql_query_rules():
    rules = [
        {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        {"check": {"function": "sql_query", "arguments": {"query": "DELETE FROM orders WHERE 1=1"}}},
    ]
    result = _filter_unsafe_sql_rules(rules)
    assert len(result) == 1
    assert result[0]["check"]["function"] == "is_not_null"


def test_filter_unsafe_sql_rules_mixed_queries():
    rules = [
        {"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}},
        {"check": {"function": "sql_query", "arguments": {"query": "TRUNCATE TABLE orders"}}},
    ]
    result = _filter_unsafe_sql_rules(rules)
    assert len(result) == 1
    assert "SELECT" in result[0]["check"]["arguments"]["query"]


def test_filter_unsafe_sql_rules_empty_array():
    assert _filter_unsafe_sql_rules([]) == []


def test_filter_unsafe_sql_rules_non_list_returns_empty():
    assert _filter_unsafe_sql_rules(None) == []
    assert _filter_unsafe_sql_rules("not a list") == []
    assert _filter_unsafe_sql_rules({"check": {}}) == []


def test_filter_unsafe_sql_rules_skips_malformed_entries():
    rules = [
        "not a rule",
        {"check": "still not a rule"},
        {"check": {"function": "sql_query", "arguments": {"query": "SELECT 1 FROM {{ v }}"}}},
    ]
    result = _filter_unsafe_sql_rules(rules)
    assert len(result) == 1
    assert result[0]["check"]["function"] == "sql_query"


def test_filter_unsafe_sql_rules_drops_non_dict_arguments():
    # arguments is a string — cannot extract query safely, rule is dropped
    rules = [{"check": {"function": "sql_query", "arguments": "DROP TABLE x"}}]
    assert _filter_unsafe_sql_rules(rules) == []


def test_filter_unsafe_sql_rules_drops_non_string_query():
    # query is an int — not a string, crash-safe, rule is dropped
    rules = [{"check": {"function": "sql_query", "arguments": {"query": 123}}}]
    assert _filter_unsafe_sql_rules(rules) == []


def test_filter_unsafe_sql_rules_logs_warning_unsafe_sql(caplog):
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE secret"}}}]
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.llm.llm_core"):
        _filter_unsafe_sql_rules(rules)
    assert any("unsafe SQL query" in r.message for r in caplog.records)


def test_filter_unsafe_sql_rules_logs_warning_non_string_query(caplog):
    rules = [{"check": {"function": "sql_query", "arguments": {"query": 123}}}]
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.llm.llm_core"):
        _filter_unsafe_sql_rules(rules)
    assert any("non-string query argument" in r.message for r in caplog.records)


def test_filter_unsafe_sql_rules_null_query_dropped():
    # null query is not a string — dropped (not deferred)
    rules = [{"check": {"function": "sql_query", "arguments": {"query": None}}}]
    assert _filter_unsafe_sql_rules(rules) == []


def test_filter_unsafe_sql_rules_missing_query_key_dropped():
    # missing "query" key → arguments.get("query") returns None → not isinstance str → dropped
    rules = [{"check": {"function": "sql_query", "arguments": {}}}]
    assert _filter_unsafe_sql_rules(rules) == []


# --- DspyRuleGeneration integration (mock generator) ---


def test_dspy_rule_generation_forward_filters_unsafe_sql():
    unsafe_rules = json.dumps([
        {"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}
    ])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(unsafe_rules)

    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert json.loads(result.quality_rules) == []


def test_dspy_rule_generation_forward_keeps_safe_sql():
    safe_rules = json.dumps([
        {"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}}
    ])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(safe_rules)

    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert len(json.loads(result.quality_rules)) == 1


def test_dspy_rule_generation_forward_invalid_json_returns_empty():
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction("not json")

    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert result.quality_rules == "[]"


def test_dspy_rule_generation_forward_empty_quality_rules_unchanged():
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction("")

    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert result.quality_rules == ""


def test_dspy_rule_generation_forward_none_quality_rules_unchanged():
    mock_gen = Mock()
    pred = Mock()
    pred.quality_rules = None
    mock_gen.return_value = pred

    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert result.quality_rules is None


# --- DspyRuleGenerationWithSchemaInference integration ---


def test_dspy_rule_generation_with_schema_inference_forward_filters_unsafe_sql():
    unsafe_rules = json.dumps([
        {"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}
    ])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(unsafe_rules)

    inner = DspyRuleGeneration(generator=mock_gen)
    module = DspyRuleGenerationWithSchemaInference(generator=inner)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert json.loads(result.quality_rules) == []


# --- DspyRuleUsingDataStats integration (mock generator) ---


def test_dspy_rule_using_data_stats_forward_filters_unsafe_sql():
    unsafe_rules = json.dumps([
        {"check": {"function": "sql_query", "arguments": {"query": "INSERT INTO bad VALUES (1)"}}}
    ])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(unsafe_rules)

    module = DspyRuleUsingDataStats(generator=mock_gen)
    result = module.forward(data_summary_stats="{}", available_functions="[]")

    assert json.loads(result.quality_rules) == []


def test_dspy_rule_using_data_stats_forward_keeps_safe_sql():
    safe_rules = json.dumps([
        {"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}}
    ])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(safe_rules)

    module = DspyRuleUsingDataStats(generator=mock_gen)
    result = module.forward(data_summary_stats="{}", available_functions="[]")

    assert len(json.loads(result.quality_rules)) == 1


def test_dspy_rule_using_data_stats_forward_invalid_json_returns_empty():
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction("not json")

    module = DspyRuleUsingDataStats(generator=mock_gen)
    result = module.forward(data_summary_stats="{}", available_functions="[]")

    assert result.quality_rules == "[]"


def test_dspy_rule_using_data_stats_forward_empty_quality_rules_unchanged():
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction("")

    module = DspyRuleUsingDataStats(generator=mock_gen)
    result = module.forward(data_summary_stats="{}", available_functions="[]")

    assert result.quality_rules == ""


def test_dspy_rule_using_data_stats_forward_none_quality_rules_unchanged():
    mock_gen = Mock()
    pred = Mock()
    pred.quality_rules = None
    mock_gen.return_value = pred

    module = DspyRuleUsingDataStats(generator=mock_gen)
    result = module.forward(data_summary_stats="{}", available_functions="[]")

    assert result.quality_rules is None
