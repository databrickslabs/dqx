import json
import logging
from unittest.mock import Mock

from databricks.labs.dqx.llm.llm_core import (
    DspyRuleGeneration,
    DspyRuleUsingDataStats,
    _filter_unsafe_sql_rules,
)


def _make_mock_prediction(quality_rules: str) -> Mock:
    pred = Mock()
    pred.quality_rules = quality_rules
    return pred


# --- _filter_unsafe_sql_rules (pure-function tests, no mocks needed) ---


def test_filter_unsafe_sql_rules_safe_query_kept():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "SELECT id, condition FROM {{ orders }}"}}}]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert len(result) == 1
    assert result[0]["check"]["arguments"]["query"] == "SELECT id, condition FROM {{ orders }}"


def test_filter_unsafe_sql_rules_drops_drop_table():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert result == []


def test_filter_unsafe_sql_rules_drops_delete():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "DELETE FROM orders WHERE 1=1"}}}]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert result == []


def test_filter_unsafe_sql_rules_drops_insert():
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "INSERT INTO bad VALUES (1)"}}}]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert result == []


def test_filter_unsafe_sql_rules_keeps_non_sql_query_rules():
    rules = [
        {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        {"check": {"function": "sql_query", "arguments": {"query": "DELETE FROM orders WHERE 1=1"}}},
    ]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert len(result) == 1
    assert result[0]["check"]["function"] == "is_not_null"


def test_filter_unsafe_sql_rules_mixed_queries():
    rules = [
        {"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}},
        {"check": {"function": "sql_query", "arguments": {"query": "TRUNCATE TABLE orders"}}},
    ]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert len(result) == 1
    assert "SELECT" in result[0]["check"]["arguments"]["query"]


def test_filter_unsafe_sql_rules_invalid_json_passthrough():
    bad_json = '[{"check": {"function": "sql_query"}'  # missing closing bracket
    assert _filter_unsafe_sql_rules(bad_json) == bad_json


def test_filter_unsafe_sql_rules_empty_array():
    result = _filter_unsafe_sql_rules("[]")
    assert json.loads(result) == []


def test_filter_unsafe_sql_rules_non_array_json_returns_empty():
    assert _filter_unsafe_sql_rules("null") == "[]"


def test_filter_unsafe_sql_rules_skips_malformed_entries():
    rules = [
        "not a rule",
        {"check": "still not a rule"},
        {"check": {"function": "sql_query", "arguments": {"query": "SELECT 1 FROM {{ v }}"}}},
    ]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert len(result) == 1
    assert result[0]["check"]["function"] == "sql_query"


def test_filter_unsafe_sql_rules_logs_warning(caplog):
    rules = [{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE secret"}}}]
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.llm.llm_core"):
        _filter_unsafe_sql_rules(json.dumps(rules))
    assert any("unsafe SQL query" in r.message for r in caplog.records)


def test_filter_unsafe_sql_rules_null_query_kept():
    # null query defaults to "" (safe); engine will reject it later via MissingParameterError
    rules = [{"check": {"function": "sql_query", "arguments": {"query": None}}}]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert len(result) == 1


def test_filter_unsafe_sql_rules_missing_query_key_kept():
    # missing "query" key also defaults to "" (safe); deferred to engine validation
    rules = [{"check": {"function": "sql_query", "arguments": {}}}]
    result = json.loads(_filter_unsafe_sql_rules(json.dumps(rules)))
    assert len(result) == 1


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
