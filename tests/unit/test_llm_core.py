import json
import logging
from unittest.mock import Mock

from databricks.labs.dqx.llm.llm_core import (
    DspyRuleGeneration,
    DspyRuleGenerationWithSchemaInference,
    DspyRuleUsingDataStats,
)


def _make_mock_prediction(quality_rules: str) -> Mock:
    pred = Mock()
    pred.quality_rules = quality_rules
    return pred


def _filter_via_forward(quality_rules: str) -> list:
    """Run the unsafe-SQL filtering through the public DspyRuleGeneration.forward() API.

    The generator is mocked to return the given (JSON string) quality_rules; forward()
    parses, filters and re-serializes them. Returns the parsed list of surviving rules.
    """
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(quality_rules)
    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")
    return json.loads(result.quality_rules)


# --- unsafe sql_query filtering via the public forward() API ---


def test_forward_keeps_safe_query():
    rules = json.dumps(
        [{"check": {"function": "sql_query", "arguments": {"query": "SELECT id, condition FROM {{ orders }}"}}}]
    )
    result = _filter_via_forward(rules)
    assert len(result) == 1
    assert result[0]["check"]["arguments"]["query"] == "SELECT id, condition FROM {{ orders }}"


def test_forward_drops_drop_table():
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}])
    assert not _filter_via_forward(rules)


def test_forward_drops_delete():
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "DELETE FROM orders WHERE 1=1"}}}])
    assert not _filter_via_forward(rules)


def test_forward_drops_insert():
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "INSERT INTO bad VALUES (1)"}}}])
    assert not _filter_via_forward(rules)


def test_forward_keeps_non_sql_query_rules():
    rules = json.dumps(
        [
            {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
            {"check": {"function": "sql_query", "arguments": {"query": "DELETE FROM orders WHERE 1=1"}}},
        ]
    )
    result = _filter_via_forward(rules)
    assert len(result) == 1
    assert result[0]["check"]["function"] == "is_not_null"


def test_forward_mixed_queries():
    rules = json.dumps(
        [
            {"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}},
            {"check": {"function": "sql_query", "arguments": {"query": "TRUNCATE TABLE orders"}}},
        ]
    )
    result = _filter_via_forward(rules)
    assert len(result) == 1
    assert "SELECT" in result[0]["check"]["arguments"]["query"]


def test_forward_non_list_returns_empty():
    assert not _filter_via_forward(json.dumps("not a list"))
    assert not _filter_via_forward(json.dumps({"check": {}}))
    assert not _filter_via_forward(json.dumps(None))


def test_forward_skips_malformed_entries():
    rules = json.dumps(
        [
            "not a rule",
            {"check": "still not a rule"},
            {"check": {"function": "sql_query", "arguments": {"query": "SELECT 1 FROM {{ v }}"}}},
        ]
    )
    result = _filter_via_forward(rules)
    assert len(result) == 1
    assert result[0]["check"]["function"] == "sql_query"


def test_forward_drops_non_dict_arguments():
    # arguments is a string — cannot extract query safely, rule is dropped
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": "DROP TABLE x"}}])
    assert not _filter_via_forward(rules)


def test_forward_drops_non_string_query():
    # query is an int — not a string, crash-safe, rule is dropped
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": 123}}}])
    assert not _filter_via_forward(rules)


def test_forward_logs_warning_unsafe_sql(caplog):
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE secret"}}}])
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.llm.llm_core"):
        _filter_via_forward(rules)
    assert any("unsafe SQL query" in r.message for r in caplog.records)


def test_forward_logs_warning_non_string_query(caplog):
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": 123}}}])
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.llm.llm_core"):
        _filter_via_forward(rules)
    assert any("non-string query argument" in r.message for r in caplog.records)


def test_forward_null_query_dropped():
    # null query is not a string — dropped (not deferred)
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": None}}}])
    assert not _filter_via_forward(rules)


def test_forward_missing_query_key_dropped():
    # missing "query" key → arguments.get("query") returns None → not isinstance str → dropped
    rules = json.dumps([{"check": {"function": "sql_query", "arguments": {}}}])
    assert not _filter_via_forward(rules)


# --- DspyRuleGeneration integration (mock generator) ---


def test_dspy_rule_generation_forward_filters_unsafe_sql():
    unsafe_rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(unsafe_rules)

    module = DspyRuleGeneration(generator=mock_gen)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert json.loads(result.quality_rules) == []


def test_dspy_rule_generation_forward_keeps_safe_sql():
    safe_rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}}])
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
    unsafe_rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "DROP TABLE orders"}}}])
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(unsafe_rules)

    inner = DspyRuleGeneration(generator=mock_gen)
    module = DspyRuleGenerationWithSchemaInference(generator=inner)
    result = module.forward(schema_info="{}", business_description="test", available_functions="[]")

    assert json.loads(result.quality_rules) == []


# --- DspyRuleUsingDataStats integration (mock generator) ---


def test_dspy_rule_using_data_stats_forward_filters_unsafe_sql():
    unsafe_rules = json.dumps(
        [{"check": {"function": "sql_query", "arguments": {"query": "INSERT INTO bad VALUES (1)"}}}]
    )
    mock_gen = Mock()
    mock_gen.return_value = _make_mock_prediction(unsafe_rules)

    module = DspyRuleUsingDataStats(generator=mock_gen)
    result = module.forward(data_summary_stats="{}", available_functions="[]")

    assert json.loads(result.quality_rules) == []


def test_dspy_rule_using_data_stats_forward_keeps_safe_sql():
    safe_rules = json.dumps([{"check": {"function": "sql_query", "arguments": {"query": "SELECT id FROM {{ v }}"}}}])
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
