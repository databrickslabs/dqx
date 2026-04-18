import json
import logging
from types import SimpleNamespace

import pytest

from databricks.labs.dqx.errors import MissingParameterError
import databricks.labs.dqx.profiler.generator as generator_module


def test_profiler_llm_disabled(generator, monkeypatch):
    """Test error when llm is not installed."""
    monkeypatch.setattr(generator_module, "LLM_ENABLED", False)

    # Attempt to generate rules should raise ImportError
    with pytest.raises(MissingParameterError, match="LLM engine not available"):
        generator.generate_dq_rules_ai_assisted(user_input="Some input")


def test_generate_dq_rules_ai_assisted_logs_dropped_invalid_rule(generator, caplog):
    """Invalid LLM-generated rules are dropped with a WARNING whose rendered message
    includes the offending rule — so operators can see what was rejected.
    Regression guard: the logger must use %-style placeholders, not `{}`."""
    invalid_rule = {
        "criticality": "error",
        "check": {"function": "nonexistent_function_xyz", "arguments": {"column": "name"}},
    }
    prediction = SimpleNamespace(quality_rules=json.dumps([invalid_rule]), reasoning="test")
    generator.llm_engine = SimpleNamespace(detect_business_rules_with_llm=lambda **_: prediction)

    with caplog.at_level(logging.WARNING, logger=generator_module.__name__):
        result = generator.generate_dq_rules_ai_assisted(user_input="anything")

    assert result == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"Expected exactly one WARNING, got {warnings}"

    rendered = warnings[0].getMessage()
    assert (
        "nonexistent_function_xyz" in rendered
    ), f"Log message must interpolate the dropped rule content, got: {rendered!r}"
    assert (
        "{}" not in rendered
    ), f"Log message still contains literal '{{}}' — logger placeholder style is wrong: {rendered!r}"
