import pytest

from databricks.labs.dqx.errors import MissingParameterError
import databricks.labs.dqx.profiler.generator as generator_module


def test_profiler_llm_disabled(generator, monkeypatch):
    """Test error when llm is not installed."""
    monkeypatch.setattr(generator_module, "LLM_ENABLED", False)

    # Attempt to generate rules should raise ImportError
    with pytest.raises(MissingParameterError, match="LLM engine not available"):
        generator.generate_dq_rules_ai_assisted(user_input="Some input")
