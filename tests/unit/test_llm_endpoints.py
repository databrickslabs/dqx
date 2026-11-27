import os

import pytest

from databricks.labs.dqx.config import LLMModelConfig
from tests.llm_endpoints import (
    AI_QUERY_TEST_ENDPOINT,
    AI_QUERY_TEST_ENDPOINT_ENV_VAR,
    resolve_ai_query_test_endpoint,
)


def test_resolve_ai_query_test_endpoint_strips_databricks_prefix():
    assert resolve_ai_query_test_endpoint("databricks/databricks-claude-sonnet-4-5", env={}) == (
        "databricks-claude-sonnet-4-5"
    )


def test_resolve_ai_query_test_endpoint_keeps_bare_endpoint_name():
    assert resolve_ai_query_test_endpoint("databricks-claude-sonnet-4-5", env={}) == "databricks-claude-sonnet-4-5"


def test_resolve_ai_query_test_endpoint_env_override_wins():
    env = {AI_QUERY_TEST_ENDPOINT_ENV_VAR: "databricks-llama-4-maverick"}
    assert resolve_ai_query_test_endpoint("databricks/databricks-claude-sonnet-4-5", env=env) == (
        "databricks-llama-4-maverick"
    )


@pytest.mark.parametrize("blank", ["", "   "])
def test_resolve_ai_query_test_endpoint_blank_override_falls_back_to_model(blank: str):
    env = {AI_QUERY_TEST_ENDPOINT_ENV_VAR: blank}
    assert resolve_ai_query_test_endpoint("databricks/databricks-claude-sonnet-4-5", env=env) == (
        "databricks-claude-sonnet-4-5"
    )


def test_ai_query_test_endpoint_default_is_library_default_without_prefix():
    library_default = LLMModelConfig().model_name
    default_endpoint = resolve_ai_query_test_endpoint(library_default, env={})
    assert not default_endpoint.startswith("databricks/")
    assert library_default.endswith(default_endpoint)


def test_ai_query_test_endpoint_follows_library_default_and_environment():
    assert AI_QUERY_TEST_ENDPOINT == resolve_ai_query_test_endpoint(LLMModelConfig().model_name, env=os.environ)
