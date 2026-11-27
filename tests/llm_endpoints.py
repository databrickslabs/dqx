"""Model Serving endpoints and models used by the LLM-backed integration tests.

The default endpoint is derived from the library default (*LLMModelConfig().model_name*), so a
change to the default reaches the tests without editing them. Set the DQX_AI_QUERY_TEST_ENDPOINT
environment variable to run the tests against another endpoint.
"""

import os
from collections.abc import Mapping

from databricks.labs.dqx.config import LLMModelConfig

AI_QUERY_TEST_ENDPOINT_ENV_VAR = "DQX_AI_QUERY_TEST_ENDPOINT"
_DATABRICKS_PROVIDER_PREFIX = "databricks/"


def resolve_ai_query_test_endpoint(model_name: str, env: Mapping[str, str]) -> str:
    """Return the Model Serving endpoint the LLM-backed tests call.

    Args:
        model_name: Model name in *LLMModelConfig* form, with or without the "databricks/" prefix.
        env: Environment to read the DQX_AI_QUERY_TEST_ENDPOINT override from.

    Returns:
        The override when it is set to a non-blank value, otherwise *model_name* without the
        "databricks/" prefix.
    """
    override = env.get(AI_QUERY_TEST_ENDPOINT_ENV_VAR, "").strip()
    if override:
        return override
    # Not the library's resolver: it is private to the anomaly module, and importing that module
    # requires the anomaly extras, which this endpoint setting should not depend on.
    return model_name.removeprefix(_DATABRICKS_PROVIDER_PREFIX)


# Endpoint for the anomaly AI-explanation and MCP server tests.
AI_QUERY_TEST_ENDPOINT = resolve_ai_query_test_endpoint(LLMModelConfig().model_name, os.environ)

# Models that some tests use on purpose instead of the default, to cover a non-default model.
RULES_GENERATOR_TEST_MODEL = "databricks/databricks-llama-4-maverick"
PROFILER_WORKFLOW_TEST_MODEL = "databricks/databricks-meta-llama-3-1-8b-instruct"
