"""Tests for the AI-assisted authoring routes (``routes/v1/generate.py`` and
``routes/v1/ai.py``, Rules Registry Phase 4A).

Follows ``test_registry_rules_routes.py``'s convention: call the route functions directly
with mocked dependencies rather than spinning up a FastAPI TestClient. The critical contract
under test is graceful degradation — AIUnavailableError/AIRateLimitExceededError/
AIResponseParseError/ValueError from the service map to clean 503/429/502/422 responses,
never a bare 500.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.models import (
    AiGenerateRuleIn,
    AiSuggestFieldIn,
    GenerateChecksIn,
)
from databricks_labs_dqx_app.backend.routes.v1 import ai as ai_routes
from databricks_labs_dqx_app.backend.routes.v1 import generate as generate_routes
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)


def _service() -> MagicMock:
    return MagicMock(name="AiRulesService")


class TestAiGenerateChecksRoute:
    async def test_unavailable_maps_to_503(self):
        service = _service()
        service.generate_checks_via_gateway = AsyncMock(side_effect=AIUnavailableError("AI is off"))

        with pytest.raises(HTTPException) as exc_info:
            await generate_routes.ai_generate_checks(
                GenerateChecksIn(user_input="desc"), service=service, user_email="a@x"
            )
        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == "AI is off"

    async def test_rate_limit_maps_to_429(self):
        service = _service()
        service.generate_checks_via_gateway = AsyncMock(side_effect=AIRateLimitExceededError(30))

        with pytest.raises(HTTPException) as exc_info:
            await generate_routes.ai_generate_checks(
                GenerateChecksIn(user_input="desc"), service=service, user_email="a@x"
            )
        assert exc_info.value.status_code == 429

    async def test_happy_path_returns_checks_and_yaml(self):
        service = _service()
        checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
        service.generate_checks_via_gateway = AsyncMock(return_value=checks)

        result = await generate_routes.ai_generate_checks(
            GenerateChecksIn(user_input="desc"), service=service, user_email="a@x"
        )

        assert result.checks == checks
        assert "is_not_null" in result.yaml_output
        assert result.validation_errors == []


class TestAiGenerateRuleRoute:
    async def test_unavailable_maps_to_503(self):
        service = _service()
        service.generate_rule = AsyncMock(side_effect=AIUnavailableError("no endpoint configured"))

        with pytest.raises(HTTPException) as exc_info:
            await ai_routes.ai_generate_rule(AiGenerateRuleIn(description="d"), service=service, user_email="a@x")
        assert exc_info.value.status_code == 503

    async def test_rate_limit_maps_to_429(self):
        service = _service()
        service.generate_rule = AsyncMock(side_effect=AIRateLimitExceededError(30))

        with pytest.raises(HTTPException) as exc_info:
            await ai_routes.ai_generate_rule(AiGenerateRuleIn(description="d"), service=service, user_email="a@x")
        assert exc_info.value.status_code == 429

    async def test_parse_error_maps_to_502(self):
        service = _service()
        service.generate_rule = AsyncMock(side_effect=AIResponseParseError("bad json"))

        with pytest.raises(HTTPException) as exc_info:
            await ai_routes.ai_generate_rule(AiGenerateRuleIn(description="d"), service=service, user_email="a@x")
        assert exc_info.value.status_code == 502

    async def test_no_valid_rule_maps_to_422(self):
        service = _service()
        service.generate_rule = AsyncMock(side_effect=ValueError("could not generate a valid rule"))

        with pytest.raises(HTTPException) as exc_info:
            await ai_routes.ai_generate_rule(AiGenerateRuleIn(description="d"), service=service, user_email="a@x")
        assert exc_info.value.status_code == 422

    async def test_happy_path_returns_proposal(self):
        service = _service()
        service.generate_rule = AsyncMock(
            return_value={
                "name": "n",
                "description": "d",
                "mode": "dqx_native",
                "dimension": "Completeness",
                "severity": "High",
                "polarity": "pass",
                "definition": {"function": "is_not_null", "arguments": {"column": "id"}},
                "author_kind": "ai_generated",
            }
        )

        result = await ai_routes.ai_generate_rule(
            AiGenerateRuleIn(description="id must not be null"), service=service, user_email="a@x"
        )

        assert result.name == "n"
        assert result.mode == "dqx_native"
        assert result.author_kind == "ai_generated"


class TestAiSuggestFieldRoute:
    async def test_unavailable_maps_to_503(self):
        service = _service()
        service.suggest_field = AsyncMock(side_effect=AIUnavailableError("AI is off"))

        with pytest.raises(HTTPException) as exc_info:
            await ai_routes.ai_suggest_field(
                AiSuggestFieldIn(field="dimension", context="ctx"), service=service, user_email="a@x"
            )
        assert exc_info.value.status_code == 503

    async def test_happy_path_returns_value(self):
        service = _service()
        service.suggest_field = AsyncMock(return_value="Completeness")

        result = await ai_routes.ai_suggest_field(
            AiSuggestFieldIn(field="dimension", context="ctx"), service=service, user_email="a@x"
        )

        assert result.value == "Completeness"
