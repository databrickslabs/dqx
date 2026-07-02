"""Tests for ``services/ai_gateway.py`` — AIGateway (Rules Registry Phase 4A).

Covers the safety rails that must hold regardless of whether AI infra is deployed:
kill-switch off / no endpoint configured -> clean AIUnavailableError (never a crash), the
per-user hourly rate limit, the happy-path serving-endpoint call shape, and the robust
JSON-fence parsing helper.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIGateway,
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


def _settings(*, enabled: bool = True, endpoint: str = "my-endpoint", rate_limit: int = 30) -> MagicMock:
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_ai_enabled.return_value = enabled
    settings.get_ai_endpoint_name.return_value = endpoint
    settings.get_ai_rate_limit_per_user_per_hour.return_value = rate_limit
    return settings


def _ws_with_response(content: str) -> MagicMock:
    ws = create_autospec(WorkspaceClient, instance=True)
    message = SimpleNamespace(content=content)
    choice = SimpleNamespace(message=message)
    ws.serving_endpoints.query.return_value = SimpleNamespace(choices=[choice])
    return ws


class TestAvailability:
    async def test_disabled_raises_unavailable(self):
        gateway = AIGateway(sp_ws=MagicMock(), app_settings=_settings(enabled=False))

        with pytest.raises(AIUnavailableError):
            await gateway.query(user_email="a@x", purpose="test", messages=[{"role": "user", "content": "hi"}])

    async def test_no_endpoint_raises_unavailable(self):
        gateway = AIGateway(sp_ws=MagicMock(), app_settings=_settings(endpoint=""))

        with pytest.raises(AIUnavailableError):
            await gateway.query(user_email="a@x", purpose="test", messages=[{"role": "user", "content": "hi"}])

    async def test_disabled_never_calls_the_serving_endpoint(self):
        ws = MagicMock()
        gateway = AIGateway(sp_ws=ws, app_settings=_settings(enabled=False))

        with pytest.raises(AIUnavailableError):
            await gateway.query(user_email="a@x", purpose="test", messages=[{"role": "user", "content": "hi"}])

        ws.serving_endpoints.query.assert_not_called()


class TestRateLimit:
    async def test_exceeding_the_hourly_quota_raises(self):
        ws = _ws_with_response("hello")
        gateway = AIGateway(sp_ws=ws, app_settings=_settings(rate_limit=2))

        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "1"}])
        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "2"}])

        with pytest.raises(AIRateLimitExceededError):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "3"}])

    async def test_rate_limit_is_per_user(self):
        ws = _ws_with_response("hello")
        gateway = AIGateway(sp_ws=ws, app_settings=_settings(rate_limit=1))

        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "1"}])
        # A different user has their own independent quota.
        await gateway.query(user_email="b@x", purpose="p", messages=[{"role": "user", "content": "1"}])

        with pytest.raises(AIRateLimitExceededError):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "2"}])

    async def test_zero_rate_limit_means_unlimited(self):
        ws = _ws_with_response("hello")
        gateway = AIGateway(sp_ws=ws, app_settings=_settings(rate_limit=0))

        for _ in range(5):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "x"}])


class TestHappyPath:
    async def test_returns_message_content_and_calls_serving_endpoint(self):
        ws = _ws_with_response("the response content")
        gateway = AIGateway(sp_ws=ws, app_settings=_settings(endpoint="rules-endpoint"))

        result = await gateway.query(
            user_email="steward@x",
            purpose="generate_rule",
            messages=[{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
            max_tokens=512,
        )

        assert result == "the response content"
        _, kwargs = ws.serving_endpoints.query.call_args
        assert kwargs["name"] == "rules-endpoint"
        assert kwargs["max_tokens"] == 512
        assert len(kwargs["messages"]) == 2

    async def test_no_message_content_raises_parse_error(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.serving_endpoints.query.return_value = SimpleNamespace(choices=[])
        gateway = AIGateway(sp_ws=ws, app_settings=_settings())

        with pytest.raises(AIResponseParseError):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])


class TestParseJsonObject:
    def test_parses_bare_json(self):
        assert AIGateway.parse_json_object('{"value": "x"}') == {"value": "x"}

    def test_parses_fenced_json_block(self):
        content = 'Sure, here it is:\n```json\n{"value": "x"}\n```\nHope that helps!'
        assert AIGateway.parse_json_object(content) == {"value": "x"}

    def test_parses_json_object_embedded_in_prose(self):
        content = 'Here you go: {"value": "x"} - let me know if you need more.'
        assert AIGateway.parse_json_object(content) == {"value": "x"}

    def test_raises_clean_error_on_garbage(self):
        with pytest.raises(AIResponseParseError):
            AIGateway.parse_json_object("not json at all")

    def test_raises_clean_error_on_non_object_json(self):
        with pytest.raises(AIResponseParseError):
            AIGateway.parse_json_object("[1, 2, 3]")
