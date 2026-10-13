"""Tests for ``services/ai_gateway.py`` — AIGateway (Rules Registry Phase 4A).

Covers the safety rails that must hold regardless of whether AI infra is deployed:
kill-switch off / no model configured -> clean AIUnavailableError (never a crash), the
per-user hourly rate limit, the happy-path Unity Gateway chat call shape, actionable errors
for a missing model or missing grants, and the robust JSON-fence parsing helper.
"""

from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import BadRequest, NotFound, PermissionDenied

from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIGateway,
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


def _settings(*, enabled: bool = True, endpoint: str = "system.ai.my-model", rate_limit: int = 30) -> MagicMock:
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_ai_enabled.return_value = enabled
    settings.get_ai_endpoint_name.return_value = endpoint
    settings.get_ai_rate_limit_per_user_per_hour.return_value = rate_limit
    return settings


def _completion(content: str, finish_reason: str | None = "stop") -> dict:
    return {"choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": finish_reason}]}


def _ws_with_response(content: str) -> MagicMock:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.api_client.do.return_value = _completion(content)
    return ws


def _request_body(ws: MagicMock, call: int = -1) -> dict:
    return ws.api_client.do.call_args_list[call].kwargs["body"]


class TestAvailability:
    async def test_disabled_raises_unavailable(self):
        gateway = AIGateway(user_ws=MagicMock(), app_settings=_settings(enabled=False))

        with pytest.raises(AIUnavailableError):
            await gateway.query(user_email="a@x", purpose="test", messages=[{"role": "user", "content": "hi"}])

    async def test_no_endpoint_raises_unavailable(self):
        gateway = AIGateway(user_ws=MagicMock(), app_settings=_settings(endpoint=""))

        with pytest.raises(AIUnavailableError):
            await gateway.query(user_email="a@x", purpose="test", messages=[{"role": "user", "content": "hi"}])

    async def test_disabled_never_calls_the_model(self):
        ws = MagicMock()
        gateway = AIGateway(user_ws=ws, app_settings=_settings(enabled=False))

        with pytest.raises(AIUnavailableError):
            await gateway.query(user_email="a@x", purpose="test", messages=[{"role": "user", "content": "hi"}])

        ws.api_client.do.assert_not_called()

    async def test_missing_model_service_names_the_settings_fix(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.side_effect = NotFound("'system.ai.my-model' does not exist.")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(AIUnavailableError, match="Settings > AI") as excinfo:
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])

        assert "system.ai.my-model" in excinfo.value.reason

    async def test_missing_grant_names_the_privileges_to_grant(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.side_effect = PermissionDenied("User does not have EXECUTE on model service")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(AIUnavailableError, match="EXECUTE on model service system.ai.my-model"):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])

    async def test_missing_app_scope_names_the_scope(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.side_effect = PermissionDenied("Provided OAuth token does not have required scopes: ai-gateway")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(AIUnavailableError, match="ai-gateway user API scope"):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])


class TestRateLimit:
    async def test_exceeding_the_hourly_quota_raises(self):
        ws = _ws_with_response("hello")
        gateway = AIGateway(user_ws=ws, app_settings=_settings(rate_limit=2))

        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "1"}])
        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "2"}])

        with pytest.raises(AIRateLimitExceededError):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "3"}])

    async def test_rate_limit_is_per_user(self):
        ws = _ws_with_response("hello")
        gateway = AIGateway(user_ws=ws, app_settings=_settings(rate_limit=1))

        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "1"}])
        # A different user has their own independent quota.
        await gateway.query(user_email="b@x", purpose="p", messages=[{"role": "user", "content": "1"}])

        with pytest.raises(AIRateLimitExceededError):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "2"}])

    async def test_zero_rate_limit_means_unlimited(self):
        ws = _ws_with_response("hello")
        gateway = AIGateway(user_ws=ws, app_settings=_settings(rate_limit=0))

        for _ in range(5):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "x"}])


class TestHappyPath:
    async def test_returns_message_content_and_calls_the_model_service(self):
        ws = _ws_with_response("the response content")
        gateway = AIGateway(user_ws=ws, app_settings=_settings(endpoint="system.ai.rules-model"))

        result = await gateway.query(
            user_email="steward@x",
            purpose="generate_rule",
            messages=[{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
            max_tokens=512,
        )

        assert result == "the response content"
        args = ws.api_client.do.call_args.args
        assert args == ("POST", "/ai-gateway/mlflow/v1/chat/completions")
        body = _request_body(ws)
        assert body["model"] == "system.ai.rules-model"
        assert body["max_tokens"] == 512
        assert body["messages"] == [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}]

    async def test_unknown_roles_are_sent_as_user(self):
        ws = _ws_with_response("ok")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "tool", "content": "x"}])

        assert _request_body(ws)["messages"] == [{"role": "user", "content": "x"}]

    async def test_no_message_content_raises_parse_error(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.return_value = {"choices": []}
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(AIResponseParseError):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])

    async def test_budget_exhausted_during_reasoning_names_the_cause(self):
        # A "length" finish with empty content means the reasoning model spent
        # the whole max_tokens budget thinking - the error must say so instead
        # of the generic "no message content".
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.return_value = _completion("", finish_reason="length")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(AIResponseParseError, match="output-token budget"):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])

    async def test_budget_exhausted_with_nonempty_truncated_content_still_names_the_cause(self):
        # Claude-family models DO emit visible content while hitting
        # max_tokens — the previous "empty content only" check missed those,
        # letting them fall through to a generic parse error downstream. The
        # gateway must catch the finish_reason regardless of content shape.
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.return_value = _completion('{"suggestions": [{"rule_id": "rr-', finish_reason="length")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(AIResponseParseError, match="output-token budget"):
            await gateway.query(user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}])

    async def test_scales_max_tokens_kwarg_through_to_the_model(self):
        # The rule-suggester scales max_tokens with column count; a fixed
        # value at the gateway would silently defeat that. Confirm the
        # kwarg is propagated end-to-end.
        ws = _ws_with_response("ok")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        await gateway.query(
            user_email="a@x",
            purpose="p",
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=9200,
        )

        assert _request_body(ws)["max_tokens"] == 9200


class TestTemperatureFallback:
    """Some models (historically the GPT-5 family) reject an explicit
    temperature. The gateway must transparently retry without it so a caller
    that wants determinism (``temperature=0``) still gets a response."""

    async def test_retries_without_temperature_when_the_model_rejects_it(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        bodies: list[dict] = []

        def _do(method, path, **kwargs):
            bodies.append(kwargs["body"])
            if "temperature" in kwargs["body"]:
                raise BadRequest("Unsupported value: 'temperature' does not support 0 with this model.")
            return _completion("ok")

        ws.api_client.do.side_effect = _do
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        result = await gateway.query(
            user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}], temperature=0
        )

        assert result == "ok"
        assert len(bodies) == 2
        assert "temperature" in bodies[0]
        assert "temperature" not in bodies[1]

    async def test_unrelated_bad_request_is_not_retried(self):
        ws = create_autospec(WorkspaceClient, instance=True)
        ws.api_client.do.side_effect = BadRequest("some other validation error")
        gateway = AIGateway(user_ws=ws, app_settings=_settings())

        with pytest.raises(BadRequest):
            await gateway.query(
                user_email="a@x", purpose="p", messages=[{"role": "user", "content": "hi"}], temperature=0
            )
        assert ws.api_client.do.call_count == 1


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

    def test_truncated_json_reports_budget_exhaustion_not_generic_parse_error(self):
        # The gpt-*-nano family hits max_tokens on wide tables and returns
        # non-empty JSON with finish_reason UNSET, so the finish-reason path
        # in _extract_content can't see it. parse_json_object must recognise
        # the imbalance-brace signal and name the real cause so users aren't
        # sent chasing a phantom bug in the parse path.
        truncated = '{"suggestions": [{"rule_id": "rr-not-null", "mapping": {"column": "id"}, "expla'
        with pytest.raises(AIResponseParseError, match="output-token budget"):
            AIGateway.parse_json_object(truncated)

    def test_generic_parse_error_when_content_is_prose_rather_than_truncated_json(self):
        # A model that returned prose instead of JSON is genuinely
        # unparsable — not a budget problem — and must not be mis-attributed
        # to truncation.
        with pytest.raises(AIResponseParseError, match="Could not parse"):
            AIGateway.parse_json_object("I'm sorry, I can't help with that request.")
