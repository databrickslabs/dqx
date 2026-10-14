"""AIGateway — the model-call substrate for the Rules Registry (Phase 4A).

Owns the *transport* and safety rails for every AI-assisted feature in the app:

- **Kill-switch**: ``ai_enabled`` (default ``False``) must be explicitly turned on by an
  admin. A deploy with no AI infra configured behaves exactly like today — every caller
  gets a clean :class:`AIUnavailableError` (mapped to HTTP 503 by the routes), never a 500.
- **Model configuration**: ``ai_endpoint_name`` names the Unity Gateway model service to
  call (for example ``system.ai.claude-sonnet-4-5``) through
  :class:`~databricks_labs_dqx_app.backend.services.model_services.ModelServices`.
  Empty/unset also raises :class:`AIUnavailableError`.
- **Per-user rate limiting**: ``ai_rate_limit_per_user_per_hour`` (default 30) caps calls per
  user per rolling hour. See :meth:`_enforce_rate_limit` for the documented in-memory
  limitation.
- **Audit log**: one structured log line per call (endpoint, purpose, output size, hashed
  user) — never prompt content or row data (AGENTS.md log-injection / LLM06 guidance).
- **Robust JSON parsing**: :meth:`parse_json_object` strips code fences / prose around a
  bare JSON object and raises a clean :class:`AIResponseParseError` instead of crashing the
  caller on malformed model output.

**Authentication**: every model call is made with the caller's own On-Behalf-Of (OBO)
``WorkspaceClient`` (see ``dependencies.get_ai_gateway``), never the app's service
principal — AI generation is a user-facing, request-scoped action, so it should run with
the same identity and UC permissions as the rest of that user's request. This is why the
app bundle requests the ``ai-gateway`` OBO scope (see ``databricks.yml``); end users need
``EXECUTE`` on the configured model service plus ``USE_CATALOG`` / ``USE_SCHEMA`` on its
catalog and schema (Databricks grants these on ``system.ai`` to all account users by
default). A missing model service or missing grant surfaces as :class:`AIUnavailableError`
naming the fix. Kill-switch + rate limiting still apply on top.

Purpose-specific prompt construction and DQX-native validation/repair of generated rule
JSON live in :class:`~databricks_labs_dqx_app.backend.services.ai_rules_service.AiRulesService`,
which depends on this gateway for the actual model call — this class has no opinion on
prompt content or rule semantics.
"""

import asyncio
import hashlib
import json
import logging
import re
import time
from collections import defaultdict, deque
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import BadRequest, NotFound, PermissionDenied

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.model_services import ModelServices

logger = logging.getLogger(__name__)

_CHAT_ROLES = frozenset({"system", "user", "assistant"})
# Databricks reports a token missing an OAuth scope as "... does not have required scopes: <scope>".
# The SDK maps it to the same PermissionDenied (error code PERMISSION_DENIED) as a missing grant,
# so the wording is the only signal; match the phrase, not any mention of "scope".
_MISSING_SCOPE_RE = re.compile(r"required scopes?\b", re.IGNORECASE)

_RATE_LIMIT_WINDOW_SECONDS = 3600.0


class AIUnavailableError(Exception):
    """Raised when the AI Gateway cannot serve a request.

    Covers the kill-switch being off, no model being configured, the configured model service
    not existing, and the caller lacking access to it.
    Routes map this to a clean HTTP 503 with :attr:`reason` as the detail — never a 500.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class AIRateLimitExceededError(Exception):
    """Raised when a caller exceeds their per-user hourly AI call quota. Maps to HTTP 429."""

    def __init__(self, limit: int) -> None:
        super().__init__(f"Rate limit of {limit} AI call(s) per hour exceeded.")
        self.limit = limit


class AIResponseParseError(Exception):
    """Raised when model output cannot be parsed into the expected JSON shape. Maps to HTTP 502."""


# Explanatory message reused when the model ran out of output budget mid-
# response. Held as a module constant so both the ``finish_reason="length"``
# path and the mid-JSON truncation heuristic (which fires for endpoints that
# do NOT set ``finish_reason``, notably ``databricks-gpt-*-nano``) name the
# same cause. Tests match on the ``"output-token budget"`` substring.
_BUDGET_EXHAUSTED_MESSAGE = (
    "The AI model hit its output-token budget before finishing its answer "
    "(reasoning models spend part of the budget thinking, and wide tables "
    "produce large mapping responses). Please try again — the caller now "
    "scales the budget with column count, so a retry should succeed."
)


class AIGateway:
    """Model-service transport with kill-switch, per-user rate limiting, and audit logging.

    Rate limiting is **per-process, in-memory** — a documented limitation. Counts reset on
    restart and are not shared across multiple uvicorn workers or app replicas. This is an
    acceptable soft usage guard for an internal authoring tool; a durable, cross-replica
    limiter would need a shared store (e.g. the OLTP database) and is out of scope for this
    phase.
    """

    def __init__(self, user_ws: WorkspaceClient, app_settings: AppSettingsService) -> None:
        # OBO WorkspaceClient — every model call runs as the calling user, not the
        # app's service principal (see the module docstring's Authentication note).
        self._models = ModelServices(user_ws)
        self._app_settings = app_settings
        # user_email -> monotonic call timestamps within the current rolling window.
        self._call_log: dict[str, deque[float]] = defaultdict(deque)

    # ------------------------------------------------------------------
    # Admin settings passthrough
    # ------------------------------------------------------------------

    def is_enabled(self) -> bool:
        """Return whether the AI kill-switch is on."""
        return self._app_settings.get_ai_enabled()

    def endpoint_name(self) -> str:
        """Return the configured model service name, or ``""`` if unset."""
        return self._app_settings.get_ai_endpoint_name()

    def rate_limit_per_hour(self) -> int:
        """Return the configured per-user hourly call cap."""
        return self._app_settings.get_ai_rate_limit_per_user_per_hour()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _enforce_rate_limit(self, user_email: str) -> None:
        """Raise :class:`AIRateLimitExceededError` if *user_email* is over quota.

        A limit of ``0`` or less is treated as "unlimited" — an explicit admin choice,
        not a misconfiguration, since the default is a positive 30.
        """
        limit = self.rate_limit_per_hour()
        if limit <= 0:
            return
        now = time.monotonic()
        window_start = now - _RATE_LIMIT_WINDOW_SECONDS
        calls = self._call_log[user_email]
        while calls and calls[0] < window_start:
            calls.popleft()
        if len(calls) >= limit:
            raise AIRateLimitExceededError(limit)
        calls.append(now)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def query(
        self,
        *,
        user_email: str,
        purpose: str,
        messages: list[dict[str, str]],
        max_tokens: int = 2048,
        temperature: float | None = None,
    ) -> str:
        """Query the configured model service and return the assistant's text content.

        Args:
            user_email: Caller identity, used for rate limiting and the (hashed) audit log.
            purpose: Short label identifying the calling feature (e.g. ``"generate_rule"``),
                recorded in the audit log — never the prompt content itself.
            messages: Chat messages as ``{"role": "system"|"user"|"assistant", "content": str}``.
            max_tokens: Hard cap on the model's output budget (OWASP LLM04 guard).
            temperature: Optional sampling temperature; omitted from the request when ``None``.

        Returns:
            The assistant message content from the first response choice.

        Raises:
            AIUnavailableError: kill-switch is off, no model is configured, or the model
                service is missing or not usable by the caller.
            AIRateLimitExceededError: the caller exceeded their hourly quota.
            AIResponseParseError: the model returned no usable message content.
        """
        if not self.is_enabled():
            raise AIUnavailableError("AI features are disabled by the administrator.")
        endpoint = self.endpoint_name()
        if not endpoint:
            raise AIUnavailableError("No AI model is configured.")

        self._enforce_rate_limit(user_email)

        chat_messages = [
            {
                "role": role if (role := message.get("role", "user")) in _CHAT_ROLES else "user",
                "content": message.get("content", ""),
            }
            for message in messages
        ]
        query_kwargs: dict[str, Any] = {
            "model": endpoint,
            "messages": chat_messages,
            "max_tokens": max_tokens,
        }
        if temperature is not None:
            query_kwargs["temperature"] = temperature

        try:
            response = await self._query_endpoint(query_kwargs)
        except NotFound as e:
            logger.warning(f"ai_gateway_model_not_found endpoint={replace_control_characters(endpoint)}")
            raise AIUnavailableError(
                f"AI model {endpoint} doesn't exist in this workspace. "
                "An administrator can choose another one in Settings > AI."
            ) from e
        except PermissionDenied as e:
            # Databricks' own error text (never prompt content) says which scope or grant is missing.
            logger.warning(
                f"ai_gateway_permission_denied endpoint={replace_control_characters(endpoint)} "
                f"error={replace_control_characters(str(e))[:300]}"
            )
            if _MISSING_SCOPE_RE.search(str(e)):
                # The user's token lacks the app's ``ai-gateway`` user API scope, not a grant.
                raise AIUnavailableError(
                    "Studio isn't allowed to call AI models for you yet. An administrator needs to add the "
                    "ai-gateway user API scope to the app (redeploying the bundle does this), then you "
                    "need to sign in again."
                ) from e
            raise AIUnavailableError(
                f"You don't have access to AI model {endpoint}. Ask a metastore admin to grant you "
                f"EXECUTE on model service {endpoint}, plus USE_CATALOG and USE_SCHEMA on its "
                "catalog and schema. If you signed in before Studio was last upgraded, sign in again first."
            ) from e
        content = self._extract_content(response)
        self._audit(user_email=user_email, endpoint=endpoint, purpose=purpose, output_size=len(content))
        return content

    async def _query_endpoint(self, query_kwargs: dict[str, Any]) -> Any:
        """Query the model service, retrying once without ``temperature`` if it's rejected.

        Some models (notably the GPT-5 family) have only accepted the
        default sampling temperature and returned a
        ``BadRequest`` for any explicit ``temperature`` — including ``0``,
        which callers pass for determinism. Rather than couple every caller
        to each endpoint's capabilities, drop the optional ``temperature``
        and retry once when (and only when) the endpoint rejects that
        specific parameter. Any other ``BadRequest`` propagates unchanged.
        """
        try:
            return await asyncio.to_thread(lambda: self._models.chat(**query_kwargs))
        except BadRequest as e:
            if "temperature" not in query_kwargs or "temperature" not in str(e).lower():
                raise
            retry_kwargs = {k: v for k, v in query_kwargs.items() if k != "temperature"}
            logger.info("Model service rejected explicit temperature; retrying with the model default")
            return await asyncio.to_thread(lambda: self._models.chat(**retry_kwargs))

    @staticmethod
    def _extract_content(response: dict[str, Any]) -> str:
        choices = [choice for choice in response.get("choices") or [] if isinstance(choice, dict)]
        # Budget-exhaustion detection runs BEFORE the "return the first non-empty
        # content" fast path: several endpoints (notably the Claude family) DO
        # emit visible content while hitting ``max_tokens``, so a check that only
        # fires on empty content misses those cases and the caller sees a
        # generic parse error later. If ANY choice ran to the budget cap, prefer
        # the named cause — a truncated response is worse than a labelled retry.
        finish_reasons = [str(choice.get("finish_reason")) for choice in choices]
        if "length" in finish_reasons:
            logger.warning(f"ai_gateway_budget_exhausted finish_reasons={finish_reasons}")
            raise AIResponseParseError(_BUDGET_EXHAUSTED_MESSAGE)
        for choice in choices:
            message = choice.get("message")
            content = message.get("content") if isinstance(message, dict) else None
            if content:
                return str(content)
        logger.warning(f"ai_gateway_empty_content finish_reasons={finish_reasons}")
        raise AIResponseParseError("The AI model returned no message content.")

    @staticmethod
    def _looks_truncated_json(content: str) -> bool:
        """Heuristic: does *content* look like a JSON object cut off mid-emission?

        ``databricks-gpt-*-nano`` endpoints hit ``max_tokens`` on wide tables
        and return non-empty, mid-JSON output with ``finish_reason`` unset —
        the ``length`` check in :meth:`_extract_content` cannot see them, so
        the caller previously fell through to the generic "unparsable"
        message. This heuristic re-attributes those responses to the same
        budget cause so the reported reason points at the real fix (a larger
        budget), not at a phantom bug in the JSON path.

        A ``{`` prefix without a matching close is the strongest signal: the
        model started a JSON object, emitted keys/values, then stopped mid-
        string or mid-value. Non-JSON prose (which parses cleanly anyway) is
        conservatively treated as NOT truncated so a legitimately malformed
        response still surfaces as a plain parse error.
        """
        stripped = content.lstrip()
        if not stripped.startswith("{"):
            return False
        # Brace imbalance survives inside string literals: a truncated string
        # still leaves an open ``{`` unmatched. Simple counts are enough — we
        # only need to distinguish "cleanly closed" from "stopped mid-way".
        return content.count("{") > content.count("}")

    def _audit(self, *, user_email: str, endpoint: str, purpose: str, output_size: int) -> None:
        """Log one audit line per call. Never logs prompt content or row/user data (CWE-117 guard)."""
        user_hash = hashlib.sha256(user_email.encode()).hexdigest()[:12]
        logger.info(
            f"ai_gateway_call endpoint={replace_control_characters(endpoint)} purpose={purpose} "
            f"user_hash={user_hash} output_size={output_size}"
        )

    # ------------------------------------------------------------------
    # Robust JSON parsing
    # ------------------------------------------------------------------

    @staticmethod
    def parse_json_object(content: str) -> dict[str, Any]:
        """Robustly parse a single JSON object out of a raw model response.

        Tries, in order: the raw content as-is, the body of a fenced ```json code block,
        then the first ``{...}`` brace span in the text. Raises a clean
        :class:`AIResponseParseError` (never a bare ``json.JSONDecodeError``) when nothing
        parses to a JSON object.
        """
        candidates = [content]
        fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", content, re.DOTALL)
        if fence_match:
            candidates.append(fence_match.group(1).strip())
        brace_match = re.search(r"\{.*\}", content, re.DOTALL)
        if brace_match:
            candidates.append(brace_match.group(0))

        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed

        # Non-parseable JSON that *starts* like a JSON object is almost always
        # a mid-emission truncation on an endpoint that does not set
        # ``finish_reason="length"`` (nano). Surface the real cause so the
        # user is not sent chasing a phantom bug in the parse path.
        if AIGateway._looks_truncated_json(content):
            raise AIResponseParseError(_BUDGET_EXHAUSTED_MESSAGE)
        raise AIResponseParseError("Could not parse a JSON object from the AI response.")
