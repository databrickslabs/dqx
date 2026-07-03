"""AIGateway — dqwatch-native serving-endpoint substrate for the Rules Registry (Phase 4A).

Owns the *transport* and safety rails for every AI-assisted feature in the app:

- **Kill-switch**: ``ai_enabled`` (default ``False``) must be explicitly turned on by an
  admin. A deploy with no AI infra configured behaves exactly like today — every caller
  gets a clean :class:`AIUnavailableError` (mapped to HTTP 503 by the routes), never a 500.
- **Endpoint configuration**: ``ai_endpoint_name`` names the Databricks serving endpoint to
  call via :meth:`WorkspaceClient.serving_endpoints.query`. Empty/unset also raises
  :class:`AIUnavailableError`.
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
app bundle requests the ``serving.serving-endpoints`` OBO scope (see ``databricks.yml``);
end users must be granted ``CAN_QUERY`` on the configured serving endpoint(s) for AI
features to work for them (kill-switch + rate limiting still apply on top).

Purpose-specific prompt construction and DQX-native validation/repair of generated rule
JSON live in :class:`~databricks_labs_dqx_app.backend.services.ai_rules_service.AiRulesService`,
which depends on this gateway for the actual model call — this class has no opinion on
prompt content or rule semantics.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections import defaultdict, deque
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

logger = logging.getLogger(__name__)

_ROLE_MAP: dict[str, ChatMessageRole] = {
    "system": ChatMessageRole.SYSTEM,
    "user": ChatMessageRole.USER,
    "assistant": ChatMessageRole.ASSISTANT,
}

_RATE_LIMIT_WINDOW_SECONDS = 3600.0


class AIUnavailableError(Exception):
    """Raised when the AI Gateway cannot serve a request.

    Covers both the kill-switch being off and no serving endpoint being configured.
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


class AIGateway:
    """Serving-endpoint transport with kill-switch, per-user rate limiting, and audit logging.

    Rate limiting is **per-process, in-memory** — a documented limitation. Counts reset on
    restart and are not shared across multiple uvicorn workers or app replicas. This is an
    acceptable soft usage guard for an internal authoring tool; a durable, cross-replica
    limiter would need a shared store (e.g. the OLTP database) and is out of scope for this
    phase.
    """

    def __init__(self, user_ws: WorkspaceClient, app_settings: AppSettingsService) -> None:
        # OBO WorkspaceClient — every model call runs as the calling user, not the
        # app's service principal (see the module docstring's Authentication note).
        self._user_ws = user_ws
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
        """Return the configured serving endpoint name, or ``""`` if unset."""
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
        max_tokens: int = 1024,
        temperature: float | None = None,
    ) -> str:
        """Query the configured serving endpoint and return the assistant's text content.

        Args:
            user_email: Caller identity, used for rate limiting and the (hashed) audit log.
            purpose: Short label identifying the calling feature (e.g. ``"generate_rule"``),
                recorded in the audit log — never the prompt content itself.
            messages: Chat messages as ``{"role": "system"|"user"|"assistant", "content": str}``.
            max_tokens: Hard cap on the endpoint's output budget (OWASP LLM04 guard).
            temperature: Optional sampling temperature; omitted from the request when ``None``.

        Returns:
            The assistant message content from the first response choice.

        Raises:
            AIUnavailableError: kill-switch is off or no endpoint is configured.
            AIRateLimitExceededError: the caller exceeded their hourly quota.
            AIResponseParseError: the endpoint returned no usable message content.
        """
        if not self.is_enabled():
            raise AIUnavailableError("AI features are disabled by the administrator.")
        endpoint = self.endpoint_name()
        if not endpoint:
            raise AIUnavailableError("No AI serving endpoint is configured.")

        self._enforce_rate_limit(user_email)

        chat_messages = [
            ChatMessage(
                role=_ROLE_MAP.get(message.get("role", "user"), ChatMessageRole.USER),
                content=message.get("content", ""),
            )
            for message in messages
        ]
        query_kwargs: dict[str, Any] = {
            "name": endpoint,
            "messages": chat_messages,
            "max_tokens": max_tokens,
        }
        if temperature is not None:
            query_kwargs["temperature"] = temperature

        response = await asyncio.to_thread(self._user_ws.serving_endpoints.query, **query_kwargs)
        content = self._extract_content(response)
        self._audit(user_email=user_email, endpoint=endpoint, purpose=purpose, output_size=len(content))
        return content

    @staticmethod
    def _extract_content(response: Any) -> str:
        choices = getattr(response, "choices", None) or []
        for choice in choices:
            message = getattr(choice, "message", None)
            content = getattr(message, "content", None) if message is not None else None
            if content:
                return str(content)
        raise AIResponseParseError("AI serving endpoint returned no message content.")

    def _audit(self, *, user_email: str, endpoint: str, purpose: str, output_size: int) -> None:
        """Log one audit line per call. Never logs prompt content or row/user data (CWE-117 guard)."""
        user_hash = hashlib.sha256(user_email.encode()).hexdigest()[:12]
        logger.info(
            f"ai_gateway_call endpoint={endpoint} purpose={purpose} "
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

        raise AIResponseParseError("Could not parse a JSON object from the AI response.")
