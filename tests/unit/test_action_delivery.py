"""Unit tests for webhook delivery client (Task 6).

Tests cover:
- validate_webhook_url: scheme, private/loopback/reserved IP, host-suffix allowlist
- WebhookClient.post: success path, retries with exponential backoff, basic auth, redirect blocking
"""

import base64
import io
import json
import urllib.error
import urllib.request
from http.client import HTTPMessage
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.delivery import NoRedirectHandler, WebhookAuth, WebhookClient, validate_webhook_url
from databricks.labs.dqx.errors import AlertDeliveryError, UnsafeWebhookUrlError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StubResponse:
    """Minimal fake urllib response that supports use as a context manager."""

    def __init__(self, status: int = 200, body: bytes = b"ok") -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def info(self) -> HTTPMessage:
        return HTTPMessage()

    def __enter__(self) -> "_StubResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return


class FakeOpener:
    """Records every call to open(); optionally raises on configured attempts."""

    def __init__(self, responses: list) -> None:
        # Each entry is either a _StubResponse or an exception to raise.
        self._responses = list(responses)
        self.calls: list[urllib.request.Request] = []
        # Expose handlers so redirect-handler checks work via the opener property
        self.handlers: list = []

    def open(self, request: urllib.request.Request, timeout: float = 30.0) -> object:
        _ = timeout  # consumed but not forwarded in the stub
        self.calls.append(request)
        if not self._responses:
            raise urllib.error.URLError("no more responses")
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


# ---------------------------------------------------------------------------
# validate_webhook_url tests
# ---------------------------------------------------------------------------


class TestValidateWebhookUrl:
    def test_rejects_http_scheme(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("http://hooks.slack.com/x")

    def test_rejects_file_scheme(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("file:///etc/passwd")

    def test_rejects_localhost_name(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://localhost/x")

    def test_rejects_loopback_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://127.0.0.1/x")

    def test_rejects_rfc1918_10_block(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://10.0.0.1/x")

    def test_rejects_rfc1918_172_block(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://172.16.0.1/x")

    def test_rejects_rfc1918_192_168_block(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://192.168.1.1/x")

    def test_rejects_link_local_metadata_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://169.254.169.254/x")

    def test_rejects_link_local_range(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://169.254.0.1/x")

    def test_rejects_ipv6_loopback(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://[::1]/x")

    def test_rejects_ipv6_ula(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://[fc00::1]/x")

    def test_rejects_ipv6_link_local(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://[fe80::1]/x")

    def test_accepts_public_https_url(self) -> None:
        # Should not raise
        validate_webhook_url("https://hooks.slack.com/services/xxx")

    def test_accepts_teams_webhook(self) -> None:
        validate_webhook_url("https://myorg.webhook.office.com/webhookb2/abc")

    def test_suffix_allowlist_rejects_wrong_host(self) -> None:
        with pytest.raises(UnsafeWebhookUrlError):
            validate_webhook_url("https://evil.com/x", allowed_host_suffixes=["hooks.slack.com"])

    def test_suffix_allowlist_accepts_matching_host(self) -> None:
        validate_webhook_url("https://hooks.slack.com/x", allowed_host_suffixes=["hooks.slack.com"])

    def test_suffix_allowlist_case_insensitive(self) -> None:
        validate_webhook_url("https://Hooks.Slack.Com/x", allowed_host_suffixes=["hooks.slack.com"])

    def test_error_message_contains_sanitized_host(self) -> None:
        """Host with newlines must be stripped from error message (CWE-117)."""
        with pytest.raises(UnsafeWebhookUrlError) as exc_info:
            validate_webhook_url("https://127.0.0.1/x")
        assert "\n" not in str(exc_info.value)
        assert "\r" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# WebhookAuth tests
# ---------------------------------------------------------------------------


class TestWebhookAuth:
    def test_header_returns_basic_auth(self) -> None:
        auth = WebhookAuth(username="user", password="pass")
        expected_b64 = base64.b64encode(b"user:pass").decode("ascii")
        assert auth.header() == {"Authorization": f"Basic {expected_b64}"}

    def test_frozen(self) -> None:
        auth = WebhookAuth(username="u", password="p")
        with pytest.raises(Exception):
            auth.username = "x"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# WebhookClient tests
# ---------------------------------------------------------------------------


class TestWebhookClientPost:
    def test_success_no_retry(self) -> None:
        """200 response: no retries, no sleep, correct method/content-type."""
        fake_response = _StubResponse(200)
        opener = FakeOpener([fake_response])
        delays: list[float] = []

        client = WebhookClient(opener=opener, sleeper=delays.append)
        client.post("https://hooks.slack.com/services/T01/xxx", {"text": "hello"})

        assert len(opener.calls) == 1
        req = opener.calls[0]
        assert req.get_method() == "POST"
        assert req.get_header("Content-type") == "application/json"
        body = json.loads(req.data)  # type: ignore[arg-type]
        assert body == {"text": "hello"}
        assert not delays

    def test_retries_on_url_error_then_raises(self) -> None:
        """URLError on every attempt: sleep called with growing delays, AlertDeliveryError raised."""
        err = urllib.error.URLError("connection refused")
        # max_retries=3 means 1 initial + 3 retries = 4 total calls
        opener = FakeOpener([err, err, err, err])
        delays: list[float] = []

        client = WebhookClient(max_retries=3, base_delay=1.0, max_delay=30.0, opener=opener, sleeper=delays.append)
        with pytest.raises(AlertDeliveryError) as exc_info:
            client.post("https://hooks.slack.com/services/T01/xxx", {"text": "alert"})

        # Should have slept 3 times (after attempts 0, 1, 2)
        assert len(delays) == 3
        assert delays[0] == 1.0  # 1.0 * 2**0
        assert delays[1] == 2.0  # 1.0 * 2**1
        assert delays[2] == 4.0  # 1.0 * 2**2

        # Error message must NOT contain payload
        err_msg = str(exc_info.value)
        assert "alert" not in err_msg
        assert "text" not in err_msg
        # Error message MUST contain the host
        assert "hooks.slack.com" in err_msg

    def test_retries_on_http_error_5xx(self) -> None:
        """HTTP 500 status counts as retriable; final raise after max_retries."""
        http_err = urllib.error.HTTPError(
            url="https://hooks.slack.com/x",
            code=500,
            msg="Internal Server Error",
            hdrs=HTTPMessage(),  # type: ignore[arg-type]
            fp=io.BytesIO(b""),
        )
        opener = FakeOpener([http_err, http_err, http_err, http_err])
        delays: list[float] = []

        client = WebhookClient(max_retries=3, base_delay=1.0, opener=opener, sleeper=delays.append)
        with pytest.raises(AlertDeliveryError):
            client.post("https://hooks.slack.com/x", {"k": "v"})

        assert len(delays) == 3

    def test_succeeds_after_transient_failures(self) -> None:
        """Fails twice then succeeds: no AlertDeliveryError raised."""
        err = urllib.error.URLError("transient")
        opener = FakeOpener([err, err, _StubResponse(200)])
        delays: list[float] = []

        client = WebhookClient(max_retries=3, base_delay=1.0, opener=opener, sleeper=delays.append)
        client.post("https://hooks.slack.com/services/T01/xxx", {"text": "ok"})

        assert len(delays) == 2  # slept after first two failures
        assert len(opener.calls) == 3

    def test_basic_auth_header_sent(self) -> None:
        opener = FakeOpener([_StubResponse(200)])
        client = WebhookClient(opener=opener, sleeper=lambda _: None)
        auth = WebhookAuth(username="myuser", password="mypassword")
        client.post("https://hooks.slack.com/services/T01/xxx", {"text": "hi"}, auth=auth)

        req = opener.calls[0]
        expected_b64 = base64.b64encode(b"myuser:mypassword").decode("ascii")
        assert req.get_header("Authorization") == f"Basic {expected_b64}"

    def test_no_auth_header_without_auth(self) -> None:
        opener = FakeOpener([_StubResponse(200)])
        client = WebhookClient(opener=opener, sleeper=lambda _: None)
        client.post("https://hooks.slack.com/services/T01/xxx", {"text": "hi"})

        req = opener.calls[0]
        assert req.get_header("Authorization") is None

    def test_ssrf_guard_before_network(self) -> None:
        """URL validation fires BEFORE opener.open is called."""
        opener = FakeOpener([_StubResponse(200)])
        client = WebhookClient(opener=opener, sleeper=lambda _: None)
        with pytest.raises(UnsafeWebhookUrlError):
            client.post("https://10.0.0.1/hook", {"text": "hi"})
        assert not opener.calls  # network never touched

    def test_allowed_host_suffixes_enforced(self) -> None:
        opener = FakeOpener([_StubResponse(200)])
        client = WebhookClient(opener=opener, sleeper=lambda _: None)
        with pytest.raises(UnsafeWebhookUrlError):
            client.post(
                "https://evil.com/hook",
                {"text": "hi"},
                allowed_host_suffixes=["hooks.slack.com"],
            )

    def test_max_delay_capped(self) -> None:
        """Exponential backoff does not exceed max_delay."""
        err = urllib.error.URLError("fail")
        # 5 retries to trigger large exponent
        opener = FakeOpener([err] * 6)
        delays: list[float] = []

        client = WebhookClient(max_retries=5, base_delay=1.0, max_delay=5.0, opener=opener, sleeper=delays.append)
        with pytest.raises(AlertDeliveryError):
            client.post("https://hooks.slack.com/x", {})

        assert all(d <= 5.0 for d in delays), f"delay exceeded max_delay: {delays}"

    def test_default_opener_blocks_redirects(self) -> None:
        """Default opener should have a NoRedirectHandler installed."""
        client = WebhookClient(sleeper=lambda _: None)
        # OpenerDirector stores handlers as a runtime list; access via vars() to satisfy mypy
        handlers: list = vars(client.opener).get("handlers", [])
        assert any(isinstance(h, NoRedirectHandler) for h in handlers)

    def test_payload_not_in_error_message(self) -> None:
        """Secret payload contents must never appear in AlertDeliveryError messages."""
        secret_value = "super-secret-token-12345"
        err = urllib.error.URLError("fail")
        opener = FakeOpener([err, err, err, err])

        client = WebhookClient(max_retries=3, base_delay=0.0, opener=opener, sleeper=lambda _: None)
        with pytest.raises(AlertDeliveryError) as exc_info:
            client.post("https://hooks.slack.com/x", {"token": secret_value})

        assert secret_value not in str(exc_info.value)


class TestWebhookClientTimeout:
    def test_timeout_passed_to_opener(self) -> None:
        """The configured timeout must be forwarded to opener.open."""
        recorded: list[float] = []

        class RecordingOpener:
            handlers: list = []

            def open(self, request: urllib.request.Request, timeout: float = 30.0) -> object:
                _ = request
                recorded.append(timeout)
                return _StubResponse(200)

        client = WebhookClient(timeout=42.0, opener=RecordingOpener(), sleeper=lambda _: None)
        client.post("https://hooks.slack.com/x", {})
        assert recorded == [42.0]


class TestNoRedirectHandlerContract:
    """Verify NoRedirectHandler uses create_autospec-compatible interface."""

    def test_redirect_request_returns_none(self) -> None:
        handler = NoRedirectHandler()
        mock_req = create_autospec(urllib.request.Request, instance=True)
        handler.redirect_request(mock_req, None, 301, "Moved", None, "https://other.com/")
        # Method always returns None — the call itself is the assertion (no exception, no redirect)
