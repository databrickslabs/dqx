"""Webhook delivery client with retry logic and SSRF guard.

Provides:
- *validate_webhook_url* — enforces HTTPS and blocks private/loopback/link-local addresses.
- *WebhookAuth* — Basic-auth credentials carrier.
- *WebhookClient* — POSTs JSON payloads with exponential-backoff retry; never follows redirects.

Security notes:
- DNS-rebinding is out of scope for this layer; host validation is done on the literal URL value.
- Payload contents and credentials are never included in log messages or raised exceptions.
- The host value is sanitized (newlines/control chars stripped) before interpolation (CWE-117).
"""

import base64
import ipaddress
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

from databricks.labs.dqx.errors import AlertDeliveryError, UnsafeWebhookUrlError

# Cloud-metadata endpoint — blocked explicitly regardless of is_link_local result.
_CLOUD_METADATA_IP = ipaddress.IPv4Address("169.254.169.254")


def _sanitize_host(host: str) -> str:
    """Strip newlines and ASCII control characters from *host* (CWE-117)."""
    return "".join(ch for ch in host if ch >= " " and ch != "\x7f")


def validate_webhook_url(url: str, allowed_host_suffixes: list[str] | None = None) -> None:
    """Validate that *url* is safe to send a webhook request to.

    Enforces HTTPS scheme and rejects hosts that resolve to private, loopback,
    link-local, or reserved address ranges.  Optionally restricts the host to a
    caller-supplied allowlist.

    Note: DNS-rebinding protection is out of scope for this layer — validation is
    performed on the literal host string in the URL.

    Args:
        url: The webhook URL to validate.
        allowed_host_suffixes: Optional list of host suffixes (e.g. ``["hooks.slack.com"]``).
            When provided the URL host must end with one of them (case-insensitive).

    Raises:
        UnsafeWebhookUrlError: When the URL fails any safety check.
    """
    parsed = urllib.parse.urlparse(url)

    if parsed.scheme != "https":
        raise UnsafeWebhookUrlError(f"Webhook URL must use HTTPS scheme; got '{parsed.scheme}'")

    raw_host = parsed.hostname or ""
    host = _sanitize_host(raw_host)

    if not host:
        raise UnsafeWebhookUrlError("Webhook URL contains no host")

    if host.lower() == "localhost":
        raise UnsafeWebhookUrlError(f"Webhook host '{host}' is not allowed (loopback)")

    # Check if the host is a literal IP address.
    try:
        addr = ipaddress.ip_address(host)
        _reject_ip(addr, host)
    except ValueError:
        # Not an IP address — domain name; proceed to suffix check.
        pass

    if allowed_host_suffixes is not None:
        host_lower = host.lower()
        if not any(host_lower.endswith(suffix.lower()) for suffix in allowed_host_suffixes):
            safe_host = _sanitize_host(host)
            raise UnsafeWebhookUrlError(f"Webhook host '{safe_host}' is not in the allowed-host-suffix list")


def _reject_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address, host: str) -> None:
    """Raise *UnsafeWebhookUrlError* if *addr* is a disallowed address.

    Args:
        addr: The parsed IP address object.
        host: The sanitized host string (used for the error message only).

    Raises:
        UnsafeWebhookUrlError: When *addr* is private, loopback, link-local, or reserved.
    """
    safe_host = _sanitize_host(host)
    reasons: list[str] = []

    if isinstance(addr, ipaddress.IPv4Address) and addr == _CLOUD_METADATA_IP:
        reasons.append("cloud metadata endpoint")

    if addr.is_loopback:
        reasons.append("loopback")
    if addr.is_private:
        reasons.append("private")
    if addr.is_link_local:
        reasons.append("link-local")
    if addr.is_reserved:
        reasons.append("reserved")

    if reasons:
        raise UnsafeWebhookUrlError(f"Webhook host '{safe_host}' is not allowed ({', '.join(reasons)})")


@dataclass(frozen=True)
class WebhookAuth:
    """HTTP Basic-auth credentials for a webhook endpoint.

    Attributes:
        username: The username portion of the Basic-auth credential.
        password: The password portion of the Basic-auth credential.
            Treat plaintext values as development-only; prefer secret-scope references
            in production (e.g. ``secret_scope/key``).  This field is never logged.
    """

    username: str
    password: str

    def header(self) -> dict[str, str]:
        """Return an HTTP Authorization header for Basic authentication.

        Returns:
            A dict with a single ``Authorization`` key whose value is
            ``Basic <base64(username:password)>``.
        """
        token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """urllib redirect handler that blocks all redirects.

    Overrides *redirect_request* to return ``None``, preventing the opener
    from following any HTTP 3xx response.  This closes a class of SSRF
    vectors where an initially-safe URL redirects to an internal address.
    """

    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


def _build_default_opener() -> urllib.request.OpenerDirector:
    """Build a urllib opener that does not follow redirects."""
    return urllib.request.build_opener(NoRedirectHandler())


class WebhookClient:
    """HTTP client that POSTs JSON payloads to webhook URLs with exponential-backoff retry.

    Redirects are never followed.  The URL is validated against SSRF rules before
    any network I/O is attempted.  Error messages never include payload contents
    or authentication credentials.

    Args:
        max_retries: Maximum number of retry attempts after the initial request fails.
        base_delay: Initial retry delay in seconds; doubles on each subsequent attempt.
        max_delay: Maximum delay cap in seconds.
        timeout: Per-request socket timeout in seconds.
        sleeper: Callable used to sleep between retries; injectable for testing.
        opener: urllib opener to use; a no-redirect opener is built by default.
    """

    def __init__(
        self,
        *,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        timeout: float = 30.0,
        sleeper: Callable[[float], None] = time.sleep,
        opener: object | None = None,
    ) -> None:
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._timeout = timeout
        self._sleeper = sleeper
        self._opener: urllib.request.OpenerDirector = (
            opener if opener is not None else _build_default_opener()  # type: ignore[assignment]
        )

    @property
    def opener(self) -> urllib.request.OpenerDirector:
        """The urllib opener used for HTTP requests."""
        return self._opener

    def post(
        self,
        url: str,
        payload: dict,
        *,
        auth: WebhookAuth | None = None,
        allowed_host_suffixes: list[str] | None = None,
    ) -> None:
        """POST a JSON *payload* to *url* with retry on transient failures.

        The URL is validated before any network call.  On repeated failures the
        client waits *base_delay * 2**attempt* seconds (capped at *max_delay*)
        between attempts and raises *AlertDeliveryError* after all retries are
        exhausted.

        Args:
            url: The webhook endpoint URL.  Must pass SSRF validation.
            payload: JSON-serialisable dict to send as the request body.
            auth: Optional Basic-auth credentials.
            allowed_host_suffixes: Optional host-suffix allowlist forwarded to
                *validate_webhook_url*.

        Raises:
            UnsafeWebhookUrlError: When *url* fails SSRF validation.
            AlertDeliveryError: When all delivery attempts fail.
        """
        # Validate BEFORE any I/O — SSRF guard.
        validate_webhook_url(url, allowed_host_suffixes)

        host = _sanitize_host(urllib.parse.urlparse(url).hostname or url)
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if auth is not None:
            headers.update(auth.header())

        body = json.dumps(payload).encode("utf-8")
        last_exc: Exception | None = None
        total_attempts = self._max_retries + 1

        for attempt in range(total_attempts):
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            try:
                with self._opener.open(req, timeout=self._timeout):  # type: ignore[union-attr]
                    return  # success
            except OSError as exc:
                last_exc = exc
                if attempt < total_attempts - 1:
                    delay = min(self._base_delay * (2**attempt), self._max_delay)
                    self._sleeper(delay)

        raise AlertDeliveryError(f"Webhook delivery to '{host}' failed after {total_attempts} attempt(s)") from last_exc
