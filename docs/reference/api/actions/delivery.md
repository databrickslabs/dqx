# databricks.labs.dqx.actions.delivery

Webhook delivery client with retry logic and SSRF guard.

Provides:

* *validate\_webhook\_url* — enforces HTTPS and blocks private/loopback/link-local addresses.
* *WebhookAuth* — Basic-auth credentials carrier.
* *WebhookClient* — POSTs JSON payloads with exponential-backoff retry; never follows redirects.

Security notes:

* DNS-rebinding is out of scope for this layer; host validation is done on the literal URL value.
* Payload contents and credentials are never included in log messages or raised exceptions.
* The host value is sanitized (newlines/control chars stripped) before interpolation (CWE-117).

### validate\_webhook\_url[​](#validate_webhook_url "Direct link to validate_webhook_url")

```python
def validate_webhook_url(url: str,
                         allowed_host_suffixes: list[str] | None = None
                         ) -> None

```

Validate that *url* is safe to send a webhook request to.

Enforces HTTPS scheme and rejects hosts that resolve to private, loopback, link-local, or reserved address ranges. Optionally restricts the host to a caller-supplied allowlist.

Note: DNS-rebinding protection is out of scope for this layer — validation is performed on the literal host string in the URL.

**Arguments**:

* `url` - The webhook URL to validate.
* `allowed_host_suffixes` - Optional list of host suffixes (e.g. *\["hooks.slack.com"]*). When provided the URL host must end with one of them (case-insensitive).

**Raises**:

* `UnsafeWebhookUrlError` - When the URL fails any safety check.

## WebhookAuth Objects[​](#webhookauth-objects "Direct link to WebhookAuth Objects")

```python
@dataclass(frozen=True)
class WebhookAuth()

```

HTTP Basic-auth credentials for a webhook endpoint.

**Attributes**:

* `username` - The username portion of the Basic-auth credential.
* `password` - The password portion of the Basic-auth credential. Treat plaintext values as development-only; prefer secret-scope references in production (e.g. *secret\_scope/key*). This field is never logged.

### header[​](#header "Direct link to header")

```python
def header() -> dict[str, str]

```

Return an HTTP Authorization header for Basic authentication.

**Returns**:

A dict with a single *Authorization* key whose value is a *Basic* scheme header containing the base64-encoded *username<!-- -->:password*.

## NoRedirectHandler Objects[​](#noredirecthandler-objects "Direct link to NoRedirectHandler Objects")

```python
class NoRedirectHandler(urllib.request.HTTPRedirectHandler)

```

urllib redirect handler that blocks all redirects.

Overrides *redirect\_request* to return *None*, preventing the opener from following any HTTP 3xx response. This closes a class of SSRF vectors where an initially-safe URL redirects to an internal address.

### redirect\_request[​](#redirect_request "Direct link to redirect_request")

```python
def redirect_request(req: urllib.request.Request, fp: IO[bytes], code: int,
                     msg: str, headers: HTTPMessage,
                     newurl: str) -> urllib.request.Request | None

```

Block all redirects (returns None so urllib does not follow them).

## WebhookClient Objects[​](#webhookclient-objects "Direct link to WebhookClient Objects")

```python
class WebhookClient()

```

HTTP client that POSTs JSON payloads to webhook URLs with exponential-backoff retry.

Redirects are never followed. The URL is validated against SSRF rules before any network I/O is attempted. Error messages never include payload contents or authentication credentials.

**Arguments**:

* `max_retries` - Maximum number of retry attempts after the initial request fails.
* `base_delay` - Initial retry delay in seconds; doubles on each subsequent attempt.
* `max_delay` - Maximum delay cap in seconds.
* `timeout` - Per-request socket timeout in seconds.
* `sleeper` - Callable used to sleep between retries; injectable for testing.
* `opener` - Opener satisfying the *\_Opener* protocol; a no-redirect *OpenerDirector* is built by default.

### opener[​](#opener "Direct link to opener")

```python
@property
def opener() -> _Opener

```

The urllib opener used for HTTP requests.

### post[​](#post "Direct link to post")

```python
def post(url: str,
         payload: dict,
         *,
         auth: WebhookAuth | None = None,
         allowed_host_suffixes: list[str] | None = None) -> None

```

POST a JSON *payload* to *url* with retry on transient failures.

The URL is validated before any network call. On repeated failures the client waits an exponentially increasing delay between attempts — starting at *base\_delay* and doubling each attempt, capped at *max\_delay* — and raises *AlertDeliveryError* after all retries are exhausted.

**Arguments**:

* `url` - The webhook endpoint URL. Must pass SSRF validation.
* `payload` - JSON-serialisable dict to send as the request body.
* `auth` - Optional Basic-auth credentials.
* `allowed_host_suffixes` - Optional host-suffix allowlist forwarded to *validate\_webhook\_url*.

**Raises**:

* `UnsafeWebhookUrlError` - When *url* fails SSRF validation.
* `AlertDeliveryError` - When all delivery attempts fail.
