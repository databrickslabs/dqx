"""Safe classification of model-serving availability errors."""

import re

# Only the head of an error message is scanned, so a large response body cannot slow classification.
_SCAN_LIMIT = 2000

_HTTP_ERROR_CLASS = "REMOTE_FUNCTION_HTTP_FAILED_ERROR"
_HTTP_STATUS_RE = re.compile(r"(?:code|status(?: code)?)\s+[\"']?(403|404|429|503)\b", re.IGNORECASE)
_HTTP_REASONS = {
    "403": "Model serving endpoint permission denied (HTTP 403)",
    "404": "Model serving endpoint is unavailable (HTTP 404)",
    "429": "Model serving endpoint is rate limited (HTTP 429)",
    "503": "Model serving endpoint is temporarily unavailable (HTTP 503)",
}

# A missing, retired, or inaccessible endpoint is now reported while ai_query is compiled, before any HTTP
# call, as "[AI_FUNCTION_COMPILATION_ERROR] ... INVALID_ARGUMENT: '<endpoint>' does not exist or is not
# accessible". Seen in CI once Claude 3.7 Sonnet was retired. Only that wording is an availability
# failure: other compilation errors (bad arguments, malformed model parameters) are regressions and
# must propagate.
_COMPILATION_ERROR_CLASS = "AI_FUNCTION_COMPILATION_ERROR"
_MISSING_ENDPOINT_RE = re.compile(r"does not exist or is not accessible", re.IGNORECASE)
_MISSING_ENDPOINT_REASON = "Model serving endpoint does not exist or is not accessible"


def _is_tagged(head: str, name: str) -> bool:
    """Whether *head* carries the ``[NAME]`` or ``[NAME.SUBCLASS]`` error tag."""
    return re.search(rf"\[{re.escape(name)}(?:\.[A-Z0-9_]+)?\]", head) is not None


def _has_error_class(error_class: str | None, head: str, name: str) -> bool:
    """Whether the failure is *name* or one of its subclasses.

    Read from *error_class* when the exception carried one, and from the message tag otherwise, which is
    the case for exceptions that are not a *PySparkException*.
    """
    if error_class is not None:
        return error_class == name or error_class.startswith(f"{name}.")
    return _is_tagged(head, name)


def endpoint_unavailable_reason(error_class: str | None, message: str) -> str | None:
    """Classify explicit endpoint availability failures without exposing response bodies.

    Other Spark failures must propagate so SQL, schema, and application regressions
    cannot turn into apparently successful test skips.
    """
    if error_class in {"PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS"}:
        return "Model serving endpoint permission denied"
    head = message[:_SCAN_LIMIT]
    is_compilation_error = _has_error_class(error_class, head, _COMPILATION_ERROR_CLASS)
    if is_compilation_error and _MISSING_ENDPOINT_RE.search(head):
        return _MISSING_ENDPOINT_REASON
    # A compilation error can wrap the HTTP failure of the endpoint lookup itself, so its status still counts.
    is_http_error = _has_error_class(error_class, head, _HTTP_ERROR_CLASS) or (
        is_compilation_error and _is_tagged(head, _HTTP_ERROR_CLASS)
    )
    if not is_http_error:
        return None
    match = _HTTP_STATUS_RE.search(head)
    return _HTTP_REASONS[match[1]] if match else None
