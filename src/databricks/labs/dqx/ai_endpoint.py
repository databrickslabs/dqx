"""Safe classification of model-serving availability errors."""

import re


def endpoint_unavailable_reason(error_class: str | None, message: str) -> str | None:
    """Classify explicit endpoint availability failures without exposing response bodies.

    Other Spark failures must propagate so SQL, schema, and application regressions
    cannot turn into apparently successful test skips.
    """
    if error_class in {"PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS"}:
        return "Model serving endpoint permission denied"
    if error_class != "REMOTE_FUNCTION_HTTP_FAILED_ERROR" and not (
        error_class is None and "[REMOTE_FUNCTION_HTTP_FAILED_ERROR]" in message[:2000]
    ):
        return None
    match = re.search(r"(?:code|status(?: code)?)\s+[\"']?(403|404|429|503)\b", message[:2000], re.IGNORECASE)
    reasons = {
        "403": "Model serving endpoint permission denied (HTTP 403)",
        "404": "Model serving endpoint is unavailable (HTTP 404)",
        "429": "Model serving endpoint is rate limited (HTTP 429)",
        "503": "Model serving endpoint is temporarily unavailable (HTTP 503)",
    }
    return reasons[match[1]] if match else None
