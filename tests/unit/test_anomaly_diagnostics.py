"""Privacy and classification checks for anomaly test error handling."""

import json

import pytest

from tests.anomaly_diagnostics import (
    dependency_provenance,
    endpoint_unavailable_reason,
    safe_traceback_frame,
    worker_traceback,
)


def test_worker_traceback_preserves_code_locations_without_payloads() -> None:
    message = '''
Traceback (most recent call last):
  File "/private-job/site-packages/shap/explainers/_tree.py", line 42, in explain
    values = customer_rows["private-column"]
  File "/private-job/site-packages/coverage/types.py", line 17, in initialize
AttributeError: module 'coverage.types' has no attribute 'private-attribute'
Authorization: Bearer private-token
https://private.invalid/path?token=private-token
'''
    result = worker_traceback(message)

    assert result == {
        "frames": [
            {"file": "shap/explainers/_tree.py", "line": 42, "function": "explain"},
            {"file": "coverage/types.py", "line": 17, "function": "initialize"},
        ],
        "error_types": ["AttributeError"],
        "hints": ["missing_python_attribute"],
    }
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize(
    "filename",
    ["/Users/private-person/secret.py", "/tmp/tests/../../private.py", "/tmp/site-packages/private_system/module.py"],
)
def test_traceback_paths_outside_public_code_are_redacted(filename: str) -> None:
    assert safe_traceback_frame(filename, 12, "evaluate") == {
        "file": "<external>",
        "line": 12,
        "function": "evaluate",
    }


def test_dependency_manifest_only_contains_allowlisted_metadata() -> None:
    manifest = dependency_provenance(
        {"numpy": "2.1.0", "pandas": "https://private.invalid/", "private-library": "private-token"},
        "3.12.3",
        wheel_sha256="a" * 64,
        revision="b" * 40,
        source="checkout",
    )
    assert manifest == {
        "python": "3.12.3",
        "packages": {"numpy": "2.1.0", "pandas": "unavailable"},
        "wheel_sha256": "a" * 64,
        "revision": "b" * 40,
        "source": "checkout",
    }
    assert "private" not in json.dumps(manifest)


def test_dependency_manifest_rejects_unexpected_provenance_values() -> None:
    manifest = dependency_provenance(
        {}, "private-python", wheel_sha256="private-token", revision="private-branch", source="private-source"
    )
    assert manifest == {
        "python": "unavailable",
        "packages": {},
        "wheel_sha256": None,
        "revision": None,
        "source": "unknown",
    }


@pytest.mark.parametrize("status", [403, 404, 429, 503])
def test_endpoint_availability_errors_have_safe_skip_reasons(status: int) -> None:
    message = f"[REMOTE_FUNCTION_HTTP_FAILED_ERROR] request failed with code {status}, private-response-body"
    reason = endpoint_unavailable_reason("REMOTE_FUNCTION_HTTP_FAILED_ERROR", message)
    assert reason is not None
    assert str(status) in reason
    assert "private" not in reason


@pytest.mark.parametrize(
    "error_class,message",
    [
        ("PARSE_SYNTAX_ERROR", "unexpected token 404"),
        ("REMOTE_FUNCTION_HTTP_FAILED_ERROR", "request failed with code 500"),
        ("REMOTE_FUNCTION_HTTP_FAILED_ERROR", "invalid response schema"),
        (None, "404 unrelated application assertion"),
    ],
)
def test_unexpected_endpoint_failures_cannot_be_skipped(error_class: str | None, message: str) -> None:
    assert endpoint_unavailable_reason(error_class, message) is None


def test_endpoint_permission_and_legacy_http_errors_are_classified() -> None:
    assert endpoint_unavailable_reason("PERMISSION_DENIED", "private permission detail") is not None
    assert (
        endpoint_unavailable_reason(None, "[REMOTE_FUNCTION_HTTP_FAILED_ERROR] request failed with code 404")
        is not None
    )


_MISSING_ENDPOINT_MESSAGE = (
    "[AI_FUNCTION_COMPILATION_ERROR] An error occurred while compiling the AI function: INVALID_ARGUMENT: "
    "'private-endpoint-name' does not exist or is not accessible. SQLSTATE: 42883; line 1 pos 7"
)


@pytest.mark.parametrize(
    "error_class,message",
    [
        ("AI_FUNCTION_COMPILATION_ERROR", _MISSING_ENDPOINT_MESSAGE),
        (None, _MISSING_ENDPOINT_MESSAGE),
        ("AI_FUNCTION_COMPILATION_ERROR.ENDPOINT_NOT_FOUND", _MISSING_ENDPOINT_MESSAGE),
        (
            None,
            _MISSING_ENDPOINT_MESSAGE.replace("[AI_FUNCTION_COMPILATION_ERROR]", "[AI_FUNCTION_COMPILATION_ERROR.X]"),
        ),
        ("AI_FUNCTION_COMPILATION_ERROR", _MISSING_ENDPOINT_MESSAGE.replace("does not exist", "Does Not Exist")),
    ],
)
def test_a_missing_or_retired_endpoint_reported_at_compile_time_is_classified(
    error_class: str | None, message: str
) -> None:
    """A retired endpoint (Claude 3.7 Sonnet) now fails while ai_query compiles, not with an HTTP status.

    An exact match on the classifier's own wording is also what proves the endpoint name is not echoed.
    """
    assert (
        endpoint_unavailable_reason(error_class, message)
        == "Model serving endpoint does not exist or is not accessible"
    )


@pytest.mark.parametrize("error_class", ["AI_FUNCTION_COMPILATION_ERROR", None])
def test_other_ai_function_compilation_errors_cannot_be_skipped(error_class: str | None) -> None:
    message = "[AI_FUNCTION_COMPILATION_ERROR] INVALID_ARGUMENT: modelParameters has an unsupported key 'tokens'"

    assert endpoint_unavailable_reason(error_class, message) is None


@pytest.mark.parametrize("error_class", ["AI_FUNCTION_COMPILATION_ERROR", None])
def test_a_compilation_error_wrapping_an_http_failure_keeps_its_status(error_class: str | None) -> None:
    """The endpoint lookup's own HTTP failure, surfaced while compiling, is still an availability failure."""
    message = (
        "[AI_FUNCTION_COMPILATION_ERROR] An error occurred while compiling the AI function: "
        "[REMOTE_FUNCTION_HTTP_FAILED_ERROR] request failed with status code 503, private-response-body"
    )

    assert (
        endpoint_unavailable_reason(error_class, message)
        == "Model serving endpoint is temporarily unavailable (HTTP 503)"
    )


def test_an_http_failure_subclass_is_classified_like_its_parent() -> None:
    message = "[REMOTE_FUNCTION_HTTP_FAILED_ERROR.TIMEOUT] request failed with code 404"

    assert endpoint_unavailable_reason("REMOTE_FUNCTION_HTTP_FAILED_ERROR.TIMEOUT", message) is not None
    assert endpoint_unavailable_reason(None, message) is not None


def test_worker_traceback_keeps_innermost_frames_with_bounded_output() -> None:
    message = (
        "\n".join(f'  File "/runtime/site-packages/pyspark/worker.py", line {line}, in invoke' for line in range(1, 51))
        + "\nTypeError: private-response-body"
    )

    result = worker_traceback(message)

    frames = result["frames"]
    assert isinstance(frames, list)
    assert [frame["line"] for frame in frames] == list(range(11, 51))
    assert result["error_types"] == ["TypeError"]
    assert "private-" not in json.dumps(result)


@pytest.mark.parametrize("location", ["Cell In[7], line 123", "File <command-123456789>, line 123"])
def test_notebook_traceback_retains_ansi_frames_and_message_free_assertions(location: str) -> None:
    message = (
        "\x1b[31m" + location + "\x1b[0m\n"
        "----> 123 assert secret_rows == private_value\n"
        "File /private/site-packages/mlflow/models/model.py:47, in load_model(path)\n"
        "    secret_token = 'do-not-copy'\n"
        "\x1b[31mAssertionError\x1b[0m\n"
    )
    result = worker_traceback(message)
    assert result == {
        "frames": [
            {"file": "<notebook>", "line": 123, "function": "<module>"},
            {"file": "mlflow/models/model.py", "line": 47, "function": "load_model"},
        ],
        "error_types": ["AssertionError"],
        "hints": [],
    }
    assert "secret" not in json.dumps(result) and "private" not in json.dumps(result)


@pytest.mark.parametrize("error_type", ["PySparkNotImplementedError", "NotImplementedError", "InvalidConfigError"])
def test_worker_traceback_retains_platform_and_configuration_error_types(error_type: str) -> None:
    message = f"{error_type}: [NOT_IMPLEMENTED] PRIVATE_PAYLOAD"
    result = worker_traceback(message)
    assert result["error_types"] == [error_type]
    assert "PRIVATE_PAYLOAD" not in json.dumps(result)
