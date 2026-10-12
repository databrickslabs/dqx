"""Behavior and privacy checks for optional anomaly test artifacts."""

import hashlib
import json
from pathlib import Path

import pytest

from tests.anomaly_diagnostics import (
    AnomalyDiagnostics,
    compute_observation,
    runtime_observation,
    case_identity,
    dependency_provenance,
    exception_chain,
    endpoint_unavailable_reason,
    safe_traceback_frame,
    worker_traceback,
)


pytest_plugins = ("pytester",)


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


def test_exception_chain_keeps_causes_but_no_values_or_source_lines() -> None:
    try:
        try:
            raise ValueError("private-input-and-token")
        except ValueError as cause:
            raise RuntimeError("private-http-response") from cause
    except RuntimeError as failure:
        result = exception_chain(failure)

    assert [entry["type"] for entry in result] == ["RuntimeError", "ValueError"]
    assert all(entry["frames"] for entry in result)
    assert "private-" not in json.dumps(result)


def test_exception_chain_respects_suppressed_context_and_cycles() -> None:
    try:
        try:
            raise ValueError("private input")
        except ValueError:
            raise RuntimeError("private context") from None
    except RuntimeError as failure:
        assert len(exception_chain(failure)) == 1
    error = RuntimeError("cyclic")
    error.__cause__ = error
    assert len(exception_chain(error)) == 1
    assert not exception_chain(None)


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


def test_case_identity_removes_parameter_values_but_distinguishes_cases() -> None:
    first = case_identity("test_scoring.py::test_collision[private-input-one]")
    second = case_identity("test_scoring.py::test_collision[private-input-two]")
    assert first["test"] == second["test"] == "test_scoring.py::test_collision"
    assert first["case_sha256"] != second["case_sha256"]
    assert "private" not in json.dumps([first, second])
    assert case_identity("../../private.py::test_case")["test"] == "<redacted>"


def test_reports_retain_failed_and_successful_attempts_and_processes(tmp_path: Path) -> None:
    directory = tmp_path / "reports"
    first = AnomalyDiagnostics(directory, tmp_path, "process-1", {"python": "3.12.3"})
    nodeid = "test_scoring.py::test_collision[private-input]"
    first.record(nodeid, "setup", "passed")
    first.record(nodeid, "call", "failed", ValueError("private-input"))
    first.record(nodeid, "teardown", "passed")
    first.record(nodeid, "setup", "passed")
    first.record(nodeid, "call", "passed")
    second = AnomalyDiagnostics(directory, tmp_path, "process-2", {"python": "3.12.3"})
    second.record(nodeid, "setup", "passed")
    second.record(nodeid, "call", "passed")

    reports = [json.loads(path.read_text()) for path in directory.glob("*.json")]
    calls = [report for report in reports if report["phase"] == "call"]
    assert len(reports) == 7
    assert {(report["process"], report["attempt"], report["outcome"]) for report in calls} == {
        ("process-1", 1, "failed"),
        ("process-1", 2, "passed"),
        ("process-2", 1, "passed"),
    }
    assert "private-input" not in json.dumps(reports)


def test_reporting_io_error_does_not_replace_test_failure(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("occupied")
    diagnostics = AnomalyDiagnostics(blocked, tmp_path, "process-1", {})
    diagnostics.record("test_scoring.py::test_case", "call", "failed", ValueError("private-input"))
    assert "original test outcome is unchanged" in caplog.text
    assert "private-input" not in caplog.text


def test_reports_reject_relative_directories_and_unsafe_identifiers(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absolute"):
        AnomalyDiagnostics(Path("relative"), tmp_path, "process-1", {})
    with pytest.raises(ValueError, match="identifier"):
        AnomalyDiagnostics(tmp_path, tmp_path, "../escape", {})
    diagnostics = AnomalyDiagnostics(tmp_path, tmp_path, "process-1", {})
    with pytest.raises(ValueError, match="outcome"):
        diagnostics.record("test_case", "call", "private-value")


def test_pytest_hook_preserves_outcomes_and_captures_setup_failures(pytester: pytest.Pytester) -> None:
    directory = pytester.path / "reports"
    pytester.makeconftest(
        '''
from pathlib import Path
import pytest
from tests.anomaly_diagnostics import AnomalyDiagnostics

def pytest_configure(config):
    root = Path(__file__).parent
    config.pluginmanager.register(AnomalyDiagnostics(root / "reports", root, "process-1", {}))

@pytest.fixture
def failed_setup():
    raise ValueError("private-setup-input")
'''
    )
    pytester.makepyfile(
        '''
import pytest

def test_pass():
    assert True

def test_fail():
    raise RuntimeError("private-call-input")

def test_skip():
    pytest.skip("private-skip-input")

def test_setup_failure(failed_setup):
    assert False
'''
    )

    result = pytester.runpytest("-q", "--tb=no", "-p", "no:cacheprovider", "-p", "no:rerunfailures")
    result.assert_outcomes(passed=1, failed=1, skipped=1, errors=1)
    reports = [json.loads(path.read_text()) for path in directory.glob("*.json")]
    failed = [report for report in reports if report["outcome"] == "failed"]
    assert len(failed) == 2
    assert {(report["phase"], report["exception_chain"][0]["type"]) for report in failed} == {
        ("call", "RuntimeError"),
        ("setup", "ValueError"),
    }
    assert "private-" not in json.dumps(reports)


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


def test_runtime_observation_records_loaded_bytes_and_separate_worker_versions() -> None:
    module = "databricks.labs.dqx.anomaly.single_model_scorer"
    raw = {
        "python": "3.12.3",
        "runtime": "17.3",
        "packages": {"pandas": "2.2.3", "host": "SECRET"},
        "module_sha256": {module: "a" * 64, "host": "SECRET"},
        "payload": "SECRET",
    }
    observed = runtime_observation(raw, {module: "a" * 64})
    assert observed["source"] == "worker" and observed["runtime"] == "17.3"
    modules = observed["modules"]
    assert isinstance(modules, dict)
    assert modules[module]["matches_wheel"] is True
    assert "SECRET" not in json.dumps(observed)
    mismatch = runtime_observation(raw, {module: "b" * 64})
    modules = mismatch["modules"]
    assert isinstance(modules, dict)
    assert modules[module]["matches_wheel"] is False
    unknown = runtime_observation({}, {})
    modules = unknown["modules"]
    assert isinstance(modules, dict)
    assert modules[module]["matches_wheel"] is None
    assert unknown["runtime"] == "unavailable"


def test_runtime_report_is_separate_from_per_test_attempts(tmp_path: Path) -> None:
    reporter = AnomalyDiagnostics(tmp_path, tmp_path, "worker-1", {})
    reporter.record_runtime({"compute": "unknown", "worker": "unavailable"})
    reporter.record("tests/test_case.py::test_case", "call", "passed")
    paths = list(tmp_path.glob("*.json"))
    assert len(paths) == 2
    assert json.loads((tmp_path / "worker-1-runtime.json").read_text())["compute"] == "unknown"


@pytest.mark.parametrize(
    "kind,expected", [("CLASSIC_PREVIEW", "classic"), (None, "unknown"), ("serverless", "unknown")]
)
def test_compute_observation_requires_measured_classic_kind(kind: str | None, expected: str) -> None:
    observed = compute_observation("private-cluster", kind=kind, source="JOB", runtime="17.3.x-scala2.13")
    assert observed["compute"] == expected
    assert observed["attached_cluster_sha256"] == hashlib.sha256(b"private-cluster").hexdigest()
    assert observed["cluster_source"] == "JOB"
    assert observed["databricks_runtime"] == "17.3.x-scala2.13"
    assert "private-cluster" not in json.dumps(observed)


def test_compute_observation_does_not_infer_from_unattached_or_unsafe_metadata() -> None:
    absent = compute_observation(None, kind="CLASSIC_PREVIEW", source="API", runtime="17.3")
    assert absent["compute"] == "unknown" and absent["cluster_kind"] is None
    unsafe = compute_observation("private-cluster", source="private-source", runtime="https://private.invalid")
    assert unsafe["cluster_source"] is None and unsafe["databricks_runtime"] == "unavailable"
    assert "private" not in json.dumps(unsafe)


@pytest.mark.parametrize("failure_source", ["import", "collection"])
def test_pytest_collection_errors_preserve_causes_and_redact_payloads(
    pytester: pytest.Pytester, failure_source: str
) -> None:
    pytester.makeconftest(
        """
from pathlib import Path
from tests.anomaly_diagnostics import AnomalyDiagnostics

def pytest_configure(config):
    root = Path(__file__).parent
    config.pluginmanager.register(AnomalyDiagnostics(root / "reports", root / "anomaly", "process-1", {}))
"""
    )
    directory = pytester.path / "anomaly"
    directory.mkdir()
    failures = {
        "import": """
def fail_import():
    try:
        raise ValueError("private-input-value")
    except ValueError as cause:
        raise ImportError("private-token https://private.invalid/") from cause
fail_import()
""",
        "collection": """
def pytest_generate_tests(metafunc):
    try:
        raise LookupError("private-parameter-value")
    except LookupError as cause:
        raise RuntimeError("private-response-body") from cause

def test_parameterized(value):
    assert value
""",
    }
    (directory / "test_failure.py").write_text(failures[failure_source])
    pytester.makepyfile(test_unrelated='raise ValueError("private-outside-scope")')

    result = pytester.runpytest("-q", "--tb=no", "-p", "no:cacheprovider", "-p", "no:rerunfailures")

    result.assert_outcomes(errors=2)
    assert result.ret == pytest.ExitCode.INTERRUPTED
    reports = [json.loads(path.read_text()) for path in (pytester.path / "reports").glob("*.json")]
    assert len(reports) == 1
    report = reports[0]
    assert report["test"] == "anomaly/test_failure.py"
    assert report["phase"] == "collect" and report["outcome"] == "failed"
    types = [entry["type"] for entry in report["exception_chain"]]
    expected = (
        ["CollectError", "ImportError", "ValueError"] if failure_source == "import" else ["RuntimeError", "LookupError"]
    )
    assert types == expected
    assert all(entry["frames"] for entry in report["exception_chain"])
    assert "private-" not in json.dumps(reports)
    assert "private.invalid" not in json.dumps(reports)
