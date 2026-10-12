"""Opt-in anomaly test reports containing code locations, never exception payloads.

The acceptance runner keeps only the final exception line. These reports retain
exception chains and worker traceback locations without copying source lines,
locals, HTTP logs, input values, or arbitrary exception messages into artifacts.
"""

import hashlib
import json
import logging
import re
import sys
import traceback
from collections import deque
from collections.abc import Generator, Mapping
from pathlib import Path

import pytest

if sys.version_info >= (3, 11):
    from builtins import BaseExceptionGroup
else:
    from exceptiongroup import BaseExceptionGroup


DEPENDENCIES = (
    "databricks-labs-dqx",
    "databricks-connect",
    "pyspark",
    "scikit-learn",
    "mlflow",
    "shap",
    "numba",
    "numpy",
    "pandas",
    "pyarrow",
    "cloudpickle",
    "coverage",
)
_PACKAGE_ROOTS = (
    "databricks/labs/dqx/",
    "databricks/sdk/",
    "databricks/connect/",
    "pyspark/",
    "sklearn/",
    "mlflow/",
    "shap/",
    "numba/",
    "numpy/",
    "pandas/",
    "pyarrow/",
    "cloudpickle/",
    "coverage/",
    "scipy/",
)
_WORKER_ERRORS = frozenset(
    (
        "AssertionError",
        "AttributeError",
        "ImportError",
        "ModuleNotFoundError",
        "KeyError",
        "IndexError",
        "TypeError",
        "ValueError",
        "RuntimeError",
        "MemoryError",
        "OverflowError",
        "PythonException",
        "SparkException",
        "PySparkRuntimeError",
        "ArrowInvalid",
        "ArrowTypeError",
        "PicklingError",
        "UnpicklingError",
    )
)
_FRAME = re.compile(r'^\s*File "([^"\n]{1,1000})", line ([0-9]{1,6})(?:, in ([\w.<>]{1,100}))?')
_ERROR = re.compile(r"^\s*([A-Za-z_][\w.]{0,150}):")
logger = logging.getLogger(__name__)


def dependency_provenance(
    versions: Mapping[str, str],
    python_version: str,
    *,
    wheel_sha256: str | None = None,
    revision: str | None = None,
    source: str = "unknown",
) -> dict[str, object]:
    """Filter supplied package metadata into a deterministic, public-safe manifest."""
    safe_versions = {
        package: value if re.fullmatch(r"[0-9][A-Za-z0-9.+_-]{0,79}", value) else "unavailable"
        for package in DEPENDENCIES
        if (value := versions.get(package)) is not None
    }
    return {
        "python": python_version if re.fullmatch(r"[0-9.]{1,20}", python_version) else "unavailable",
        "packages": safe_versions,
        "wheel_sha256": wheel_sha256 if wheel_sha256 and re.fullmatch(r"[a-f0-9]{64}", wheel_sha256) else None,
        "revision": revision if revision and re.fullmatch(r"[a-f0-9]{40,64}", revision) else None,
        "source": source if source in {"checkout", "installed", "worker", "unknown"} else "unknown",
    }


SCORER_MODULES = (
    "databricks.labs.dqx.anomaly.single_model_scorer",
    "databricks.labs.dqx.anomaly.ensemble_scorer",
    "databricks.labs.dqx.anomaly.explainability",
)


def runtime_observation(raw: object, expected: Mapping[str, str]) -> dict[str, object]:
    """Allowlist observed worker versions and compare loaded files with the wheel."""
    if not isinstance(raw, dict):
        raise ValueError("Invalid anomaly worker provenance")
    packages = raw.get("packages")
    versions = (
        {key: value for key, value in packages.items() if isinstance(key, str) and isinstance(value, str)}
        if isinstance(packages, dict)
        else {}
    )
    python = raw.get("python")
    observed = dependency_provenance(versions, python if isinstance(python, str) else "unavailable", source="worker")
    hashes = raw.get("module_sha256")
    hashes = hashes if isinstance(hashes, dict) else {}
    modules = {}
    for module in SCORER_MODULES:
        digest = hashes.get(module)
        digest = digest if isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest) else None
        wheel_digest = expected.get(module)
        modules[module] = {
            "loaded_sha256": digest,
            "wheel_sha256": wheel_digest,
            "matches_wheel": digest == wheel_digest if digest and wheel_digest else None,
        }
    observed["modules"] = modules
    runtime = raw.get("runtime")
    observed["runtime"] = (
        runtime if isinstance(runtime, str) and re.fullmatch(r"[0-9][A-Za-z0-9.+_-]{0,60}", runtime) else "unavailable"
    )
    return observed


def compute_observation(
    cluster_id: str | None, *, kind: str | None = None, source: str | None = None, runtime: str | None = None
) -> dict[str, object]:
    """Describe measured cluster metadata without publishing its identifier.

    Only the Clusters API's explicit classic kind establishes compute type.
    Missing metadata never turns a configured serverless flag into evidence.
    """
    return {
        "compute": "classic" if cluster_id and kind == "CLASSIC_PREVIEW" else "unknown",
        "attached_cluster_sha256": hashlib.sha256(cluster_id.encode()).hexdigest() if cluster_id else None,
        "cluster_kind": kind if cluster_id and kind == "CLASSIC_PREVIEW" else None,
        "cluster_source": (
            source
            if cluster_id and source in {"API", "JOB", "MODELS", "PIPELINE", "PIPELINE_MAINTENANCE", "SQL", "UI"}
            else None
        ),
        "databricks_runtime": (
            runtime
            if cluster_id and runtime and re.fullmatch(r"[0-9][A-Za-z0-9.+_-]{0,79}", runtime)
            else "unavailable"
        ),
    }


def safe_traceback_frame(filename: str, line: int, function: str) -> dict[str, object]:
    """Keep public package-relative code locations without exposing machine paths."""
    normalized = filename.replace("\\", "/")
    safe_path = "<external>"
    for marker in ("/databricks/labs/dqx/", "/tests/", "/site-packages/"):
        if marker not in normalized:
            continue
        prefix = marker.removeprefix("/") if marker != "/site-packages/" else ""
        candidate = prefix + normalized.rsplit(marker, 1)[1]
        if (
            candidate.startswith((*_PACKAGE_ROOTS, "tests/"))
            and re.fullmatch(r"[A-Za-z0-9_./-]{1,240}\.py", candidate)
            and ".." not in candidate.split("/")
        ):
            safe_path = candidate
        break
    return {
        "file": safe_path,
        "line": line,
        "function": function if re.fullmatch(r"[A-Za-z_][\w.<>]{0,100}|<module>", function) else "<unknown>",
    }


def worker_traceback(message: str) -> dict[str, object]:
    """Extract only frame locations and known error classes from a worker message."""
    frames: deque[dict[str, object]] = deque(maxlen=40)
    errors: list[str] = []
    message = message[-100_000:]
    for line in message.splitlines():
        frame = _FRAME.match(line)
        if frame:
            frames.append(safe_traceback_frame(frame[1], int(frame[2]), frame[3] or "<unknown>"))
        error = _ERROR.match(line)
        if error and (name := error[1].rsplit(".", 1)[-1]) in _WORKER_ERRORS and name not in errors:
            errors.append(name)
    hints = {
        "missing_python_dependency": "No module named" in message,
        "missing_python_attribute": "has no attribute" in message,
        "python_import_failure": "cannot import name" in message,
        "worker_result_schema": "SCHEMA_MISMATCH_FOR_PANDAS_UDF" in message or "ArrowTypeError" in message,
        "model_serialization": "PicklingError" in message or "UnpicklingError" in message,
    }
    return {
        "frames": list(frames),
        "error_types": errors,
        "hints": [name for name, present in hints.items() if present],
    }


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


def spark_error_condition(exception: BaseException) -> str | None:
    """Read only a public Spark condition identifier, never its message parameters."""
    try:
        method = getattr(exception, "getCondition", None)
        if not callable(method):
            method = getattr(exception, "getErrorClass", None)
        condition: object = method() if callable(method) else None
    except Exception:
        # A diagnostic accessor failure must not replace the original test error.
        return None
    if (
        isinstance(condition, str)
        and len(condition) <= 128
        and re.fullmatch(r"[A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)*", condition)
    ):
        return condition
    return None


def exception_chain(exception: BaseException | None) -> list[dict[str, object]]:
    """Describe up to eight grouped or chained exceptions without retaining payloads.

    Traverse group members depth first, including each member's cause or context.
    The shared limit and visited set also bound nested groups and cyclic chains.
    """
    chain: list[dict[str, object]] = []
    visited: set[int] = set()
    pending = [exception] if exception is not None else []
    while pending and len(chain) < 8:
        current = pending.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))
        frames = [
            safe_traceback_frame(frame.filename, frame.lineno or 0, frame.name)
            for frame in traceback.extract_tb(current.__traceback__, limit=-40)
        ]
        chain.append(
            {
                "type": type(current).__name__,
                "spark_condition": spark_error_condition(current),
                "frames": frames,
                "worker_traceback": worker_traceback(str(current)),
            }
        )
        cause = current.__cause__ or (None if current.__suppress_context__ else current.__context__)
        if cause is not None:
            pending.append(cause)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(reversed(current.exceptions[:8]))
    return chain


def case_identity(nodeid: str) -> dict[str, str]:
    """Identify parameterized cases by digest while excluding parameter input values."""
    name = nodeid.partition("[")[0].replace("\\", "/")
    if not re.fullmatch(r"[A-Za-z0-9_./:]{1,300}", name) or ".." in name.split("/"):
        name = "<redacted>"
    return {"test": name, "case_sha256": hashlib.sha256(nodeid.encode()).hexdigest()}


class AnomalyDiagnostics:
    """Record every test phase without changing pytest's original outcome.

    The caller supplies an absolute artifact directory and a process identifier.
    The hook is restricted to the supplied anomaly test directory; other suites
    retain their existing reporting behavior even when collected together.
    """

    def __init__(
        self,
        directory: Path,
        test_root: Path,
        process_id: str,
        provenance: Mapping[str, object],
    ) -> None:
        if not directory.is_absolute():
            raise ValueError("The anomaly diagnostic directory must be absolute")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", process_id):
            raise ValueError("Invalid diagnostic process identifier")
        self.directory = directory
        self.test_root = test_root.resolve()
        self.process_id = process_id
        self.provenance = dict(provenance)
        self.attempts: dict[str, int] = {}
        self.runtime_recorded = False

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_makereport(
        self, item: pytest.Item, call: pytest.CallInfo[None]
    ) -> Generator[None, pytest.TestReport, pytest.TestReport]:
        """Persist a phase report after pytest has decided its outcome."""
        report = yield
        if item.path.is_relative_to(self.test_root) and report.when is not None:
            exception = call.excinfo.value if call.excinfo is not None else None
            self.record(report.nodeid, report.when, report.outcome, exception)
        return report

    def pytest_exception_interact(
        self,
        node: pytest.Item | pytest.Collector,
        call: pytest.CallInfo[object],
        report: pytest.TestReport | pytest.CollectReport,
    ) -> None:
        """Keep collection exceptions before pytest reduces them to rendered prose."""
        if isinstance(report, pytest.CollectReport) and report.failed and node.path.is_relative_to(self.test_root):
            exception = call.excinfo.value if call.excinfo is not None else None
            self.record(report.nodeid, "collect", report.outcome, exception)

    def record(self, nodeid: str, phase: str, outcome: str, exception: BaseException | None = None) -> None:
        """Write a distinct report for each phase, test attempt, and Python process."""
        if phase not in {"collect", "setup", "call", "teardown"} or outcome not in {
            "passed",
            "failed",
            "skipped",
            "rerun",
        }:
            raise ValueError("Invalid pytest diagnostic outcome")
        identity = case_identity(nodeid)
        case = identity["case_sha256"]
        if phase in {"collect", "setup"}:
            self.attempts[case] = self.attempts.get(case, 0) + 1
        attempt = self.attempts.get(case, 1)
        report = {
            **identity,
            "process": self.process_id,
            "attempt": attempt,
            "phase": phase,
            "outcome": outcome,
            "provenance": self.provenance,
            "exception_chain": exception_chain(exception),
            "privacy": "Exception prose, source lines, locals, test parameters, and captured logs are omitted.",
        }
        filename = f"{self.process_id}-{case}-{attempt}-{phase}.json"
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            (self.directory / filename).write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        except OSError:
            logger.warning("Could not write anomaly diagnostic report; the original test outcome is unchanged")

    def record_runtime(self, evidence: Mapping[str, object]) -> None:
        """Retain allowlisted runtime evidence without changing test outcomes."""
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            (self.directory / f"{self.process_id}-runtime.json").write_text(
                json.dumps(dict(evidence), indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        except OSError:
            logger.warning("Could not write anomaly runtime provenance; test outcomes are unchanged")
