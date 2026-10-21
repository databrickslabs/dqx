"""Sanitize packaged anomaly test failures and classify endpoint availability."""

import re
from collections import deque
from collections.abc import Mapping

from databricks.labs.dqx.ai_endpoint import endpoint_unavailable_reason


__all__ = [
    "DEPENDENCIES",
    "dependency_provenance",
    "endpoint_unavailable_reason",
    "safe_traceback_frame",
    "worker_traceback",
]


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
        "PySparkNotImplementedError",
        "NotImplementedError",
        "InvalidConfigError",
        "ArrowInvalid",
        "ArrowTypeError",
        "PicklingError",
        "UnpicklingError",
    )
)
_FRAME = re.compile(r'^\s*File "([^"\n]{1,1000})", line ([0-9]{1,6})(?:, in ([\w.<>]{1,100}))?')
_ERROR = re.compile(r"^\s*([A-Za-z_][\w.]{0,150})(?::|$)")
_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_NOTEBOOK_FRAME = re.compile(r"^\s*(?:Cell In\[[0-9]+\]|File <command-[A-Za-z0-9_-]+>), line ([0-9]{1,6})")
_IPYTHON_FRAME = re.compile(r"^\s*File ([^\n]{1,1000}?):([0-9]{1,6})(?:, in ([\w.<>]+))?")


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
    for raw_line in message.splitlines():
        line = _ANSI.sub("", raw_line)
        frame = _FRAME.match(line) or _IPYTHON_FRAME.match(line)
        if frame:
            frames.append(safe_traceback_frame(frame[1], int(frame[2]), frame[3] or "<unknown>"))
        notebook_frame = _NOTEBOOK_FRAME.match(line)
        if notebook_frame:
            frames.append({"file": "<notebook>", "line": int(notebook_frame[1]), "function": "<module>"})
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
