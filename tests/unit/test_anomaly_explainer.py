"""Pure validation for the contributor-message API; Spark output lives in integration tests."""

from collections.abc import Callable
import json
import subprocess
import sys
from typing import NoReturn
from unittest.mock import create_autospec

import cloudpickle

import pytest
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DataType

from databricks.labs.dqx.anomaly.explainability import add_top_contributors_to_message
from databricks.labs.dqx.errors import InvalidParameterError


@pytest.mark.parametrize("columns", [[], ["anomaly_contributions", "other"]])
def test_add_top_contributors_requires_a_public_severity_source(columns: list[str]) -> None:
    df = create_autospec(DataFrame, instance=True)
    df.columns = columns
    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(df, threshold=60.0)


@pytest.mark.parametrize("top_n,expected", [(2, "a (80%), b (20%)"), (0, ""), (-1, "a (80%)")])
def test_contributor_udf_deserializes_without_dqx_on_worker(
    monkeypatch: pytest.MonkeyPatch, top_n: int, expected: str
) -> None:
    """The real public entry point must send a self-contained formatter to Spark."""
    payloads: list[bytes] = []

    class UdfCaptured(RuntimeError):
        pass

    def capture_udf(function: Callable[[dict[str, float | None] | None], str], _return_type: DataType) -> NoReturn:
        payloads.append(cloudpickle.dumps(function))
        raise UdfCaptured()

    monkeypatch.setattr(F, "udf", capture_udf)
    frame = create_autospec(DataFrame, instance=True)
    frame.columns = ["severity_percentile", "anomaly_contributions"]
    with pytest.raises(UdfCaptured):
        add_top_contributors_to_message(frame, threshold=60.0, top_n=top_n)

    worker = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            """
import importlib.abc
import json
import sys
import cloudpickle

class MissingDqx(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "databricks.labs" or fullname.startswith("databricks.labs."):
            raise ModuleNotFoundError("No module named 'databricks.labs'")
        return None

sys.meta_path.insert(0, MissingDqx())
formatter = cloudpickle.loads(sys.stdin.buffer.read())
print(json.dumps([
    formatter({"a": 79.5, "b": 20.5, "zero": 0.0, "missing": None}),
    formatter(None),
    formatter({}),
    formatter({"zero": 0.0, "missing": None}),
]))
""",
        ],
        input=payloads[0],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert worker.returncode == 0, worker.stderr.decode()
    assert json.loads(worker.stdout) == [expected, "", "", ""]
