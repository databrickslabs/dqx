"""Packaged model ownership survives partial deletion failures."""

from pathlib import Path

import pytest

pytest_plugins = ["pytester"]


@pytest.mark.parametrize("failure_phase", ["list", "version", "model"])
def test_packaged_cleanup_attempts_every_owned_model_after_failure(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, failure_phase: str
) -> None:
    pytester.makeconftest(
        rf'''
from pathlib import Path
from unittest.mock import create_autospec
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.catalog import ModelVersionInfo, SchemaInfo
from tests.e2e.conftest import make_anomaly_model_intent

@pytest.fixture
def packaged_anomaly_schema():
    return SchemaInfo(full_name="catalog.schema")

@pytest.fixture
def ws():
    workspace = create_autospec(WorkspaceClient, instance=True)
    def operation(phase, full_name, version=None):
        with Path("cleanup-attempts").open("a") as out:
            out.write(f"{{phase}}:{{full_name}}:{{version}}\n")
        if phase == {failure_phase!r} and full_name.endswith(".first") and version != 2:
            raise PermissionDenied("SECRET_MARKER synthetic rejection")
    def versions(*, full_name):
        operation("list", full_name)
        return [ModelVersionInfo(version=1), ModelVersionInfo(version=2)]
    workspace.model_versions.list.side_effect = versions
    workspace.model_versions.delete.side_effect = lambda **kw: operation("version", **kw)
    workspace.registered_models.delete.side_effect = lambda **kw: operation("model", **kw)
    return workspace
'''
    )
    pytester.makepyfile(
        '''
def test_training(make_anomaly_model_intent):
    make_anomaly_model_intent(suffix="first")
    make_anomaly_model_intent(suffix="second")
    raise RuntimeError("Original training failure")
'''
    )
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest("-q", "--tb=line", "-p", "no:cacheprovider", "-p", "no:rerunfailures")
    result.assert_outcomes(failed=1, errors=1)
    attempts = (Path(pytester.path) / "cleanup-attempts").read_text().splitlines()
    assert "model:catalog.schema.first:None" in attempts
    assert "version:catalog.schema.second:1" in attempts
    assert "version:catalog.schema.second:2" in attempts
    assert "model:catalog.schema.second:None" in attempts
    if failure_phase != "list":
        assert "version:catalog.schema.first:2" in attempts
    output = result.stdout.str()
    assert "Original training failure" in output
    assert "Packaged anomaly model cleanup failed" in output
    assert "SECRET_MARKER" not in output
