"""Tests for ``app/scripts/build_app.py``."""

import importlib.util
import tomllib
from pathlib import Path

import pytest

BUILD_APP_PATH = Path(__file__).resolve().parent.parent / "scripts" / "build_app.py"


@pytest.mark.parametrize(
    ("runner", "tool", "extra_args"),
    [
        ("_run_orval", "orval", []),
        ("_run_vite_build", "vite", ["build"]),
    ],
)
@pytest.mark.parametrize(
    ("os_name", "expected_suffix"),
    [("posix", ""), ("nt", ".cmd")],
)
def test_node_runners_pick_platform_shim(monkeypatch, runner, tool, extra_args, os_name, expected_suffix):
    captured = []
    build_app = _load_build_app()

    def _fake_run(cmd, **_kwargs):
        captured.append(cmd)

    monkeypatch.setattr(build_app.os, "name", os_name)
    monkeypatch.setattr(build_app.subprocess, "run", _fake_run)

    getattr(build_app, runner)()

    cmd = captured[0]
    assert cmd[0] == str(build_app.NODE_BIN / f"{tool}{expected_suffix}")
    assert cmd[1:] == extra_args


def test_openapi_dump_uses_the_frozen_app_lock(tmp_path):
    build_app = _load_build_app()

    command = build_app.openapi_dump_command(tmp_path / "openapi.json")

    assert command[:5] == ["uv", "run", "--frozen", "--exact", "--all-extras"]


def test_release_pyproject_pins_the_published_dqx_and_drops_local_settings():
    build_app = _load_build_app()
    source = build_app.PYPROJECT.read_text(encoding="utf-8")

    release = build_app.release_pyproject(source, "0.17.0")

    parsed = tomllib.loads(release)
    assert "databricks-labs-dqx[llm,datacontract]==0.17.0" in parsed["project"]["dependencies"]
    assert "dependency-groups" not in parsed
    assert "uv" not in parsed.get("tool", {})


def test_deploy_tree_installs_the_published_dqx_from_the_committed_lock(monkeypatch, tmp_path):
    """A DAB deploy must install the same pinned DQX and lock as Marketplace, never the checkout."""
    build_app = _load_build_app()
    monkeypatch.setattr(build_app, "BUILD_DIR", tmp_path)
    (tmp_path / "_vendor" / "dqx").mkdir(parents=True)  # left by an older vendoring build

    build_app.assemble_deploy_tree()

    dqx_version = build_app.dqx_release_version()
    pyproject = tomllib.loads((tmp_path / "pyproject.toml").read_text(encoding="utf-8"))
    assert f"databricks-labs-dqx[llm,datacontract]=={dqx_version}" in pyproject["project"]["dependencies"]
    assert "sources" not in pyproject.get("tool", {}).get("uv", {})
    assert (tmp_path / "uv.lock").read_text(encoding="utf-8") == build_app.release_lock_text(dqx_version)
    assert not (tmp_path / "_vendor").exists()


def test_release_lock_drops_resolver_options():
    build_app = _load_build_app()

    lock = tomllib.loads(build_app.release_lock_text(build_app.dqx_release_version()))

    assert "options" not in lock


@pytest.mark.parametrize("dqx_version", ["0.15.0", "99.0.0"])  # never the pinned release
def test_release_lock_rejects_another_dqx_version(dqx_version):
    build_app = _load_build_app()

    with pytest.raises(RuntimeError, match="DQX version"):
        build_app.release_lock_text(dqx_version)


def test_lock_dqx_check_compares_normalized_versions():
    """uv records PEP 440 normal form, so a hyphenated pre-release pin still matches its lock."""
    build_app = _load_build_app()
    lock = '[[package]]\nname = "databricks-labs-dqx"\nversion = "0.18.0rc1"\n'

    build_app.require_lock_resolves_dqx(lock, "0.18.0-rc1")


def _load_build_app():
    spec = importlib.util.spec_from_file_location("build_app", BUILD_APP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
