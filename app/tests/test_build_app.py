"""Tests for ``app/scripts/build_app.py``."""

from __future__ import annotations

import importlib.util
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


def _load_build_app():
    spec = importlib.util.spec_from_file_location("build_app", BUILD_APP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
