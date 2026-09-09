"""Tests for ``app/scripts/build_app.py``."""

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


def test_openapi_dump_uses_the_frozen_app_lock(tmp_path):
    build_app = _load_build_app()

    command = build_app.openapi_dump_command(tmp_path / "openapi.json")

    assert command[:5] == ["uv", "run", "--frozen", "--exact", "--all-extras"]


def test_local_dqx_path_reads_the_uv_source():
    build_app = _load_build_app()

    pyproject = {"tool": {"uv": {"sources": {"databricks-labs-dqx": {"path": "../"}}}}}

    assert build_app._local_dqx_path(pyproject) == "../"


@pytest.mark.parametrize(
    "pyproject",
    [
        {},
        {"tool": {"uv": {"sources": {}}}},
        # Pinned to the published wheel: nothing to vendor.
        {"tool": {"uv": {"sources": {"databricks-labs-dqx": {"index": "pypi"}}}}},
    ],
)
def test_local_dqx_path_is_none_without_a_path_source(pyproject):
    build_app = _load_build_app()

    assert build_app._local_dqx_path(pyproject) is None


def test_retarget_rewrites_both_deploy_files(monkeypatch, tmp_path):
    build_app = _load_build_app()
    monkeypatch.setattr(build_app, "BUILD_DIR", tmp_path)

    (tmp_path / "pyproject.toml").write_text('databricks-labs-dqx = { path = "../" }\n', encoding="utf-8")
    (tmp_path / "uv.lock").write_text(
        'source = { directory = "../" }\n{ name = "databricks-labs-dqx", directory = "../" },\n',
        encoding="utf-8",
    )

    build_app._retarget_dqx_source("../")

    pyproject = (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    lock = (tmp_path / "uv.lock").read_text(encoding="utf-8")

    assert f'path = "{build_app.DQX_VENDOR_REL}"' in pyproject
    assert lock.count(f'directory = "{build_app.DQX_VENDOR_REL}"') == 2
    # A path escaping the deploy tree is what crashed the Apps container: uv
    # cannot read metadata from a directory that was never uploaded.
    assert '"../"' not in pyproject + lock


@pytest.mark.parametrize("missing", ["pyproject.toml", "uv.lock"])
def test_retarget_fails_loudly_when_the_source_moved(monkeypatch, tmp_path, missing):
    build_app = _load_build_app()
    monkeypatch.setattr(build_app, "BUILD_DIR", tmp_path)

    contents = {
        "pyproject.toml": 'databricks-labs-dqx = { path = "../" }\n',
        "uv.lock": 'source = { directory = "../" }\n',
    }
    contents[missing] = "# nothing to retarget\n"
    for name, text in contents.items():
        (tmp_path / name).write_text(text, encoding="utf-8")

    with pytest.raises(SystemExit):
        build_app._retarget_dqx_source("../")


def test_vendor_copies_the_library_without_caches(monkeypatch, tmp_path):
    build_app = _load_build_app()
    dqx_dir = tmp_path / "dqx"
    build_dir = tmp_path / ".build"
    monkeypatch.setattr(build_app, "DQX_DIR", dqx_dir)
    monkeypatch.setattr(build_app, "BUILD_DIR", build_dir)

    (dqx_dir / "src" / "databricks" / "labs" / "dqx" / "__pycache__").mkdir(parents=True)
    (dqx_dir / "src" / "databricks" / "labs" / "dqx" / "engine.py").write_text("", encoding="utf-8")
    (dqx_dir / "src" / "databricks" / "labs" / "dqx" / "__pycache__" / "engine.pyc").write_text("", encoding="utf-8")
    for name in build_app.DQX_VENDOR_FILES:
        (dqx_dir / name).write_text("", encoding="utf-8")

    build_app._vendor_dqx_library()

    dest = build_dir / build_app.DQX_VENDOR_REL
    for name in build_app.DQX_VENDOR_FILES:
        assert (dest / name).is_file()
    assert (dest / "src" / "databricks" / "labs" / "dqx" / "engine.py").is_file()
    assert not (dest / "src" / "databricks" / "labs" / "dqx" / "__pycache__").exists()


def test_vendor_file_set_matches_the_dqx_checkout():
    """The vendored copy must carry everything hatchling reads to build DQX."""
    build_app = _load_build_app()

    for name in build_app.DQX_VENDOR_FILES:
        assert (build_app.DQX_DIR / name).is_file(), f"{name} missing from the DQX checkout"
    assert (build_app.DQX_DIR / "src" / "databricks" / "labs" / "dqx").is_dir()


def _load_build_app():
    spec = importlib.util.spec_from_file_location("build_app", BUILD_APP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
