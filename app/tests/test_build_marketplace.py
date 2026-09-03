"""Artifact-contract tests for the Databricks Marketplace source folder."""

import hashlib
import os
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest
import yaml

from scripts import build_marketplace as marketplace_builder
from scripts.build_app import build_task_runner_wheel

build = marketplace_builder.build

APP_DIR = Path(__file__).resolve().parent.parent
MANIFEST = APP_DIR / "marketplace_templates" / "manifest.yaml"
APP_YAML = APP_DIR / "marketplace_templates" / "app.yaml"
DQX_VERSION = marketplace_builder.dqx_release_version()

EXPECTED_SCOPES = [
    "sql",
    "sql.alerts",
    "sql.alerts-legacy",
    "sql.dashboards",
    "sql.data-sources",
    "sql.dbsql-permissions",
    "sql.queries",
    "sql.queries-legacy",
    "sql.query-history",
    "sql.statement-execution",
    "sql.warehouses",
    "vectorsearch.vector-search-endpoints",
    "vectorsearch.vector-search-indexes",
    "serving.serving-endpoints",
    "serving.serving-endpoints-data-plane",
    "files.files",
    "dashboards.genie",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read",
]


def test_marketplace_release_uses_the_configured_dqx_version(tmp_path: Path) -> None:
    bundle = tmp_path / "databricks.yml"
    bundle.write_text('variables:\n  dqx_version:\n    default: "0.16.0"\n', encoding="utf-8")

    assert marketplace_builder.dqx_release_version(bundle) == "0.16.0"


@pytest.fixture(scope="module")
def marketplace_artifact(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output_dir = tmp_path_factory.mktemp("marketplace") / "artifact"
    build(output_dir=output_dir, dqx_version=DQX_VERSION)
    return output_dir


def test_marketplace_artifact_is_self_contained(marketplace_artifact: Path) -> None:
    assert (marketplace_artifact / "manifest.yaml").is_file()
    assert (marketplace_artifact / "app.yaml").is_file()
    assert list((marketplace_artifact / "tasks").glob("databricks_labs_dqx_task_runner-*.whl"))
    combined = "\n".join(path.read_text(errors="ignore") for path in marketplace_artifact.rglob("*.toml"))
    assert "../" not in combined
    assert "_vendor/dqx" not in combined
    assert f"databricks-labs-dqx[llm,datacontract]=={DQX_VERSION}" in combined

    task_wheel = next((marketplace_artifact / "tasks").glob("databricks_labs_dqx_task_runner-*.whl"))
    with zipfile.ZipFile(task_wheel) as wheel:
        metadata_name = next(name for name in wheel.namelist() if name.endswith(".dist-info/METADATA"))
        metadata = wheel.read(metadata_name).decode("utf-8")
    assert f"Requires-Dist: databricks-labs-dqx=={DQX_VERSION}" in metadata


def test_marketplace_artifact_contains_only_runtime_inputs(marketplace_artifact: Path) -> None:
    assert (marketplace_artifact / "src" / "databricks_labs_dqx_app" / "__dist__" / "index.html").is_file()
    relative_files = [path.relative_to(marketplace_artifact) for path in marketplace_artifact.rglob("*")]
    forbidden_names = {"node_modules", "package.json", "yarn.lock", "bun.lock", "tsconfig.json"}
    assert not any(forbidden_names.intersection(path.parts) for path in relative_files)
    assert not any("ui" in path.parts for path in relative_files)
    assert not any(path.suffix in {".ts", ".tsx"} for path in relative_files)

    pyproject = tomllib.loads((marketplace_artifact / "pyproject.toml").read_text(encoding="utf-8"))
    assert "dependency-groups" not in pyproject
    assert "databricks-labs-dqx" not in pyproject.get("tool", {}).get("uv", {}).get("sources", {})
    assert {"pytest", "ruff", "basedpyright"}.isdisjoint(pyproject.get("tool", {}))

    task_files = [path for path in (marketplace_artifact / "tasks").iterdir() if path.is_file()]
    assert len(task_files) == 1
    assert task_files[0].suffix == ".whl"

    lock = tomllib.loads((marketplace_artifact / "uv.lock").read_text(encoding="utf-8"))
    locked_names = {package["name"] for package in lock["package"]}
    assert locked_names.isdisjoint({"basedpyright", "pytest", "pytest-asyncio", "pytest-cov", "pytest-mock", "ruff"})


def test_main_ignores_generated_marketplace_artifact() -> None:
    generated_backend = APP_DIR / "marketplace" / "src" / "databricks_labs_dqx_app" / "backend" / "app.py"

    completed = subprocess.run(
        ["git", "check-ignore", "--no-index", "--quiet", str(generated_backend)],
        cwd=APP_DIR,
        check=False,
    )

    assert completed.returncode == 0


def test_marketplace_lock_uses_only_public_package_registries(marketplace_artifact: Path) -> None:
    lock_text = (marketplace_artifact / "uv.lock").read_text(encoding="utf-8")
    lock = tomllib.loads(lock_text)
    registry_sources = {
        source["registry"]
        for package in lock["package"]
        if isinstance((source := package.get("source")), dict) and isinstance(source.get("registry"), str)
    }
    assert ".release-wheels" not in lock_text
    assert registry_sources == {"https://pypi.org/simple"}


def test_marketplace_template_lock_matches_app_runtime_closure() -> None:
    """Guard the committed Marketplace lock against silently drifting from the app deps.

    The Marketplace lock is a tracked template (``marketplace_templates/uv.lock``), not
    regenerated at build time, so an app dependency change that isn't mirrored there would
    ship a stale runtime graph. Its package set must equal the *runtime closure* of
    ``app/uv.lock`` — the app lock minus its development-only packages. If this fails,
    refresh the Marketplace lock (see DEPLOYMENT.md).
    """
    app_lock = tomllib.loads((APP_DIR / "uv.lock").read_text(encoding="utf-8"))
    template_names = {
        package["name"]
        for package in tomllib.loads((APP_DIR / "marketplace_templates" / "uv.lock").read_text(encoding="utf-8"))[
            "package"
        ]
    }
    pkgs = {package["name"]: package for package in app_lock["package"]}

    # Fixpoint walk of the app package's runtime dependencies (activating their extras) and
    # each reached package's dependencies + optional-dependencies. dev-dependencies groups are
    # never followed, so development-only packages fall out of the closure.
    reached: set[str] = {"databricks-labs-dqx-app"}
    active_extras: dict[str, set[str]] = {}
    pending: list[str] = []

    def visit(dependency: dict) -> None:
        name = dependency["name"]
        extras = set(dependency.get("extra", []))
        if name not in reached or extras - active_extras.get(name, set()):
            pending.append(name)
        reached.add(name)
        active_extras.setdefault(name, set()).update(extras)

    for dependency in pkgs["databricks-labs-dqx-app"].get("dependencies", []):
        visit(dependency)
    while pending:
        package = pkgs.get(pending.pop())
        if package is None:
            continue
        for dependency in package.get("dependencies", []):
            visit(dependency)
        for extra in active_extras.get(package["name"], set()):
            for dependency in package.get("optional-dependencies", {}).get(extra, []):
                visit(dependency)

    assert template_names == reached, (
        "marketplace_templates/uv.lock is stale vs app/uv.lock — regenerate it (see DEPLOYMENT.md). "
        f"missing={sorted(reached - template_names)} unexpected={sorted(template_names - reached)}"
    )


def test_manifest_has_exact_resource_contract() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    assert manifest["user_api_scopes"] == EXPECTED_SCOPES
    assert manifest["resource_specs"] == [
        {"name": "dqx-sql-warehouse", "sql_warehouse_spec": {"permission": "CAN_USE"}},
        {"name": "dqx-lakebase", "postgres_spec": {"permission": "CAN_CONNECT_AND_CREATE"}},
        {
            "name": "dqx-wheels-volume",
            "uc_securable_spec": {"securable_type": "VOLUME", "permission": "WRITE_VOLUME"},
        },
    ]


def test_app_yaml_has_single_worker_and_resource_bindings() -> None:
    app_config = yaml.safe_load(APP_YAML.read_text(encoding="utf-8"))
    assert app_config["command"] == [
        "uv",
        "run",
        "--no-dev",
        "uvicorn",
        "databricks_labs_dqx_app.backend.app:app",
        "--workers",
        "1",
    ]
    env = {entry["name"]: {key: value for key, value in entry.items() if key != "name"} for entry in app_config["env"]}
    assert env == {
        "DATABRICKS_WAREHOUSE_ID": {"valueFrom": "dqx-sql-warehouse"},
        "DQX_WHEELS_VOLUME": {"valueFrom": "dqx-wheels-volume"},
        "DQX_TMP_SCHEMA": {"value": "dqx_studio_tmp"},
        "DQX_GENIE_SCHEMA": {"value": "genie"},
        "DQX_LAKEBASE_SCHEMA": {"value": "dqx_studio"},
        "DQX_ADMIN_GROUP": {"value": "admins"},
    }


def test_marketplace_build_is_repeatable_and_removes_stale_files(marketplace_artifact: Path) -> None:
    before = _file_hashes(marketplace_artifact)
    (marketplace_artifact / "stale-file").write_text("must be removed\n", encoding="utf-8")

    build(output_dir=marketplace_artifact, dqx_version=DQX_VERSION)

    assert _file_hashes(marketplace_artifact) == before


def test_marketplace_build_ignores_parent_uv_configuration(
    tmp_path: Path, marketplace_artifact: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    (app_dir / "pyproject.toml").write_text(
        '[tool.uv]\nexclude-newer-package = { "databricks-sdk" = false }\n',
        encoding="utf-8",
    )
    output_dir = app_dir / "marketplace"
    output_dir.mkdir()
    (output_dir / "uv.lock").write_bytes((marketplace_artifact / "uv.lock").read_bytes())
    monkeypatch.setattr(marketplace_builder, "APP_DIR", app_dir)
    monkeypatch.setattr(marketplace_builder, "MARKETPLACE_DIR", output_dir)

    build(output_dir=output_dir, dqx_version="0.15.0")

    lock = tomllib.loads((output_dir / "uv.lock").read_text(encoding="utf-8"))
    assert "options" not in lock


def test_task_runner_build_uses_configured_primary_package_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    captured_index = tmp_path / "captured-index"
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/bin/sh
printf '%s' "$UV_INDEX_URL" > "$CAPTURED_INDEX_FILE"
while [ "$1" != "--out-dir" ]; do shift; done
mkdir -p "$2"
touch "$2/databricks_labs_dqx_task_runner-0.1.0-py3-none-any.whl"
""",
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")
    monkeypatch.setenv("CAPTURED_INDEX_FILE", str(captured_index))
    monkeypatch.setenv("UV_INDEX_URL", "https://mirror.example/simple")

    build_task_runner_wheel(tmp_path / "wheel", "0.16.0")

    assert captured_index.read_text(encoding="utf-8") == "https://mirror.example/simple"


def test_marketplace_check_accepts_generated_artifact(
    marketplace_artifact: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(marketplace_builder, "MARKETPLACE_DIR", marketplace_artifact)
    monkeypatch.setattr(sys, "argv", ["build_marketplace.py", "--check"])

    assert marketplace_builder.main() == 0


def test_marketplace_build_rejects_protected_destinations_without_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app_dir = _configure_protected_tree(tmp_path, monkeypatch)
    protected_targets = [
        tmp_path,
        app_dir,
        app_dir / ".build",
        app_dir / ".build" / "nested",
        app_dir / "marketplace_templates",
        app_dir / "marketplace_templates" / "nested",
        app_dir / "src",
        app_dir / "scripts" / "generated",
    ]

    for target in protected_targets:
        before = _file_hashes(tmp_path)
        with pytest.raises(ValueError, match="protected"):
            build(output_dir=target, dqx_version="0.15.0")
        assert _file_hashes(tmp_path) == before


def test_marketplace_build_rejects_protected_symlink_alias_without_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app_dir = _configure_protected_tree(tmp_path, monkeypatch)
    alias = tmp_path / "app-alias"
    alias.symlink_to(app_dir, target_is_directory=True)
    before = _file_hashes(app_dir)

    with pytest.raises(ValueError, match="protected"):
        build(output_dir=alias / "src" / "generated", dqx_version="0.15.0")

    assert _file_hashes(app_dir) == before
    assert not (app_dir / "src" / "generated").exists()


def test_marketplace_build_rejects_existing_file_without_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_file = tmp_path / "artifact"
    output_file.write_bytes(b"sentinel")

    def unexpected_build_input_check() -> None:
        raise AssertionError("destination validation must run before build input checks")

    monkeypatch.setattr(marketplace_builder, "_require_build_inputs", unexpected_build_input_check)

    with pytest.raises(ValueError, match="directory"):
        build(output_dir=output_file, dqx_version="0.15.0")

    assert output_file.read_bytes() == b"sentinel"


def _configure_protected_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    app_dir = tmp_path / "app"
    for relative in (".build", "marketplace", "marketplace_templates", "scripts", "src"):
        directory = app_dir / relative
        directory.mkdir(parents=True)
        (directory / "sentinel").write_text(f"{relative}\n", encoding="utf-8")
    (app_dir / "pyproject.toml").write_text("[project]\nname = 'sentinel'\n", encoding="utf-8")
    monkeypatch.setattr(marketplace_builder, "APP_DIR", app_dir)
    monkeypatch.setattr(marketplace_builder, "BUILD_DIR", app_dir / ".build")
    monkeypatch.setattr(marketplace_builder, "TEMPLATE_DIR", app_dir / "marketplace_templates")
    monkeypatch.setattr(marketplace_builder, "MARKETPLACE_DIR", app_dir / "marketplace")
    monkeypatch.setattr(marketplace_builder, "PYPROJECT", app_dir / "pyproject.toml")
    return app_dir


def _file_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }
