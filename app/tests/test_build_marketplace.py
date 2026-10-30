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
TEMPLATE_LOCK = APP_DIR / "marketplace_templates" / "uv.lock"
DQX_PACKAGE = "databricks-labs-dqx"
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
    "ai-gateway",
    "ai-functions",
    "workspace.workspace",
    "mcp.functions",
    "files.files",
    "genie",
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


def test_marketplace_template_lock_pins_the_release_dqx_version() -> None:
    """The release pins *dqx_version* from the bundle; the copied lock must resolve that same DQX.

    *dqx_version* is bumped by hand in a Studio release (make fmt does not change it), together
    with the template lock. A lagging lock makes the app and task runner install different DQX
    versions, so refresh the lock (see DEPLOYMENT.md) whenever the bundle's *dqx_version* moves.
    """
    template = tomllib.loads(TEMPLATE_LOCK.read_text(encoding="utf-8"))
    locked = {package["version"] for package in template["package"] if package["name"] == DQX_PACKAGE}

    assert locked == {DQX_VERSION}


def test_marketplace_build_rejects_a_lock_for_another_dqx_version(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="DQX version"):
        build(output_dir=tmp_path / "artifact", dqx_version="0.15.0")
    assert not (tmp_path / "artifact").exists()


def _runtime_closure(lock_path: Path, *, through_dqx: bool) -> set[str]:
    """Names the app package reaches at runtime in *lock_path*, activating extras.

    Follows each reached package's dependencies and the optional dependencies of its active
    extras; dev-dependency groups are never followed, so development-only packages fall out.
    With *through_dqx* off, DQX itself is reached but its own dependencies are not followed.
    """
    packages = {package["name"]: package for package in tomllib.loads(lock_path.read_text(encoding="utf-8"))["package"]}
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

    for dependency in packages["databricks-labs-dqx-app"].get("dependencies", []):
        visit(dependency)
    while pending:
        name = pending.pop()
        package = packages.get(name)
        if package is None or (name == DQX_PACKAGE and not through_dqx):
            continue
        for dependency in package.get("dependencies", []):
            visit(dependency)
        for extra in active_extras.get(name, set()):
            for dependency in package.get("optional-dependencies", {}).get(extra, []):
                visit(dependency)
    return reached


def test_marketplace_template_lock_matches_the_app_dependencies() -> None:
    """Guard the committed Marketplace lock against silently drifting from the app's own deps.

    The Marketplace lock is a tracked template, not regenerated at build time, so an app
    dependency change that isn't mirrored there would ship a stale runtime graph. What the
    app's own dependencies reach must match ``app/uv.lock``. DQX's own dependencies are left
    out: ``app/uv.lock`` resolves DQX from this checkout, while the template resolves the
    published *dqx_version*, so they legitimately differ until Studio moves to a newer DQX.
    If this fails, refresh the Marketplace lock (see DEPLOYMENT.md).
    """
    expected = _runtime_closure(APP_DIR / "uv.lock", through_dqx=False)
    actual = _runtime_closure(TEMPLATE_LOCK, through_dqx=False)

    assert actual == expected, (
        "marketplace_templates/uv.lock is stale vs app/uv.lock — regenerate it (see DEPLOYMENT.md). "
        f"missing={sorted(expected - actual)} unexpected={sorted(actual - expected)}"
    )


def _app_package(lock_path: Path) -> dict:
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    return next(package for package in lock["package"] if package["name"] == "databricks-labs-dqx-app")


def test_marketplace_template_lock_matches_the_app_dependency_specifiers() -> None:
    """The template must lock the app's own requirements and their versions, as app/uv.lock does.

    The deploy installs the template with *uv run --frozen*, which never checks it against the
    pyproject, so a changed or bumped app dependency must fail here rather than ship stale.
    DQX is pinned to *dqx_version* in the template and resolved from the checkout in app/uv.lock,
    so it is left out.
    """
    app_lock, template = _app_package(APP_DIR / "uv.lock"), _app_package(TEMPLATE_LOCK)

    def requirements(package: dict) -> list[dict]:
        return sorted(
            (entry for entry in package["metadata"]["requires-dist"] if entry["name"] != DQX_PACKAGE),
            key=lambda entry: entry["name"],
        )

    assert requirements(template) == requirements(app_lock)

    def direct_versions(lock_path: Path, package: dict) -> dict[str, str]:
        versions = {
            p["name"]: p.get("version") for p in tomllib.loads(lock_path.read_text(encoding="utf-8"))["package"]
        }
        return {d["name"]: versions[d["name"]] for d in package["dependencies"] if d["name"] != DQX_PACKAGE}

    assert direct_versions(TEMPLATE_LOCK, template) == direct_versions(APP_DIR / "uv.lock", app_lock)


def test_marketplace_template_lock_holds_exactly_its_runtime_closure() -> None:
    """The template, published DQX included, must be complete and carry nothing unreachable."""
    names = {package["name"] for package in tomllib.loads(TEMPLATE_LOCK.read_text(encoding="utf-8"))["package"]}
    reached = _runtime_closure(TEMPLATE_LOCK, through_dqx=True)

    assert names == reached, f"missing={sorted(reached - names)} unreachable={sorted(names - reached)}"


def test_manifest_has_exact_resource_contract() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    assert manifest["user_api_scopes"] == EXPECTED_SCOPES
    assert manifest["resource_specs"] == [
        {"name": "dqx-sql-warehouse", "sql_warehouse_spec": {"permission": "CAN_MANAGE"}},
        {"name": "dqx-lakebase", "postgres_spec": {"permission": "CAN_CONNECT_AND_CREATE"}},
    ]


def test_app_yaml_has_single_worker_and_resource_bindings() -> None:
    app_yaml = yaml.safe_load(APP_YAML.read_text(encoding="utf-8"))
    env = {item["name"]: item for item in app_yaml["env"]}

    assert env == {
        "DATABRICKS_WAREHOUSE_ID": {"name": "DATABRICKS_WAREHOUSE_ID", "valueFrom": "dqx-sql-warehouse"},
        "DQX_LAKEBASE_SCHEMA": {"name": "DQX_LAKEBASE_SCHEMA", "value": "dqx_studio"},
        "DQX_ADMIN_GROUP": {"name": "DQX_ADMIN_GROUP", "value": "admins"},
    }
    assert len(app_yaml["env"]) == len(env)
    assert not {"DQX_WHEELS_VOLUME", "DQX_CATALOG", "DQX_PREFIX", "DQX_USER_GROUPS"} & env.keys()
    assert app_yaml["command"][-2:] == ["--workers", "1"]
    # Install exactly the committed lock; never re-resolve at container start.
    assert app_yaml["command"][:3] == ["uv", "run", "--frozen"]


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

    build(output_dir=output_dir, dqx_version=DQX_VERSION)

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
