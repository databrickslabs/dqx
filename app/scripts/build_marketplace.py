"""Build the generated Databricks Marketplace source folder."""

import argparse
import os
import re
import shutil
import tempfile
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

import yaml

if __package__:
    from scripts.build_app import (
        RELEASE_VERSION,
        build_task_runner_wheel,
        dqx_release_version,
        release_lock_text,
        release_pyproject,
        require_lock_resolves_dqx,
    )
else:
    from build_app import (
        RELEASE_VERSION,
        build_task_runner_wheel,
        dqx_release_version,
        release_lock_text,
        release_pyproject,
        require_lock_resolves_dqx,
    )

APP_DIR = Path(__file__).resolve().parent.parent
BUILD_DIR = APP_DIR / ".build"
TEMPLATE_DIR = APP_DIR / "marketplace_templates"
MARKETPLACE_DIR = APP_DIR / "marketplace"
PYPROJECT = APP_DIR / "pyproject.toml"

RUNTIME_FILES = ("README.md", "requirements.txt")
TEXT_SUFFIXES = {
    ".css",
    ".html",
    ".js",
    ".json",
    ".map",
    ".md",
    ".py",
    ".pyi",
    ".svg",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
ALLOWED_LOCK_HOSTS = {"files.pythonhosted.org", "pypi.org"}
PUBLIC_PACKAGE_REGISTRY = "https://pypi.org/simple"
ALLOWED_PACKAGE_REGISTRIES = {PUBLIC_PACKAGE_REGISTRY}
DEVELOPMENT_PACKAGES = {"basedpyright", "pytest", "pytest-asyncio", "pytest-cov", "pytest-mock", "ruff"}


def build(output_dir: Path, dqx_version: str) -> None:
    """Build a deterministic, runtime-only Marketplace source folder.

    Args:
        output_dir: Destination directory to replace with the generated tree.
        dqx_version: Exact published DQX version required by the app and runner.
    """
    _validate_output_dir(output_dir)
    if RELEASE_VERSION.fullmatch(dqx_version) is None:
        raise ValueError("dqx_version must be a release version")
    _require_build_inputs()

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".marketplace-build-", dir=output_dir.parent))
    try:
        _assemble_runtime_tree(staging, dqx_version)
        _validate_artifact(staging, dqx_version)
        _replace_directory(staging, output_dir)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _validate_output_dir(output_dir: Path) -> None:
    if output_dir.name in {"", ".", ".."} or output_dir.is_symlink():
        raise ValueError("output_dir must be a non-symlink directory path")
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError("output_dir must be a directory path")

    try:
        absolute_output = Path(os.path.abspath(output_dir))
        resolved_output = output_dir.resolve(strict=False)
        absolute_marketplace = Path(os.path.abspath(MARKETPLACE_DIR))
        resolved_marketplace = MARKETPLACE_DIR.resolve(strict=False)
        protected_inputs = tuple(path.resolve(strict=False) for path in (APP_DIR, BUILD_DIR, TEMPLATE_DIR, PYPROJECT))
    except (OSError, RuntimeError) as error:
        raise ValueError("output_dir could not be safely resolved") from error

    if absolute_output == absolute_marketplace and resolved_output == resolved_marketplace:
        return
    if any(
        resolved_output.is_relative_to(protected) or protected.is_relative_to(resolved_output)
        for protected in protected_inputs
    ):
        raise ValueError("output_dir overlaps protected Marketplace build inputs")


def _require_build_inputs() -> None:
    required = [
        BUILD_DIR / "pyproject.toml",
        BUILD_DIR / "src" / "databricks_labs_dqx_app" / "__dist__" / "index.html",
        TEMPLATE_DIR / "manifest.yaml",
        TEMPLATE_DIR / "app.yaml",
        TEMPLATE_DIR / "uv.lock",
    ]
    if not all(path.is_file() for path in required):
        raise RuntimeError("Marketplace build inputs are missing; run the application build first")


def _assemble_runtime_tree(staging: Path, dqx_version: str) -> None:
    # Checked first: a lock for another DQX fails before the task-runner wheel build.
    (staging / "uv.lock").write_text(release_lock_text(dqx_version), encoding="utf-8", newline="\n")
    for name in RUNTIME_FILES:
        _copy_file(BUILD_DIR / name, staging / name)
    _copy_tree(BUILD_DIR / "src", staging / "src")
    _copy_file(TEMPLATE_DIR / "manifest.yaml", staging / "manifest.yaml")
    _copy_file(TEMPLATE_DIR / "app.yaml", staging / "app.yaml")

    pyproject_text = (BUILD_DIR / "pyproject.toml").read_text(encoding="utf-8")
    (staging / "pyproject.toml").write_text(
        release_pyproject(pyproject_text, dqx_version),
        encoding="utf-8",
        newline="\n",
    )

    build_task_runner_wheel(staging / "tasks", dqx_version)


def _copy_tree(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.rglob("*"), key=lambda item: item.relative_to(source).as_posix()):
        relative = path.relative_to(source)
        if path.is_symlink():
            raise RuntimeError("Runtime source contains a symlink")
        if path.is_dir():
            (destination / relative).mkdir(parents=True, exist_ok=True)
        elif path.is_file() and "__pycache__" not in relative.parts and path.suffix != ".pyc":
            _copy_file(path, destination / relative)


def _copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    content = source.read_bytes()
    if source.suffix.lower() in TEXT_SUFFIXES:
        content = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    destination.write_bytes(content)
    destination.chmod(0o644)


def _validate_artifact(root: Path, dqx_version: str) -> None:
    for name in ("manifest.yaml", "app.yaml"):
        try:
            document = yaml.safe_load((root / name).read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            raise RuntimeError("Marketplace YAML validation failed") from error
        if not isinstance(document, dict):
            raise RuntimeError("Marketplace YAML root must be a mapping")

    pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject.get("project", {}).get("dependencies", [])
    expected_dependency = f"databricks-labs-dqx[llm,datacontract]=={dqx_version}"
    if expected_dependency not in dependencies or "dependency-groups" in pyproject:
        raise RuntimeError("Marketplace application dependencies are not release-only")

    lock_text = (root / "uv.lock").read_text(encoding="utf-8")
    forbidden = ("../", "_vendor/dqx", "directory =", "path =", "file://", ".release-wheels")
    if any(value in lock_text for value in forbidden):
        raise RuntimeError("Marketplace lock contains an escaping dependency source")
    hosts = {urlsplit(url).hostname for url in re.findall(r'https?://[^"\s]+', lock_text)}
    if not hosts.issubset(ALLOWED_LOCK_HOSTS):
        raise RuntimeError("Marketplace lock contains a non-public package source")

    lock = tomllib.loads(lock_text)
    for package in lock.get("package", []):
        if not isinstance(package, dict) or not isinstance((source := package.get("source")), dict):
            continue
        registry = source.get("registry")
        if isinstance(registry, str) and registry not in ALLOWED_PACKAGE_REGISTRIES:
            raise RuntimeError("Marketplace lock contains a non-public package registry")
    require_lock_resolves_dqx(lock_text, dqx_version)
    locked_names = {package.get("name") for package in lock.get("package", []) if isinstance(package, dict)}
    if not DEVELOPMENT_PACKAGES.isdisjoint(locked_names):
        raise RuntimeError("Marketplace lock contains development dependencies")

    unexpected_roots = {path.name for path in root.iterdir()} - {
        "README.md",
        "app.yaml",
        "manifest.yaml",
        "pyproject.toml",
        "requirements.txt",
        "src",
        "tasks",
        "uv.lock",
    }
    if unexpected_roots:
        raise RuntimeError("Marketplace artifact contains unexpected root entries")
    if (root / "_vendor").exists() or (root / "src" / "databricks_labs_dqx_app" / "ui").exists():
        raise RuntimeError("Marketplace artifact contains development source")


def _replace_directory(staging: Path, output_dir: Path) -> None:
    if not output_dir.exists():
        os.replace(staging, output_dir)
        return
    backup = Path(tempfile.mkdtemp(prefix=".marketplace-backup-", dir=output_dir.parent))
    backup.rmdir()
    os.replace(output_dir, backup)
    try:
        os.replace(staging, output_dir)
    except OSError:
        os.replace(backup, output_dir)
        raise
    shutil.rmtree(backup)


def _file_contents(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def _check() -> bool:
    if not MARKETPLACE_DIR.is_dir():
        return False
    with tempfile.TemporaryDirectory(prefix=".marketplace-check-") as temp_dir:
        candidate = Path(temp_dir) / "marketplace"
        build(candidate, dqx_release_version())
        return _file_contents(candidate) == _file_contents(MARKETPLACE_DIR)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail when the committed artifact differs")
    args = parser.parse_args()
    try:
        if args.check:
            if not _check():
                print("Marketplace artifact differs; run uv run --frozen python app/scripts/build_marketplace.py.")
                return 1
            print("Marketplace artifact is up to date.")
            return 0
        build(MARKETPLACE_DIR, dqx_release_version())
    except (OSError, RuntimeError, ValueError) as error:
        raise SystemExit(f"error: {error}") from None
    print("Marketplace artifact generated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
