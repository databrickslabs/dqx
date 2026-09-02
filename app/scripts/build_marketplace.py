"""Build the generated Databricks Marketplace source folder."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import tomllib
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

import yaml

if __package__:
    from scripts.build_app import build_task_runner_wheel
else:
    from build_app import build_task_runner_wheel

APP_DIR = Path(__file__).resolve().parent.parent
BUILD_DIR = APP_DIR / ".build"
TEMPLATE_DIR = APP_DIR / "marketplace_templates"
MARKETPLACE_DIR = APP_DIR / "marketplace"
PYPROJECT = APP_DIR / "pyproject.toml"
DAB_CONFIG = APP_DIR / "databricks.yml"

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
RELEASE_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.-]*)")


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
        BUILD_DIR / "uv.lock",
        BUILD_DIR / "src" / "databricks_labs_dqx_app" / "__dist__" / "index.html",
        TEMPLATE_DIR / "manifest.yaml",
        TEMPLATE_DIR / "app.yaml",
    ]
    if not all(path.is_file() for path in required):
        raise RuntimeError("Marketplace build inputs are missing; run the application build first")


def _assemble_runtime_tree(staging: Path, dqx_version: str) -> None:
    for name in RUNTIME_FILES:
        _copy_file(BUILD_DIR / name, staging / name)
    _copy_tree(BUILD_DIR / "src", staging / "src")
    _copy_file(TEMPLATE_DIR / "manifest.yaml", staging / "manifest.yaml")
    _copy_file(TEMPLATE_DIR / "app.yaml", staging / "app.yaml")

    pyproject_text = (BUILD_DIR / "pyproject.toml").read_text(encoding="utf-8")
    (staging / "pyproject.toml").write_text(
        _release_pyproject(pyproject_text, dqx_version),
        encoding="utf-8",
        newline="\n",
    )

    lock_seed = MARKETPLACE_DIR / "uv.lock"
    if not lock_seed.is_file():
        lock_seed = BUILD_DIR / "uv.lock"
    seed_text = lock_seed.read_text(encoding="utf-8")
    seed_text = _remove_toml_table(seed_text, "options.exclude-newer-package")
    seed_text = _remove_toml_table(seed_text, "options")
    (staging / "uv.lock").write_text(seed_text, encoding="utf-8", newline="\n")
    _lock_release(staging)
    build_task_runner_wheel(staging / "tasks", dqx_version)


def _release_pyproject(source: str, dqx_version: str) -> str:
    lines = source.replace("\r\n", "\n").replace("\r", "\n").splitlines()
    dependency_index = next(
        (index for index, line in enumerate(lines) if '"databricks-labs-dqx[llm,datacontract]' in line),
        None,
    )
    if dependency_index is None:
        raise RuntimeError("Application DQX dependency declaration changed")

    comment_start = dependency_index
    while comment_start > 0 and lines[comment_start - 1].lstrip().startswith("#"):
        comment_start -= 1
    lines[comment_start : dependency_index + 1] = [f'    "databricks-labs-dqx[llm,datacontract]=={dqx_version}",']
    text = "\n".join(lines) + "\n"
    text = _remove_toml_table(text, "dependency-groups")
    text = _remove_toml_table(text, "tool.uv.sources", remove_leading_comments=True)
    text = _remove_toml_table(text, "tool.uv")
    for table in ("tool.pytest.ini_options", "tool.ruff.lint", "tool.ruff", "tool.basedpyright"):
        text = _remove_toml_table(text, table, remove_leading_comments=True)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _remove_toml_table(source: str, table: str, *, remove_leading_comments: bool = False) -> str:
    lines = source.splitlines()
    header = f"[{table}]"
    try:
        start = lines.index(header)
    except ValueError:
        return source
    end = next((index for index in range(start + 1, len(lines)) if lines[index].startswith("[")), len(lines))
    if remove_leading_comments:
        while start > 0 and lines[start - 1].lstrip().startswith("#"):
            start -= 1
    del lines[start:end]
    while start < len(lines) - 1 and lines[start] == "" and lines[start + 1] == "":
        del lines[start]
    return "\n".join(lines).rstrip() + "\n"


def _lock_release(staging: Path) -> None:
    lock_env = os.environ.copy()
    lock_env["UV_FROZEN"] = "0"
    lock_env["UV_NO_PROGRESS"] = "1"
    wheelhouse = staging / ".release-wheels"
    filename, public_url, expected_hash, upload_time = _stage_locked_wheel(
        staging / "uv.lock",
        wheelhouse,
        "litellm",
    )
    try:
        subprocess.run(
            ["uv", "lock", "--directory", ".", "--find-links", ".release-wheels", "--no-config"],
            cwd=staging,
            check=True,
            env=lock_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError("Marketplace dependency locking failed") from error
    finally:
        shutil.rmtree(wheelhouse, ignore_errors=True)
    lock = staging / "uv.lock"
    lock_text = _normalize_newlines(lock.read_text(encoding="utf-8"))
    local_wheel = f"{{ path = {json.dumps(filename)} }}"
    public_wheel_parts = [f"url = {json.dumps(public_url)}", f"hash = {json.dumps(expected_hash)}"]
    if upload_time is not None:
        public_wheel_parts.append(f"upload-time = {json.dumps(upload_time)}")
    public_wheel = "{ " + ", ".join(public_wheel_parts) + " }"
    lock_text, replacements = re.subn(
        re.escape(local_wheel),
        public_wheel,
        lock_text,
    )
    if replacements > 1:
        raise RuntimeError("Marketplace lock did not contain the staged release wheel")
    package_marker = '[[package]]\nname = "litellm"\n'
    package_start = lock_text.find(package_marker)
    package_end = lock_text.find("\n[[package]]", package_start + len(package_marker))
    if package_start < 0:
        raise RuntimeError("Marketplace lock did not contain the staged LiteLLM package")
    if package_end < 0:
        package_end = len(lock_text)
    package_block = lock_text[package_start:package_end]
    staged_source = 'source = { registry = ".release-wheels" }'
    public_source = f'source = {{ registry = "{PUBLIC_PACKAGE_REGISTRY}" }}'
    staged_source_count = package_block.count(staged_source)
    public_source_count = package_block.count(public_source)
    if package_block.count(public_wheel) != 1:
        raise RuntimeError("Marketplace lock did not contain the verified LiteLLM wheel")
    if replacements == 0 and (staged_source_count != 0 or public_source_count != 1):
        raise RuntimeError("Marketplace lock did not contain the public LiteLLM source")
    if replacements == 1 and (staged_source_count != 1 or public_source_count != 0):
        raise RuntimeError("Marketplace lock did not contain the staged LiteLLM source")
    package_block = package_block.replace(staged_source, public_source)
    lock_text = lock_text[:package_start] + package_block + lock_text[package_end:]
    lock_text = re.sub(
        r'registry = "https://[^"]*"',
        f'registry = "{PUBLIC_PACKAGE_REGISTRY}"',
        lock_text,
    )
    lock_text = re.sub(
        r'url = "https://[^/"]+/packages/',
        'url = "https://files.pythonhosted.org/packages/',
        lock_text,
    )
    lock_text = re.sub(r", size = [0-9]+", "", lock_text)
    lock.write_text(lock_text, encoding="utf-8", newline="\n")


def _stage_locked_wheel(
    lock_path: Path,
    wheelhouse: Path,
    package_name: str,
) -> tuple[str, str, str, str | None]:
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    packages = lock.get("package", [])
    package = next(
        (
            item
            for item in packages
            if isinstance(item, dict) and item.get("name") == package_name and isinstance(item.get("wheels"), list)
        ),
        None,
    )
    if package is None:
        raise RuntimeError("Required release-lock package is missing")
    wheel = next(
        (
            item
            for item in package["wheels"]
            if isinstance(item, dict) and isinstance(item.get("url"), str) and "py3-none-any.whl" in item["url"]
        ),
        None,
    )
    if wheel is None or not isinstance(wheel.get("hash"), str):
        raise RuntimeError("Required release-lock wheel is missing")
    url = wheel["url"]
    expected_hash = wheel["hash"]
    raw_upload_time = wheel.get("upload-time")
    upload_time = raw_upload_time if isinstance(raw_upload_time, str) else None
    parsed = urlsplit(url)
    if parsed.hostname != "files.pythonhosted.org":
        raise RuntimeError("Release-lock wheel does not use the public package CDN")
    filename = Path(parsed.path).name
    if not filename.endswith(".whl"):
        raise RuntimeError("Release-lock wheel filename is invalid")

    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            content = response.read(50 * 1024 * 1024 + 1)
    except OSError as error:
        raise RuntimeError("Required release-lock wheel download failed") from error
    if len(content) > 50 * 1024 * 1024:
        raise RuntimeError("Required release-lock wheel exceeds the size limit")
    algorithm, expected_digest = expected_hash.split(":", 1)
    if algorithm != "sha256" or hashlib.sha256(content).hexdigest() != expected_digest:
        raise RuntimeError("Required release-lock wheel hash does not match")
    wheelhouse.mkdir()
    (wheelhouse / filename).write_bytes(content)
    return filename, url, expected_hash, upload_time


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


def _normalize_newlines(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _file_contents(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def dqx_release_version(bundle_path: Path = DAB_CONFIG) -> str:
    """Return the published DQX version configured for Studio releases."""
    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    try:
        version = bundle["variables"]["dqx_version"]["default"]
    except (KeyError, TypeError) as error:
        raise RuntimeError("Bundle DQX version is missing") from error
    if not isinstance(version, str) or RELEASE_VERSION.fullmatch(version) is None:
        raise RuntimeError("Bundle DQX version is invalid")
    return version


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
