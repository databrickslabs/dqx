"""Regenerate the committed Marketplace lock (*marketplace_templates/uv.lock*).

The lock is resolved for the release *pyproject.toml* (DQX pinned to *dqx_version*)
through the configured package index, using *app/uv.lock* as version preferences so
the app's own dependencies stay in step with it. Every registry and file URL is then
rewritten to public PyPI, so customers always install from PyPI whichever index
resolved the lock. A proxy that holds back new uploads (the internal proxy waits
7 days) delays a refresh for a fresh DQX release until it serves that version.
"""

import argparse
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

if __package__:
    from scripts.build_app import (
        APP_DIR,
        DAB_CONFIG,
        DQX_PACKAGE,
        PYPROJECT,
        RELEASE_VERSION,
        TEMPLATE_LOCK,
        dqx_release_version,
        normalize_newlines,
        release_pyproject,
        require_lock_resolves_dqx,
    )
else:
    from build_app import (
        APP_DIR,
        DAB_CONFIG,
        DQX_PACKAGE,
        PYPROJECT,
        RELEASE_VERSION,
        TEMPLATE_LOCK,
        dqx_release_version,
        normalize_newlines,
        release_pyproject,
        require_lock_resolves_dqx,
    )

PUBLIC_REGISTRY = "https://pypi.org/simple"
PUBLIC_FILES_HOST = "https://files.pythonhosted.org"
# make exports UV_FROZEN (which would skip resolution) and a cwd-relative UV_BUILD_CONSTRAINT.
_UNSET_UV_ENV = frozenset({"UV_FROZEN", "UV_LOCKED", "UV_BUILD_CONSTRAINT"})


def normalize_lock_urls(lock_text: str) -> str:
    """Point every registry and file URL in *lock_text* at public PyPI and drop file sizes.

    Matches *make lock-app-dependencies*, so a lock resolved through a private proxy
    is identical to one resolved against public PyPI.
    """
    lock_text = re.sub(r'registry = "https://[^"]*"', f'registry = "{PUBLIC_REGISTRY}"', lock_text)
    lock_text = re.sub(r'url = "https://[^/"]+/packages/', f'url = "{PUBLIC_FILES_HOST}/packages/', lock_text)
    return re.sub(r", size = \d+", "", lock_text)


def resolve_lock(dqx_version: str) -> str:
    """Resolve and return the Marketplace lock text for *dqx_version*.

    Args:
        dqx_version: Published DQX version to pin.

    Returns:
        The normalized lock text, pointing only at public PyPI.
    """
    if RELEASE_VERSION.fullmatch(dqx_version) is None:
        raise ValueError("dqx_version must be a release version")
    with tempfile.TemporaryDirectory(prefix=".marketplace-lock-") as temp_dir:
        project = Path(temp_dir)
        (project / "pyproject.toml").write_text(
            release_pyproject(PYPROJECT.read_text(encoding="utf-8"), dqx_version), encoding="utf-8", newline="\n"
        )
        shutil.copy2(APP_DIR / "README.md", project / "README.md")
        # Seed with app/uv.lock so uv keeps the app's own dependency versions as preferences.
        shutil.copy2(APP_DIR / "uv.lock", project / "uv.lock")

        env = {key: value for key, value in os.environ.items() if key not in _UNSET_UV_ENV}
        try:
            subprocess.run(["uv", "lock", "--project", str(project)], check=True, env=env, cwd=project)
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"uv lock failed for DQX {dqx_version}. If the index doesn't serve this DQX release yet "
                "(a proxy may hold back new uploads), retry once it does."
            ) from error
        lock_text = normalize_newlines((project / "uv.lock").read_text(encoding="utf-8"))
    lock_text = normalize_lock_urls(lock_text)
    require_lock_resolves_dqx(lock_text, dqx_version)
    return lock_text


def set_bundle_dqx_version(dqx_version: str, bundle_path: Path = DAB_CONFIG) -> None:
    """Set the *dqx_version* default in *databricks.yml*, leaving the rest of the file untouched."""
    text = bundle_path.read_text(encoding="utf-8")
    updated, count = re.subn(r'(?m)^(  dqx_version:\n    default: )"[^"]*"$', rf'\g<1>"{dqx_version}"', text)
    if count != 1:
        raise RuntimeError("Bundle dqx_version declaration changed")
    bundle_path.write_text(updated, encoding="utf-8", newline="\n")


def main() -> None:
    """Regenerate *marketplace_templates/uv.lock* and keep *dqx_version* in step."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dqx-version", help="Published DQX version to pin (default: dqx_version in databricks.yml)")
    dqx_version = parser.parse_args().dqx_version or dqx_release_version()

    lock_text = resolve_lock(dqx_version)
    TEMPLATE_LOCK.write_text(lock_text, encoding="utf-8", newline="\n")
    if dqx_version != dqx_release_version():
        set_bundle_dqx_version(dqx_version)
    print(f"Wrote {TEMPLATE_LOCK.relative_to(APP_DIR)} for {DQX_PACKAGE}=={dqx_version}.")


if __name__ == "__main__":
    main()
