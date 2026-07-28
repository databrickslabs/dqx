#!/usr/bin/env python3
"""Snapshot the current (unreleased) documentation into a frozen, versioned copy.

DQX serves versioned documentation with Docusaurus: the live ``docs/dqx/docs``
folder holds the unreleased ("Next") docs, and each release is frozen into
``docs/dqx/versioned_docs/version-<X>/`` with a matching entry in
``docs/dqx/versions.json`` and a sidebar in ``docs/dqx/versioned_sidebars``.

This script automates creating that snapshot when cutting a release (for example,
snapshotting the ``0.15.0`` docs while releasing ``0.16.0``). It:

  1. Resolves the version to snapshot (``--version`` or ``src/databricks/labs/dqx/__about__.py``).
  2. Runs Docusaurus ``docs:version <X>`` so the snapshot, sidebar, and
     ``versions.json`` are produced exactly the way Docusaurus expects.
  3. Rewrites links in the *frozen* snapshot that target a moving reference
     (the ``main`` branch or another release) so they point at this release's
     tag ``v<X>`` — the frozen docs should reference frozen sources.
  4. Bumps ``lastVersion`` in ``docusaurus.config.ts`` to the new snapshot so the
     newest release is the default served version.

The script only edits the local working tree; it does not commit or push.

Prerequisite: generate the API reference (``pydoc-markdown``) before running this so the
snapshot freezes this release's API docs. ``make docs-version`` does this for you; if you
invoke the script directly, run ``uv run --group docs pydoc-markdown`` first.

Usage::

    # snapshot the version from __about__.py
    python .github/scripts/snapshot_docs_version.py

    # snapshot an explicit version
    python .github/scripts/snapshot_docs_version.py --version 0.15.0

    # associate the snapshot with the release PR (recorded in the summary only)
    python .github/scripts/snapshot_docs_version.py --pr 1234

    # show what would change without touching anything
    python .github/scripts/snapshot_docs_version.py --dry-run
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

# Repo-root-relative paths. The script is location-independent: it resolves the
# repo root from its own location so it can be run from anywhere.
REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs" / "dqx"
ABOUT_FILE = REPO_ROOT / "src" / "databricks" / "labs" / "dqx" / "__about__.py"
VERSIONS_JSON = DOCS_DIR / "versions.json"
CONFIG_FILE = DOCS_DIR / "docusaurus.config.ts"

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")

# GitHub source links in the docs that point at a moving ref (main) or another
# release tag. In a frozen snapshot these must be pinned to this release's tag so
# the versioned docs keep pointing at the matching frozen source.
GITHUB_REF_RE = re.compile(r"(https://github\.com/databrickslabs/dqx/blob/)(main|v\d+\.\d+\.\d+)(/)")

# `lastVersion: '<X>'` in docusaurus.config.ts — the default served version.
LAST_VERSION_RE = re.compile(r"(lastVersion:\s*')([^']*)(')")


def read_version_from_about() -> str:
    """Read ``__version__`` from ``__about__.py``.

    Returns:
        The version string, e.g. ``0.15.0``.

    Raises:
        SystemExit: If the version cannot be found.
    """
    content = ABOUT_FILE.read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*"(?P<version>[\d.]+)"', content)
    if not match:
        raise SystemExit(f"Could not find __version__ in {ABOUT_FILE}")
    return match.group("version")


def load_existing_versions() -> list[str]:
    """Return the versions already snapshotted, or an empty list if none."""
    if not VERSIONS_JSON.exists():
        return []
    import json

    return json.loads(VERSIONS_JSON.read_text(encoding="utf-8"))


def run_docusaurus_version(version: str, dry_run: bool) -> None:
    """Invoke the Docusaurus ``docs:version`` command to freeze the current docs.

    Args:
        version: The version label to create, e.g. ``0.15.0``.
        dry_run: If True, print the command instead of running it.
    """
    cmd = ["yarn", "--cwd", str(DOCS_DIR), "docusaurus", "docs:version", version]
    if dry_run:
        print(f"[dry-run] would run: {' '.join(cmd)}")
        return
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)


def pin_github_links(version: str, dry_run: bool) -> int:
    """Pin moving GitHub source links in the frozen snapshot to this release's tag.

    Args:
        version: The snapshot version, e.g. ``0.15.0`` (tag ``v0.15.0``).
        dry_run: If True, report changes without writing.

    Returns:
        The number of files updated.
    """
    snapshot_dir = DOCS_DIR / "versioned_docs" / f"version-{version}"
    if not snapshot_dir.exists():
        if dry_run:
            # In a dry run the Docusaurus step did not actually create the snapshot,
            # so there is nothing on disk to scan. Report the intent and move on.
            print(f"[dry-run] would pin GitHub links to v{version} in versioned_docs/version-{version}/")
            return 0
        raise SystemExit(f"Expected snapshot directory not found: {snapshot_dir}")

    replacement = rf"\g<1>v{version}\g<3>"
    updated = 0
    for mdx in snapshot_dir.rglob("*.mdx"):
        content = mdx.read_text(encoding="utf-8")
        new_content = GITHUB_REF_RE.sub(replacement, content)
        if new_content != content:
            updated += 1
            rel = mdx.relative_to(REPO_ROOT)
            if dry_run:
                print(f"[dry-run] would pin GitHub links to v{version} in {rel}")
            else:
                mdx.write_text(new_content, encoding="utf-8")
                print(f"Pinned GitHub links to v{version} in {rel}")
    return updated


def bump_last_version(version: str, dry_run: bool) -> None:
    """Set ``lastVersion`` in docusaurus.config.ts to the new snapshot.

    The newest frozen release is the default version served at ``/docs/``.

    Args:
        version: The snapshot version to make the default, e.g. ``0.15.0``.
        dry_run: If True, report the change without writing.
    """
    content = CONFIG_FILE.read_text(encoding="utf-8")
    match = LAST_VERSION_RE.search(content)
    if not match:
        print(
            f"WARNING: could not find `lastVersion: '...'` in {CONFIG_FILE.relative_to(REPO_ROOT)}; "
            "leaving the default served version unchanged.",
            file=sys.stderr,
        )
        return
    current = match.group(2)
    if current == version:
        return
    new_content = LAST_VERSION_RE.sub(rf"\g<1>{version}\g<3>", content, count=1)
    rel = CONFIG_FILE.relative_to(REPO_ROOT)
    if dry_run:
        print(f"[dry-run] would set lastVersion '{current}' -> '{version}' in {rel}")
    else:
        CONFIG_FILE.write_text(new_content, encoding="utf-8")
        print(f"Set lastVersion '{current}' -> '{version}' in {rel}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--version",
        help="Version to snapshot (e.g. 0.15.0). Defaults to the version in __about__.py.",
    )
    parser.add_argument(
        "--pr",
        help="Release PR number to associate with this snapshot (recorded in the run summary).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without modifying any files.",
    )
    args = parser.parse_args(argv)

    version = args.version or read_version_from_about()
    if not VERSION_RE.match(version):
        raise SystemExit(f"Version must look like X.Y.Z, got: {version!r}")

    # `yarn` is required to drive the Docusaurus CLI. Fail early with a clear message.
    if not args.dry_run and shutil.which("yarn") is None:
        raise SystemExit("`yarn` was not found on PATH. Run `make docs-install` first.")

    existing = load_existing_versions()
    if version in existing:
        raise SystemExit(
            f"Version {version} is already snapshotted (present in {VERSIONS_JSON.relative_to(REPO_ROOT)}). "
            "Nothing to do."
        )

    pr_note = f" (release PR #{args.pr})" if args.pr else ""
    print(f"Snapshotting current docs as version {version}{pr_note}...")

    run_docusaurus_version(version, args.dry_run)
    pinned = pin_github_links(version, args.dry_run)
    bump_last_version(version, args.dry_run)

    print()
    print(f"Done. Snapshot for {version} created{pr_note}.")
    print(f"  - versioned_docs/version-{version}/ (GitHub links pinned in {pinned} file(s))")
    print(f"  - versioned_sidebars/version-{version}-sidebars.json")
    print(f"  - versions.json updated")
    print(f"  - docusaurus.config.ts lastVersion set to {version}")
    print()
    print("Review the changes, then commit them as part of the release PR"
          f"{'' if not args.pr else f' (#{args.pr})'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
