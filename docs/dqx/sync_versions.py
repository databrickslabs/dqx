"""Propagate the DQX version (source of truth: ``src/databricks/labs/dqx/__version__.py``) to the
places that must track it: versioned GitHub source URLs in the docs, and the ``dqx_version`` bundle
variable in the DQX Studio app's and the MCP server's ``databricks.yml`` (the derived
``databricks-labs-dqx==${{var.dqx_version}}`` pin and in-repo wheel filename follow from it).

Runs as part of ``make fmt`` — bump ``__version__.py``, run ``make fmt``, and the new version
propagates everywhere. Idempotent: re-running with an unchanged version rewrites nothing.
"""

import re
from pathlib import Path


def get_dqx_version(version_path: Path) -> str:
    """Extract the version string from the __version__.py file."""
    content = version_path.read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*"(?P<version>[\d.]+)"', content)
    if not match:
        raise ValueError(f"Version not found in {version_path}")
    return match.group("version")


def update_mdx_files(mdx_dir: Path, version: str):
    """Update all .mdx files in the directory and subdirectories by replacing
    GitHub URLs pointing to source code in main branch to the versioned one."""
    mdx_files = list(mdx_dir.rglob("*.mdx"))  # Recursive search

    if not mdx_files:
        return

    pattern = re.compile(r"https://github.com/databrickslabs/dqx/blob/(main|v\d+\.\d+\.\d+)/")
    replacement = f"https://github.com/databrickslabs/dqx/blob/v{version}/"

    for mdx_file in mdx_files:
        content = mdx_file.read_text(encoding="utf-8")
        updated_content = pattern.sub(replacement, content)

        if updated_content != content:
            mdx_file.write_text(updated_content, encoding="utf-8")
            print(f"Updated GitHub URLs in {mdx_file} to point to the latest DQX released version")


def update_dqx_pins(version: str):
    """Bump the pinned ``databricks-labs-dqx`` version in the bundle files that track it.

    Running this as part of ``make fmt`` keeps those references in lockstep with __version__.py: bump
    __version__.py, run ``make fmt``, and the new version propagates. Any extras (e.g.
    ``[llm,datacontract]``) are preserved.

    The MCP server's and the DQX Studio app's ``databricks.yml`` both express the version through a
    single ``dqx_version`` bundle variable: the ``==`` production pin and the in-repo wheel filename
    both derive from it via ``${var.dqx_version}``. So the literal ``==<version>`` pin regex below
    intentionally does NOT match there (it has no digits to rewrite); the ``dqx_version`` default is
    bumped instead, and DAB propagates it to the derived pin and wheel name.

    The Studio app is safe to bump on ``make fmt`` even though the production pin resolves from the
    registry: production deploys happen at/after release when the version is published, and a
    development deploy overrides ``dqx_task_dependency`` to a locally-built wheel (see
    ``app/target.dev.yml.example``), so neither CI nor a dev deploy resolves the not-yet-published pin.
    """
    # Match ``databricks-labs-dqx`` with optional extras, pinned with a LITERAL ``==<version>``. Only
    # the version digits are rewritten; the package name and any extras are kept verbatim. A pin that
    # derives its version (``==${var.dqx_version}``) has no digits here and is left untouched.
    pin_pattern = re.compile(r"(databricks-labs-dqx(?:\[[^\]]*\])?==)\d+\.\d+\.\d+")
    pin_replacement = rf"\g<1>{version}"

    # Match the ``dqx_version`` bundle variable's ``default:`` in an MCP-style databricks.yml. This is
    # the single source the derived pin + wheel filename hang off, so bumping it propagates the rest.
    var_pattern = re.compile(r'(dqx_version:\s*\n\s*default:\s*")\d+\.\d+\.\d+(")')
    var_replacement = rf"\g<1>{version}\g<2>"

    # Bundle files whose ``dqx_version`` variable is the single source the derived pin + wheel name
    # hang off. The app/tasks + app pyproject.toml deps are deliberately NOT listed: the task runner's
    # dep is provided by the job env spec (unpinned there), and the app's dep resolves from the parent
    # checkout via [tool.uv.sources] (also unpinned) — so neither carries a version to rewrite.
    pin_files = [
        Path("mcp-server/databricks.yml"),
        Path("app/databricks.yml"),
    ]

    for pin_file in pin_files:
        if not pin_file.exists():
            continue
        content = pin_file.read_text(encoding="utf-8")
        updated_content = var_pattern.sub(var_replacement, pin_pattern.sub(pin_replacement, content))

        if updated_content != content:
            pin_file.write_text(updated_content, encoding="utf-8")
            print(f"Updated databricks-labs-dqx version references in {pin_file} to {version}")


def main():
    version_file = Path("src/databricks/labs/dqx/__version__.py")
    mdx_dir = Path("docs/dqx/docs")

    version = get_dqx_version(version_file)
    update_mdx_files(mdx_dir, version)
    update_dqx_pins(version)


if __name__ == "__main__":
    main()
