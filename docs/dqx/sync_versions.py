"""Propagate the DQX version (source of truth: ``src/databricks/labs/dqx/__about__.py``) to the
places that must track it: versioned GitHub source URLs in the docs, and the pinned
``databricks-labs-dqx==<version>`` dependency in the DQX Studio app and the MCP server.

Runs as part of ``make fmt`` — bump ``__about__.py``, run ``make fmt``, and the new version
propagates everywhere. Idempotent: re-running with an unchanged version rewrites nothing.
"""

import re
from pathlib import Path


def get_dqx_version(about_path: Path) -> str:
    """Extract the version string from the __about__.py file."""
    content = about_path.read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*"(?P<version>[\d.]+)"', content)
    if not match:
        raise ValueError(f"Version not found in {about_path}")
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
    """Pin ``databricks-labs-dqx==<version>`` in the files that install DQX from the registry.

    The DQX Studio app (``app/``) and the MCP server (``mcp-server/``) both install a *pinned*,
    published ``databricks-labs-dqx`` release rather than building from source, so the pin must be
    bumped to the repo's version on every release. Running this as part of ``make fmt`` keeps it in
    lockstep with __about__.py: bump __about__.py, run ``make fmt``, and the new version propagates
    everywhere. Any extras (e.g. ``[llm,datacontract]``) are preserved.

    The MCP server's ``databricks.yml`` goes one step further: its ``dqx_version`` bundle variable is
    the single value a release edits, and the ``==`` pin and the in-repo wheel filename both derive
    from it via ``${var.dqx_version}``. So the literal ``==<version>`` pin regex below intentionally
    does NOT match there (it has no digits to rewrite); the ``dqx_version`` default is bumped instead,
    and DAB propagates it to the pin and the wheel name.
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

    pin_files = [
        Path("app/pyproject.toml"),
        Path("app/databricks.yml"),
        Path("mcp-server/databricks.yml"),
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
    about_file = Path("src/databricks/labs/dqx/__about__.py")
    mdx_dir = Path("docs/dqx/docs")

    version = get_dqx_version(about_file)
    update_mdx_files(mdx_dir, version)
    update_dqx_pins(version)


if __name__ == "__main__":
    main()
