import re
from pathlib import Path


def get_dqx_version(about_path: Path) -> str:
    """Extract the version string from the __about__.py file."""
    content = about_path.read_text()
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
        content = mdx_file.read_text()
        updated_content = pattern.sub(replacement, content)

        if updated_content != content:
            mdx_file.write_text(updated_content)
            print(f"Updated GitHub URLs in {mdx_file} to point to the latest DQX released version")


def main():
    about_file = Path("src/databricks/labs/dqx/__about__.py")
    mdx_dir = Path("docs/dqx/docs")

    version = get_dqx_version(about_file)
    update_mdx_files(mdx_dir, version)


if __name__ == "__main__":
    main()
