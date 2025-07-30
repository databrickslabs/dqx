import re
from pathlib import Path
from typing import Any

import yaml


def extract_yaml_from_mdx(mdx_file_path: str | Path) -> tuple[bool, list[dict[str, Any]]]:
    """Extract all YAML examples from a given MDX file.

    Args:
        mdx_file_path: Path to the MDX file to extract YAML from

    Returns:
        Tuple of (success: bool, yaml_content: List[Dict[str, Any]])

    Raises:
        FileNotFoundError: If the MDX file doesn't exist
    """
    mdx_file = Path(mdx_file_path)

    if not mdx_file.exists():
        msg = f"MDX file not found: {mdx_file}"
        raise FileNotFoundError(msg)

    content = mdx_file.read_text(encoding='utf-8')

    # Extract YAML from code blocks
    yaml_pattern = r'```(?:yaml|yml)\n(.*?)\n```'
    yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)

    if not yaml_matches:
        return False, []

    # Combine all YAML blocks
    all_yaml_content: list[dict[str, Any]] = []

    for yaml_content in yaml_matches:
        # Validate each YAML block
        try:
            parsed_yaml = yaml.safe_load(yaml_content)
            if not parsed_yaml:  # Skip empty YAML blocks
                continue

            if isinstance(parsed_yaml, list):
                all_yaml_content.extend(parsed_yaml)
            else:
                all_yaml_content.append(parsed_yaml)
        except yaml.YAMLError:
            # Skip invalid YAML blocks
            continue

    return len(all_yaml_content) > 0, all_yaml_content


def extract_checks_yml_examples(repo_root: str | Path) -> tuple[bool, list[dict[str, Any]]]:
    """Extract all YAML examples from both quality_rules.mdx and quality_checks.mdx.

    Args:
        repo_root: Root directory of the repository.

    Returns:
        Tuple of (success: bool, combined_yaml_content: List[Dict[str, Any]])
    """
    repo_root = Path(repo_root)

    # Setup paths
    resources_dir = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"

    # Create resources directory
    resources_dir.mkdir(parents=True, exist_ok=True)

    # Create __init__.py
    init_file = resources_dir / "__init__.py"
    init_file.write_text("# Resources package\n")

    # Define MDX files to extract from
    mdx_files = [
        {
            "path": repo_root / "docs" / "dqx" / "docs" / "reference" / "quality_rules.mdx",
            "output": "quality_rules_examples.yml",
            "description": "quality rules reference examples",
        },
        {
            "path": repo_root / "docs" / "dqx" / "docs" / "guide" / "quality_checks.mdx",
            "output": "quality_checks_examples.yml",
            "description": "quality checks guide examples",
        },
    ]

    all_combined_content: list[dict[str, Any]] = []
    success_count = 0

    for mdx_info in mdx_files:
        try:
            extraction_success, yaml_content = extract_yaml_from_mdx(mdx_info["path"])

            if extraction_success and yaml_content:
                # Add to combined content
                all_combined_content.extend(yaml_content)
                success_count += 1

        except FileNotFoundError:
            # Skip missing files
            continue

    # Create combined file
    if all_combined_content:
        combined_output = resources_dir / "quality_checks_all_examples.yml"
        combined_yaml = yaml.dump(all_combined_content, default_flow_style=False, sort_keys=False)
        combined_output.write_text(combined_yaml)

    return success_count > 0, all_combined_content


# Import BuildHookInterface - this will work in the build environment
try:
    from hatchling.plugin.interface import BuildHookInterface
except ImportError:
    try:
        from hatchling.builders.hooks.plugin.interface import BuildHookInterface
    except ImportError:
        # Last resort - define a minimal interface
        class BuildHookInterface:
            def __init__(self, root: str, config: dict[str, Any], *_args, **_kwargs):
                self.root = root
                self.config = config


class ExtractDocsResourcesHook(BuildHookInterface):
    """Build hook to extract YAML examples from documentation files into resources."""

    PLUGIN_NAME = "extract-resources"

    def initialize(self, _version: str, _build_data: dict[str, Any]) -> None:
        """Extract resources before build.

        Args:
            _version: The version being built (unused)
            _build_data: Build configuration data (unused)
        """
        try:
            success, _ = extract_checks_yml_examples(repo_root=self.root)
            if not success:
                # Silent failure - don't interrupt the build process
                pass
        except (FileNotFoundError, PermissionError, OSError):
            # Silent failure - don't interrupt the build process
            pass
