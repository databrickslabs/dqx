#!/usr/bin/env python3
"""Build hook for extracting YAML examples from documentation files.

This module provides a custom hatch build hook that automatically extracts
YAML code examples from MDX documentation files and packages them as resources
in the wheel distribution.
"""

import re
from pathlib import Path
from typing import Any, TypedDict

import yaml


class MdxFileInfo(TypedDict):
    """Type definition for MDX file information."""
    path: Path
    output: str
    description: str


def extract_yaml_from_mdx(mdx_file_path: str | Path) -> tuple[bool, list[dict[str, Any]]]:
    """Extract all YAML examples from a given MDX file.

    This function scans an MDX file for YAML code blocks (marked with ```yaml or ```yml)
    and extracts their content. Each YAML block is validated and parsed.

    Args:
        mdx_file_path: Path to the MDX file to extract YAML from. Can be a string
            or Path object.

    Returns:
        A tuple containing:
            - bool: True if extraction was successful and YAML content was found,
              False otherwise
            - List[Dict[str, Any]]: List of parsed YAML objects from all code blocks

    Raises:
        FileNotFoundError: If the specified MDX file doesn't exist.

    Example:
        >>> success, content = extract_yaml_from_mdx("docs/examples.mdx")
        >>> if success:
        ...     print(f"Found {len(content)} YAML examples")
    """
    mdx_file: Path = Path(mdx_file_path)

    if not mdx_file.exists():
        msg: str = f"MDX file not found: {mdx_file}"
        raise FileNotFoundError(msg)

    content: str = mdx_file.read_text(encoding='utf-8')

    # Extract YAML from code blocks using regex pattern
    yaml_pattern: str = r'```(?:yaml|yml)\n(.*?)\n```'
    yaml_matches: list[str] = re.findall(yaml_pattern, content, re.DOTALL)

    if not yaml_matches:
        return False, []

    # Combine all YAML blocks into a single list
    all_yaml_content: list[dict[str, Any]] = []

    yaml_content: str
    for yaml_content in yaml_matches:
        # Validate each YAML block by parsing it
        try:
            parsed_yaml: Any = yaml.safe_load(yaml_content)
            if not parsed_yaml:  # Skip empty YAML blocks
                continue

            # Handle both single objects and lists of objects
            if isinstance(parsed_yaml, list):
                all_yaml_content.extend(parsed_yaml)
            else:
                all_yaml_content.append(parsed_yaml)
        except yaml.YAMLError:
            # Skip invalid YAML blocks silently
            continue

    return len(all_yaml_content) > 0, all_yaml_content


def extract_checks_yml_examples(
    repo_root: str | Path
) -> tuple[bool, list[dict[str, Any]]]:
    """Extract all YAML examples from quality documentation files.

    This function extracts YAML examples from both quality_rules.mdx and
    quality_checks.mdx files, combines them, and creates a resource file
    in the package structure.

    Args:
        repo_root: Root directory of the repository. Can be a string or Path object.

    Returns:
        A tuple containing:
            - bool: True if extraction was successful from at least one file,
              False if no files were processed successfully
            - List[Dict[str, Any]]: Combined list of all extracted YAML objects
              from all processed documentation files

    Note:
        This function creates the following directory structure:
        - src/databricks/labs/dqx/llm/resources/
        - src/databricks/labs/dqx/llm/resources/__init__.py
        - src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml

    Example:
        >>> success, examples = extract_checks_yml_examples("/path/to/repo")
        >>> if success:
        ...     print(f"Extracted {len(examples)} quality check examples")
    """
    repo_root_path: Path = Path(repo_root)

    # Setup paths for resource creation
    resources_dir: Path = (
        repo_root_path / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"
    )

    # Create resources directory if it doesn't exist
    resources_dir.mkdir(parents=True, exist_ok=True)

    # Create package initialization file
    init_file: Path = resources_dir / "__init__.py"
    init_file.write_text("# Resources package\n")

    # Define source documentation files to process
    mdx_files: list[MdxFileInfo] = [
        {
            "path": repo_root_path / "docs" / "dqx" / "docs" / "reference" / "quality_rules.mdx",
            "output": "quality_rules_examples.yml",
            "description": "quality rules reference examples",
        },
        {
            "path": repo_root_path / "docs" / "dqx" / "docs" / "guide" / "quality_checks.mdx",
            "output": "quality_checks_examples.yml",
            "description": "quality checks guide examples",
        },
    ]

    all_combined_content: list[dict[str, Any]] = []
    success_count: int = 0

    mdx_info: MdxFileInfo
    for mdx_info in mdx_files:
        try:
            extraction_success: bool
            yaml_content: list[dict[str, Any]]
            extraction_success, yaml_content = extract_yaml_from_mdx(mdx_info["path"])

            if extraction_success and yaml_content:
                # Add extracted content to combined collection
                all_combined_content.extend(yaml_content)
                success_count += 1

        except FileNotFoundError:
            # Skip missing files - don't fail the build
            continue

    # Create combined output file if we have content
    if all_combined_content:
        combined_output: Path = resources_dir / "quality_checks_all_examples.yml"
        combined_yaml: str = yaml.dump(
            all_combined_content,
            default_flow_style=False,
            sort_keys=False
        )
        combined_output.write_text(combined_yaml)

    return success_count > 0, all_combined_content


# Import BuildHookInterface with fallbacks for different hatchling versions
try:
    from hatchling.plugin.interface import BuildHookInterface
except ImportError:
    try:
        from hatchling.builders.hooks.plugin.interface import BuildHookInterface
    except ImportError:
        # Last resort - define a minimal interface for older versions
        class BuildHookInterface:
            """Fallback BuildHookInterface for older hatchling versions."""

            def __init__(
                self,
                root: str,
                config: dict[str, Any],
                *_args: Any,
                **_kwargs: Any
            ) -> None:
                self.root = root
                self.config = config


class ExtractDocsResourcesHook(BuildHookInterface):
    """Build hook to extract YAML examples from documentation files into resources.

    This hook runs during the build process and automatically extracts YAML code
    examples from documentation files, creating resource files that are packaged
    with the distribution.

    Attributes:
        PLUGIN_NAME: Identifier for this build hook plugin.

    Example:
        This hook is configured in pyproject.toml as:

        [tool.hatch.build.hooks.custom]
        path = "hatch_build_hook.py"
        dependencies = ["PyYAML"]
    """

    PLUGIN_NAME: str = "extract-resources"

    def initialize(self, _version: str, _build_data: dict[str, Any]) -> None:
        """Extract resources before build starts.

        This method is called by hatchling during the build process. It extracts
        YAML examples from documentation files and creates resource files.

        Args:
            _version: The version being built (unused but required by interface).
            _build_data: Build configuration data (unused but required by interface).

        Note:
            This method handles all exceptions silently to avoid interrupting
            the build process. If extraction fails, the build continues without
            the resource files.
        """
        try:
            success: bool
            _: list[dict[str, Any]]  # Unused return value
            success, _ = extract_checks_yml_examples(repo_root=self.root)

            if not success:
                # Silent failure - don't interrupt the build process
                pass
        except (FileNotFoundError, PermissionError, OSError):
            # Silent failure for any file system issues
            pass
