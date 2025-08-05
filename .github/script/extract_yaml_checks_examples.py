import logging
import re
from pathlib import Path
from typing import List, Dict, Any
from databricks.labs.blueprint.logger import install_logger
import yaml


logger = logging.getLogger(__name__)


def extract_yaml_from_content(content: str, source_name: str = "content") -> List[Dict[str, Any]]:
    """Extract all YAML examples from MDX content string.

    :param content: The MDX content string to extract YAML from
    :param source_name: Name of the source for logging purposes (default: "content")
    :return: List of parsed YAML objects from all valid blocks
    """

    # Extract YAML from code blocks
    yaml_pattern = r'```(?:yaml|yml)\n(.*?)\n```'
    yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)

    logger.info(f"Found {len(yaml_matches)} YAML code blocks in {source_name}")

    if not yaml_matches:
        logger.warning(f"No YAML code blocks found in {source_name}")
        return []

    # Combine all YAML blocks
    all_yaml_content = []

    for i, yaml_content in enumerate(yaml_matches):
        logger.debug(
            f"Processing YAML block {i+1}/{len(yaml_matches)} from {source_name} (length: {len(yaml_content)} characters)"
        )

        # Validate each YAML block
        try:
            parsed_yaml = yaml.safe_load(yaml_content)
            if not parsed_yaml:  # Skip empty YAML blocks
                logger.debug(f"  - Skipped empty YAML block {i+1}")
                continue

            if isinstance(parsed_yaml, list):
                all_yaml_content.extend(parsed_yaml)
                logger.debug(f"  - Added {len(parsed_yaml)} items from YAML block {i+1}")
            else:
                all_yaml_content.append(parsed_yaml)
                logger.debug(f"  - Added 1 item from YAML block {i+1}")
        except yaml.YAMLError as e:
            logger.warning(f"  - Invalid YAML in block {i+1}: {e}")
            continue

    return all_yaml_content


def extract_yaml_from_mdx(mdx_file_path: str) -> List[Dict[str, Any]]:
    """Extract all YAML examples from a given MDX file.

    :param mdx_file_path: Path to the MDX file to extract YAML from
    :raises FileNotFoundError: If the MDX file does not exist
    :return: List of parsed YAML objects from all valid blocks
    """

    mdx_file = Path(mdx_file_path)

    if not mdx_file.exists():
        logger.error(f"MDX file not found: {mdx_file}")
        return []

    logger.info(f"Reading MDX file: {mdx_file}")
    content = mdx_file.read_text(encoding='utf-8')

    return extract_yaml_from_content(content, mdx_file.name)


def extract_yaml_checks_examples() -> bool:
    """Extract all YAML examples from both quality_rules.mdx and quality_checks.mdx.

    Creates a combined YAML file with all examples from the documentation files
    in the LLM resources directory for use in language model processing.

    :return: True if extraction was successful, False otherwise
    """

    # Setup paths
    repo_root = Path(".")
    resources_dir = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"

    # Create resources directory
    resources_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created resources directory: {resources_dir}")

    # Create __init__.py
    init_file = resources_dir / "__init__.py"
    init_file.write_text("# Resources package\n")
    logger.debug(f"Created __init__.py: {init_file}")

    # Define MDX files to extract from
    mdx_files = [
        {
            "path": repo_root / "docs" / "dqx" / "docs" / "reference" / "quality_rules.mdx",
            "description": "quality rules reference examples",
        },
        {
            "path": repo_root / "docs" / "dqx" / "docs" / "guide" / "quality_checks.mdx",
            "description": "quality checks guide examples",
        },
    ]

    all_combined_content = []
    success_count = 0

    for mdx_info in mdx_files:
        logger.info(f"Processing {mdx_info['description']}")
        yaml_content = extract_yaml_from_mdx(mdx_info["path"])

        if yaml_content:
            # Add to combined content
            all_combined_content.extend(yaml_content)
            success_count += 1
        else:
            logger.warning(f"No YAML content extracted from {mdx_info['path']}")

    # Create combined file
    if all_combined_content:
        logger.info("Creating combined file")
        combined_output = resources_dir / "yaml_checks_examples.yml"
        combined_yaml = yaml.dump(all_combined_content, default_flow_style=False, sort_keys=False)
        combined_output.write_text(combined_yaml)
        logger.info(f"Created combined file with {len(all_combined_content)} total YAML items: {combined_output}")
        logger.debug(f"Combined file size: {combined_output.stat().st_size} bytes")

    return success_count > 0


if __name__ == "__main__":
    install_logger()
    logging.root.setLevel(logging.INFO)

    logger.info("Extracting YAML examples from MDX files...")
    success = extract_yaml_checks_examples()
    if success:
        logger.info("YAML extraction completed successfully!")
    else:
        logger.error("YAML extraction failed!")
