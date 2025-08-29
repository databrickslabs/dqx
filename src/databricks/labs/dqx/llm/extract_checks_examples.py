import logging
import re
from pathlib import Path
from typing import Any
import yaml


logger = logging.getLogger(__name__)

repo_root = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
RESOURCES_DIR = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"
MDX_DOCS_WITH_YAML_CHECKS = [
    repo_root / "docs" / "dqx" / "docs" / "reference" / "quality_checks.mdx",
    repo_root / "docs" / "dqx" / "docs" / "guide" / "quality_checks_definition.mdx",
]


def extract_yaml_checks_from_content(content: str, source_name: str = "content") -> list[dict[str, Any]]:
    """
    Extract all YAML examples from MDX content string.

    Args:
        content: The MDX content string to extract YAML from
        source_name: Name of the source for logging purposes (default: "content")

    Returns:
        List of parsed YAML objects from all valid blocks
    """

    # Extract YAML from code blocks
    yaml_pattern = r'```(?:yaml|yml)\s*(.*?)(?:\n)?```'
    yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)

    logger.info(f"Found {len(yaml_matches)} YAML code blocks in {source_name}")

    if not yaml_matches:
        logger.warning(f"No YAML code blocks found in {source_name}")
        return []

    # Combine all YAML blocks
    all_yaml_content = []

    for i, yaml_content in enumerate(yaml_matches):
        logger.debug(
            f"Processing YAML block {i+1}/{len(yaml_matches)} from {source_name} (length: {len(yaml_content)} chars)"
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


def extract_yaml_checks_from_mdx(mdx_file_path: str) -> list[dict[str, Any]]:
    """
    Extract all YAML examples from a given MDX file.

    Args:
        mdx_file_path: Path to the MDX file to extract YAML from

    Returns:
        List of parsed YAML objects from all valid blocks

    Raises:
        FileNotFoundError: If the MDX file does not exist
    """

    mdx_file = Path(mdx_file_path)

    if not mdx_file.exists():
        logger.error(f"MDX file not found: {mdx_file}")
        return []

    logger.info(f"Reading MDX file: {mdx_file}")
    content = mdx_file.read_text(encoding='utf-8')

    return extract_yaml_checks_from_content(content, mdx_file.name)


def extract_yaml_checks_examples(output_file_path: Path | None = None) -> bool:
    """
    Extract all YAML examples from both quality_rules.mdx and quality_checks.mdx.

    Creates a combined YAML file with all examples from the documentation files
    in the LLM resources directory for use in language model processing.

    Args:
        output_file_path: Path to the output file to write the combined YAML content

    Returns:
        True if extraction was successful, False otherwise
    """
    all_combined_content = []
    success_count = 0

    for mdx_file in MDX_DOCS_WITH_YAML_CHECKS:
        mdx_path = mdx_file.as_posix()
        logger.info(f"Processing {mdx_path}")
        yaml_content = extract_yaml_checks_from_mdx(mdx_path)

        if yaml_content:
            all_combined_content.extend(yaml_content)
            success_count += 1
        else:
            logger.warning(f"No YAML content extracted from {mdx_path}")

    if all_combined_content:
        logger.info("Creating combined file")
        if not output_file_path:
            output_file_path = RESOURCES_DIR / "yaml_checks_examples.yml"
        combined_yaml = yaml.dump(all_combined_content, default_flow_style=False, sort_keys=False)
        output_file_path.write_text(combined_yaml)
        logger.info(f"Created combined file with {len(all_combined_content)} total YAML items: {output_file_path}")
        logger.debug(f"Combined file size: {output_file_path.stat().st_size} bytes")

    return success_count > 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s][%(levelname)s] %(message)s',
    )

    logger.info("Extracting YAML examples from MDX files...")
    success = extract_yaml_checks_examples()
    if success:
        logger.info("YAML extraction completed successfully!")
    else:
        logger.error("YAML extraction failed!")
