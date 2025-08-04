#!/usr/bin/env python3
"""
Standalone script to extract YAML examples from MDX files.
This replaces the hatch build hook to avoid integration test conflicts.
"""

import re
import yaml
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def extract_yaml_from_mdx(mdx_file_path):
    """Extract all YAML examples from a given MDX file"""
    try:
        mdx_file = Path(mdx_file_path)

        if not mdx_file.exists():
            logger.warning(f"MDX file not found: {mdx_file}")
            return False, []

        logger.info(f"Reading MDX file: {mdx_file}")
        content = mdx_file.read_text(encoding='utf-8')

        # Extract YAML from code blocks
        yaml_pattern = r'```(?:yaml|yml)\n(.*?)\n```'
        yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)

        logger.info(f"Found {len(yaml_matches)} YAML code blocks in {mdx_file.name}")

        if not yaml_matches:
            logger.warning(f"No YAML code blocks found in {mdx_file.name}")
            return False, []

        # Combine all YAML blocks
        all_yaml_content = []

        for i, yaml_content in enumerate(yaml_matches):
            try:
                parsed_yaml = yaml.safe_load(yaml_content)
                if not parsed_yaml:  # Skip empty YAML blocks
                    logger.debug(f"Skipped empty YAML block {i+1}")
                    continue

                if isinstance(parsed_yaml, list):
                    all_yaml_content.extend(parsed_yaml)
                    logger.debug(f"Added {len(parsed_yaml)} items from YAML block {i+1}")
                else:
                    all_yaml_content.append(parsed_yaml)
                    logger.debug(f"Added 1 item from YAML block {i+1}")
            except yaml.YAMLError as e:
                logger.warning(f"Invalid YAML in block {i+1}: {e}")
                continue

        return len(all_yaml_content) > 0, all_yaml_content

    except Exception as e:
        logger.warning(f"Error processing MDX file {mdx_file_path}: {e}")
        return False, []


def extract_checks_yml():
    """Extract all YAML examples from both quality_rules.mdx and quality_checks.mdx"""
    try:
        # Get the repository root (assuming this script is in .github/script/)
        script_dir = Path(__file__).parent
        repo_root = script_dir.parent.parent

        logger.info(f"Repository root: {repo_root}")

        # Setup paths
        resources_dir = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"

        # Create resources directory
        try:
            resources_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created resources directory: {resources_dir}")
        except (OSError, PermissionError) as e:
            logger.error(f"Could not create resources directory {resources_dir}: {e}")
            return False

        # Create __init__.py
        init_file = resources_dir / "__init__.py"
        try:
            if not init_file.exists():
                init_file.write_text("# Resources package\n")
                logger.info(f"Created __init__.py: {init_file}")
        except (OSError, PermissionError) as e:
            logger.error(f"Could not create __init__.py file {init_file}: {e}")
            return False

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

        all_combined_content = []
        success_count = 0
        total_files = len(mdx_files)

        for mdx_info in mdx_files:
            logger.info(f"Processing {mdx_info['description']}")
            success, yaml_content = extract_yaml_from_mdx(mdx_info["path"])

            if success and yaml_content:
                # Add to combined content
                all_combined_content.extend(yaml_content)
                success_count += 1
                logger.info(f"✓ Successfully extracted {len(yaml_content)} items from {mdx_info['path'].name}")
            else:
                logger.warning(f"✗ No YAML content extracted from {mdx_info['path']}")

        # Create combined file
        if all_combined_content:
            logger.info("Creating combined file")
            combined_output = resources_dir / "quality_checks_all_examples.yml"
            try:
                combined_yaml = yaml.dump(
                    all_combined_content, default_flow_style=False, sort_keys=False, allow_unicode=True
                )
                combined_output.write_text(combined_yaml)
                logger.info(
                    f"✓ Created combined file with {len(all_combined_content)} total YAML items: {combined_output}"
                )
                logger.info(f"✓ Combined file size: {combined_output.stat().st_size} bytes")
            except (OSError, yaml.YAMLError, PermissionError) as e:
                logger.error(f"Could not create combined YAML file {combined_output}: {e}")
                return False

        if success_count > 0:
            logger.info(f"🎉 YAML extraction completed successfully! ({success_count}/{total_files} files processed)")
            return True
        else:
            logger.warning("⚠️  YAML extraction found no content!")
            return False

    except Exception as e:
        logger.error(f"YAML extraction failed with unexpected error: {e}")
        return False


def main():
    """Main entry point"""
    logger.info("Starting YAML extraction from MDX files...")

    success = extract_checks_yml()

    if success:
        logger.info("YAML extraction completed successfully!")
        exit(0)
    else:
        logger.error("YAML extraction failed!")
        exit(1)


if __name__ == "__main__":
    main()
