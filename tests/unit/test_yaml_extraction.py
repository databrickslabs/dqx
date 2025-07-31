#!/usr/bin/env python3
"""
Simple test for YAML extraction functionality in hatch_build_hook.py
"""

import logging
import subprocess
import sys
from pathlib import Path

import yaml

# Configure logger
logger = logging.getLogger(__name__)


def create_yaml_file():
    """Create the YAML file by running the hatch build hook."""
    script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "hatch_build_hook.py"
    repo_root = Path(__file__).parent.parent.parent

    logger.info("🔧 Creating YAML file using build hook...")

    # Execute the build hook script directly
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=repo_root,
        check=False,
        env={'FORCE_EXTRACTION': 'true'},  # Signal to run extraction regardless of environment
    )

    if result.returncode == 0:
        logger.info("✅ YAML file creation successful")
        return True

    logger.error(f"❌ YAML file creation failed: {result.stderr}")
    if result.stdout:
        logger.error(f"Build hook output: {result.stdout.strip()}")
    return False


def test_yaml_file_creation():
    """Test: Create and verify YAML file generation."""
    # Step 1: Create the YAML file
    assert create_yaml_file(), "YAML file creation should succeed"

    # Step 2: Verify the file exists
    yaml_file = (
        Path(__file__).parent.parent.parent / "src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml"
    )
    assert yaml_file.exists(), "YAML file should be generated"

    # Step 3: Verify file is not empty
    file_size = yaml_file.stat().st_size
    assert file_size > 0, "YAML file should not be empty"

    logger.info(f"✅ YAML file created and verified ({file_size} bytes)")


def test_yaml_content_valid():
    """Test: Validate YAML content structure and data."""
    yaml_file = (
        Path(__file__).parent.parent.parent / "src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml"
    )

    # Ensure file exists (should be created by previous test)
    if not yaml_file.exists():
        # Create it if it doesn't exist
        assert create_yaml_file(), "YAML file creation should succeed"

    assert yaml_file.exists(), "YAML file should exist"

    # Load and validate YAML content
    with open(yaml_file, 'r', encoding='utf-8') as f:
        yaml_content = yaml.safe_load(f)

    # Check basic structure
    assert isinstance(yaml_content, list), "YAML should be a list"
    assert len(yaml_content) > 0, "YAML list should not be empty"

    # Check first item has required fields
    first_item = yaml_content[0]
    assert 'criticality' in first_item, "Items should have 'criticality' field"
    assert 'check' in first_item, "Items should have 'check' field"
    assert 'function' in first_item['check'], "Check should have 'function' field"

    logger.info(f"✅ YAML content is valid ({len(yaml_content)} items)")


def test_yaml_structure_details():
    """Test: Validate detailed YAML structure and sample content."""
    yaml_file = (
        Path(__file__).parent.parent.parent / "src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml"
    )

    assert yaml_file.exists(), "YAML file should exist"

    with open(yaml_file, 'r', encoding='utf-8') as f:
        yaml_content = yaml.safe_load(f)

    # Validate we have multiple items
    assert len(yaml_content) >= 10, "Should have at least 10 quality check examples"

    # Check that we have different types of checks
    functions_found = set()
    criticalities_found = set()

    for item in yaml_content:
        if 'check' in item and 'function' in item['check']:
            functions_found.add(item['check']['function'])
        if 'criticality' in item:
            criticalities_found.add(item['criticality'])

    # Verify we have variety in checks
    assert len(functions_found) >= 5, f"Should have at least 5 different check functions, found: {functions_found}"
    assert len(criticalities_found) >= 2, f"Should have multiple criticality levels, found: {criticalities_found}"

    logger.info(
        f"✅ YAML structure validated: {len(functions_found)} functions, {len(criticalities_found)} criticality levels"
    )
