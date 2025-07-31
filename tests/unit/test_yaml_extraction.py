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


def run_hatch_build_hook():
    """Run the hatch build hook by calling it directly."""
    script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "hatch_build_hook.py"
    repo_root = Path(__file__).parent.parent.parent

    # Execute the build hook script directly with a simple test
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=repo_root,
        check=False,
        env={'FORCE_EXTRACTION': 'true'},  # Signal to run extraction regardless of environment
    )

    if result.returncode == 0:
        logger.info("✅ Build hook executed successfully")
        return True

    logger.error(f"❌ Build hook failed: {result.stderr}")
    if result.stdout:
        logger.error(f"Build hook output: {result.stdout.strip()}")
    return False


def test_yaml_file_exists():
    """Test 1: Check if YAML file was generated."""
    # First run the build hook
    assert run_hatch_build_hook(), "Build hook should execute successfully"

    yaml_file = (
        Path(__file__).parent.parent.parent / "src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml"
    )

    assert yaml_file.exists(), "YAML file should be generated"

    file_size = yaml_file.stat().st_size
    assert file_size > 0, "YAML file should not be empty"

    logger.info(f"✅ YAML file exists ({file_size} bytes)")


def test_yaml_content_valid():
    """Test 2: Check if YAML content is valid and has correct structure."""
    yaml_file = (
        Path(__file__).parent.parent.parent / "src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml"
    )

    assert yaml_file.exists(), "YAML file should exist"

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
