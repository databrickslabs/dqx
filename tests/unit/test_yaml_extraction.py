#!/usr/bin/env python3
import logging
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

# Configure logger
logger = logging.getLogger(__name__)


def test_run_hatch_build_hook_script():
    """Test that the hatch_build_hook.py script runs successfully."""
    script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "hatch_build_hook.py"

    # Test script execution
    test_script = f"""
import logging
import sys
sys.path.insert(0, '{script_path.parent}')

from hatch_build_hook import extract_checks_yml_examples

# Configure basic logging for subprocess
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Run the extraction function
success, yaml_content = extract_checks_yml_examples()
print(f'SUCCESS={{success}}')
print(f'COUNT={{len(yaml_content)}}')

# Check if resource file was created
from pathlib import Path
resource_file = Path('src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml')
print(f'FILE_EXISTS={{resource_file.exists()}}')
if resource_file.exists():
    print(f'FILE_SIZE={{resource_file.stat().st_size}}')

logger.info("Script execution completed successfully")
"""

    # Run the test script
    result = subprocess.run(
        [sys.executable, "-c", test_script],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent,
        check=False,
    )

    # Check that script ran without errors
    assert result.returncode == 0, f"Script failed with error: {result.stderr}"

    # Parse output
    output_lines = result.stdout.strip().split('\n')
    success_line = [line for line in output_lines if line.startswith('SUCCESS=')]
    count_line = [line for line in output_lines if line.startswith('COUNT=')]
    file_exists_line = [line for line in output_lines if line.startswith('FILE_EXISTS=')]

    assert len(success_line) > 0, "Should output SUCCESS status"
    assert len(count_line) > 0, "Should output COUNT"
    assert len(file_exists_line) > 0, "Should output FILE_EXISTS status"

    # Verify successful execution
    success = success_line[0].split('=')[1] == 'True'
    count = int(count_line[0].split('=')[1])
    file_exists = file_exists_line[0].split('=')[1] == 'True'

    assert success, "Script should execute successfully"
    assert count > 0, "Should extract some YAML content"
    assert file_exists, "Should create resource file"


def test_validate_generated_yaml_content():
    """Test that the generated YAML content is valid and well-formed."""
    resource_file = Path("src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml")

    # Check if file exists (should be created by previous test or real execution)
    if not resource_file.exists():
        pytest.skip("Resource file not found - run the build hook first")

    # Validate YAML content
    with open(resource_file, 'r', encoding='utf-8') as f:
        yaml_content = yaml.safe_load(f)

    assert yaml_content is not None, "YAML should be valid"
    assert isinstance(yaml_content, list), "YAML should be a list"
    assert len(yaml_content) > 0, "YAML should contain data"

    # Check structure of first item
    if len(yaml_content) > 0:
        first_item = yaml_content[0]
        assert 'criticality' in first_item, "Items should have criticality"
        assert 'check' in first_item, "Items should have check"
        assert 'function' in first_item['check'], "Check should have function"


def test_cleanup_generated_files():
    """Clean up generated files after successful test execution."""
    resource_file = Path("src/databricks/labs/dqx/llm/resources/quality_checks_all_examples.yml")
    resources_dir = Path("src/databricks/labs/dqx/llm/resources")
    init_file = resources_dir / "__init__.py"

    # Remove the generated YAML file
    if resource_file.exists():
        resource_file.unlink()
        logger.info(f"Deleted generated YAML file: {resource_file}")

    # Remove the __init__.py file
    if init_file.exists():
        init_file.unlink()
        logger.info(f"Deleted init file: {init_file}")

    # Remove the resources directory if it's empty
    if resources_dir.exists() and not any(resources_dir.iterdir()):
        resources_dir.rmdir()
        logger.info(f"Deleted empty directory: {resources_dir}")

    # Remove parent directories if they're empty (going up the chain)
    llm_dir = resources_dir.parent
    if llm_dir.exists() and llm_dir.name == "llm" and not any(llm_dir.iterdir()):
        llm_dir.rmdir()
        logger.info(f"Deleted empty directory: {llm_dir}")

    logger.info("Cleanup completed successfully")
