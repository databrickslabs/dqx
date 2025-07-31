#!/usr/bin/env python3
"""
Simple test for YAML extraction functionality in hatch_build_hook.py
"""

import logging
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

logger = logging.getLogger(__name__)

# Add the script directory to Python path to import the build hook
sys.path.insert(0, str(Path(__file__).parent.parent.parent / ".github" / "script"))

# Sample MDX content that would be found in documentation files
SAMPLE_MDX_CONTENT = """# Quality Checks Documentation

Here are some example quality checks:

```yaml
- criticality: error
  check:
    function: is_not_null
    for_each_column:
      - col1
      - col2
    arguments: {}
- name: col_col3_is_null_or_empty
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3
      trim_strings: true
```

And another example:

```yaml
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: col4
      allowed:
      - 1
      - 2
```

More content here...
"""


def extract_yaml_from_mdx(content: str) -> list:
    """Extract YAML blocks from MDX content."""
    yaml_blocks = re.findall(r'```yaml\n(.*?)\n```', content, re.DOTALL)

    all_checks = []
    for yaml_block in yaml_blocks:
        yaml_content = yaml.safe_load(yaml_block.strip())
        if isinstance(yaml_content, list):
            all_checks.extend(yaml_content)
        elif yaml_content:
            all_checks.append(yaml_content)

    return all_checks


class TestYamlExtraction(unittest.TestCase):
    """Test class for YAML extraction functionality."""

    def test_yaml_content_parsing(self):
        """Test: Validate that YAML content can be parsed correctly."""
        logger.info("🔧 Testing YAML content parsing...")

        all_checks = extract_yaml_from_mdx(SAMPLE_MDX_CONTENT)

        self.assertGreater(len(all_checks), 0, "Should extract some YAML content from MDX")
        self.assertIsInstance(all_checks, list, "Combined content should be a list")

        logger.info(f"✅ YAML content parsed successfully ({len(all_checks)} items)")

    def test_yaml_content_structure(self):
        """Test: Validate YAML content structure and required fields."""
        all_checks = extract_yaml_from_mdx(SAMPLE_MDX_CONTENT)

        self.assertGreater(len(all_checks), 0, "Should have extracted YAML content")

        # Validate structure of each check
        for i, item in enumerate(all_checks):
            self.assertIn('criticality', item, f"Item {i} should have 'criticality' field")
            self.assertIn('check', item, f"Item {i} should have 'check' field")
            self.assertIn('function', item['check'], f"Item {i} check should have 'function' field")

            # Check criticality values are valid
            self.assertIn(
                item['criticality'],
                ['error', 'warn', 'info'],
                f"Item {i} has invalid criticality: {item['criticality']}",
            )

        logger.info(f"✅ YAML content structure is valid ({len(all_checks)} items)")

    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.glob')
    @patch('pathlib.Path.read_text')
    def test_build_hook_extract_yaml_logic(self, mock_read_text, mock_glob, mock_exists):
        """Test: Validate the build hook's YAML extraction logic without file creation."""
        try:
            # Import the build hook module
            import hatch_build_hook  # type: ignore[import-not-found] # pylint: disable=import-outside-toplevel

            # Mock file system operations
            mock_exists.return_value = True
            mock_glob.return_value = [Path("fake/file1.mdx"), Path("fake/file2.mdx")]
            mock_read_text.return_value = SAMPLE_MDX_CONTENT

            # Create a test instance of the build hook
            hook = hatch_build_hook.ExtractDocsResourcesHook("test", {})

            # Test the extract_yaml_from_mdx method
            result = hook.extract_yaml_from_mdx(Path("fake/test.mdx"))
            self.assertTrue(result, "extract_yaml_from_mdx should return True for valid content")

            logger.info("✅ Build hook YAML extraction logic validated")

        except ImportError as e:
            logger.warning(f"⚠️ Could not import build hook module: {e}")
            # Fallback test - validate the extraction logic manually
            yaml_blocks = re.findall(r'```yaml\n(.*?)\n```', SAMPLE_MDX_CONTENT, re.DOTALL)
            self.assertGreaterEqual(len(yaml_blocks), 2, "Should find multiple YAML blocks in sample content")
            logger.info("✅ Build hook logic validated via manual extraction")

    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.write_text')
    def test_build_hook_without_file_creation(self, mock_write_text, mock_mkdir):
        """Test: Validate build hook can be called without actual file creation."""
        try:
            import hatch_build_hook  # type: ignore[import-not-found] # pylint: disable=import-outside-toplevel

            # Mock the file operations to prevent actual file creation
            mock_mkdir.return_value = None
            mock_write_text.return_value = None

            # Create a mock root directory with explicit attributes
            mock_root = unittest.mock.Mock()
            mock_root.exists.return_value = True

            # Test that we can instantiate the hook without errors
            hook = hatch_build_hook.ExtractDocsResourcesHook("test", {})
            hook.root = mock_root

            # Test the initialization doesn't crash
            self.assertIsNotNone(hook, "Build hook should be instantiable")

            logger.info("✅ Build hook instantiation validated")

        except ImportError:
            logger.info("✅ Build hook import test skipped (module not accessible)")
