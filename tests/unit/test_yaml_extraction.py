#!/usr/bin/env python3
"""
YAML extraction functionality in hatch_build_hook.py
"""

import logging
import re
import sys
import unittest
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Add the script directory to Python path to import the build hook
sys.path.insert(0, str(Path(__file__).parent.parent.parent / ".github" / "script"))

# Sample MDX content that would be found in documentation files
SAMPLE_MDX_CONTENT = r"""# Data Quality Checks Documentation

Here are example quality checks for testing:

```yaml
- criticality: error
  check:
    function: is_not_null
    for_each_column:
      - user_id
      - email_address
    arguments: {}
- name: email_validation_check
  criticality: warn
  check:
    function: matching_regex
    arguments:
      column: email_address
      pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
```

Additional quality check examples:

```yaml
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: age
      min_value: 0
      max_value: 120
- criticality: warn
  check:
    function: foreign_key
    arguments:
      column: department_id
      ref_table: departments
      ref_column: id
```

More documentation content here...
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

        logger.info(f" YAML content parsed successfully ({len(all_checks)} items)")

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

        logger.info(f"   YAML content structure is valid ({len(all_checks)} items)")
