"""
 Unit tests for extract_yaml_checks_examples.py functionality
"""

import importlib.util
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Import extract_yaml_checks_examples module using importlib
script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "extract_yaml_checks_examples.py"
spec = importlib.util.spec_from_file_location("extract_yaml_checks_examples", script_path)
extract_yaml_module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
spec.loader.exec_module(extract_yaml_module)  # type: ignore[union-attr]
extract_yaml_from_content = extract_yaml_module.extract_yaml_from_content


def test_extract_yaml_works() -> None:
    """Test that YAML extraction works and creates valid YAML"""

    # Simple test content
    mdx_content = """
# Test

```yaml
name: test
value: 42
```
"""

    # Test extraction directly from content
    yaml_data = extract_yaml_from_content(mdx_content, "test_content")

    # Verify it works
    expected = [{"name": "test", "value": 42}]
    assert yaml_data == expected

    logger.info("✅ YAML extraction test passed!")


def test_extract_yaml_multiple_blocks() -> None:
    """Test extraction from multiple YAML blocks"""

    mdx_content = """
# Test Multiple Blocks

```yaml
- name: check1
  type: test
```

```yaml
- name: check2
  type: validation
  value: 123
```
"""

    # Test extraction directly from content
    yaml_data = extract_yaml_from_content(mdx_content, "test_multiple")

    expected = [{"name": "check1", "type": "test"}, {"name": "check2", "type": "validation", "value": 123}]
    assert yaml_data == expected

    logger.info("✅ Multiple YAML blocks test passed!")


def test_extract_yaml_invalid_yaml() -> None:
    """Test extraction with invalid YAML syntax"""

    mdx_content = """
# Test Invalid YAML

```yaml
name: test
  invalid_indentation: bad
- list_item_wrong_level
```
"""

    # Test extraction directly from content
    yaml_data = extract_yaml_from_content(mdx_content, "test_invalid")

    # Should return empty list for invalid YAML
    expected: list[dict[str, Any]] = []
    assert yaml_data == expected

    logger.info("✅ Invalid YAML test passed!")


def test_extract_yaml_empty_file() -> None:
    """Test extraction from content with no YAML blocks"""

    mdx_content = """
# Test No YAML

This file has no YAML blocks.

```javascript
console.log("Not YAML");
```
"""

    # Test extraction directly from content
    yaml_data = extract_yaml_from_content(mdx_content, "test_empty")

    expected: list[dict[str, Any]] = []
    assert yaml_data == expected

    logger.info("✅ Empty YAML test passed!")


def test_extract_yaml_missing_file() -> None:
    """Test extraction from a non-existent file"""

    # Test with a file that doesn't exist
    yaml_data = extract_yaml_module.extract_yaml_from_mdx("/nonexistent/path/file.mdx")

    # Should return empty list for missing file
    expected: list[dict[str, Any]] = []
    assert yaml_data == expected

    logger.info("✅ Missing file test passed!")


def test_extract_yaml_file_generation() -> None:
    """Test that the main function generates the output YAML file"""

    # Get the expected output file path
    repo_root = Path(".").resolve()
    expected_file = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources" / "yaml_checks_examples.yml"

    # Remove file if it exists to test generation
    if expected_file.exists():
        expected_file.unlink()

    # Call the main extraction function
    success = extract_yaml_module.extract_yaml_checks_examples()

    # Verify the function returned success
    assert success is True

    # Verify the file was created
    assert expected_file.exists(), f"Expected output file not created: {expected_file}"

    # Verify the file has content
    content = expected_file.read_text()
    assert len(content) > 0, "Generated YAML file is empty"

    # Verify it contains YAML content
    assert content.strip().startswith(("-", "name:", "checks:")), "File doesn't appear to contain YAML content"

    logger.info("✅ YAML file generation test passed!")


def test_extract_yaml_backticks_not_newline() -> None:
    """Test extraction when ``` is not on a new line (inline with text)"""

    mdx_content = """
# Test Backticks Not On New Line

Here is some text ```yaml
name: inline_backticks
value: 123
```

Normal block:
```yaml
- name: normal_block
  type: standard
```

Another inline case: Here's code ```yaml
checks:
  - name: inline_check2
    type: validation
```
"""

    # Test extraction directly from content
    yaml_data = extract_yaml_from_content(mdx_content, "test_backticks_inline")

    # Current behavior: extracts ALL YAML blocks, even those with inline ```
    # This includes blocks where ``` is not on a new line
    expected = [
        {"name": "inline_backticks", "value": 123},  # from inline: Here is some text ```yaml
        {"name": "normal_block", "type": "standard"},  # from properly formatted block
        {"checks": [{"name": "inline_check2", "type": "validation"}]},  # from inline: Here's code ```yaml
    ]
    assert yaml_data == expected

    logger.info("✅ Backticks not on new line test passed!")
