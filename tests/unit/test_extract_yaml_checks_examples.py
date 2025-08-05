import importlib.util
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Import extract_yaml_checks_examples module
script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "extract_yaml_checks_examples.py"
spec = importlib.util.spec_from_file_location("extract_yaml_checks_examples", script_path)
extract_yaml_module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
spec.loader.exec_module(extract_yaml_module)  # type: ignore[union-attr]


def test_extract_valid_yaml() -> None:
    """Test that YAML extraction works and creates valid YAML"""

    # Simple test content
    mdx_content = """
# Test

```yaml
name: test
value: 42
```
"""

    yaml_data = extract_yaml_module.extract_yaml_checks_from_content(mdx_content, "test_content")

    expected = [{"name": "test", "value": 42}]
    assert yaml_data == expected


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

    yaml_data = extract_yaml_module.extract_yaml_checks_from_content(mdx_content, "test_multiple")

    expected = [{"name": "check1", "type": "test"}, {"name": "check2", "type": "validation", "value": 123}]
    assert yaml_data == expected


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

    yaml_data = extract_yaml_module.extract_yaml_checks_from_content(mdx_content, "test_invalid")

    expected: list[dict[str, Any]] = []
    assert yaml_data == expected


def test_extract_yaml_empty_file() -> None:
    """Test extraction from content with no YAML blocks"""

    mdx_content = """
# Test No YAML

This file has no YAML blocks.

```javascript
console.log("Not YAML");
```
"""

    yaml_data = extract_yaml_module.extract_yaml_checks_from_content(mdx_content, "test_empty")

    expected: list[dict[str, Any]] = []
    assert yaml_data == expected


def test_extract_yaml_missing_file() -> None:
    """Test extraction from a non-existent file"""

    # Test with a file that doesn't exist
    yaml_data = extract_yaml_module.extract_yaml_checks_from_mdx("/nonexistent/path/file.mdx")

    # Should return empty list for missing file
    expected: list[dict[str, Any]] = []
    assert yaml_data == expected


def test_extract_generated_yaml() -> None:
    """Test that the main function generates the output YAML file"""
    repo_root = Path(__file__).resolve().parent.parent.parent
    expected_file = repo_root / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources" / "yaml_checks_examples.yml"

    if expected_file.exists():
        expected_file.unlink()

    success = extract_yaml_module.extract_yaml_checks_examples()

    assert success
    assert expected_file.exists(), f"Expected output file not created: {expected_file}"
    content = expected_file.read_text()
    assert len(content) > 0, "Generated YAML file is empty"
    assert content.strip().startswith(("-", "name:", "checks:")), "File doesn't appear to contain YAML content"


def test_extract_yaml_backticks_not_newline() -> None:
    """Test extraction when ``` is not on a new line (inline with text)"""

    mdx_content = """
# Test Backticks Not On New Line

Here is some text ```yaml
name: inline_backticks
value: 123
```

Normal block:
```yaml- name: normal_block
  type: standard
```

Another inline case: Here's code ```yaml
checks:
  - name: inline_check2
    type: validation```
"""

    yaml_data = extract_yaml_module.extract_yaml_checks_from_content(mdx_content, "test_backticks_inline")

    # Current behavior: extracts ALL YAML blocks, even those with inline ```
    # This includes blocks where ``` is not on a new line
    expected = [
        {"name": "inline_backticks", "value": 123},  # from inline: Here is some text ```yaml
        {"name": "normal_block", "type": "standard"},  # from properly formatted block
        {"checks": [{"name": "inline_check2", "type": "validation"}]},  # from inline: Here's code ```yaml
    ]
    assert yaml_data == expected
