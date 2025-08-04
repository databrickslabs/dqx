"""
Simple test for extract_yaml.py functionality
"""

import importlib.util
from pathlib import Path

# Import extract_yaml module using importlib
script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "extract_yaml.py"
spec = importlib.util.spec_from_file_location("extract_yaml", script_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load extract_yaml from {script_path}")
extract_yaml_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extract_yaml_module)
extract_yaml_from_content = extract_yaml_module.extract_yaml_from_content


def test_extract_yaml_works():
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
    success, yaml_data = extract_yaml_from_content(mdx_content, "test_content")

    # Verify it works
    assert success
    assert len(yaml_data) == 1
    assert yaml_data[0]["name"] == "test"
    assert yaml_data[0]["value"] == 42

    print("✅ YAML extraction test passed!")


def test_extract_yaml_multiple_blocks():
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
    success, yaml_data = extract_yaml_from_content(mdx_content, "test_multiple")

    assert success
    assert len(yaml_data) == 2
    assert yaml_data[0]["name"] == "check1"
    assert yaml_data[1]["name"] == "check2"
    assert yaml_data[1]["value"] == 123

    print("✅ Multiple YAML blocks test passed!")


def test_extract_yaml_empty_file():
    """Test extraction from content with no YAML blocks"""

    mdx_content = """
# Test No YAML

This file has no YAML blocks.

```javascript
console.log("Not YAML");
```
"""

    # Test extraction directly from content
    success, yaml_data = extract_yaml_from_content(mdx_content, "test_empty")

    assert not success
    assert len(yaml_data) == 0

    print("✅ Empty YAML test passed!")


def test_extract_yaml_invalid_yaml():
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
    success, yaml_data = extract_yaml_from_content(mdx_content, "test_invalid")

    # Should still return False for invalid YAML
    assert not success
    assert len(yaml_data) == 0

    print("✅ Invalid YAML test passed!")
