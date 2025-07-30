#!/usr/bin/env python3
import tempfile
from pathlib import Path

import pytest

from databricks.labs.dqx.llm.docs_extractor import extract_checks_yml_examples, extract_yaml_from_mdx


def test_extract_yaml_from_mdx_success():
    """Test successful YAML extraction from MDX file."""
    mdx_content = """# Test
```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1
```
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.mdx', delete=False, encoding='utf-8') as f:
        f.write(mdx_content)
        temp_path = Path(f.name)

    try:
        success, yaml_content = extract_yaml_from_mdx(temp_path)

        assert success is True
        assert len(yaml_content) == 1
        assert yaml_content[0]["criticality"] == "error"
        assert yaml_content[0]["check"]["function"] == "is_not_null"
    finally:
        temp_path.unlink()


def test_extract_yaml_from_mdx_no_yaml():
    """Test extraction from MDX file with no YAML blocks."""
    mdx_content = "# Test\nNo YAML here."

    with tempfile.NamedTemporaryFile(mode='w', suffix='.mdx', delete=False, encoding='utf-8') as f:
        f.write(mdx_content)
        temp_path = Path(f.name)

    try:
        success, yaml_content = extract_yaml_from_mdx(temp_path)
        assert success is False
        assert yaml_content == []
    finally:
        temp_path.unlink()


def test_extract_yaml_from_mdx_file_not_found():
    """Test extraction from non-existent MDX file."""
    with pytest.raises(FileNotFoundError, match="MDX file not found"):
        extract_yaml_from_mdx(Path("non_existent_file.mdx"))


def test_extract_yaml_from_real_quality_rules_mdx():
    """Test YAML extraction from the real quality_rules.mdx file."""
    quality_rules_path = Path("docs/dqx/docs/reference/quality_rules.mdx")

    if not quality_rules_path.exists():
        pytest.skip(f"File {quality_rules_path} not found")

    success, yaml_content = extract_yaml_from_mdx(quality_rules_path)

    assert success is True
    assert len(yaml_content) > 0

    # Verify basic structure
    first_item = yaml_content[0]
    assert "criticality" in first_item
    assert "check" in first_item


def test_extract_checks_yml_examples():
    """Test extraction using real repository structure."""
    success, yaml_content = extract_checks_yml_examples()

    # Should not crash regardless of environment
    assert isinstance(success, bool)
    assert isinstance(yaml_content, list)

    # If running in repo root with docs, should succeed
    if Path("docs/dqx/docs/reference/quality_rules.mdx").exists():
        assert success is True
        assert len(yaml_content) > 0
