#!/usr/bin/env python3
import sys
import tempfile
from pathlib import Path

# Add project root to Python path so we can import hatch_build_hook
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pytest
import yaml

# Import from the build hook file directly
from hatch_build_hook import extract_checks_yml_examples, extract_yaml_from_mdx


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


def test_extract_checks_yml_examples():
    """Test that extract_checks_yml creates resources with valid YAML content."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_repo = Path(temp_dir)
        
        # Create simple test documentation
        docs_dir = temp_repo / "docs" / "dqx" / "docs" / "reference"
        docs_dir.mkdir(parents=True, exist_ok=True)
        
        test_content = """# Test
```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: test_col
```
"""
        (docs_dir / "quality_rules.mdx").write_text(test_content)
        
        # Run extraction
        success, yaml_content = extract_checks_yml_examples(repo_root=temp_repo)
        
        # 1. Verify extraction creates resources
        resources_dir = temp_repo / "src" / "databricks" / "labs" / "dqx" / "llm" / "resources"
        yaml_file = resources_dir / "quality_checks_all_examples.yml"
        assert yaml_file.exists(), "Should create YAML resource file"
        
        # 2. Verify content is not empty
        assert len(yaml_content) > 0, "Content should not be empty"
        yaml_file_content = yaml_file.read_text()
        assert len(yaml_file_content.strip()) > 0, "YAML file should not be empty"
        
        # 3. Verify content is valid YAML
        parsed_yaml = yaml.safe_load(yaml_file_content)
        assert parsed_yaml is not None, "Should be valid YAML"
        assert len(parsed_yaml) > 0, "Parsed YAML should contain data"


def test_extract_checks_yml_examples_using_real_repository():
    """Test extraction using real repository structure (integration test)."""
    success, yaml_content = extract_checks_yml_examples()

    # Should not crash regardless of environment
    assert isinstance(success, bool)
    assert isinstance(yaml_content, list)

    # If running in repo root with docs, should succeed
    if Path("docs/dqx/docs/reference/quality_rules.mdx").exists():
        assert success is True
        assert len(yaml_content) > 0
