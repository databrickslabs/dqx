import tempfile
import pytest
from pathlib import Path
import re
import yaml
import os


def extract_yaml_from_mdx_standalone(mdx_file_path):
    """Standalone version of YAML extraction logic for testing"""
    
    mdx_file = Path(mdx_file_path)
    
    if not mdx_file.exists():
        return False, []
    
    try:
        content = mdx_file.read_text(encoding='utf-8')
    except (OSError, UnicodeDecodeError):
        return False, []
    
    # Extract YAML from code blocks (same logic as original)
    yaml_pattern = r'```(?:yaml|yml)\n(.*?)\n```'
    yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)
    
    if not yaml_matches:
        return False, []
    
    # Combine all YAML blocks
    all_yaml_content = []
    
    for yaml_content in yaml_matches:
        try:
            parsed_yaml = yaml.safe_load(yaml_content)
            if not parsed_yaml:  # Skip empty YAML blocks
                continue
            
            if isinstance(parsed_yaml, list):
                all_yaml_content.extend(parsed_yaml)
            else:
                all_yaml_content.append(parsed_yaml)
        except yaml.YAMLError:
            continue
    
    return len(all_yaml_content) > 0, all_yaml_content


def test_extract_yaml_from_mdx_basic():
    """Test basic YAML extraction from MDX content"""
    
    # Sample MDX content with YAML blocks
    mdx_content = """# Test Document

Here's some markdown content.

```yaml
name: test_check
description: Test check
rules:
  - column: id
    check: not_null
```

More content here.

```yml
- name: another_check
  description: Another test
  column: status
  check: unique
```
"""
    
    # Create temporary MDX file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mdx', delete=False) as temp_file:
        temp_file.write(mdx_content)
        temp_file_path = temp_file.name
    
    try:
        # Test extraction using standalone function
        success, yaml_content = extract_yaml_from_mdx_standalone(temp_file_path)
        
        # Assertions
        assert success is True
        assert len(yaml_content) == 2  # Should extract 2 YAML blocks
        
        # Check first YAML block
        first_yaml = yaml_content[0]
        assert first_yaml['name'] == 'test_check'
        assert first_yaml['description'] == 'Test check'
        assert 'rules' in first_yaml
        
        # Check second YAML block  
        second_yaml = yaml_content[1]
        assert second_yaml['name'] == 'another_check'
        assert second_yaml['column'] == 'status'
        
    finally:
        # Cleanup
        os.unlink(temp_file_path)


def test_extract_yaml_from_mdx_no_yaml():
    """Test MDX file with no YAML blocks"""
    
    mdx_content = """# Test Document

This is just markdown content with no YAML.

```python
print("This is Python code, not YAML")
```
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mdx', delete=False) as temp_file:
        temp_file.write(mdx_content)
        temp_file_path = temp_file.name
    
    try:
        success, yaml_content = extract_yaml_from_mdx_standalone(temp_file_path)
        
        assert success is False
        assert yaml_content == []
        
    finally:
        os.unlink(temp_file_path)


def test_extract_yaml_from_mdx_invalid_file():
    """Test with non-existent file"""
    
    success, yaml_content = extract_yaml_from_mdx_standalone("/non/existent/file.mdx")
    
    assert success is False
    assert yaml_content == []


def test_extract_yaml_from_mdx_invalid_yaml():
    """Test MDX with invalid YAML content"""
    
    mdx_content = """# Test Document

```yaml
invalid: yaml: content:
  - unclosed: [bracket
    missing: quote"
```
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mdx', delete=False) as temp_file:
        temp_file.write(mdx_content)
        temp_file_path = temp_file.name
    
    try:
        success, yaml_content = extract_yaml_from_mdx_standalone(temp_file_path)
        
        # Should handle invalid YAML gracefully
        assert success is False  # No valid YAML extracted
        assert yaml_content == []
        
    finally:
        os.unlink(temp_file_path)


def test_extract_yaml_mixed_content():
    """Test MDX with mixed valid and invalid YAML"""
    
    mdx_content = """# Test Document

```yaml
name: valid_check
description: This is valid
```

```yaml
invalid: yaml: content:
  - unclosed: [bracket
```

```yml
name: another_valid
description: This is also valid
```
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mdx', delete=False) as temp_file:
        temp_file.write(mdx_content)
        temp_file_path = temp_file.name
    
    try:
        success, yaml_content = extract_yaml_from_mdx_standalone(temp_file_path)
        
        # Should extract only valid YAML blocks
        assert success is True
        assert len(yaml_content) == 2  # Only 2 valid blocks
        
        assert yaml_content[0]['name'] == 'valid_check'
        assert yaml_content[1]['name'] == 'another_valid'
        
    finally:
        os.unlink(temp_file_path) 