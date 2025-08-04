"""
Simple test for extract_yaml.py functionality
"""

import importlib.util
import os
import tempfile
from pathlib import Path

# Import extract_yaml module using importlib
script_path = Path(__file__).parent.parent.parent / ".github" / "script" / "extract_yaml.py"
spec = importlib.util.spec_from_file_location("extract_yaml", script_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load extract_yaml from {script_path}")
extract_yaml_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extract_yaml_module)
extract_yaml_from_mdx = extract_yaml_module.extract_yaml_from_mdx


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

    # Create temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".mdx", delete=False) as f:
        f.write(mdx_content)
        temp_path = f.name

    try:
        # Test extraction
        success, yaml_data = extract_yaml_from_mdx(temp_path)

        # Verify it works
        assert success
        assert len(yaml_data) == 1
        assert yaml_data[0]["name"] == "test"
        assert yaml_data[0]["value"] == 42

        print("✅ YAML extraction test passed!")

    finally:
        # Clean up
        os.unlink(temp_path)
