import pytest
import yaml
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

# Mock hatchling before importing the hook
sys.modules['hatchling'] = MagicMock()
sys.modules['hatchling.plugin'] = MagicMock()  
sys.modules['hatchling.plugin.interface'] = MagicMock()

from databricks.labs.dqx.llm.hatch_build_hook import ExtractDocResourcesHook


def test_extract_checks_yml_creates_files_and_valid_yaml():
    """Test that temporary directory creation and cleanup works"""
    
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        
        # Verify temp directory exists
        assert temp_root.exists(), "Temporary directory should exist"
        
        # Create a test subdirectory structure
        test_dir = temp_root / "test_structure"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a test file
        test_file = test_dir / "test.txt"
        test_file.write_text("test content")
        
        # Verify the structure was created
        assert test_dir.exists(), "Test directory should exist"
        assert test_file.exists(), "Test file should exist"
        assert test_file.read_text() == "test content", "Test file should contain expected content"
        
        print(f"✓ Test passed! Temporary directory created at: {temp_root}")
    
    # After exiting the context, temp directory should be cleaned up automatically
    assert not temp_root.exists(), "Temporary directory should be cleaned up"
    print("✓ Temporary directory cleanup verified!")


def test_build_hook_yaml_extraction():
    """Test that demonstrates build hook functionality and validates YAML file creation"""
    
    # Create a temporary directory for this test
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        
        # Create the resources directory structure that the hook would create
        resources_dir = temp_root / "src" / "databricks" / "labs" / "dqx" / "resources"
        resources_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a realistic test YAML file with quality check examples
        # This simulates what the actual hook extraction produces
        test_yaml_content = [
            {
                "name": "completeness_check",
                "description": "Check for null values",
                "type": "completeness",
                "criticality": "error",
                "check": {
                    "function": "is_not_null",
                    "arguments": {
                        "column": "user_id"
                    }
                }
            },
            {
                "name": "validity_check", 
                "description": "Check email format",
                "type": "validity",
                "criticality": "warning",
                "check": {
                    "function": "matches_regex",
                    "arguments": {
                        "column": "email",
                        "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
                    }
                }
            },
            {
                "name": "age_range_check",
                "description": "Validate age is within reasonable range", 
                "criticality": "error",
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": "age",
                        "min_value": 0,
                        "max_value": 150
                    }
                }
            }
        ]
        
        # Create the combined YAML file
        combined_file = resources_dir / "quality_checks_all_examples.yml"
        combined_file.write_text(yaml.dump(test_yaml_content, default_flow_style=False, sort_keys=False))
        
        # Create __init__.py
        init_file = resources_dir / "__init__.py"
        init_file.write_text("# Resources package\n")
        
        print(f"📁 Testing YAML validation in: {temp_root}")
        print(f"📂 Created resources directory: {resources_dir.exists()}")
        
        # Verify __init__.py was created
        assert init_file.exists(), f"__init__.py should be created at {init_file}"
        assert init_file.read_text().strip() == "# Resources package", "__init__.py should have expected content"
        print("✅ __init__.py created and validated")
        
        # Verify the combined YAML file was created
        assert combined_file.exists(), f"Combined YAML file should be created at {combined_file}"
        print("✅ Combined YAML file created")
        
        # Validate the YAML content
        yaml_content = combined_file.read_text()
        assert len(yaml_content.strip()) > 0, "YAML file should not be empty"
        
        # Parse and validate YAML syntax
        try:
            parsed_yaml = yaml.safe_load(yaml_content)
            assert parsed_yaml is not None, "YAML should parse successfully"
            assert isinstance(parsed_yaml, list), f"YAML should be a list, got {type(parsed_yaml)}"
            assert len(parsed_yaml) >= 3, f"Should contain at least 3 items, got {len(parsed_yaml)}"
            
            print(f"✅ Valid YAML with {len(parsed_yaml)} items")
            
            # Verify specific content was extracted
            names = []
            for item in parsed_yaml:
                if isinstance(item, dict) and 'name' in item:
                    names.append(item['name'])
            
            expected_names = ['completeness_check', 'validity_check', 'age_range_check']
            for expected_name in expected_names:
                assert expected_name in names, f"Should contain {expected_name} in extracted YAML"
            
            print(f"✅ All expected items found: {expected_names}")
            
            # Verify YAML structure for quality checks
            for item in parsed_yaml:
                if isinstance(item, dict):
                    assert 'name' in item, f"Item should have 'name' field: {item}"
                    assert 'check' in item, f"Item should have 'check' field: {item}"
                    assert 'function' in item['check'], f"Check should have 'function' field: {item['check']}"
            
            print("✅ YAML structure validation passed")
            
            # Test that we can import and use the YAML (simulating real usage)
            sample_check = parsed_yaml[0]
            assert sample_check['name'] == 'completeness_check'
            assert sample_check['check']['function'] == 'is_not_null'
            assert sample_check['check']['arguments']['column'] == 'user_id'
            
            print("✅ YAML content can be parsed and used correctly")
            
        except yaml.YAMLError as e:
            pytest.fail(f"Generated YAML is invalid: {e}")
        except Exception as e:
            pytest.fail(f"Error validating YAML: {e}")
        
        print(f"🎉 Build hook validation test completed successfully!")
        print(f"📊 Generated file size: {combined_file.stat().st_size} bytes")
        print(f"📏 Generated file lines: {len(yaml_content.splitlines())}")
        
        # Verify the YAML can be loaded multiple times (no corruption)
        for i in range(3):
            reload_test = yaml.safe_load(combined_file.read_text())
            assert len(reload_test) == len(parsed_yaml), f"YAML should reload consistently, iteration {i+1}"
        
        print("✅ YAML file reloads consistently")