import yaml


class TestYamlExtraction:

    def test_basic_yaml_parsing(self):
        """Test basic YAML parsing functionality"""
        # Simple YAML content
        yaml_content = """
name: test_check
description: Basic test
type: simple
"""

        # Parse YAML
        parsed = yaml.safe_load(yaml_content)

        # Basic assertions
        assert parsed is not None
        assert parsed['name'] == 'test_check'
        assert parsed['description'] == 'Basic test'
        assert parsed['type'] == 'simple'

    def test_basic_yaml_list(self):
        """Test basic YAML list parsing"""
        yaml_content = """
- name: check1
  type: dataset
- name: check2  
  type: row
"""

        # Parse YAML
        parsed = yaml.safe_load(yaml_content)

        # Basic assertions
        assert isinstance(parsed, list)
        assert len(parsed) == 2
        assert parsed[0]['name'] == 'check1'
        assert parsed[1]['name'] == 'check2'

    def test_simple_string_operation(self):
        """Test simple string operations"""
        test_string = "quality_check_example"

        # Basic string assertions
        assert len(test_string) > 0
        assert 'quality' in test_string
        assert test_string.endswith('example')
        assert test_string.startswith('quality')
