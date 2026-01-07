"""Integration tests for SDP converter."""

from pathlib import Path

import pytest

from databricks.labs.dqx.check_funcs import is_not_null, is_in_range
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.sdp.converter import SDPMigrationConverter


class TestSDPConverterIntegration:
    """Integration tests for SDP converter."""

    def test_convert_yaml_file(self, tmp_path: Path):
        """Test converting YAML file to SDP format."""
        # Create a temporary YAML file
        yaml_content = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1

- criticality: error
  check:
    function: is_in_range
    arguments:
      column: col2
      min_limit: 1
      max_limit: 10
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        results = converter.convert_yaml(yaml_file)

        assert len(results) == 2
        assert all(r.supported for r in results)
        assert any("IS NOT NULL" in r.expression for r in results)
        assert any(">=" in r.expression and "<=" in r.expression for r in results)

    def test_convert_yaml_to_python_error(self, tmp_path: Path):
        """Test converting YAML file to Python decorator format for error criticality."""
        yaml_content = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        python_code = converter.convert_yaml_to_python(yaml_file)

        assert "@dp.expect_or_fail" in python_code
        assert "@dp.expect" not in python_code
        assert "col1" in python_code
        assert "IS NOT NULL" in python_code

    def test_convert_yaml_to_python_warn(self, tmp_path: Path):
        """Test converting YAML file to Python decorator format for warn criticality."""
        yaml_content = """
- criticality: warn
  check:
    function: is_not_null
    arguments:
      column: col1
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        python_code = converter.convert_yaml_to_python(yaml_file)

        assert "@dp.expect" in python_code
        assert "@dp.expect_or_fail" not in python_code
        assert "col1" in python_code
        assert "IS NOT NULL" in python_code

    def test_convert_yaml_to_sql_error(self, tmp_path: Path):
        """Test converting YAML file to SQL CONSTRAINT format for error criticality."""
        yaml_content = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        sql_statements = converter.convert_yaml_to_sql(yaml_file)

        assert len(sql_statements) == 1
        assert "CONSTRAINT" in sql_statements[0]
        assert "EXPECT" in sql_statements[0]
        assert "ON VIOLATION FAIL UPDATE" in sql_statements[0]
        assert "col1" in sql_statements[0]

    def test_convert_yaml_to_sql_warn(self, tmp_path: Path):
        """Test converting YAML file to SQL CONSTRAINT format for warn criticality."""
        yaml_content = """
- criticality: warn
  check:
    function: is_not_null
    arguments:
      column: col1
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        sql_statements = converter.convert_yaml_to_sql(yaml_file)

        assert len(sql_statements) == 1
        assert "CONSTRAINT" in sql_statements[0]
        assert "EXPECT" in sql_statements[0]
        assert "ON VIOLATION FAIL UPDATE" not in sql_statements[0]
        assert "col1" in sql_statements[0]

    def test_convert_yaml_with_filter(self, tmp_path: Path):
        """Test converting YAML file with filter."""
        yaml_content = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1
  filter: col2 > 0
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        results = converter.convert_yaml(yaml_file)

        assert len(results) == 1
        assert results[0].supported is True
        assert "AND" in results[0].expression
        assert "col2 > 0" in results[0].expression

    def test_convert_yaml_with_custom_name(self, tmp_path: Path):
        """Test converting YAML file with custom name."""
        yaml_content = """
- criticality: error
  name: custom_check_name
  check:
    function: is_not_null
    arguments:
      column: col1
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        results = converter.convert_yaml(yaml_file)

        assert len(results) == 1
        assert results[0].name == "custom_check_name"

    def test_convert_yaml_file_not_found(self):
        """Test error handling for missing YAML file."""
        converter = SDPMigrationConverter()
        with pytest.raises(FileNotFoundError):
            converter.convert_yaml("nonexistent.yaml")

    def test_convert_checks_end_to_end(self):
        """Test end-to-end conversion workflow."""
        from databricks.labs.dqx.rule import Criticality

        converter = SDPMigrationConverter()

        # Create rules with different criticality levels
        rules = [
            DQRowRule(check_func=is_not_null, column="col1", criticality=Criticality.ERROR.value),
            DQRowRule(
                check_func=is_in_range,
                column="col2",
                check_func_kwargs={"min_limit": 1, "max_limit": 10},
                criticality=Criticality.WARN.value,
            ),
        ]

        # Convert to results
        results = converter.convert_checks(rules)
        assert len(results) == 2
        assert all(r.supported for r in results)

        # Generate Python output
        python_code = converter.to_python_decorator(results)
        assert "@dp.expect_or_fail" in python_code  # for error
        assert "@dp.expect" in python_code  # for warn
        assert len(python_code.split("\n")) == 2

        # Generate SQL output
        sql_statements = converter.to_sql_constraints(results)
        assert len(sql_statements) == 2
        assert all("CONSTRAINT" in stmt for stmt in sql_statements)
        # One should have ON VIOLATION FAIL UPDATE (error), one should not (warn)
        assert sum("ON VIOLATION FAIL UPDATE" in stmt for stmt in sql_statements) == 1

    def test_convert_yaml_with_multiple_checks(self, tmp_path: Path):
        """Test converting YAML file with multiple checks."""
        yaml_content = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1

- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col2

- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col3
"""
        yaml_file = tmp_path / "test_checks.yaml"
        yaml_file.write_text(yaml_content)

        converter = SDPMigrationConverter()
        results = converter.convert_yaml(yaml_file)

        assert len(results) == 3
        assert all(r.supported for r in results)

        # Check that all have different names
        names = [r.name for r in results]
        assert len(set(names)) == 3
