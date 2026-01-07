"""Unit tests for SDP converter."""

import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import (
    is_in_list,
    is_in_range,
    is_not_empty,
    is_not_null,
    regex_match,
    sql_expression,
)
from databricks.labs.dqx.rule import Criticality, DQRowRule
from databricks.labs.dqx.sdp.converter import SDPMigrationConverter
from databricks.labs.dqx.sdp.function_mappers import _format_limit, map_to_sql


class TestFunctionMappers:
    """Test function mappers."""

    def test_map_is_not_null(self):
        """Test is_not_null mapper."""
        result = map_to_sql("is_not_null", "col1")
        assert result == "col1 IS NOT NULL"

    def test_map_is_not_empty(self):
        """Test is_not_empty mapper."""
        result = map_to_sql("is_not_empty", "col1")
        assert result == "col1 IS NOT NULL AND col1 <> ''"

    def test_map_is_not_null_and_not_empty(self):
        """Test is_not_null_and_not_empty mapper."""
        result = map_to_sql("is_not_null_and_not_empty", "col1", trim_strings=False)
        assert result == "col1 IS NOT NULL AND col1 <> ''"

    def test_map_is_not_null_and_not_empty_with_trim(self):
        """Test is_not_null_and_not_empty mapper with trim."""
        result = map_to_sql("is_not_null_and_not_empty", "col1", trim_strings=True)
        assert result == "col1 IS NOT NULL AND trim(col1) <> ''"

    def test_map_is_in_list(self):
        """Test is_in_list mapper."""
        result = map_to_sql("is_in_list", "col1", allowed=[1, 2, 3])
        assert result == "col1 IN ('1', '2', '3')"

    def test_map_is_in_list_case_insensitive(self):
        """Test is_in_list mapper with case insensitive."""
        result = map_to_sql("is_in_list", "col1", allowed=["A", "B"], case_sensitive=False)
        assert "lower" in result.lower()

    def test_map_is_not_in_list(self):
        """Test is_not_in_list mapper."""
        result = map_to_sql("is_not_in_list", "col1", forbidden=[100, 200])
        assert result == "col1 NOT IN ('100', '200')"

    def test_map_is_in_range(self):
        """Test is_in_range mapper."""
        result = map_to_sql("is_in_range", "col1", min_limit=1, max_limit=10)
        assert result == "col1 >= 1 AND col1 <= 10"

    def test_map_is_in_range_min_only(self):
        """Test is_in_range mapper with min only."""
        result = map_to_sql("is_in_range", "col1", min_limit=1)
        assert result == "col1 >= 1"

    def test_map_is_in_range_max_only(self):
        """Test is_in_range mapper with max only."""
        result = map_to_sql("is_in_range", "col1", max_limit=10)
        assert result == "col1 <= 10"

    def test_map_is_in_range_with_column_reference(self):
        """Test is_in_range mapper with column reference."""
        result = map_to_sql("is_in_range", "col1", min_limit="col2", max_limit="col2 * 2")
        assert result == "col1 >= col2 AND col1 <= col2 * 2"

    def test_map_is_not_in_range(self):
        """Test is_not_in_range mapper."""
        result = map_to_sql("is_not_in_range", "col1", min_limit=11, max_limit=20)
        assert result == "col1 < 11 OR col1 > 20"

    def test_map_is_equal_to(self):
        """Test is_equal_to mapper."""
        result = map_to_sql("is_equal_to", "col1", value=2)
        assert result == "col1 = 2"

    def test_map_is_equal_to_with_column(self):
        """Test is_equal_to mapper with column reference."""
        result = map_to_sql("is_equal_to", "col1", value="col2")
        assert result == "col1 = col2"

    def test_map_is_not_equal_to(self):
        """Test is_not_equal_to mapper."""
        result = map_to_sql("is_not_equal_to", "col1", value="'unknown'")
        assert "'unknown'" in result

    def test_map_is_not_less_than(self):
        """Test is_not_less_than mapper."""
        result = map_to_sql("is_not_less_than", "col1", limit=0)
        assert result == "col1 >= 0"

    def test_map_is_not_greater_than(self):
        """Test is_not_greater_than mapper."""
        result = map_to_sql("is_not_greater_than", "col1", limit=10)
        assert result == "col1 <= 10"

    def test_map_regex_match(self):
        """Test regex_match mapper."""
        result = map_to_sql("regex_match", "col1", regex="[0-9]+")
        assert "RLIKE" in result
        assert "[0-9]+" in result

    def test_map_regex_match_negate(self):
        """Test regex_match mapper with negate."""
        result = map_to_sql("regex_match", "col1", regex="[0-9]+", negate=True)
        assert "NOT" in result

    def test_map_sql_expression(self):
        """Test sql_expression mapper."""
        result = map_to_sql("sql_expression", "col1", expression="col1 >= col2")
        assert result == "col1 >= col2"

    def test_map_sql_expression_negate(self):
        """Test sql_expression mapper with negate."""
        result = map_to_sql("sql_expression", "col1", expression="col1 >= col2", negate=True)
        assert result == "NOT (col1 >= col2)"

    def test_map_unsupported_function(self):
        """Test unsupported function returns None."""
        result = map_to_sql("unsupported_function", "col1")
        assert result is None

    def test_format_limit_literal(self):
        """Test _format_limit with literal value."""
        result = _format_limit(42)
        assert result == "42"

    def test_format_limit_string_column(self):
        """Test _format_limit with column name."""
        result = _format_limit("col2")
        assert result == "col2"

    def test_format_limit_expression(self):
        """Test _format_limit with expression."""
        result = _format_limit("col2 * 2")
        assert result == "col2 * 2"


class TestSDPMigrationConverter:
    """Test SDPMigrationConverter class."""

    def test_convert_rule_is_not_null(self):
        """Test converting is_not_null rule."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1")
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert result.expression == "col1 IS NOT NULL"
        assert "col1" in result.name
        # Name should contain either the full function name or a normalized version
        assert "is_not_null" in result.name or "is_null" in result.name or "not_null" in result.name

    def test_convert_rule_with_name(self):
        """Test converting rule with custom name."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1", name="custom_check_name")
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert result.name == "custom_check_name"

    def test_convert_rule_with_filter(self):
        """Test converting rule with filter."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1", filter="col2 > 0")
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert "(col1 IS NOT NULL) AND (col2 > 0)" == result.expression

    def test_convert_rule_is_in_range(self):
        """Test converting is_in_range rule."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_in_range, column="col1", check_func_kwargs={"min_limit": 1, "max_limit": 10})
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert "col1 >= 1 AND col1 <= 10" == result.expression

    def test_convert_rule_is_in_list(self):
        """Test converting is_in_list rule."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_in_list, column="col1", check_func_kwargs={"allowed": [1, 2, 3]})
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert "IN" in result.expression

    def test_convert_rule_unsupported(self):
        """Test converting unsupported rule."""
        converter = SDPMigrationConverter()

        # Create a function that's not registered in the mapper
        # but returns a valid Column so DQRowRule can be created
        def unsupported_func(column):
            """A function that's not in the mapper registry."""
            # Return a valid Column so DQRowRule validation passes
            if isinstance(column, str):
                return F.col(column)
            return column

        # Set the __name__ attribute to something not in the mapper registry
        unsupported_func.__name__ = "unsupported_function_xyz123"

        # Create a rule with this function
        rule = DQRowRule(check_func=unsupported_func, column="col1")
        result = converter.convert_rule(rule)

        assert result.supported is False
        assert "not supported" in result.error_message.lower()

    def test_convert_checks_multiple(self):
        """Test converting multiple checks."""
        converter = SDPMigrationConverter()
        rules = [
            DQRowRule(check_func=is_not_null, column="col1"),
            DQRowRule(check_func=is_not_empty, column="col2"),
        ]
        results = converter.convert_checks(rules)

        assert len(results) == 2
        assert all(r.supported for r in results)

    def test_to_python_decorator_warn(self):
        """Test Python decorator output generation for warn criticality."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1", criticality=Criticality.WARN.value)
        result = converter.convert_rule(rule)
        python_code = converter.to_python_decorator([result])

        assert "@dp.expect" in python_code
        assert "@dp.expect_or_fail" not in python_code
        assert result.name in python_code
        assert result.expression in python_code

    def test_to_python_decorator_error(self):
        """Test Python decorator output generation for error criticality."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1", criticality=Criticality.ERROR.value)
        result = converter.convert_rule(rule)
        python_code = converter.to_python_decorator([result])

        assert "@dp.expect_or_fail" in python_code
        # The output should not contain the old @dp.expect decorator
        # Split by lines to check each decorator separately
        lines = python_code.split("\n")
        for line in lines:
            if line.strip():  # Skip empty lines
                assert "@dp.expect_or_fail" in line or "@dp.expect" not in line
        assert result.name in python_code
        assert result.expression in python_code

    def test_to_sql_constraints_warn(self):
        """Test SQL CONSTRAINT output generation for warn criticality."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1", criticality=Criticality.WARN.value)
        result = converter.convert_rule(rule)
        sql_statements = converter.to_sql_constraints([result])

        assert len(sql_statements) == 1
        assert "CONSTRAINT" in sql_statements[0]
        assert "EXPECT" in sql_statements[0]
        assert "ON VIOLATION FAIL UPDATE" not in sql_statements[0]
        assert result.name in sql_statements[0]
        assert result.expression in sql_statements[0]

    def test_to_sql_constraints_error(self):
        """Test SQL CONSTRAINT output generation for error criticality."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1", criticality=Criticality.ERROR.value)
        result = converter.convert_rule(rule)
        sql_statements = converter.to_sql_constraints([result])

        assert len(sql_statements) == 1
        assert "CONSTRAINT" in sql_statements[0]
        assert "EXPECT" in sql_statements[0]
        assert "ON VIOLATION FAIL UPDATE" in sql_statements[0]
        assert result.name in sql_statements[0]
        assert result.expression in sql_statements[0]

    def test_name_sanitization(self):
        """Test name sanitization."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col-1.test", name="check-name with spaces!")
        result = converter.convert_rule(rule)

        # Name should be sanitized
        assert " " not in result.name
        assert "-" not in result.name or result.name.replace("_", "").replace("-", "").isalnum()

    def test_name_generation_from_column(self):
        """Test name generation from column and function."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(check_func=is_not_null, column="col1")
        result = converter.convert_rule(rule)

        # Name should contain column and function
        assert "col1" in result.name or "is_not_null" in result.name

    def test_sql_expression_with_negate(self):
        """Test sql_expression with negate."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(
            check_func=sql_expression,
            check_func_kwargs={"expression": "col1 > 0", "negate": True},
        )
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert "NOT" in result.expression

    def test_regex_match_with_negate(self):
        """Test regex_match with negate."""
        converter = SDPMigrationConverter()
        rule = DQRowRule(
            check_func=regex_match,
            column="col1",
            check_func_kwargs={"regex": "[0-9]+", "negate": True},
        )
        result = converter.convert_rule(rule)

        assert result.supported is True
        assert "NOT" in result.expression
