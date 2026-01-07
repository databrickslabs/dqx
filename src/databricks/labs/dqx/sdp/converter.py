import logging
import re
from pathlib import Path
from typing import Any

import yaml

from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.rule import Criticality, DQRowRule
from databricks.labs.dqx.sdp.function_mappers import map_to_sql
from databricks.labs.dqx.sdp.models import SDPMigrationResult
from databricks.labs.dqx.utils import get_column_name_or_alias, normalize_col_str

logger = logging.getLogger(__name__)

__name_sanitize_re__ = re.compile(r"[^a-zA-Z0-9_]+")


class SDPMigrationConverter:
    """Converter for migrating DQX checks to SDP Expectations format.

    This class provides methods to convert DQX quality checks (DQRowRule objects or YAML files)
    into Spark Declarative Pipeline (SDP) Expectations format. The converter supports:

    - Converting individual DQRowRule objects
    - Converting YAML files containing check definitions
    - Generating Python decorator format (@dp.expect)
    - Generating SQL CONSTRAINT format (CONSTRAINT ... EXPECT)
    - Handling filters by incorporating them into expectation expressions
    - Automatic name generation and sanitization

    Supported Check Functions:
        The converter supports a subset of row-level check functions including:
        - is_not_null
        - is_not_empty
        - is_not_null_and_not_empty
        - is_in_list
        - is_not_in_list
        - is_in_range
        - is_not_in_range
        - is_equal_to
        - is_not_equal_to
        - is_not_less_than
        - is_not_greater_than
        - regex_match
        - sql_expression

        Unsupported functions will return a result with `supported=False` and an error message.

    Note:
        Only DQRowRule objects are supported. DQDatasetRule and other rule types are not
        currently supported for SDP conversion.
    """

    def convert_rule(self, rule: DQRowRule) -> SDPMigrationResult:
        """Convert a single DQRowRule to SDP Expectation format.

        This method converts a DQRowRule object to an SDP Expectation format. It extracts
        the check function name, column information, and arguments, then maps them to a
        SQL expression suitable for SDP Expectations.

        If the rule has a filter, it will be incorporated into the expectation expression
        using AND logic: (check_condition) AND (filter).

        Args:
            rule: The DQRowRule to convert. Must be a row-level rule with exactly one column.

        Returns:
            SDPMigrationResult containing:
                - name: Sanitized expectation name
                - expression: SQL expression for the expectation
                - original_rule: The original DQRowRule that was converted
                - supported: Whether the conversion was successful
                - error_message: Error message if conversion failed
        """
        try:
            func_name = rule.check_func.__name__
            column = self._extract_column(rule)
            kwargs = rule.check_func_kwargs.copy()

            sql_expr = map_to_sql(func_name, column, **kwargs)
            if sql_expr is None:
                return SDPMigrationResult(
                    name="",
                    expression="",
                    original_rule=rule,
                    supported=False,
                    error_message=f"Check function '{func_name}' is not supported for SDP conversion",
                )

            if rule.filter:
                sql_expr = f"({sql_expr}) AND ({rule.filter})"

            name = self._generate_name(rule, func_name, column)

            return SDPMigrationResult(
                name=name,
                expression=sql_expr,
                original_rule=rule,
                supported=True,
                error_message=None,
            )
        except Exception as e:
            logger.warning(f"Failed to convert rule: {e}")
            return SDPMigrationResult(
                name="",
                expression="",
                original_rule=rule,
                supported=False,
                error_message=str(e),
            )

    def convert_checks(self, checks: list[DQRowRule]) -> list[SDPMigrationResult]:
        """Convert multiple DQRowRule objects to SDP Expectation format.

        This method processes a list of DQRowRule objects and converts each one to
        SDP Expectation format. Non-DQRowRule objects in the list will be skipped
        with a warning and will have `supported=False` in the result.

        Args:
            checks: List of DQRowRule objects to convert. Non-DQRowRule objects
                will be skipped with a warning.

        Returns:
            List of SDPMigrationResult objects, one for each input check.
        """
        results = []
        for check in checks:
            if not isinstance(check, DQRowRule):
                logger.warning(f"Skipping non-DQRowRule check: {type(check)}")
                results.append(
                    SDPMigrationResult(
                        name="",
                        expression="",
                        original_rule=check,
                        supported=False,
                        error_message=f"Only DQRowRule is supported, got {type(check)}",
                    )
                )
                continue
            results.append(self.convert_rule(check))
        return results

    def convert_yaml(self, yaml_path: str | Path) -> list[SDPMigrationResult]:
        """Convert checks from a YAML file to SDP Expectation format.

        This method loads checks from a YAML file, deserializes them to DQRowRule objects,
        and converts them to SDP Expectation format. Only row-level rules (DQRowRule)
        are converted; dataset-level rules are skipped with a warning.

        The YAML file should follow the standard DQX check definition format.

        Args:
            yaml_path: Path to the YAML file containing check definitions. Can be a
                string or Path object.

        Returns:
            List of SDPMigrationResult objects, one for each row-level check in the file.

        Raises:
            FileNotFoundError: If the YAML file does not exist.
        """
        path = Path(yaml_path)
        if not path.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        checks = deserialize_checks(self._load_yaml_file(path))
        row_rules = [rule for rule in checks if isinstance(rule, DQRowRule)]

        if len(row_rules) != len(checks):
            logger.warning(f"Some checks were not DQRowRule and were skipped: {len(checks) - len(row_rules)}")

        return self.convert_checks(row_rules)

    def convert_yaml_to_python(self, yaml_path: str | Path) -> str:
        """Convert YAML file to Python decorator format.

        This is a convenience method that loads checks from a YAML file and generates
        Python code with @dp.expect decorators suitable for use in Spark Declarative
        Pipeline code.

        Args:
            yaml_path: Path to the YAML file containing check definitions.

        Returns:
            Python code string with @dp.expect decorators. Each line contains one
            decorator. Unsupported checks are skipped with a warning.
        """
        results = self.convert_yaml(yaml_path)
        return self.to_python_decorator(results)

    def convert_yaml_to_sql(self, yaml_path: str | Path) -> list[str]:
        """Convert YAML file to SQL CONSTRAINT format.

        This is a convenience method that loads checks from a YAML file and generates
        SQL CONSTRAINT statements suitable for use in Spark Declarative Pipeline SQL
        code.

        Args:
            yaml_path: Path to the YAML file containing check definitions.

        Returns:
            List of SQL CONSTRAINT statements. Each statement follows the format:
            ``CONSTRAINT name EXPECT (expression)``. Unsupported checks are skipped
            with a warning.
        """
        results = self.convert_yaml(yaml_path)
        return self.to_sql_constraints(results)

    def to_python_decorator(self, results: list[SDPMigrationResult]) -> str:
        """Generate Python decorator code from conversion results.

        This method takes a list of SDPMigrationResult objects and generates Python
        code with @dp.expect or @dp.expect_or_fail decorators based on criticality.
        Only results with `supported=True` are included in the output.

        - For "warn" criticality: uses @dp.expect
        - For "error" criticality: uses @dp.expect_or_fail

        Args:
            results: List of SDPMigrationResult objects from previous conversions.

        Returns:
            Python code string with @dp.expect or @dp.expect_or_fail decorators,
            one per line. Unsupported checks are automatically skipped.
        """
        lines = []
        for result in results:
            if not result.supported:
                logger.warning(f"Skipping unsupported check: {result.error_message}")
                continue

            # Determine decorator based on criticality
            criticality = self._get_criticality(result)
            if criticality == Criticality.ERROR.value:
                decorator = f'@dp.expect_or_fail("{result.name}", "{result.expression}")'
            else:
                # Default to @dp.expect for "warn" or unknown criticality
                decorator = f'@dp.expect("{result.name}", "{result.expression}")'

            lines.append(decorator)
        return "\n".join(lines)

    def to_sql_constraints(self, results: list[SDPMigrationResult]) -> list[str]:
        """Generate SQL CONSTRAINT statements from conversion results.

        This method takes a list of SDPMigrationResult objects and generates SQL
        CONSTRAINT statements. Only results with `supported=True` are included in
        the output.

        - For "warn" criticality: uses ``CONSTRAINT name EXPECT (expression)``
        - For "error" criticality: uses ``CONSTRAINT name EXPECT (expression) ON VIOLATION FAIL UPDATE``

        Args:
            results: List of SDPMigrationResult objects from previous conversions.

        Returns:
            List of SQL CONSTRAINT statements. Unsupported checks are
            automatically skipped.
        """
        constraints = []
        for result in results:
            if not result.supported:
                logger.warning(f"Skipping unsupported check: {result.error_message}")
                continue

            # Determine SQL syntax based on criticality
            criticality = self._get_criticality(result)
            if criticality == Criticality.ERROR.value:
                constraint = f'CONSTRAINT {result.name} EXPECT ({result.expression}) ON VIOLATION FAIL UPDATE'
            else:
                # Default to standard EXPECT for "warn" or unknown criticality
                constraint = f'CONSTRAINT {result.name} EXPECT ({result.expression})'

            constraints.append(constraint)
        return constraints

    def _extract_column(self, rule: DQRowRule) -> str:
        """Extract column name or expression from a DQRowRule.

        Args:
            rule: The DQRowRule to extract column from.

        Returns:
            Column name or expression as string. Returns empty string for functions
            that don't require a column (e.g., sql_expression).

        Raises:
            ValueError: If the rule requires a column but none is provided.
        """
        # sql_expression doesn't require a column - it uses the expression from kwargs
        if rule.check_func.__name__ == "sql_expression":
            return ""

        if rule.column is not None:
            if isinstance(rule.column, str):
                return rule.column
            return get_column_name_or_alias(rule.column, normalize=False, allow_simple_expressions_only=False)
        elif rule.columns is not None and len(rule.columns) == 1:
            col = rule.columns[0]
            if isinstance(col, str):
                return col
            return get_column_name_or_alias(col, normalize=False, allow_simple_expressions_only=False)
        else:
            raise ValueError("Rule must have exactly one column for SDP conversion")

    def _generate_name(self, rule: DQRowRule, func_name: str, column: str) -> str:
        """Generate a sanitized name for the expectation.

        Args:
            rule: The DQRowRule.
            func_name: Name of the check function.
            column: Column name or expression (may be empty for sql_expression).

        Returns:
            Sanitized expectation name.
        """
        if rule.name:
            name = rule.name
        else:
            if column:
                col_normalized = normalize_col_str(column.replace(".", "_").replace("(", "_").replace(")", "_"))
                name = f"{col_normalized}_{func_name}"
            else:
                # For functions without columns (e.g., sql_expression), use just the function name
                name = func_name

        sanitized = __name_sanitize_re__.sub("_", name)
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")

        if not sanitized:
            sanitized = f"expectation_{func_name}"

        return sanitized

    def _get_criticality(self, result: SDPMigrationResult) -> str:
        """Extract criticality from a migration result.

        Args:
            result: The SDPMigrationResult to extract criticality from.

        Returns:
            Criticality string ("warn" or "error"). Defaults to "error" if not found.
        """
        if isinstance(result.original_rule, DQRowRule):
            return result.original_rule.criticality
        # If original_rule is a dict (from YAML), try to extract criticality
        elif isinstance(result.original_rule, dict):
            return result.original_rule.get("criticality", Criticality.ERROR.value)
        # Default to error if we can't determine
        return Criticality.ERROR.value

    def _load_yaml_file(self, path: Path) -> list[dict[str, Any]]:
        """Load and parse a YAML file.

        Args:
            path: Path to the YAML file.

        Returns:
            List of check dictionaries.
        """

        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or []
