import abc
import inspect
import logging
from enum import Enum
from dataclasses import dataclass, field
import functools as ft
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from pyspark.sql import Column
import pyspark.sql.functions as F
from databricks.labs.dqx.utils import get_column_as_string

logger = logging.getLogger(__name__)


CHECK_FUNC_REGISTRY: dict[str, str] = {}


def register_rule(rule_type: str) -> Callable:
    def wrapper(func: Callable) -> Callable:
        CHECK_FUNC_REGISTRY[func.__name__] = rule_type
        return func

    return wrapper


class Criticality(Enum):
    """Enum class to represent criticality of the check."""

    WARN = "warn"
    ERROR = "error"


class DefaultColumnNames(Enum):
    """Enum class to represent columns in the dataframe that will be used for error and warning reporting."""

    ERRORS = "_errors"
    WARNINGS = "_warnings"


class ColumnArguments(Enum):
    """Enum class that is used as input parsing for custom column naming."""

    ERRORS = "errors"
    WARNINGS = "warnings"


@dataclass(frozen=True)
class ExtraParams:
    """Class to represent extra parameters for DQEngine."""

    result_column_names: dict[str, str] = field(default_factory=dict)
    run_time: datetime = datetime.now()
    user_metadata: dict[str, str] = field(default_factory=dict)


class SingleColumnMixin:
    """
    Mixin to handle column-related functionalities.
    """

    def _get_column_as_string_expr(self, column: str | Column) -> Column:
        """Spark Column expression representing the column(s) as a string (not normalized).
        :return: A Spark Column object representing the column(s) as a string (not normalized).
        """
        return F.array(F.lit(get_column_as_string(column)))

    def _build_column_args(self, column: str | Column | None, valid_params: Iterable[str]) -> list:
        """
        Builds positional args list for single column if accepted.
        'column' can also be provided via args and kwargs.
        Therefore, we can only perform basic validation here.
        """
        if column is not None and "column" in valid_params:
            return [column]
        return []


class MultipleColumnsMixin:
    """
    Mixin to handle columns-related functionalities.
    """

    def _get_columns_as_string_expr(self, columns: list[str | Column]) -> Column:
        """Spark Column expression representing the column(s) as a string (not normalized).
        :return: A Spark Column object representing the column(s) as a string (not normalized).
        """
        return F.array(*[F.lit(get_column_as_string(column)) for column in columns])

    def _build_columns_args(self, columns: list[str | Column] | None, valid_params: Iterable[str]) -> list:
        """
        Builds positional args list for columns if accepted.
        'columns' can also be provided via args and kwargs.
        Therefore, we can only perform basic validation here.
        """
        if columns is not None and "columns" in valid_params:
            if not columns:
                raise ValueError("'columns' cannot be empty.")
            for col in columns:
                if col is None:
                    raise ValueError("'columns' list contains a None element.")
            return [columns]
        return []


class DQRuleTypeMixin:

    _expected_rule_type: str  # to be defined in subclasses
    _alternative_rules: list[str]  # e.g., "DQRowRule" or "DQDatasetRule"

    def _validate_rule_type(self, check_func: Callable) -> None:
        """
        Validate that the given check function is registered as the expected rule type.

        In this context:
        - The **_expected_rule_type** indicates the type the check function must be registered as
        (e.g., "raw", "dataset").
        - If the check function is registered with a different type, a ValueError is raised.
        - The error message advises to use the provided **_alternative_rules** (e.g., "DQRowRule", "DQDatasetRule").

        Args:
            check_func: The function name to validate.

        Raises:
            ValueError: If the check function exists in the registry but is not of the expected rule type.
        """
        rule_type = self._determine_rule_type(check_func)
        alternative_rules = " or ".join(self._alternative_rules)

        # skip validation if rule cannot be determined to leave room for custom rules without annotation
        if rule_type and rule_type != self._expected_rule_type:
            raise ValueError(
                f"Function '{check_func.__name__}' is not a {self._expected_rule_type}-level rule. "
                f"Use {alternative_rules} instead."
            )

    def _determine_rule_type(self, check_func: Callable) -> str | None:
        """Determine the rule type registered for the check function."""
        return CHECK_FUNC_REGISTRY.get(check_func.__name__, None)  # default to None


@dataclass(frozen=True)
class DQRule(abc.ABC, DQRuleTypeMixin, SingleColumnMixin, MultipleColumnsMixin):
    """Represents a data quality rule that applies a quality check function to column(s) or
    column expression(s). This class includes the following attributes:
    * `check_func` - The function used to perform the quality check.
    * `name` (optional) - A custom name for the check; autogenerated if not provided.
    * `criticality` (optional) - Defines the severity level of the check:
        - `error`: Critical issues.
        - `warn`: Potential issues.
    * `column` (optional) - A single column to which the check function is applied.
    * `columns` (optional) - A list of columns to which the check function is applied.
    * `filter` (optional) - A filter expression to apply the check only to rows meeting specific conditions.
    * `check_func_args` (optional) - Positional arguments for the check function (excluding `column`).
    * `check_func_kwargs` (optional) - Keyword arguments for the check function (excluding `column`).
    * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
    """

    check_func: Callable
    name: str = ""
    criticality: str = Criticality.ERROR.value
    column: str | Column | None = None
    columns: list[str | Column] | None = None  # some checks require list of columns instead of column
    filter: str | None = None
    check_func_args: list[Any] = field(default_factory=list)
    check_func_kwargs: dict[str, Any] = field(default_factory=dict)
    user_metadata: dict[str, str] | None = None

    def __post_init__(self):
        self._validate_rule_type(self.check_func)
        self._initialize_column_if_missing()
        self._initialize_columns_if_missing()
        self._validate_attributes()
        check_condition = self.get_check_condition()
        self._initialize_name_if_missing(check_condition)

    def _initialize_column_if_missing(self):
        """Handle scenarios where 'column' is provided in check_func_kwargs but not as an attribute."""
        if "column" in self.check_func_kwargs:
            if self.column is None:
                object.__setattr__(self, "column", self.check_func_kwargs.get("column"))

    def _initialize_columns_if_missing(self):
        """Handle scenarios where 'columns' is provided in check_func_kwargs but not as an attribute."""
        if "columns" in self.check_func_kwargs:
            if self.columns is None:
                object.__setattr__(self, "columns", self.check_func_kwargs.get("columns"))

    def _initialize_name_if_missing(self, check_condition: Column):
        """If name not provided directly, update it based on the condition."""
        if not self.name:
            normalized_name = get_column_as_string(check_condition, normalize=True)
            object.__setattr__(self, "name", normalized_name)

    def _validate_attributes(self) -> None:
        """Validate input attributes."""
        if self.column is not None and self.columns is not None:
            raise ValueError("Both 'column' and 'columns' cannot be provided at the same time.")

    @abc.abstractmethod
    def get_check_condition(self) -> Column:
        """
        Compute the check condition for the rule.

        :return: The Spark Column representing the check condition.
        """

    @ft.cached_property
    def check_criticality(self) -> str:
        """Criticality of the check.

        :return: string describing criticality - `warn` or `error`.
        :raises ValueError: if criticality is invalid.
        """
        criticality = self.criticality
        if criticality not in {Criticality.WARN.value, Criticality.ERROR.value}:
            raise ValueError(
                f"Invalid 'criticality' value: '{criticality}'. "
                f"Expected '{Criticality.WARN.value}' or '{Criticality.ERROR.value}'. "
                f"Check details: {self.name}"
            )
        return criticality

    @ft.cached_property
    def columns_as_string_expr(self) -> Column:
        """Spark Column expression representing the column(s) as a string (not normalized).
        :return: A Spark Column object representing the column(s) as a string (not normalized).
        """
        if self.column is not None:
            return self._get_column_as_string_expr(self.column)
        if self.columns is not None:
            return self._get_columns_as_string_expr(self.columns)
        return F.lit(None).cast("array<string>")

    def prepare_check_func_args_and_kwargs(self) -> tuple[list, dict]:
        """
        Prepares positional arguments and keyword arguments for the check function.
        Includes only arguments supported by the check function and skips empty values.
        """
        sig = inspect.signature(self.check_func)

        args = self._build_args(sig)
        kwargs = self._build_kwargs(sig)

        return args, kwargs

    def _build_args(self, sig: inspect.Signature) -> list:
        """
        Builds the list of positional arguments for the check function.
        Include column and columns in the args if they are provided but not optional.
        """
        args: list[Any] = []

        if not self._is_optional_argument(sig, "column"):
            args += self._build_column_args(self.column, sig.parameters)

        if not self._is_optional_argument(sig, "columns"):
            args += self._build_columns_args(self.columns, sig.parameters)

        args += self.check_func_args
        return args

    def _build_kwargs(self, sig: inspect.Signature) -> dict:
        """
        Builds the dictionary of keyword arguments for the check function.
        Include column and columns in the kwargs if they are provided but optional.
        """
        kwargs = dict(self.check_func_kwargs)  # Copy to avoid side effects

        if self._is_optional_argument(sig, "column"):
            if self.column is not None:
                kwargs["column"] = self.column
        else:
            kwargs.pop("column", None)  # Ensure required args aren't duplicated in kwargs

        if self._is_optional_argument(sig, "columns"):
            if self.columns is not None:
                kwargs["columns"] = self.columns
        else:
            kwargs.pop("columns", None)  # Ensure required args aren't duplicated in kwargs

        # Push down filter if supported
        if self.filter and "row_filter" in sig.parameters:
            kwargs["row_filter"] = self.filter

        return kwargs

    def _is_optional_argument(self, signature: inspect.Signature, arg_name: str):
        """Returns True if the argument exists and is optional, False if required, None if not present."""
        param = signature.parameters.get(arg_name)

        if param is None:
            return None  # Argument not present
        return param.default is not inspect.Parameter.empty


@dataclass(frozen=True)
class DQRowRule(DQRule):
    """
    Represents a row-level data quality rule that applies a quality check function to a column or column expression.
    Works with check functions that take a single column or no column as input.
    """

    _expected_rule_type: str = "row"
    _alternative_rules: list[str] = field(default_factory=lambda: ["DQDatasetRule"])

    def get_check_condition(self) -> Column:
        """
        Compute the check condition for this rule.

        :return: The Spark Column representing the check condition.
        """
        check_condition = self.check  # lazy evaluation of check function parameters
        return check_condition

    @ft.cached_property
    def check(self) -> Column:
        args, kwargs = self.prepare_check_func_args_and_kwargs()
        condition = self.check_func(*args, **kwargs)
        return condition


@dataclass(frozen=True)
class DQDatasetRule(DQRule):
    """
    Represents a dataset-level data quality rule that applies a quality check function to a column or
    column expression or list of columns depending on the check function.
    Either column or columns can be provided but not both. The rules are applied to the entire dataset or group of rows
    rather than individual rows. Failed checks are appended to the result columns in the same way as row-level rules.
    """

    _expected_rule_type: str = "dataset"
    _alternative_rules: list[str] = field(default_factory=lambda: ["DQRowRule"])

    def get_check_condition(self) -> Column:
        """
        Compute the check condition for this rule.

        :return: The Spark Column representing the check condition.
        """
        check_condition, _ = self.check  # lazy evaluation of check function parameters
        return check_condition

    @ft.cached_property
    def check(self) -> tuple[Column, Callable]:
        args, kwargs = self.prepare_check_func_args_and_kwargs()
        condition, apply_func = self.check_func(*args, **kwargs)
        return condition, apply_func


@dataclass(frozen=True)
class DQForEachColRule(DQRuleTypeMixin):
    """Represents a data quality rule that applies to a quality check function
    repeatedly on each specified column of the provided list of columns.
    This class includes the following attributes:
    * `columns` - A list of column names or expressions to which the check function should be applied.
    * `check_func` - The function used to perform the quality check.
    * `name` (optional) - A custom name for the check; autogenerated if not provided.
    * `criticality` - The severity level of the check:
        - `'warn'` for potential issues.
        - `'error'` for critical issues.
    * `filter` (optional) - A filter expression to apply the check only to rows meeting specific conditions.
    * `check_func_args` (optional) - Positional arguments for the check function (excluding column names).
    * `check_func_kwargs` (optional) - Keyword arguments for the check function (excluding column names).
    * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
    """

    columns: list[str | Column | list[str | Column]]
    check_func: Callable
    name: str = ""
    criticality: str = Criticality.ERROR.value
    filter: str | None = None
    check_func_args: list[Any] = field(default_factory=list)
    check_func_kwargs: dict[str, Any] = field(default_factory=dict)
    user_metadata: dict[str, str] | None = None

    def get_rules(self) -> list[DQRule]:
        """Build a list of rules for a set of columns.

        :return: list of dq rules
        """
        rules: list[DQRule] = []
        for column in self.columns:
            rule_type = self._determine_rule_type(self.check_func)
            effective_column = column if not isinstance(column, list) else None
            effective_columns = column if isinstance(column, list) else None

            if rule_type == "dataset":  # user must register dataset-level rules
                rules.append(
                    DQDatasetRule(
                        column=effective_column,
                        columns=effective_columns,
                        check_func=self.check_func,
                        check_func_kwargs=self.check_func_kwargs,
                        check_func_args=self.check_func_args,
                        name=self.name,
                        criticality=self.criticality,
                        filter=self.filter,
                        user_metadata=self.user_metadata,
                    )
                )
            else:  # default to row-level rule
                rules.append(
                    DQRowRule(
                        column=effective_column,
                        columns=effective_columns,
                        check_func=self.check_func,
                        check_func_kwargs=self.check_func_kwargs,
                        check_func_args=self.check_func_args,
                        name=self.name,
                        criticality=self.criticality,
                        filter=self.filter,
                        user_metadata=self.user_metadata,
                    )
                )
        return rules


@dataclass(frozen=True)
class ChecksValidationStatus:
    """Class to represent the validation status."""

    _errors: list[str] = field(default_factory=list)

    def add_error(self, error: str):
        """Add an error to the validation status."""
        self._errors.append(error)

    def add_errors(self, errors: list[str]):
        """Add an error to the validation status."""
        self._errors.extend(errors)

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors in the validation status."""
        return bool(self._errors)

    @property
    def errors(self) -> list[str]:
        """Get the list of errors in the validation status."""
        return self._errors

    def to_string(self) -> str:
        """Convert the validation status to a string."""
        if self.has_errors:
            return "\n".join(self._errors)
        return "No errors found"

    def __str__(self) -> str:
        """String representation of the ValidationStatus class."""
        return self.to_string()
