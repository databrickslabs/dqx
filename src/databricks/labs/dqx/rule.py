import abc
import hashlib
import inspect
import json
import logging
from enum import Enum
import functools as ft
from collections.abc import Callable, Iterable
from typing import Annotated, Any, ClassVar

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator
from pydantic.json_schema import WithJsonSchema
from pyspark.sql import Column
import pyspark.sql.functions as F
from databricks.labs.dqx.utils import get_column_name_or_alias, normalize_bound_args, SparkColumn
from databricks.labs.dqx.errors import InvalidCheckError, InvalidParameterError

logger = logging.getLogger(__name__)

__all__ = [
    "CHECK_FUNC_REGISTRY",
    "CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION",
    "compute_rule_fingerprint",
    "Criticality",
    "DQDatasetRule",
    "DQForEachColRule",
    "DQRule",
    "DQRowRule",
    "MultipleColumnsMixin",
    "SingleColumnMixin",
    "normalize_bound_args",
    "register_for_original_columns_preselection",
    "CHECK_FUNC_MIN_DBR_VERSION_ATTRIBUTE",
    "register_rule",
    "requires_dbr_version",
]

CHECK_FUNC_REGISTRY: dict[str, str] = {}
CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION: set[str] = set()

CHECK_FUNC_MIN_DBR_VERSION_ATTRIBUTE = "dqx_requires_dbr_version"


def register_rule(rule_type: str) -> Callable:
    def wrapper(func: Callable) -> Callable:
        CHECK_FUNC_REGISTRY[func.__name__] = rule_type
        return func

    return wrapper


def register_for_original_columns_preselection() -> Callable:
    def wrapper(func: Callable) -> Callable:
        CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION.add(func.__name__)
        return func

    return wrapper


def requires_dbr_version(version: str) -> Callable:
    """Annotates a check function with a minimum Databricks Runtime version requirement.

    Parses *version* into a *(major, minor)* tuple and sets it as the
    *dqx_requires_dbr_version* attribute on the decorated function. The engine reads
    this attribute before execution and raises an error if the current Databricks
    Runtime version is lower than the required one, failing the entire run.

    Example usage:

    ```python
    @requires_dbr_version("15.1")
    @register_rule("row")
    def my_check(column: str) -> Column:
        ...
    ```

    Args:
        version: The minimum required Databricks Runtime version in *"major.minor"* format (e.g., *"15.1"*).

    Returns:
        A decorator that annotates the function with the DBR version requirement.

    Raises:
        ValueError: If *version* is not a valid *"major.minor"* string with non-negative integer components.
    """
    parts = version.split(".")
    if len(parts) != 2:
        raise ValueError(f"DBR version must be in 'major.minor' format, got: '{version}'")

    try:
        major, minor = int(parts[0]), int(parts[1])
    except ValueError as e:
        raise ValueError(f"DBR version components must be integers, got: '{version}'") from e

    if major < 0 or minor < 0:
        raise ValueError(f"DBR version components must be non-negative, got: '{version}'")

    parsed: tuple[int, int] = (major, minor)

    def wrapper(func: Callable) -> Callable:
        setattr(func, CHECK_FUNC_MIN_DBR_VERSION_ATTRIBUTE, parsed)
        return func

    return wrapper


class Criticality(Enum):
    """Enum class to represent criticality of the check."""

    WARN = "warn"
    ERROR = "error"


class SingleColumnMixin:
    """
    Mixin to handle column-related functionalities.
    """

    def _get_column_as_string_expr(self, column: str | Column) -> Column:
        """Spark Column expression representing the column(s) as a string (not normalized).

        Returns:
            A Spark Column object representing the column(s) as a string (not normalized).
        """
        return F.array(F.lit(get_column_name_or_alias(column)))

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

        Returns:
            A Spark Column object representing the column(s) as a string (not normalized).
        """
        return F.array(*[F.lit(get_column_name_or_alias(column)) for column in columns])

    def _build_columns_args(self, columns: list[str | Column] | None, valid_params: Iterable[str]) -> list:
        """
        Builds positional args list for columns if accepted.
        'columns' can also be provided via args and kwargs.
        Therefore, we can only perform basic validation here.

        Raises:
            InvalidCheckError: If 'columns' is an empty list or contains None elements.
        """
        if columns is not None and "columns" in valid_params:
            if not columns:
                raise InvalidCheckError("'columns' cannot be empty.")
            for col in columns:
                if col is None:
                    raise InvalidCheckError("'columns' list contains a None element.")
            return [columns]
        return []


class DQRuleTypeMixin:
    _expected_rule_type: ClassVar[str]  # to be defined in subclasses
    _alternative_rules: ClassVar[list[str]]  # e.g., "DQRowRule" or "DQDatasetRule"

    def _validate_rule_type(self, check_func: Callable) -> None:
        """
        Validate that the given check function is registered as the expected rule type.

        In this context:
        - The **_expected_rule_type** indicates the type the check function must be registered as
        (e.g., "raw", "dataset").
        - If the check function is registered with a different type, an InvalidCheckError is raised.
        - The error message advises to use the provided **_alternative_rules** (e.g., "DQRowRule", "DQDatasetRule").

        Args:
            check_func: The function name to validate.

        Raises:
            InvalidCheckError: If the check function exists in the registry but is not of the expected rule type.
        """
        rule_type = self._determine_rule_type(check_func)
        alternative_rules = " or ".join(self._alternative_rules)

        # skip validation if rule cannot be determined to leave room for custom rules without annotation
        if rule_type and rule_type != self._expected_rule_type:
            raise InvalidCheckError(
                f"Function '{check_func.__name__}' is not a {self._expected_rule_type}-level rule. "
                f"Use {alternative_rules} instead."
            )

    def _determine_rule_type(self, check_func: Callable) -> str | None:
        """Determine the rule type registered for the check function."""
        return CHECK_FUNC_REGISTRY.get(check_func.__name__, None)  # default to None


class DQRule(BaseModel, abc.ABC, DQRuleTypeMixin, SingleColumnMixin, MultipleColumnsMixin):
    """Represents a data quality rule that applies a quality check function to column(s) or
    column expression(s). This class includes the following attributes:
    * *check_func* - The function used to perform the quality check.
    * *name* (optional) - A custom name for the check; autogenerated if not provided.
    * *criticality* (optional) - Defines the severity level of the check:
        - *error*: Critical issues.
        - *warn*: Potential issues.
    * *column* (optional) - A single column to which the check function is applied.
    * *columns* (optional) - A list of columns to which the check function is applied.
    * *filter* (optional) - A filter expression to apply the check only to rows meeting specific conditions.
    * *check_func_args* (optional) - Positional arguments for the check function (excluding *column*).
    * *check_func_kwargs* (optional) - Keyword arguments for the check function (excluding *column*).
    * *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
    * *message_expr* (optional) - User-defined expression used as the check failure message. Accepts either
        a Spark SQL expression string or a Spark *Column* expression. The expression is evaluated as-is.
        Any column references, casts, or rule-identifying literals must be supplied directly by the caller
        (e.g., ``F.concat(F.lit('age_positive: value '), F.col('age').cast('string'))`` or
        ``"concat('age_positive: value ', cast(age as string))"``). The same message is shared across all
        rules generated from a ``DQForEachColRule``.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    check_func: Annotated[Callable, WithJsonSchema({"type": "string"})]
    name: str = ""
    criticality: str = Criticality.ERROR.value
    column: str | SparkColumn | None = None
    columns: list[str | SparkColumn] | None = None  # some checks require list of columns instead of column
    filter: str | None = None
    check_func_args: list[Any] = []
    check_func_kwargs: dict[str, Any] = {}
    # Values are intentionally *Any*, not *str*: pre-migration dataclasses never enforced the type,
    # so released checks carry non-string values (e.g. {"confidence": 0.95}). Table backends persist
    # these faithfully, so narrowing to dict[str, str] would regress load of such checks.
    user_metadata: dict[str, Any] | None = None
    message_expr: str | SparkColumn | None = None

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise InvalidCheckError(str(exc)) from exc

    @model_validator(mode='before')
    @classmethod
    def _reject_none_columns(cls, data: Any) -> Any:
        """Raise the DQX-specific error for None elements in *columns* before Pydantic type validation.

        Pydantic would otherwise reject a None element with a generic ValidationError; callers expect
        InvalidCheckError. Runs before field coercion so the raw input list is inspected as provided.

        Args:
            data: Raw input passed to the model (a mapping of field values when constructed with keywords).

        Returns:
            The unmodified input data.

        Raises:
            InvalidCheckError: If *columns* is a list containing a None element.
        """
        if isinstance(data, dict):
            columns = data.get("columns")
            if isinstance(columns, list) and any(column is None for column in columns):
                raise InvalidCheckError("'columns' list contains a None element.")
        return data

    @model_validator(mode='before')
    @classmethod
    def _populate_columns_from_kwargs(cls, data: Any) -> Any:
        """Promote column/columns from check_func_kwargs into top-level fields when not set directly.

        Running this as a mode='before' validator means the promoted values go through the same
        SparkColumn/type coercion as if the caller had passed them explicitly, without needing
        object.__setattr__ on the frozen model.

        Args:
            data: Raw input mapping before field coercion.

        Returns:
            Possibly updated input mapping with column/columns set from check_func_kwargs.
        """
        if not isinstance(data, dict):
            return data
        check_func_kwargs: dict = data.get("check_func_kwargs") or {}
        if data.get("column") is None and "column" in check_func_kwargs:
            data = {**data, "column": check_func_kwargs["column"]}
        if data.get("columns") is None and "columns" in check_func_kwargs:
            data = {**data, "columns": check_func_kwargs["columns"]}
        return data

    @model_validator(mode='after')
    def _validate_and_initialize(self) -> 'DQRule':
        self._validate_rule_type(self.check_func)
        self._validate_attributes()
        check_condition = self.get_check_condition()
        self._initialize_name_if_missing(check_condition)
        if isinstance(self.message_expr, str):
            self._validate_message_expression(self.message_expr)
        return self

    @abc.abstractmethod
    def get_check_condition(self) -> Column:
        """
        Compute the check condition for the rule.

        Returns:
            The Spark Column representing the check condition.
        """

    @ft.cached_property
    def columns_as_string_expr(self) -> Column:
        """Spark Column expression representing the column(s) as a string (not normalized).

        Returns:
            A Spark Column object representing the column(s) as a string (not normalized).
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

    def replace(self, **changes: Any) -> "DQRule":
        """Return a new rule instance with the given field overrides.

        Unlike *model_copy*, this rebuilds the instance through the constructor so validation
        re-runs and *functools.cached_property* derived state (e.g. *rule_fingerprint*,
        *columns_as_string_expr*) is recomputed from the updated fields rather than copied stale.
        *model_copy(update=...)* shallow-copies the instance dict, which carries over the already
        cached values and would therefore ignore the updated fields.

        Note that regular fields are copied verbatim and only re-validated, not re-derived: an
        already-populated *name* is preserved as-is (it is not regenerated from a changed
        *column*/*columns*), and a *column*/*columns* already promoted from *check_func_kwargs*
        is not re-promoted. Current callers only override *check_func_kwargs*, for which this is
        the intended behaviour.

        Args:
            **changes: Field values to override on the new instance.

        Returns:
            A new, fully validated rule of the same concrete type.
        """
        fields = {name: getattr(self, name) for name in type(self).model_fields}
        fields.update(changes)
        return type(self)(**fields)

    @ft.cached_property
    def rule_fingerprint(self) -> str:
        """Compute a deterministic SHA-256 hash of a single rule definition.

        Returns:
            A hex-encoded SHA-256 hash string.
        """
        return compute_rule_fingerprint(self.to_dict())

    def to_dict(self) -> dict:
        """
        Converts a DQRule instance into a structured dictionary.
        """
        args, kwargs = self.prepare_check_func_args_and_kwargs()
        sig = inspect.signature(self.check_func)
        bound_args = sig.bind_partial(*args, **kwargs)
        # allow_simple_expressions_only=False: to_dict() is used for fingerprinting and metadata only,
        # not for round-trip serialization. Complex Column expressions (e.g. F.try_element_at(...))
        # are serialized as their string representation here. Round-trip storage uses
        # normalize_bound_args with the default allow_simple_expressions_only=True, which rejects
        # complex expressions — so a check with a complex Column arg can never be stored, and no
        # inconsistency between the fingerprint and the stored arguments can arise.
        full_args = {
            key: normalize_bound_args(val, allow_simple_expressions_only=False)
            for key, val in bound_args.arguments.items()
        }

        metadata = {
            "name": self.name,
            "criticality": self.criticality,
            "check": {
                "function": self.check_func.__name__,
                "arguments": full_args,
            },
        }
        if self.filter:
            metadata["filter"] = self.filter

        if self.user_metadata:
            metadata["user_metadata"] = self.user_metadata
        # Only string expressions can be round-tripped through metadata; Column objects are
        # in-process Spark expressions with no canonical YAML/JSON representation.
        if isinstance(self.message_expr, str):
            metadata["message_expr"] = self.message_expr
        elif self.message_expr is not None:
            logger.warning("Message expressions of type 'Column' cannot be serialized; falling back to default message")
        return metadata

    def _initialize_name_if_missing(self, check_condition: Column):
        """If name not provided directly, update it based on the condition."""
        if not self.name:
            normalized_name = get_column_name_or_alias(check_condition, normalize=True)
            # object.__setattr__ is Pydantic's recommended way to write a field on a frozen
            # model from within a mode='after' validator.  This cannot be promoted to a
            # mode='before' validator because the name is derived from the computed check
            # condition, which requires the model to be fully initialised first.
            object.__setattr__(self, "name", normalized_name)

    def _validate_attributes(self) -> None:
        """Verify input attributes."""
        criticality = self.criticality
        if criticality not in {Criticality.WARN.value, Criticality.ERROR.value}:
            raise InvalidCheckError(
                f"Invalid 'criticality' value: '{criticality}'. "
                f"Expected '{Criticality.WARN.value}' or '{Criticality.ERROR.value}'. "
                f"Check details: {self.name}"
            )

        if self.column is not None and self.columns is not None:
            raise InvalidCheckError("Both 'column' and 'columns' cannot be provided at the same time.")

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

        # Push down check filter as row_filter if supported by the check function
        if self.filter and "row_filter" in sig.parameters:
            kwargs["row_filter"] = self.filter

        return kwargs

    def _is_optional_argument(self, signature: inspect.Signature, arg_name: str):
        """Returns True if the argument exists and is optional, False if required, None if not present."""
        param = signature.parameters.get(arg_name)

        if param is None:
            return None  # Argument not present
        return param.default is not inspect.Parameter.empty

    @staticmethod
    def _validate_message_expression(message_expr: str) -> None:
        """
        Checks that the message expression is a logically valid Spark SQL expression.

        Args:
            message_expr: Message expression

        Raises:
            InvalidParameterError: If the expression is not a logically valid Spark SQL expression.
        """
        try:
            F.expr(message_expr)
        except Exception as exc:
            raise InvalidParameterError(
                f"Custom message expression '{message_expr}' is not a valid Spark SQL expression."
            ) from exc


class DQRowRule(DQRule):
    """
    Represents a row-level data quality rule that applies a quality check function to a column or column expression.
    Works with check functions that take a single column or no column as input.
    """

    _expected_rule_type: ClassVar[str] = "row"
    _alternative_rules: ClassVar[list[str]] = ["DQDatasetRule"]

    def get_check_condition(self) -> Column:
        """
        Compute the check condition for this rule.

        Returns:
            The Spark Column representing the check condition.
        """
        check_condition = self.check  # lazy evaluation of check function parameters
        return check_condition

    @ft.cached_property
    def check(self) -> Column:
        args, kwargs = self.prepare_check_func_args_and_kwargs()
        condition = self.check_func(*args, **kwargs)
        return condition


class DQDatasetRule(DQRule):
    """
    Represents a dataset-level data quality rule that applies a quality check function to a column or
    column expression or list of columns depending on the check function.
    Either column or columns can be provided but not both. The rules are applied to the entire dataset or group of rows
    rather than individual rows. Failed checks are appended to the result columns in the same way as row-level rules.
    """

    _expected_rule_type: ClassVar[str] = "dataset"
    _alternative_rules: ClassVar[list[str]] = ["DQRowRule"]

    def get_check_condition(self) -> Column:
        """
        Compute the check condition for this rule.

        Returns:
            The Spark Column representing the check condition.
        """
        check_condition, _, _ = self.check  # lazy evaluation of check function parameters
        return check_condition

    @ft.cached_property
    def check(self) -> tuple[Column, Callable, str | None]:
        """Return (condition, apply_func, and optionally info_column_name)."""
        args, kwargs = self.prepare_check_func_args_and_kwargs()
        result = self.check_func(*args, **kwargs)
        if len(result) == 3:
            condition, apply_func, info_column_name = result[0], result[1], result[2]
            return condition, apply_func, info_column_name
        condition, apply_func = result[0], result[1]
        return condition, apply_func, None


class DQForEachColRule(BaseModel, DQRuleTypeMixin):
    """Represents a data quality rule that applies to a quality check function
    repeatedly on each specified column of the provided list of columns.
    This class includes the following attributes:
    * *columns* - A list of column names or expressions to which the check function should be applied.
    * *check_func* - The function used to perform the quality check.
    * *name* (optional) - A custom name for the check; autogenerated if not provided.
    * *criticality* - The severity level of the check:
        - *warn* for potential issues.
        - *error* for critical issues.
    * *filter* (optional) - A filter expression to apply the check only to rows meeting specific conditions.
    * *check_func_args* (optional) - Positional arguments for the check function (excluding column names).
    * *check_func_kwargs* (optional) - Keyword arguments for the check function (excluding column names).
    * *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    columns: list[str | SparkColumn | list[str | SparkColumn]]
    check_func: Annotated[Callable, WithJsonSchema({"type": "string"})]
    name: str = ""
    criticality: str = Criticality.ERROR.value
    filter: str | None = None
    check_func_args: list[Any] = []
    check_func_kwargs: dict[str, Any] = {}
    # See DQRule.user_metadata: kept permissive (dict[str, Any]) to preserve released behaviour
    # of checks carrying non-string metadata values.
    user_metadata: dict[str, Any] | None = None
    message_expr: str | SparkColumn | None = None

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise InvalidCheckError(str(exc)) from exc

    def get_rules(self) -> list[DQRule]:
        """Build a list of rules for a set of columns.

        Returns:
            list of dq rules
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
                        message_expr=self.message_expr,
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
                        message_expr=self.message_expr,
                    )
                )
        return rules


def compute_rule_fingerprint(check_dict: dict) -> str:
    """Compute a deterministic SHA-256 hash of a single rule definition.

    Normalizes the check dict before hashing so the same logical rule yields the same
    fingerprint regardless of storage backend or code path (Delta, Lakebase, engine).
    Normalization turns variants into a single canonical form.
    After normalization, all equivalent rules hash to the same fingerprint.

    Args:
        check_dict: Dictionary representing a single check rule.

    Returns:
        A hex-encoded SHA-256 hash string.
    """

    def _normalize_value_for_serialization(val: Any) -> Any:
        """Recursively normalize a value for JSON/serialization. Idempotent."""
        if isinstance(val, dict):
            return {k: _normalize_value_for_serialization(v) for k, v in val.items()}
        return normalize_bound_args(val)

    check_dict = _normalize_value_for_serialization(check_dict)

    check_inner = check_dict.get("check") or {}
    for_each_column = check_inner.get("for_each_column")
    # Normalize to list: Spark ArrayType or other sources may return non-list iterables
    if for_each_column is not None and not isinstance(for_each_column, list):
        for_each_column = list(for_each_column)
    fingerprint_data = {
        "name": check_dict.get("name"),
        "criticality": check_dict.get("criticality", "error"),
        "function": check_inner.get("function"),
        "arguments": check_inner.get("arguments"),
        "filter": check_dict.get("filter"),
        "for_each_column": sorted(for_each_column) if for_each_column else None,
    }
    canonical = json.dumps(fingerprint_data, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()
