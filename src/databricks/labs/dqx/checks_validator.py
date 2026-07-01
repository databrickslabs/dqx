import logging
import functools as ft
import inspect
from collections.abc import Callable
from types import UnionType
from typing import Any, get_origin, get_args

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from pydantic_core import ErrorDetails

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import Criticality

logger = logging.getLogger(__name__)


class CheckBlock(BaseModel):
    """Pydantic schema for the inner 'check' block of a rule metadata dict.

    Validates the structural shape (function name present, for_each_column is a non-empty list,
    arguments is a dict) but does NOT validate that the function exists in the registry or that
    the arguments satisfy the function signature — those checks happen in *ChecksValidator*.

    Uses *extra="ignore"* to preserve the pre-migration behaviour: unknown keys in the check block
    are tolerated (the hand-rolled validator never rejected them). Unknown check-function
    *arguments* are still reported by *ChecksValidator* via signature validation.
    """

    model_config = ConfigDict(extra="ignore")

    function: str
    arguments: dict[str, Any] = {}
    for_each_column: list[Any] | None = None

    @field_validator("for_each_column")
    @classmethod
    def _non_empty_for_each_column(cls, value: list[Any] | None) -> list[Any] | None:
        if value is not None and len(value) == 0:
            raise ValueError("'for_each_column' must not be empty")
        return value


class CheckSpec(BaseModel):
    """Pydantic schema for a single top-level rule metadata dict.

    Validates the structural shape (required 'check' key, optional known keys, criticality enum)
    but does NOT resolve functions or validate argument types — those are done by *ChecksValidator*
    after structural validation passes.

    Uses *extra="ignore"* to preserve the pre-migration behaviour: unknown top-level keys are
    tolerated (the hand-rolled validator never rejected them, and storage backends persist extra
    columns alongside the check). Rejecting them would be a breaking change for existing check
    definitions and would fail the load -> apply round-trip for stored checks.
    """

    model_config = ConfigDict(extra="ignore")

    check: CheckBlock
    criticality: str = Criticality.ERROR.value
    name: str | None = None
    filter: str | None = None
    user_metadata: dict[str, str] | None = None
    message_expr: str | None = None

    @field_validator("criticality")
    @classmethod
    def _valid_criticality(cls, value: str) -> str:
        valid = {c.value for c in Criticality}
        if value not in valid:
            raise ValueError(
                f"Invalid 'criticality' value: '{value}'. "
                f"Expected '{Criticality.WARN.value}' or '{Criticality.ERROR.value}'."
            )
        return value


class ChecksValidationStatus(BaseModel):
    """Class to represent the validation status.

    This model is used as a mutable accumulator: *add_error* and *add_errors* append to the
    *errors* list in place.  Pydantic instantiates a fresh copy of the ``[]`` default for each
    instance, so the list is never shared between instances created via the constructor.  The
    only sharing risk is a shallow ``model_copy()`` (without ``deep=True``); this model is never
    shallow-copied, but use ``model_copy(deep=True)`` if that ever changes.
    """

    model_config = ConfigDict(extra="forbid")

    errors: list[str] = []

    def add_error(self, error: str):
        """Add an error to the validation status."""
        self.errors.append(error)

    def add_errors(self, errors: list[str]):
        """Add errors to the validation status."""
        self.errors.extend(errors)

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors in the validation status."""
        return bool(self.errors)

    def to_string(self) -> str:
        """Convert the validation status to a string."""
        if self.has_errors:
            return "\n".join(self.errors)
        return "No errors found"

    def __str__(self) -> str:
        """String representation of the ValidationStatus class."""
        return self.to_string()


class ChecksValidator:
    """
    Validates declarative quality rules (checks).

    Structural shape (required keys, known top-level keys, criticality enum value, for_each_column
    type) is validated via Pydantic (*CheckSpec*).  After structural validation passes, per-check-
    function argument validation is run against *inspect.signature*: required parameters must be
    present (*for_each_column* may supply *column* or *columns*); a top-level rule *filter* is not
    a substitute for missing arguments.  Unknown argument names and type mismatches (where
    annotations exist) are also reported.
    """

    @staticmethod
    def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
        """Validate a list of check metadata dicts.

        Args:
            checks: List of check metadata dicts to validate.
            custom_check_functions: Optional mapping of custom function names to callables.
            validate_custom_check_functions: If False, unknown/unregistered functions are tolerated
                (used by LLM and profiler paths).

        Returns:
            A *ChecksValidationStatus* accumulating all errors found.
        """
        status = ChecksValidationStatus()

        for check in checks:
            logger.debug(f"Processing check definition: {check}")
            if isinstance(check, dict):
                status.add_errors(
                    ChecksValidator._validate_checks_dict(
                        check, custom_check_functions, validate_custom_check_functions
                    )
                )
            else:
                status.add_error(f"Unsupported check type: {type(check)}")

        return status

    @staticmethod
    def _validate_checks_dict(
        check: dict, custom_check_functions: dict[str, Callable] | None, validate_custom_check_functions: bool
    ) -> list[str]:
        """Validate the structure and content of a single check metadata dict.

        First runs Pydantic structural validation (*CheckSpec.model_validate*) to catch shape
        errors (missing 'check' key, bad criticality, unexpected top-level fields, for_each_column
        not a list / empty).  Then runs signature-based argument validation against the resolved
        check function.

        Args:
            check: The check dict to validate.
            custom_check_functions: Optional mapping of custom function names to callables.
            validate_custom_check_functions: If False, unknown functions are silently skipped.

        Returns:
            A list of error messages (empty on success).
        """
        # --- Structural validation via Pydantic ---
        structural_errors = ChecksValidator._pydantic_validate_structure(check)
        if structural_errors:
            return structural_errors

        # Structure is valid; now run signature-based argument validation.
        return ChecksValidator._validate_check_block(check, custom_check_functions, validate_custom_check_functions)

    @staticmethod
    def _pydantic_validate_structure(check: dict) -> list[str]:
        """Run *CheckSpec.model_validate* on a single check dict and collect errors.

        Translates Pydantic *ValidationError* into human-readable strings that match the
        existing error format expected by callers and tests.  The original *check* dict is
        included in error messages for context, as the hand-rolled validator did before.

        Args:
            check: The raw check dict to validate structurally.

        Returns:
            A list of error message strings (empty when the check passes structural validation).
        """
        try:
            CheckSpec.model_validate(check)
            return []
        except ValidationError as exc:
            return [
                msg
                for pydantic_error in exc.errors()
                for msg in (ChecksValidator._translate_pydantic_error(pydantic_error, check),)
                if msg
            ]

    @staticmethod
    def _translate_pydantic_error(error: ErrorDetails, check: dict) -> str | None:
        """Translate a single Pydantic validation error dict into a human-readable message.

        Args:
            error: One error entry from *ValidationError.errors()*.
            check: The original check dict, included in messages for context.

        Returns:
            A human-readable error string, or *None* if the error should be skipped.
        """
        loc = " -> ".join(str(loc_part) for loc_part in error["loc"]) if error["loc"] else ""
        msg = error["msg"]
        error_type = error.get("type", "")

        if error_type == "missing" and loc == "check":
            return f"'check' field is missing: {check}"
        if error_type in {"dict_type", "model_type"} and loc == "check":
            return f"'check' field should be a dictionary: {check}"
        if error_type == "missing" and loc == "check -> function":
            return f"'function' field is missing in the 'check' block: {check}"
        if "criticality" in loc:
            # Strip Pydantic's "Value error, " prefix to match the existing message format.
            clean_msg = msg.replace("Value error, ", "")
            return f"{clean_msg} Check details: {check}"
        if "for_each_column" in loc:
            return ChecksValidator._translate_for_each_column_error(msg, check)
        if loc == "check -> arguments" and error_type == "dict_type":
            return f"'arguments' should be a dictionary in the 'check' block: {check}"
        return f"{msg}: {check}"

    @staticmethod
    def _translate_for_each_column_error(pydantic_msg: str, check: dict) -> str:
        """Translate a Pydantic for_each_column validation error into the expected message.

        Args:
            pydantic_msg: The raw Pydantic error message string.
            check: The original check dict, included in messages for context.

        Returns:
            The human-readable error string in the expected format.
        """
        clean = pydantic_msg.replace("Value error, ", "")
        if "non-empty" in clean or "must not be empty" in clean:
            return f"'for_each_column' should not be empty in the 'check' block: {check}"
        return f"'for_each_column' should be a list in the 'check' block: {check}"

    @staticmethod
    def _validate_check_block(
        check: dict, custom_check_functions: dict[str, Callable] | None, validate_custom_check_functions: bool
    ) -> list[str]:
        """Validate the function reference and per-function argument types/presence.

        Structural validation (via Pydantic) has already passed, so 'check' and 'function' are
        guaranteed to be present and well-typed here.

        Args:
            check: The check configuration to validate.
            custom_check_functions: Optional mapping of custom function names to callables.
            validate_custom_check_functions: If False, unknown functions are silently skipped.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.
        """
        check_block = check["check"]
        func_name = check_block["function"]
        func = resolve_check_function(func_name, custom_check_functions, fail_on_missing=False)
        if not callable(func):
            if validate_custom_check_functions:
                return [f"function '{func_name}' is not defined: {check}"]
            return []

        arguments = check_block.get("arguments", {})
        for_each_column = check_block.get("for_each_column", [])

        return ChecksValidator._validate_check_function_arguments(arguments, func, for_each_column, check)

    @staticmethod
    def _validate_check_function_arguments(
        arguments: dict, func: Callable, for_each_column: list, check: dict
    ) -> list[str]:
        """Validate arguments against the function signature.

        Args:
            arguments: A dictionary of arguments to validate.
            func: The function for which the arguments are being validated.
            for_each_column: A list of columns to iterate over for the check.
            check: A dictionary containing the check configuration.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.
        """
        if not isinstance(arguments, dict):
            return [f"'arguments' should be a dictionary in the 'check' block: {check}"]

        @ft.lru_cache(None)
        def cached_signature(check_func):
            return inspect.signature(check_func)

        func_parameters = cached_signature(func).parameters

        effective_arguments = dict(arguments)  # make a copy to avoid modifying the original
        if for_each_column:
            errors: list[str] = []
            for col_or_cols in for_each_column:
                if "columns" in func_parameters:
                    effective_arguments["columns"] = col_or_cols
                else:
                    effective_arguments["column"] = col_or_cols
                errors.extend(ChecksValidator._validate_func_args(effective_arguments, func, check, func_parameters))
            return errors
        return ChecksValidator._validate_func_args(effective_arguments, func, check, func_parameters)

    @staticmethod
    def _validate_func_args(arguments: dict, func: Callable, check: dict, func_parameters: Any) -> list[str]:
        """Validate argument names and types against the function signature.

        Args:
            arguments: A dictionary of argument names and their values to be validated.
            func: The function whose arguments are being validated.
            check: A dictionary containing additional context or information for error messages.
            func_parameters: The parameters of the function as obtained from its signature.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.
        """
        errors: list[str] = []
        if not arguments and func_parameters:
            errors.append(
                f"No arguments provided for function '{func.__name__}' in the 'arguments' block: {check}. "
                f"Expected arguments are: {list(func_parameters.keys())}"
            )
            return errors
        for arg, value in arguments.items():
            if arg not in func_parameters:
                expected_args = list(func_parameters.keys())
                errors.append(
                    f"Unexpected argument '{arg}' for function '{func.__name__}' in the 'arguments' block: {check}. "
                    f"Expected arguments are: {expected_args}"
                )
            else:
                expected_type = func_parameters[arg].annotation
                if get_origin(expected_type) is list:
                    expected_type_args = get_args(expected_type)
                    errors.extend(ChecksValidator._validate_func_list_args(arg, func, check, expected_type_args, value))
                elif not ChecksValidator._check_type(value, expected_type):
                    expected_type_name = getattr(expected_type, '__name__', str(expected_type))
                    errors.append(
                        f"Argument '{arg}' should be of type '{expected_type_name}' for function '{func.__name__}' "
                        f"in the 'arguments' block: {check}"
                    )
        errors.extend(ChecksValidator._missing_required_arguments(arguments, func, check, func_parameters))
        return errors

    @staticmethod
    def _missing_required_arguments(
        arguments: dict[str, Any], func: Callable, check: dict, func_parameters: Any
    ) -> list[str]:
        """Ensure every parameter without a default is present in *arguments*.

        Metadata must supply all required check-function parameters (e.g. *column* for *regex_match*).

        Args:
            arguments: Provided argument dict.
            func: The check function being validated.
            check: The full check dict for error context.
            func_parameters: Signature parameters of the function.

        Returns:
            A list of error messages for missing required arguments.
        """
        errors: list[str] = []
        for name, param in func_parameters.items():
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            if param.default is not inspect.Parameter.empty:
                continue
            if name not in arguments:
                errors.append(
                    f"Missing required argument '{name}' for function '{func.__name__}' in the 'arguments' block: "
                    f"{check}. Expected arguments include: {list(func_parameters.keys())}"
                )
        return errors

    @staticmethod
    def _check_type(value, expected_type) -> bool:
        origin = get_origin(expected_type)
        args = get_args(expected_type)

        if expected_type is inspect.Parameter.empty:
            return True  # no type hint, assume valid

        if origin is UnionType:
            # Handle Optional[X] as Union[X, NoneType]
            return ChecksValidator._check_union_type(args, value)

        if origin is list:
            return ChecksValidator._check_list_type(args, value)

        if origin is dict:
            return ChecksValidator._check_dict_type(args, value)

        if origin is tuple:
            return ChecksValidator._check_tuple_type(args, value)

        if origin:
            return isinstance(value, origin)
        return isinstance(value, expected_type)

    @staticmethod
    def _check_union_type(args, value):
        return any(ChecksValidator._check_type(value, arg) for arg in args)

    @staticmethod
    def _check_list_type(args, value):
        if not isinstance(value, list):
            return False
        if not args:
            return True  # no inner type to check
        return all(ChecksValidator._check_type(item, args[0]) for item in value)

    @staticmethod
    def _check_dict_type(args, value):
        if not isinstance(value, dict):
            return False
        if not args or len(args) != 2:
            return True
        return all(
            ChecksValidator._check_type(k, args[0]) and ChecksValidator._check_type(v, args[1])
            for k, v in value.items()
        )

    @staticmethod
    def _check_tuple_type(args, value):
        if not isinstance(value, tuple):
            return False
        if len(args) == 2 and args[1] is Ellipsis:
            return all(ChecksValidator._check_type(item, args[0]) for item in value)
        return len(value) == len(args) and all(ChecksValidator._check_type(item, arg) for item, arg in zip(value, args))

    @staticmethod
    def _validate_func_list_args(
        arguments: dict, func: Callable, check: dict, expected_type_args: tuple[type, ...], value: list[Any]
    ) -> list[str]:
        """Validate list-typed arguments against expected item types.

        Args:
            arguments: A dictionary of argument names and their values to be validated.
            func: The function whose arguments are being validated.
            check: A dictionary containing additional context or information for error messages.
            expected_type_args: Expected types for the list items.
            value: The value of the argument to validate.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.
        """
        if not isinstance(value, list):
            return [
                f"Argument '{arguments}' should be of type 'list' for function '{func.__name__}' "
                f"in the 'arguments' block: {check}"
            ]

        errors: list[str] = []
        for i, item in enumerate(value):
            if not isinstance(item, expected_type_args):
                expected_type_name = '|'.join(getattr(arg, '__name__', str(arg)) for arg in expected_type_args)
                errors.append(
                    f"Item {i} in argument '{arguments}' should be of type '{expected_type_name}' "
                    f"for function '{func.__name__}' in the 'arguments' block: {check}"
                )
        return errors
