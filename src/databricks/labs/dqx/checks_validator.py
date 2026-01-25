import logging
import functools as ft
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from types import UnionType
from typing import Any, get_origin, get_args

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import Criticality

logger = logging.getLogger(__name__)


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


class ChecksValidator:
    """
    Class to validate quality rules (checks).
    """

    @staticmethod
    def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
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
        """
        Validates the structure and content of a given check dictionary.

        Args:
            check: The check dictionary to validate.
            custom_check_functions: A dictionary containing custom check functions.
            validate_custom_check_functions: If True, validate custom check functions.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.
        """
        errors: list[str] = []

        if "criticality" in check and check["criticality"] not in [c.value for c in Criticality]:
            errors.append(
                f"Invalid 'criticality' value: '{check['criticality']}'. "
                f"Expected '{Criticality.WARN.value}' or '{Criticality.ERROR.value}'. "
                f"Check details: {check}"
            )

        if "check" not in check:
            errors.append(f"'check' field is missing: {check}")
        elif not isinstance(check["check"], dict):
            errors.append(f"'check' field should be a dictionary: {check}")
        else:
            errors.extend(
                ChecksValidator._validate_check_block(check, custom_check_functions, validate_custom_check_functions)
            )

        return errors

    @staticmethod
    def _validate_check_block(
        check: dict, custom_check_functions: dict[str, Callable] | None, validate_custom_check_functions: bool
    ) -> list[str]:
        """
        Validates a check block within a configuration.

        Args:
            check: The check configuration to validate.
            custom_check_functions: A dictionary containing custom check functions.
            validate_custom_check_functions: If True, validate custom check functions.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.

        Raises:
            InvalidCheckError: if the function is not found and fail_on_missing is True.
        """
        check_block = check["check"]

        if "function" not in check_block:
            return [f"'function' field is missing in the 'check' block: {check}"]

        func_name = check_block["function"]
        func = resolve_check_function(func_name, custom_check_functions, fail_on_missing=False)
        if not callable(func):
            if validate_custom_check_functions:
                return [f"function '{func_name}' is not defined: {check}"]
            return []

        arguments = check_block.get("arguments", {})
        for_each_column = check_block.get("for_each_column", [])

        if "for_each_column" in check_block and for_each_column is not None:
            if not isinstance(for_each_column, list):
                return [f"'for_each_column' should be a list in the 'check' block: {check}"]

            if len(for_each_column) == 0:
                return [f"'for_each_column' should not be empty in the 'check' block: {check}"]

        return ChecksValidator._validate_check_function_arguments(arguments, func, for_each_column, check)

    @staticmethod
    def _validate_check_function_arguments(
        arguments: dict, func: Callable, for_each_column: list, check: dict
    ) -> list[str]:
        """
        Validates the provided arguments for a given function and updates the errors list if any validation fails.

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
        """
        Validates the arguments passed to a function against its signature.

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
        """
        Validates the list arguments passed to a function against its signature.

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
