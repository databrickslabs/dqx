import logging
import functools as ft
import inspect
from collections.abc import Callable
from types import UnionType
from typing import Any, get_origin, get_args

from pydantic import BaseModel, ConfigDict, ValidationError, ValidationInfo, field_validator, model_validator
from pydantic_core import ErrorDetails

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import Criticality

logger = logging.getLogger(__name__)


class CheckBlock(BaseModel):
    """Pydantic schema for the inner 'check' block of a rule metadata dict.

    Validates the structural shape (function name present, for_each_column is a non-empty list,
    arguments is a dict). Function resolution and signature-based argument validation are performed
    by *CheckSpec*'s semantic validator, which needs this well-formed block to run.

    Uses *extra="ignore"* to preserve the pre-migration behaviour: unknown keys in the check block
    are tolerated (the hand-rolled validator never rejected them). Unknown check-function
    *arguments* are still reported by *CheckSpec*'s signature validation.
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

    This is the single validated representation of a declarative check. Structural validation
    (required 'check' key, known field types, for_each_column shape) is done by Pydantic field
    validation. Semantic validation — criticality enum value, function resolution and signature-
    based argument validation — runs in *_validate_semantics* (a *model_validator*) so a bare
    *CheckSpec.model_validate(check, context=...)* fully validates a check with no second pass.

    The *model_validate* call accepts a *context* dict with:
        - *raw_check*: the original check dict, included verbatim in error messages for context.
        - *custom_check_functions*: optional mapping of custom function names to callables.
        - *validate_custom_check_functions*: if False, unknown/unregistered functions are tolerated
          (used by the LLM and profiler paths).

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
    # Values are intentionally *Any*, not *str*: the pre-migration validator never inspected
    # user_metadata, so checks carrying non-string values (e.g. {"confidence": 0.95}) loaded fine.
    # Narrowing to dict[str, str] would reject them (Pydantic v2 won't coerce int/float/bool to str)
    # and regress released behaviour. Keep it permissive to preserve the load -> apply round-trip.
    user_metadata: dict[str, Any] | None = None
    message_expr: str | None = None

    @model_validator(mode="after")
    def _validate_semantics(self, info: ValidationInfo) -> "CheckSpec":
        """Validate criticality value, function reference, and argument names/types in one pass.

        Pydantic skips *model_validator(mode="after")* when any field validation fails, so a
        malformed 'check' block (missing / not-a-dict / missing function / bad for_each_column or
        arguments shape) short-circuits here — matching the pre-migration validator, which could
        not resolve a function to validate arguments against. When the check block is well-formed,
        a bad *criticality* value and argument errors are collected together and raised as a single
        *ValueError*, so a single validation pass reports as many problems as the hand-rolled
        validator did.

        Args:
            info: Pydantic validation info; *info.context* carries *raw_check* and the custom-
                function resolution options (see the class docstring).

        Returns:
            The validated model.

        Raises:
            ValueError: If the criticality value is invalid, the function is unresolved, or the
                supplied arguments do not satisfy the function signature.
        """
        context = info.context or {}
        raw_check = context.get("raw_check")
        check_repr = raw_check if raw_check is not None else self.model_dump(exclude_none=True)
        custom_check_functions = context.get("custom_check_functions")
        validate_custom_check_functions = context.get("validate_custom_check_functions", True)

        messages = self._criticality_errors(check_repr)
        messages += self._function_and_argument_errors(
            check_repr, custom_check_functions, validate_custom_check_functions
        )
        if messages:
            raise ValueError("\n".join(messages))
        return self

    def _criticality_errors(self, check: dict) -> list[str]:
        """Return the criticality-value error message (as a one-item list) or an empty list."""
        valid = {c.value for c in Criticality}
        if self.criticality in valid:
            return []
        return [
            f"Invalid 'criticality' value: '{self.criticality}'. "
            f"Expected '{Criticality.WARN.value}' or '{Criticality.ERROR.value}'. "
            f"Check details: {check}"
        ]

    def _function_and_argument_errors(
        self, check: dict, custom_check_functions: dict[str, Callable] | None, validate_custom_check_functions: bool
    ) -> list[str]:
        """Resolve the check function and validate the supplied arguments against its signature.

        Args:
            check: The original check dict, included in messages for context.
            custom_check_functions: Optional mapping of custom function names to callables.
            validate_custom_check_functions: If False, unknown functions are silently skipped.

        Returns:
            A list of error messages (empty on success).
        """
        func = resolve_check_function(self.check.function, custom_check_functions, fail_on_missing=False)
        if not callable(func):
            if validate_custom_check_functions:
                return [f"function '{self.check.function}' is not defined: {check}"]
            return []

        return self._validate_check_function_arguments(func, check)

    def _validate_check_function_arguments(self, func: Callable, check: dict) -> list[str]:
        """Validate the check arguments against the function signature.

        Args:
            func: The resolved check function.
            check: The original check dict, included in messages for context.

        Returns:
            A list of error messages if any validation fails, otherwise an empty list.
        """

        @ft.lru_cache(None)
        def cached_signature(check_func):
            return inspect.signature(check_func)

        func_parameters = cached_signature(func).parameters

        effective_arguments = dict(self.check.arguments)  # copy to avoid modifying the original
        for_each_column = self.check.for_each_column or []
        if for_each_column:
            errors: list[str] = []
            for col_or_cols in for_each_column:
                if "columns" in func_parameters:
                    effective_arguments["columns"] = col_or_cols
                else:
                    effective_arguments["column"] = col_or_cols
                errors.extend(self._validate_func_args(effective_arguments, func, check, func_parameters))
            return errors
        return self._validate_func_args(effective_arguments, func, check, func_parameters)

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
                    errors.extend(CheckSpec._validate_func_list_args(arg, func, check, expected_type_args, value))
                elif not CheckSpec._check_type(value, expected_type):
                    expected_type_name = getattr(expected_type, "__name__", str(expected_type))
                    errors.append(
                        f"Argument '{arg}' should be of type '{expected_type_name}' for function '{func.__name__}' "
                        f"in the 'arguments' block: {check}"
                    )
        errors.extend(CheckSpec._missing_required_arguments(arguments, func, check, func_parameters))
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
            return CheckSpec._check_union_type(args, value)

        if origin is list:
            return CheckSpec._check_list_type(args, value)

        if origin is dict:
            return CheckSpec._check_dict_type(args, value)

        if origin is tuple:
            return CheckSpec._check_tuple_type(args, value)

        if origin:
            return isinstance(value, origin)
        return isinstance(value, expected_type)

    @staticmethod
    def _check_union_type(args, value):
        return any(CheckSpec._check_type(value, arg) for arg in args)

    @staticmethod
    def _check_list_type(args, value):
        if not isinstance(value, list):
            return False
        if not args:
            return True  # no inner type to check
        return all(CheckSpec._check_type(item, args[0]) for item in value)

    @staticmethod
    def _check_dict_type(args, value):
        if not isinstance(value, dict):
            return False
        if not args or len(args) != 2:
            return True
        return all(CheckSpec._check_type(k, args[0]) and CheckSpec._check_type(v, args[1]) for k, v in value.items())

    @staticmethod
    def _check_tuple_type(args, value):
        if not isinstance(value, tuple):
            return False
        if len(args) == 2 and args[1] is Ellipsis:
            return all(CheckSpec._check_type(item, args[0]) for item in value)
        return len(value) == len(args) and all(CheckSpec._check_type(item, arg) for item, arg in zip(value, args))

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
                expected_type_name = "|".join(getattr(arg, "__name__", str(arg)) for arg in expected_type_args)
                errors.append(
                    f"Item {i} in argument '{arguments}' should be of type '{expected_type_name}' "
                    f"for function '{func.__name__}' in the 'arguments' block: {check}"
                )
        return errors


class ChecksValidationStatus(BaseModel):
    """Class to represent the validation status.

    This model is used as a mutable accumulator: *add_error* and *add_errors* append to the
    *errors* list in place.  Pydantic instantiates a fresh copy of the *[]* default for each
    instance, so the list is never shared between instances created via the constructor.  The
    only sharing risk is a shallow *model_copy()* (without *deep=True*); this model is never
    shallow-copied, but use *model_copy(deep=True)* if that ever changes.
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

    All validation lives on the *CheckSpec* Pydantic model: structural shape via field validation,
    and semantic checks (criticality enum value, function resolution, signature-based argument
    validation) via its *model_validator*. This class is a thin orchestration layer that runs
    *CheckSpec.model_validate* per check and translates any Pydantic *ValidationError* into the
    human-readable messages callers and tests expect. *_validate_and_parse* additionally returns the
    parsed specs so callers (e.g. the deserializer) reuse them instead of parsing a second time.
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
        status, _ = ChecksValidator._validate_and_parse(checks, custom_check_functions, validate_custom_check_functions)
        return status

    @staticmethod
    def _validate_and_parse(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> tuple[ChecksValidationStatus, list[CheckSpec | None]]:
        """Validate checks and return both the status and the parsed specs.

        Each check is validated exactly once via *CheckSpec.model_validate*. On success the parsed
        *CheckSpec* is returned so callers can build rules from the typed representation without a
        second parse; on failure *None* is returned in its place and the errors are accumulated.

        Args:
            checks: List of check metadata dicts to validate.
            custom_check_functions: Optional mapping of custom function names to callables.
            validate_custom_check_functions: If False, unknown/unregistered functions are tolerated.

        Returns:
            A *(status, specs)* tuple; *specs* is index-aligned with *checks* (each entry is the
            parsed *CheckSpec* or *None* when that check failed validation).
        """
        status = ChecksValidationStatus()
        specs: list[CheckSpec | None] = []

        for check in checks:
            logger.debug(f"Processing check definition: {check}")
            if not isinstance(check, dict):
                status.add_error(f"Unsupported check type: {type(check)}")
                specs.append(None)
                continue
            try:
                spec = CheckSpec.model_validate(
                    check,
                    context={
                        "raw_check": check,
                        "custom_check_functions": custom_check_functions,
                        "validate_custom_check_functions": validate_custom_check_functions,
                    },
                )
                specs.append(spec)
            except ValidationError as exc:
                specs.append(None)
                status.add_errors(ChecksValidator._translate_validation_error(exc, check))

        return status, specs

    @staticmethod
    def _translate_validation_error(exc: ValidationError, check: dict) -> list[str]:
        """Translate every error in a *ValidationError* into human-readable messages.

        Args:
            exc: The Pydantic validation error raised for a single check.
            check: The original check dict, included in messages for context.

        Returns:
            A flat list of translated error messages.
        """
        messages: list[str] = []
        for pydantic_error in exc.errors():
            messages.extend(ChecksValidator._translate_pydantic_error(pydantic_error, check))
        return messages

    @staticmethod
    def _translate_pydantic_error(error: ErrorDetails, check: dict) -> list[str]:
        """Translate a single Pydantic validation error dict into human-readable message(s).

        Structural (field-level) errors translate to one message each. The semantic
        *model_validator* raises a single *ValueError* whose text is one or more already-formatted
        DQX messages joined by newlines (loc is empty); those are split back into individual
        messages here.

        Args:
            error: One error entry from *ValidationError.errors()*.
            check: The original check dict, included in messages for context.

        Returns:
            A list of human-readable error strings (empty if the error should be skipped).
        """
        loc_parts = error["loc"]
        loc = " -> ".join(str(loc_part) for loc_part in loc_parts) if loc_parts else ""
        msg = error["msg"]
        error_type = error.get("type", "")

        # Semantic errors (criticality value, function resolution, argument validation) come from
        # the model-level validator: type "value_error" with an empty loc. Their text is already
        # in the final DQX format; strip Pydantic's "Value error, " prefix and split on newlines.
        if error_type == "value_error" and not loc_parts:
            clean_msg = msg.replace("Value error, ", "", 1)
            return [line for line in clean_msg.split("\n") if line]

        if error_type == "missing" and loc == "check":
            return [f"'check' field is missing: {check}"]
        if error_type in {"dict_type", "model_type"} and loc == "check":
            return [f"'check' field should be a dictionary: {check}"]
        if error_type == "missing" and loc == "check -> function":
            return [f"'function' field is missing in the 'check' block: {check}"]
        if loc == "check -> for_each_column":
            return [ChecksValidator._translate_for_each_column_error(error_type, check)]
        if loc == "check -> arguments" and error_type == "dict_type":
            return [f"'arguments' should be a dictionary in the 'check' block: {check}"]
        # Fallback: name the offending field so the message stays actionable, matching the
        # field-level precision the DQRule model reports at apply time (e.g. "check -> function").
        if loc:
            return [f"Invalid value for '{loc}': {msg}. Check details: {check}"]
        return [f"{msg}: {check}"]

    @staticmethod
    def _translate_for_each_column_error(error_type: str, check: dict) -> str:
        """Translate a Pydantic for_each_column validation error into the expected message.

        Matches on the stable Pydantic error *type* rather than the (version-dependent) message
        text: the *_non_empty_for_each_column* field validator raises *ValueError* -> type
        *value_error* for an empty list, while a non-list input yields *list_type*.

        Args:
            error_type: The Pydantic error type (e.g. *value_error*, *list_type*).
            check: The original check dict, included in messages for context.

        Returns:
            The human-readable error string in the expected format.
        """
        if error_type == "value_error":
            return f"'for_each_column' should not be empty in the 'check' block: {check}"
        return f"'for_each_column' should be a list in the 'check' block: {check}"
