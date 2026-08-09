import logging
import functools as ft
import inspect
from collections.abc import Callable
from typing import Any, get_origin, get_args

from pydantic import (
    BaseModel,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import ErrorDetails, SchemaError

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import Criticality

logger = logging.getLogger(__name__)


# Bounded caches: *_cached_signature* is called with user-supplied *custom_check_functions* (a
# fresh closure per notebook/session, potentially capturing large state), so an unbounded cache
# (*maxsize=None*) would pin every distinct callable for the whole process lifetime and grow the
# driver's memory monotonically. A bounded LRU keeps the cross-check reuse while capping retention.
_CACHE_MAXSIZE = 1024


@ft.lru_cache(maxsize=_CACHE_MAXSIZE)
def _cached_signature(func: Callable) -> inspect.Signature:
    """Return the (cached) signature of a check function.

    Keyed on the resolved *func*, so *inspect.signature* runs once per distinct function across a
    whole validation pass rather than once per check.
    """
    return inspect.signature(func)


@ft.lru_cache(maxsize=_CACHE_MAXSIZE)
def _type_adapter(annotation: Any) -> TypeAdapter[Any]:
    """Build (and cache) a strict *TypeAdapter* for a check-function parameter annotation.

    Cached on the annotation so each distinct parameter type builds its validation schema once.
    *arbitrary_types_allowed* lets pydantic fall back to *isinstance* for non-pydantic types such
    as *pyspark.sql.Column*.
    """
    return TypeAdapter(annotation, config=ConfigDict(arbitrary_types_allowed=True))


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
    *CheckSpec.validate_check(check, ...)* fully validates a check with no second pass.

    Pydantic skips *model_validator(mode="after")* when any field fails, so a malformed 'check'
    block would suppress the sibling *criticality* error raised there. The pre-migration validator
    always reported criticality regardless of the check block, so *ChecksValidator.validate_and_parse*
    reproduces that check (via *criticality_errors*) on the field-failure path to preserve parity.

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
        validator did. On the field-failure path this validator does not run, so
        *ChecksValidator.validate_and_parse* supplements the criticality check to preserve parity.

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

        messages = self.criticality_errors(check_repr)
        messages += self._function_and_argument_errors(
            check_repr, custom_check_functions, validate_custom_check_functions
        )
        if messages:
            raise ValueError("\n".join(messages))
        return self

    @classmethod
    def criticality_errors(cls, check: dict) -> list[str]:
        """Return the criticality-value error (as a one-item list) or an empty list.

        Reads the criticality from the raw *check* dict so it can run both inside
        *_validate_semantics* and standalone (from *ChecksValidator.validate_and_parse*) when a
        field error has skipped the model validator. Non-string criticality values are left to the
        field-level type error and skipped here, so they are not reported twice.

        Args:
            check: The check dict to read *criticality* from and include in the message.

        Returns:
            A single-element list with the error message, or an empty list when valid.
        """
        criticality = check.get("criticality", Criticality.ERROR.value)
        valid = {c.value for c in Criticality}
        if not isinstance(criticality, str) or criticality in valid:
            return []
        return [
            f"Invalid 'criticality' value: '{criticality}'. "
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
        func_parameters = _cached_signature(func).parameters

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
                elif not CheckSpec._matches_type(value, expected_type):
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
    def _matches_type(value: object, expected_type: Any) -> bool:
        """Return whether *value* satisfies *expected_type* under strict validation.

        Delegates runtime type checking to *pydantic.TypeAdapter* in strict mode (no lax coercion,
        so a stringy *"5"* does not pass an *int* parameter) rather than a hand-rolled type walker,
        keeping argument validation consistent with how *CheckSpec* validates its own fields.
        Annotations pydantic cannot build a schema for — or that are unhashable and so cannot be
        cached — are treated as satisfied, mirroring the previous leniency toward unrecognised
        annotations.

        A *bool* is re-checked against its *int* supertype: strict pydantic rejects a *bool* for an
        *int* parameter, but the pre-migration *isinstance*-based validator accepted it
        (*isinstance(True, int)* is True). Re-validating *int(value)* restores that acceptance
        wherever *int* is valid (e.g. *int* or *int | str*), so a stored check carrying a
        *true* / *false* literal on an integer parameter still passes the load -> apply round-trip.

        Args:
            value: The argument value supplied in the check metadata.
            expected_type: The parameter annotation to validate against.

        Returns:
            True if *value* is valid for *expected_type* (or the type cannot be schematised).
        """
        if expected_type is inspect.Parameter.empty:
            return True  # no type hint, assume valid
        try:
            adapter = _type_adapter(expected_type)
        except (SchemaError, TypeError):
            # Stay lenient as before rather than crashing: TypeError covers both an annotation
            # pydantic cannot schematise (*PydanticSchemaGenerationError* subclasses *TypeError*)
            # and an unhashable annotation (the *lru_cache* key raises *TypeError* before the
            # adapter is even built).
            return True
        if CheckSpec._strict_matches(adapter, value):
            return True
        if isinstance(value, bool):
            return CheckSpec._strict_matches(adapter, int(value))
        return False

    @staticmethod
    def _strict_matches(adapter: TypeAdapter[Any], value: object) -> bool:
        """Return whether *value* passes *adapter* under strict validation (no lax coercion)."""
        try:
            adapter.validate_python(value, strict=True)
        except ValidationError:
            return False
        return True

    @staticmethod
    def _validate_func_list_args(
        arguments: dict, func: Callable, check: dict, expected_type_args: tuple[type, ...], value: list[Any]
    ) -> list[str]:
        """Validate list-typed arguments against expected item types.

        Each item is checked with the same *_matches_type* used for scalar arguments, so the two
        paths agree on a given logical type (e.g. a *bool* is accepted for both *int* and
        *list[int]*, not one and rejected by the other).

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

        item_type = expected_type_args[0] if expected_type_args else inspect.Parameter.empty
        errors: list[str] = []
        for i, item in enumerate(value):
            if not CheckSpec._matches_type(item, item_type):
                expected_type_name = "|".join(getattr(arg, "__name__", str(arg)) for arg in expected_type_args)
                errors.append(
                    f"Item {i} in argument '{arguments}' should be of type '{expected_type_name}' "
                    f"for function '{func.__name__}' in the 'arguments' block: {check}"
                )
        return errors

    @classmethod
    def validate_check(
        cls,
        check: dict,
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> "CheckSpec":
        """Validate and parse a single check dict, binding the semantic-validation context.

        This is the supported entry point: it wires up the *context* that *_validate_semantics* (and
        the *criticality_errors* check it runs) rely on, so callers cannot accidentally invoke *model_validate*
        without it (which would run strict function validation against the projected model and
        ignore the *validate_custom_check_functions* tolerance flag).

        Args:
            check: The check metadata dict to validate.
            custom_check_functions: Optional mapping of custom function names to callables.
            validate_custom_check_functions: If False, unknown/unregistered functions are tolerated
                (used by the LLM and profiler paths).

        Returns:
            The validated *CheckSpec*.

        Raises:
            ValidationError: If the check fails structural or semantic validation.
        """
        return cls.model_validate(
            check,
            context={
                "raw_check": check,
                "custom_check_functions": custom_check_functions,
                "validate_custom_check_functions": validate_custom_check_functions,
            },
        )


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

    All validation lives on the *CheckSpec* Pydantic model: structural shape and the criticality
    value via field validation, and the remaining semantic checks (function resolution, signature-
    based argument validation) via its *model_validator*. This class is a thin orchestration layer
    that runs *CheckSpec.validate_check* per check and translates any Pydantic *ValidationError*
    into the human-readable messages callers and tests expect. *validate_and_parse* additionally
    returns the parsed specs so callers (e.g. the deserializer) reuse them instead of parsing a
    second time.
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
        status, _ = ChecksValidator.validate_and_parse(checks, custom_check_functions, validate_custom_check_functions)
        return status

    @staticmethod
    def validate_and_parse(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> tuple[ChecksValidationStatus, list[CheckSpec | None]]:
        """Validate checks and return both the status and the parsed specs.

        Each check is validated exactly once via *CheckSpec.validate_check*. On success the parsed
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
                spec = CheckSpec.validate_check(check, custom_check_functions, validate_custom_check_functions)
                specs.append(spec)
            except ValidationError as exc:
                specs.append(None)
                messages = ChecksValidator._translate_validation_error(exc, check)
                # A field-level failure (e.g. a malformed 'check' block) skips the model validator
                # that checks criticality. The pre-migration validator always reported criticality
                # regardless of the check block, so supplement it here to preserve that parity.
                if ChecksValidator._model_validator_skipped(exc):
                    messages = CheckSpec.criticality_errors(check) + messages
                status.add_errors(messages)

        return status, specs

    @staticmethod
    def _model_validator_skipped(exc: ValidationError) -> bool:
        """Return True if a field-level error prevented the *_validate_semantics* model validator.

        Field errors carry a non-empty *loc*; the model validator raises a root *value_error* with
        an empty *loc*. So the presence of any error with a *loc* means a field failed and the
        model validator (which checks criticality) did not run.

        Args:
            exc: The Pydantic validation error raised for a single check.

        Returns:
            True if the model validator was skipped due to a field-level error.
        """
        return any(error["loc"] for error in exc.errors())

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
