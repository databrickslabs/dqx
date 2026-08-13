# databricks.labs.dqx.checks\_validator

## CheckBlock Objects[​](#checkblock-objects "Direct link to CheckBlock Objects")

```python
class CheckBlock(BaseModel)

```

Pydantic schema for the inner 'check' block of a rule metadata dict.

Validates the structural shape (function name present, for\_each\_column is a non-empty list, arguments is a dict). Function resolution and signature-based argument validation are performed by *CheckSpec*'s semantic validator, which needs this well-formed block to run.

Uses *extra="ignore"* to preserve the pre-migration behaviour: unknown keys in the check block are tolerated (the hand-rolled validator never rejected them). Unknown check-function *arguments* are still reported by *CheckSpec*'s signature validation.

## CheckSpec Objects[​](#checkspec-objects "Direct link to CheckSpec Objects")

```python
class CheckSpec(BaseModel)

```

Pydantic schema for a single top-level rule metadata dict.

This is the single validated representation of a declarative check. Structural validation (required 'check' key, known field types, for\_each\_column shape) is done by Pydantic field validation. Semantic validation — criticality enum value, function resolution and signature- based argument validation — runs in *\_validate\_semantics* (a *model\_validator*) so a bare *CheckSpec.validate\_check(check, ...)* fully validates a check with no second pass.

Pydantic skips *model\_validator(mode="after")* when any field fails, so a malformed 'check' block would suppress the sibling *criticality* error raised there. The pre-migration validator always reported criticality regardless of the check block, so *ChecksValidator.validate\_and\_parse* reproduces that check (via *criticality\_errors*) on the field-failure path to preserve parity.

The *model\_validate* call accepts a *context* dict with:

* *raw\_check*: the original check dict, included verbatim in error messages for context.
* *custom\_check\_functions*: optional mapping of custom function names to callables.
* *validate\_custom\_check\_functions*: if False, unknown/unregistered functions are tolerated (used by the LLM and profiler paths).

Uses *extra="ignore"* to preserve the pre-migration behaviour: unknown top-level keys are tolerated (the hand-rolled validator never rejected them, and storage backends persist extra columns alongside the check). Rejecting them would be a breaking change for existing check definitions and would fail the load -> apply round-trip for stored checks.

### criticality\_errors[​](#criticality_errors "Direct link to criticality_errors")

```python
@classmethod
def criticality_errors(cls, check: dict) -> list[str]

```

Return the criticality-value error (as a one-item list) or an empty list.

Reads the criticality from the raw *check* dict so it can run both inside *\_validate\_semantics* and standalone (from *ChecksValidator.validate\_and\_parse*) when a field error has skipped the model validator. Non-string criticality values are left to the field-level type error and skipped here, so they are not reported twice.

**Arguments**:

* `check` - The check dict to read *criticality* from and include in the message.

**Returns**:

A single-element list with the error message, or an empty list when valid.

### validate\_check[​](#validate_check "Direct link to validate_check")

```python
@classmethod
def validate_check(
        cls,
        check: dict,
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True) -> "CheckSpec"

```

Validate and parse a single check dict, binding the semantic-validation context.

This is the supported entry point: it wires up the *context* that *\_validate\_semantics* (and the *criticality\_errors* check it runs) rely on, so callers cannot accidentally invoke *model\_validate* without it (which would run strict function validation against the projected model and ignore the *validate\_custom\_check\_functions* tolerance flag).

**Arguments**:

* `check` - The check metadata dict to validate.
* `custom_check_functions` - Optional mapping of custom function names to callables.
* `validate_custom_check_functions` - If False, unknown/unregistered functions are tolerated (used by the LLM and profiler paths).

**Returns**:

The validated *CheckSpec*.

**Raises**:

* `ValidationError` - If the check fails structural or semantic validation.

## ChecksValidationStatus Objects[​](#checksvalidationstatus-objects "Direct link to ChecksValidationStatus Objects")

```python
class ChecksValidationStatus(BaseModel)

```

Class to represent the validation status.

This model is used as a mutable accumulator: *add\_error* and *add\_errors* append to the *errors* list in place. Pydantic instantiates a fresh copy of the *\[]* default for each instance, so the list is never shared between instances created via the constructor. The only sharing risk is a shallow *model\_copy()* (without *deep=True*); this model is never shallow-copied, but use *model\_copy(deep=True)* if that ever changes.

### add\_error[​](#add_error "Direct link to add_error")

```python
def add_error(error: str)

```

Add an error to the validation status.

### add\_errors[​](#add_errors "Direct link to add_errors")

```python
def add_errors(errors: list[str])

```

Add errors to the validation status.

### has\_errors[​](#has_errors "Direct link to has_errors")

```python
@property
def has_errors() -> bool

```

Check if there are any errors in the validation status.

### to\_string[​](#to_string "Direct link to to_string")

```python
def to_string() -> str

```

Convert the validation status to a string.

### \_\_str\_\_[​](#__str__ "Direct link to __str__")

```python
def __str__() -> str

```

String representation of the ValidationStatus class.

## ChecksValidator Objects[​](#checksvalidator-objects "Direct link to ChecksValidator Objects")

```python
class ChecksValidator()

```

Validates declarative quality rules (checks).

All validation lives on the *CheckSpec* Pydantic model: structural shape and the criticality value via field validation, and the remaining semantic checks (function resolution, signature- based argument validation) via its *model\_validator*. This class is a thin orchestration layer that runs *CheckSpec.validate\_check* per check and translates any Pydantic *ValidationError* into the human-readable messages callers and tests expect. *validate\_and\_parse* additionally returns the parsed specs so callers (e.g. the deserializer) reuse them instead of parsing a second time.

### validate\_checks[​](#validate_checks "Direct link to validate_checks")

```python
@staticmethod
def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True
) -> ChecksValidationStatus

```

Validate a list of check metadata dicts.

**Arguments**:

* `checks` - List of check metadata dicts to validate.
* `custom_check_functions` - Optional mapping of custom function names to callables.
* `validate_custom_check_functions` - If False, unknown/unregistered functions are tolerated (used by LLM and profiler paths).

**Returns**:

A *ChecksValidationStatus* accumulating all errors found.

### validate\_and\_parse[​](#validate_and_parse "Direct link to validate_and_parse")

```python
@staticmethod
def validate_and_parse(
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    validate_custom_check_functions: bool = True
) -> tuple[ChecksValidationStatus, list[CheckSpec | None]]

```

Validate checks and return both the status and the parsed specs.

Each check is validated exactly once via *CheckSpec.validate\_check*. On success the parsed *CheckSpec* is returned so callers can build rules from the typed representation without a second parse; on failure *None* is returned in its place and the errors are accumulated.

**Arguments**:

* `checks` - List of check metadata dicts to validate.
* `custom_check_functions` - Optional mapping of custom function names to callables.
* `validate_custom_check_functions` - If False, unknown/unregistered functions are tolerated.

**Returns**:

A *(status, specs)* tuple; *specs* is index-aligned with *checks* (each entry is the parsed *CheckSpec* or *None* when that check failed validation).
