# databricks.labs.dqx.checks\_validator

## ChecksValidationStatus Objects[​](#checksvalidationstatus-objects "Direct link to ChecksValidationStatus Objects")

```python
@dataclass(frozen=True)
class ChecksValidationStatus()

```

Class to represent the validation status.

### add\_error[​](#add_error "Direct link to add_error")

```python
def add_error(error: str)

```

Add an error to the validation status.

### add\_errors[​](#add_errors "Direct link to add_errors")

```python
def add_errors(errors: list[str])

```

Add an error to the validation status.

### has\_errors[​](#has_errors "Direct link to has_errors")

```python
@property
def has_errors() -> bool

```

Check if there are any errors in the validation status.

### errors[​](#errors "Direct link to errors")

```python
@property
def errors() -> list[str]

```

Get the list of errors in the validation status.

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

Ensures each check references a defined function and that merged *arguments* satisfy the function signature: required parameters must be present (*for\_each\_column* may supply *column* or *columns*); a top-level rule *filter* is not a substitute for missing arguments. Unknown argument names and type mismatches (where annotations exist) are reported.
