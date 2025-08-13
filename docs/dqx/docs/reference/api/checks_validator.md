---
sidebar_label: checks_validator
title: databricks.labs.dqx.checks_validator
---

## ChecksValidationStatus Objects

```python
@dataclass(frozen=True)
class ChecksValidationStatus()
```

Class to represent the validation status.

#### add\_error

```python
def add_error(error: str)
```

Add an error to the validation status.

#### add\_errors

```python
def add_errors(errors: list[str])
```

Add an error to the validation status.

#### has\_errors

```python
@property
def has_errors() -> bool
```

Check if there are any errors in the validation status.

#### errors

```python
@property
def errors() -> list[str]
```

Get the list of errors in the validation status.

#### to\_string

```python
def to_string() -> str
```

Convert the validation status to a string.

#### \_\_str\_\_

```python
def __str__() -> str
```

String representation of the ValidationStatus class.

## ChecksValidator Objects

```python
class ChecksValidator()
```

Class to validate quality rules (checks).

