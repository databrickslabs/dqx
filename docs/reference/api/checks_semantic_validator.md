# databricks.labs.dqx.checks_semantic_validator

Semantic (ruleset-level) validation for DQ checks.

## ChecksSemanticValidationMode Objects[​](#checkssemanticvalidationmode-objects "Direct link to ChecksSemanticValidationMode Objects")

```python
class ChecksSemanticValidationMode()

```

Controls how semantic validation issues are surfaced.

#### WARN[​](#warn "Direct link to WARN")

Log warnings but continue

#### FAIL[​](#fail "Direct link to FAIL")

Raise an exception if any issues are found

## ChecksSemanticValidator Objects[​](#checkssemanticvalidator-objects "Direct link to ChecksSemanticValidator Objects")

```python
class ChecksSemanticValidator()

```

Provides semantic validation for a collection of DQ rules.

Detects ruleset-level issues such as:

* Duplicate rules: two rules with the same function, arguments, criticality, and filter.
* Conflicting rules: two rules targeting the same function and column but with different arguments (e.g. two *is_in_range* checks with different thresholds).

**Notes**:

Rules that use raw Spark SQL expressions (via the *sql_expression* function) are not deeply inspected — only structured metadata (function name, column, arguments) is compared. Document this limitation when such checks are used.

Usage::

# Just get a list of issues:

issues = ChecksSemanticValidator.validate_ruleset(checks)

# Or apply with configurable behavior:

ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.WARN) ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.FAIL)

### detect_duplicates[​](#detect_duplicates "Direct link to detect_duplicates")

```python
@staticmethod
def detect_duplicates(checks: list[dict]) -> list[str]

```

Detect rules that are completely identical.

Two rules are duplicates when they share the same function, arguments, criticality, and filter expression.

**Arguments**:

* `checks` - The ruleset to inspect.

**Returns**:

A list of issue message strings, empty if no duplicates found.

### detect_conflicts[​](#detect_conflicts "Direct link to detect_conflicts")

```python
@staticmethod
def detect_conflicts(checks: list[dict]) -> list[str]

```

Detect rules targeting the same function and column with different arguments.

For example, two *is_in_range* checks on *age* with different min/max thresholds would be flagged, as this is likely a misconfiguration.

**Arguments**:

* `checks` - The ruleset to inspect.

**Returns**:

A list of issue message strings, empty if no conflicts found.

### validate_ruleset[​](#validate_ruleset "Direct link to validate_ruleset")

```python
@staticmethod
def validate_ruleset(checks: list[dict]) -> list[str]

```

Run all semantic checks and return a combined list of issue messages.

**Arguments**:

* `checks` - The ruleset to inspect.

**Returns**:

A list of issue strings. Empty list means the ruleset is semantically clean.

### apply[​](#apply "Direct link to apply")

```python
@staticmethod
def apply(checks: list[dict],
          mode: str | None = ChecksSemanticValidationMode.WARN) -> None

```

Run semantic validation and surface issues according to the chosen mode.

This is the main entry point called from *validate_checks*, *save_checks*, and *load_checks* with configurable behavior.

**Arguments**:

* `checks` - The ruleset to inspect.
* `mode` - One of *ChecksSemanticValidationMode.WARN* (default), *ChecksSemanticValidationMode.FAIL*, or *None*. In WARN mode, issues are logged as warnings and execution continues. In FAIL mode, a *ValueError* is raised listing all issues found. When *None*, semantic validation is skipped entirely.

**Raises**:

* `ValueError` - If *mode* is FAIL and any semantic issues are detected.
* `ValueError` - If an unsupported mode value is passed.
