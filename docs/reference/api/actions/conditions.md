# databricks.labs.dqx.actions.conditions

Safe AST-based condition evaluator for DQX action gating.

Conditions are arithmetic/boolean expressions evaluated against a metrics dict (e.g. *"error\_row\_count > 0 or warning\_row\_count > 0"*). They are parsed with *ast.parse* in *"eval"* mode and walked by a restricted visitor that allows only a small, safe subset of AST nodes. Any node outside that allowlist — including *ast.Call*, *ast.Attribute*, *ast.Subscript*, lambdas, and all comprehensions — raises *InvalidConditionError*.

Security: *eval()* / *exec()* / *compile(mode="exec")* are never used. The evaluator is purely structural, operating on the parsed AST.

## ConditionEvaluator Objects[​](#conditionevaluator-objects "Direct link to ConditionEvaluator Objects")

```python
class ConditionEvaluator()

```

Safe evaluator for DQX action-gating condition expressions.

Conditions are simple arithmetic/boolean expressions of the form *"error\_row\_count > 0 or warning\_row\_count > 0"*. They are parsed via *ast.parse* (*mode="eval"*) and evaluated by a restricted AST walker that allows only:

* Literals: *ast.Constant* with *int*, *float*, *bool*, or *str*
* Names: *ast.Name* — resolved from the *metrics* dict at evaluate time; numeric strings are coerced to *float* automatically
* Boolean ops: and, or
* Unary ops: not, unary minus, unary plus
* Binary ops: add, subtract, multiply, divide, floor-divide, modulo, power
* Comparisons: lt, le, gt, ge, eq, ne

Any other node type (calls, attribute access, subscripts, lambdas, comprehensions, …) raises *InvalidConditionError*.

A full-tree structural pre-pass is performed unconditionally before any evaluation, so short-circuit evaluation cannot bypass the allowlist.

Usage:

```python
ConditionEvaluator.validate("error_row_count > 0")
result = ConditionEvaluator.evaluate("error_row_count > 0", {"error_row_count": 5})

```

### validate[​](#validate "Direct link to validate")

```python
@staticmethod
def validate(condition: str) -> None

```

Validate *condition* syntax and structure without requiring metrics.

Parses and walks every node in the AST, rejecting disallowed node types and syntax errors. Name resolution is *not* performed — unknown metric names are only caught at *evaluate* time. Call this method at *DQAction* construction time to surface malformed conditions early.

**Arguments**:

* `condition` - The condition expression string to validate.

**Raises**:

* `InvalidConditionError` - If *condition* has a syntax error or contains an AST node type that is not allowed.

### evaluate[​](#evaluate "Direct link to evaluate")

```python
@staticmethod
def evaluate(condition: str, metrics: dict[str, object]) -> bool

```

Evaluate *condition* against *metrics* and return a bool result.

Parses *condition*, performs a full-tree structural validation pass, resolves *ast.Name* nodes from *metrics* (coercing numeric strings to *float*), and returns the final truth value.

**Arguments**:

* `condition` - The condition expression string to evaluate.
* `metrics` - Mapping of metric name to value. Values that are numeric strings are coerced to *float* for arithmetic/comparison.

**Returns**:

Boolean result of the evaluated condition.

**Raises**:

* `InvalidConditionError` - If *condition* has a syntax error, contains a disallowed AST node, or references a name not present in *metrics*.
