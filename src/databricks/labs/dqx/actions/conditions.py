"""Safe AST-based condition evaluator for DQX action gating.

Conditions are arithmetic/boolean expressions evaluated against a metrics dict
(e.g. ``"error_row_count > 0 or warning_row_count > 0"``).  They are parsed
with :func:`ast.parse` in ``"eval"`` mode and walked by a restricted visitor
that allows only a small, safe subset of AST nodes.  Any node outside that
allowlist — including :class:`ast.Call`, :class:`ast.Attribute`,
:class:`ast.Subscript`, lambdas, and all comprehensions — raises
:class:`~databricks.labs.dqx.errors.InvalidConditionError`.

Security: ``eval()`` / ``exec()`` / ``compile(mode="exec")`` are never used.
The evaluator is purely structural, operating on the parsed AST.
"""

import ast
import collections.abc

from typing import SupportsFloat, cast

from databricks.labs.dqx.errors import InvalidConditionError

# ---------------------------------------------------------------------------
# Typed operator wrapper functions
# ---------------------------------------------------------------------------
# These thin wrappers give each operator an explicit signature that mypy can
# verify.  Arithmetic/ordering operators require numeric operands; they cast
# to float here — the existing _coerce_numeric call in _eval_name already
# converts numeric strings, so a non-float/int at this point means the user
# passed a non-numeric metric value, which Python will surface as TypeError
# and the callers convert to InvalidConditionError.


def _op_not(val: object) -> bool:
    """Logical not of *val*."""
    return not val


def _op_neg(val: object) -> object:
    """Arithmetic negation of *val* (must be numeric)."""
    return -cast(float, val)


def _op_pos(val: object) -> object:
    """Arithmetic identity of *val* (must be numeric)."""
    return +cast(float, val)


def _op_add(lhs: object, rhs: object) -> object:
    """Add *lhs* and *rhs* (must be numeric)."""
    return cast(float, lhs) + cast(float, rhs)


def _op_sub(lhs: object, rhs: object) -> object:
    """Subtract *rhs* from *lhs* (must be numeric)."""
    return cast(float, lhs) - cast(float, rhs)


def _op_mul(lhs: object, rhs: object) -> object:
    """Multiply *lhs* and *rhs* (must be numeric)."""
    return cast(float, lhs) * cast(float, rhs)


def _op_truediv(lhs: object, rhs: object) -> object:
    """Divide *lhs* by *rhs* (must be numeric)."""
    return cast(float, lhs) / cast(float, rhs)


def _op_floordiv(lhs: object, rhs: object) -> object:
    """Floor-divide *lhs* by *rhs* (must be numeric)."""
    return cast(float, lhs) // cast(float, rhs)


def _op_mod(lhs: object, rhs: object) -> object:
    """Modulo *lhs* by *rhs* (must be numeric)."""
    return cast(float, lhs) % cast(float, rhs)


def _op_pow(lhs: object, rhs: object) -> object:
    """Raise *lhs* to the power *rhs* (must be numeric).

    Operands are coerced to *float* before exponentiation. *typing.cast* is a runtime
    no-op, so casting alone would leave ``int ** int`` to build an unbounded
    arbitrary-precision integer (a CPU/memory denial-of-service for conditions such as
    ``a ** b`` with large integer metrics). Real *float* coercion bounds the result —
    an oversized exponentiation raises *OverflowError*, which the caller maps to
    *InvalidConditionError*.
    """
    return float(cast(str | SupportsFloat, lhs)) ** float(cast(str | SupportsFloat, rhs))


def _op_lt(lhs: object, rhs: object) -> bool:
    """Return whether *lhs* is less than *rhs* (must be numeric)."""
    return cast(float, lhs) < cast(float, rhs)


def _op_le(lhs: object, rhs: object) -> bool:
    """Return whether *lhs* is less than or equal to *rhs* (must be numeric)."""
    return cast(float, lhs) <= cast(float, rhs)


def _op_gt(lhs: object, rhs: object) -> bool:
    """Return whether *lhs* is greater than *rhs* (must be numeric)."""
    return cast(float, lhs) > cast(float, rhs)


def _op_ge(lhs: object, rhs: object) -> bool:
    """Return whether *lhs* is greater than or equal to *rhs* (must be numeric)."""
    return cast(float, lhs) >= cast(float, rhs)


def _op_eq(lhs: object, rhs: object) -> bool:
    """Return *lhs* == *rhs* (valid for any object)."""
    return lhs == rhs


def _op_ne(lhs: object, rhs: object) -> bool:
    """Return *lhs* != *rhs* (valid for any object)."""
    return lhs != rhs


# ---------------------------------------------------------------------------
# Operator maps — typed Callable values so callers need no type: ignore
# ---------------------------------------------------------------------------

_BOOL_OPS: dict[type, str] = {
    ast.And: "and",
    ast.Or: "or",
}

_UNARY_OPS: dict[type[ast.unaryop], collections.abc.Callable[[object], object]] = {
    ast.Not: _op_not,
    ast.USub: _op_neg,
    ast.UAdd: _op_pos,
}

_BIN_OPS: dict[type[ast.operator], collections.abc.Callable[[object, object], object]] = {
    ast.Add: _op_add,
    ast.Sub: _op_sub,
    ast.Mult: _op_mul,
    ast.Div: _op_truediv,
    ast.FloorDiv: _op_floordiv,
    ast.Mod: _op_mod,
    ast.Pow: _op_pow,
}

_CMP_OPS: dict[type[ast.cmpop], collections.abc.Callable[[object, object], bool]] = {
    ast.Lt: _op_lt,
    ast.LtE: _op_le,
    ast.Gt: _op_gt,
    ast.GtE: _op_ge,
    ast.Eq: _op_eq,
    ast.NotEq: _op_ne,
}

# Constant types allowed in conditions
_ALLOWED_CONSTANT_TYPES = (int, float, bool, str)

# ---------------------------------------------------------------------------
# Structural allowlist: every node type that may appear in a valid condition.
# Used by the full-tree pre-pass (_validate_tree) — default-deny.
# ---------------------------------------------------------------------------

_ALLOWED_NODE_TYPES: frozenset[type] = frozenset(
    {
        ast.Expression,
        ast.BoolOp,
        ast.UnaryOp,
        ast.BinOp,
        ast.Compare,
        ast.Name,
        ast.Constant,
        # Boolean op singletons
        ast.And,
        ast.Or,
        # Unary op singletons
        ast.Not,
        ast.USub,
        ast.UAdd,
        # Binary op singletons
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.FloorDiv,
        ast.Mod,
        ast.Pow,
        # Comparison op singletons
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
        ast.Eq,
        ast.NotEq,
        # Context node that Python attaches to ast.Name (load context only)
        ast.Load,
        # Abstract base classes that Python includes in ast.walk output
        ast.boolop,
        ast.unaryop,
        ast.operator,
        ast.cmpop,
        ast.expr,
        ast.expr_context,
    }
)


def _validate_tree(tree: ast.AST) -> None:
    """Walk every node in *tree* and reject any node type not in the allowlist.

    This full-tree pre-pass is unconditionally called before any evaluation so
    that short-circuit evaluation cannot bypass the structural check.  A single
    disallowed node anywhere in the tree — even in a branch that would never be
    reached at runtime — causes an :class:`~databricks.labs.dqx.errors.InvalidConditionError`.

    Args:
        tree: The root AST node (typically an :class:`ast.Expression`).

    Raises:
        InvalidConditionError: If any node type is not in the allowlist.
    """
    for node in ast.walk(tree):
        node_type = type(node)
        if node_type not in _ALLOWED_NODE_TYPES:
            raise InvalidConditionError(
                f"AST node type '{node_type.__name__}' is not allowed in conditions; "
                "only arithmetic, comparison, boolean, and literal expressions are permitted"
            )
        # Additional constant-type check — the node type ast.Constant is allowed
        # but only for specific value types.
        if isinstance(node, ast.Constant) and not isinstance(node.value, _ALLOWED_CONSTANT_TYPES):
            raise InvalidConditionError(f"Constant type '{type(node.value).__name__}' is not allowed in conditions")


def _coerce_numeric(value: object) -> object:
    """Coerce a numeric string to *float* for arithmetic/comparison.

    If *value* is already a number, it is returned as-is.  If it is a
    *str* that parses as a float, the float is returned.  All other types are
    returned unchanged so that the normal Python operator will produce a
    meaningful ``TypeError`` if the expression is type-incompatible.
    """
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return value
    return value


# ---------------------------------------------------------------------------
# Node-specific evaluation helpers
# ---------------------------------------------------------------------------

# Shared evaluator signature: takes an AST node and optional metrics, returns object.
_EvaluatorFn = collections.abc.Callable[["ast.AST", "dict[str, object] | None"], object]


def _eval_constant(node: ast.AST, _metrics: dict[str, object] | None = None) -> object:
    """Evaluate a :class:`ast.Constant` node.

    The *_metrics* parameter is accepted (and ignored) so that all evaluators
    in the dispatch table share the same ``(node, metrics)`` signature.
    Constant-type validation has already been performed by the pre-pass.
    """
    return cast(ast.Constant, node).value


def _eval_name(node: ast.Name, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.Name` node against *metrics*.

    Returns a placeholder when *metrics* is *None* (validate-only mode).
    """
    if metrics is None:
        return 0  # placeholder that satisfies downstream arithmetic
    name = node.id
    if name not in metrics:
        raise InvalidConditionError(f"Condition references unknown metric '{name}'; available: {sorted(metrics)}")
    return _coerce_numeric(metrics[name])


def _eval_boolop(node: ast.AST, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.BoolOp` (``and`` / ``or``) node.

    Short-circuit evaluation is safe here because the full-tree pre-pass has
    already validated every node in the tree before any evaluation begins.
    """
    boolop_node = cast(ast.BoolOp, node)
    if isinstance(boolop_node.op, ast.And):
        result: object = True
        for value_node in boolop_node.values:
            result = _walk(value_node, metrics)
            if not result:
                return result
        return result
    # Or
    result = False
    for value_node in boolop_node.values:
        result = _walk(value_node, metrics)
        if result:
            return result
    return result


def _eval_unaryop(node: ast.AST, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.UnaryOp` (``not``, ``-``, ``+``) node."""
    unary_node = cast(ast.UnaryOp, node)
    op_func = _UNARY_OPS.get(type(unary_node.op))
    if op_func is None:
        raise InvalidConditionError(f"Unary operator '{type(unary_node.op).__name__}' is not allowed in conditions")
    operand = _walk(unary_node.operand, metrics)
    try:
        return op_func(operand)
    except (TypeError, OverflowError) as exc:
        raise InvalidConditionError(f"Operator error in condition: {exc}") from exc


def _eval_binop(node: ast.AST, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.BinOp` (+, -, *, /, //, %, **) node."""
    bin_node = cast(ast.BinOp, node)
    op_func = _BIN_OPS.get(type(bin_node.op))
    if op_func is None:
        raise InvalidConditionError(f"Binary operator '{type(bin_node.op).__name__}' is not allowed in conditions")
    left = _walk(bin_node.left, metrics)
    right = _walk(bin_node.right, metrics)
    try:
        return op_func(left, right)
    except (ZeroDivisionError, TypeError, OverflowError, ValueError) as exc:
        raise InvalidConditionError(f"Operator error in condition: {exc}") from exc


def _eval_compare(node: ast.AST, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.Compare` node, including chained comparisons."""
    cmp_node = cast(ast.Compare, node)
    for cmp_op in cmp_node.ops:
        if type(cmp_op) not in _CMP_OPS:
            raise InvalidConditionError(f"Comparison operator '{type(cmp_op).__name__}' is not allowed in conditions")
    left = _walk(cmp_node.left, metrics)
    for cmp_op, comparator_node in zip(cmp_node.ops, cmp_node.comparators):
        right = _walk(comparator_node, metrics)
        op_func = _CMP_OPS[type(cmp_op)]
        try:
            if not op_func(left, right):
                return False
        except (TypeError, OverflowError) as exc:
            raise InvalidConditionError(f"Operator error in condition: {exc}") from exc
        left = right
    return True


# ---------------------------------------------------------------------------
# Dispatch table  (node type → evaluator)
# ---------------------------------------------------------------------------

_EVALUATORS: dict[type[ast.AST], _EvaluatorFn] = {
    ast.Constant: _eval_constant,
    ast.BoolOp: _eval_boolop,
    ast.UnaryOp: _eval_unaryop,
    ast.BinOp: _eval_binop,
    ast.Compare: _eval_compare,
}


# ---------------------------------------------------------------------------
# Core AST walker
# ---------------------------------------------------------------------------


def _walk(node: ast.AST, metrics: dict[str, object] | None) -> object:
    """Recursively evaluate *node* against *metrics*.

    When *metrics* is *None* (validate-only mode) the function performs a
    structural walk without resolving :class:`ast.Name` nodes — unknown names
    are not flagged at this stage.  The full-tree allowlist check is performed
    by :func:`_validate_tree` before this function is ever called.
    """
    if isinstance(node, ast.Expression):
        return _walk(node.body, metrics)

    if isinstance(node, ast.Name):
        return _eval_name(node, metrics)

    evaluator = _EVALUATORS.get(type(node))
    if evaluator is not None:
        return evaluator(node, metrics)

    raise InvalidConditionError(
        f"AST node type '{type(node).__name__}' is not allowed in conditions; "
        "only arithmetic, comparison, boolean, and literal expressions are permitted"
    )


# ---------------------------------------------------------------------------
# Shared parse helper
# ---------------------------------------------------------------------------


def _parse_condition(condition: str) -> ast.Expression:
    """Parse *condition* string into an :class:`ast.Expression` tree.

    Raises:
        InvalidConditionError: If *condition* is empty or has a syntax error.
    """
    if not condition or not condition.strip():
        raise InvalidConditionError("Condition expression must not be empty")
    try:
        return ast.parse(condition, mode="eval")
    except SyntaxError as exc:
        raise InvalidConditionError(f"Condition has a syntax error: {exc}") from exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class ConditionEvaluator:
    """Safe evaluator for DQX action-gating condition expressions.

    Conditions are simple arithmetic/boolean expressions of the form
    ``"error_row_count > 0 or warning_row_count > 0"``.  They are parsed via
    :func:`ast.parse` (``mode="eval"``) and evaluated by a restricted AST
    walker that allows only:

    - Literals: :class:`ast.Constant` with *int*, *float*, *bool*, or *str*
    - Names: :class:`ast.Name` — resolved from the *metrics* dict at evaluate
      time; numeric strings are coerced to *float* automatically
    - Boolean ops: ``and``, ``or``
    - Unary ops: ``not``, ``-``, ``+``
    - Binary ops: ``+``, ``-``, ``*``, ``/``, ``//``, ``%``, ``**``
    - Comparisons: ``<``, ``<=``, ``>``, ``>=``, ``==``, ``!=``

    Any other node type (calls, attribute access, subscripts, lambdas,
    comprehensions, …) raises :class:`~databricks.labs.dqx.errors.InvalidConditionError`.

    A full-tree structural pre-pass is performed unconditionally before any
    evaluation, so short-circuit evaluation cannot bypass the allowlist.

    Usage:

    ```python
    ConditionEvaluator.validate("error_row_count > 0")
    result = ConditionEvaluator.evaluate("error_row_count > 0", {"error_row_count": 5})
    ```
    """

    @staticmethod
    def validate(condition: str) -> None:
        """Validate *condition* syntax and structure without requiring metrics.

        Parses and walks every node in the AST, rejecting disallowed node types
        and syntax errors.  Name resolution is *not* performed — unknown metric
        names are only caught at :meth:`evaluate` time.  Call this method at
        *DQAction* construction time to surface malformed conditions early.

        Args:
            condition: The condition expression string to validate.

        Raises:
            InvalidConditionError: If *condition* has a syntax error or
                contains an AST node type that is not allowed.
        """
        tree = _parse_condition(condition)
        _validate_tree(tree)
        _walk(tree, metrics=None)

    @staticmethod
    def evaluate(condition: str, metrics: dict[str, object]) -> bool:
        """Evaluate *condition* against *metrics* and return a bool result.

        Parses *condition*, performs a full-tree structural validation pass,
        resolves :class:`ast.Name` nodes from *metrics* (coercing numeric
        strings to *float*), and returns the final truth value.

        Args:
            condition: The condition expression string to evaluate.
            metrics: Mapping of metric name to value. Values that are numeric
                strings are coerced to *float* for arithmetic/comparison.

        Returns:
            Boolean result of the evaluated condition.

        Raises:
            InvalidConditionError: If *condition* has a syntax error,
                contains a disallowed AST node, or references a name not
                present in *metrics*.
        """
        tree = _parse_condition(condition)
        _validate_tree(tree)
        result = _walk(tree, metrics=metrics)
        return bool(result)
