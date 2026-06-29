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
import operator

from databricks.labs.dqx.errors import InvalidConditionError

# ---------------------------------------------------------------------------
# Operator maps
# ---------------------------------------------------------------------------

_BOOL_OPS: dict[type, str] = {
    ast.And: "and",
    ast.Or: "or",
}

_UNARY_OPS: dict[type, object] = {
    ast.Not: operator.not_,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

_BIN_OPS: dict[type, object] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_CMP_OPS: dict[type, object] = {
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
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


def _eval_constant(node: ast.Constant, _metrics: dict[str, object] | None = None) -> object:
    """Evaluate a :class:`ast.Constant` node.

    The *_metrics* parameter is accepted (and ignored) so that all evaluators
    in the dispatch table share the same ``(node, metrics)`` signature.
    Constant-type validation has already been performed by the pre-pass.
    """
    return node.value


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


def _eval_boolop(node: ast.BoolOp, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.BoolOp` (``and`` / ``or``) node.

    Short-circuit evaluation is safe here because the full-tree pre-pass has
    already validated every node in the tree before any evaluation begins.
    """
    if isinstance(node.op, ast.And):
        result: object = True
        for value_node in node.values:
            result = _walk(value_node, metrics)
            if not result:
                return result
        return result
    # Or
    result = False
    for value_node in node.values:
        result = _walk(value_node, metrics)
        if result:
            return result
    return result


def _eval_unaryop(node: ast.UnaryOp, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.UnaryOp` (``not``, ``-``, ``+``) node."""
    op_func = _UNARY_OPS.get(type(node.op))
    if op_func is None:
        raise InvalidConditionError(f"Unary operator '{type(node.op).__name__}' is not allowed in conditions")
    operand = _walk(node.operand, metrics)
    try:
        return op_func(operand)  # type: ignore[operator]
    except (TypeError, OverflowError) as exc:
        raise InvalidConditionError(f"Operator error in condition: {exc}") from exc


def _eval_binop(node: ast.BinOp, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.BinOp` (+, -, *, /, //, %, **) node."""
    op_func = _BIN_OPS.get(type(node.op))
    if op_func is None:
        raise InvalidConditionError(f"Binary operator '{type(node.op).__name__}' is not allowed in conditions")
    left = _walk(node.left, metrics)
    right = _walk(node.right, metrics)
    try:
        return op_func(left, right)  # type: ignore[operator]
    except (ZeroDivisionError, TypeError, OverflowError) as exc:
        raise InvalidConditionError(f"Operator error in condition: {exc}") from exc


def _eval_compare(node: ast.Compare, metrics: dict[str, object] | None) -> object:
    """Evaluate a :class:`ast.Compare` node, including chained comparisons."""
    for cmp_op in node.ops:
        if type(cmp_op) not in _CMP_OPS:
            raise InvalidConditionError(f"Comparison operator '{type(cmp_op).__name__}' is not allowed in conditions")
    left = _walk(node.left, metrics)
    for cmp_op, comparator_node in zip(node.ops, node.comparators):
        right = _walk(comparator_node, metrics)
        op_func = _CMP_OPS[type(cmp_op)]
        try:
            if not op_func(left, right):  # type: ignore[operator]
                return False
        except (TypeError, OverflowError) as exc:
            raise InvalidConditionError(f"Operator error in condition: {exc}") from exc
        left = right
    return True


# ---------------------------------------------------------------------------
# Dispatch table  (node type → evaluator)
# ---------------------------------------------------------------------------

_EVALUATORS: dict[type, object] = {
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
        return evaluator(node, metrics)  # type: ignore[call-arg,operator]

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

    Usage::

        ConditionEvaluator.validate("error_row_count > 0")
        result = ConditionEvaluator.evaluate("error_row_count > 0", {"error_row_count": 5})
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
