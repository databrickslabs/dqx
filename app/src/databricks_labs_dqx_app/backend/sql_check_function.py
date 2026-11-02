"""Which DQX check a stored SQL-authored rule body materializes to.

A ``sql`` / ``lowcode`` rule body is stored in one of two shapes: a bare boolean
``predicate`` (row-level, becomes ``sql_expression``) or a whole ``sql_query``
(dataset-level, becomes ``sql_query``). The materializer uses this helper to
choose the check it emits, and the rule Test service uses the same helper (via
the ``check_function`` it is sent) to choose the builder it runs, so the two
cannot disagree about a rule's shape.
"""

from collections.abc import Mapping
from typing import Literal

SqlCheckFunction = Literal["sql_query", "sql_expression"]


def sql_check_function(body: Mapping[str, object]) -> SqlCheckFunction:
    """Return the DQX check function a stored SQL-authored *body* materializes to.

    A body carrying a ``sql_query`` key is a dataset-level query; any other SQL body
    is a row-level ``sql_expression``.

    Args:
        body: The stored rule body (``dq_resolved_rules`` definition body) for a
            ``sql`` or ``lowcode`` rule.

    Returns:
        ``"sql_query"`` for a dataset-level query body, otherwise ``"sql_expression"``.
    """
    return "sql_query" if "sql_query" in body else "sql_expression"
