"""Resolve DQX check functions by name and inspect their signatures.

The resolver is shared by the check-functions route (whose introspected
parameters drive the UI's ``fnSupportsNegate``), the registry service and the
materializer. The latter two use :func:`supports_negate` to decide whether a
stored polarity can be passed to the function as ``negate``.
"""

import inspect
from collections.abc import Callable
from functools import lru_cache
from typing import Any

from databricks_labs_dqx_app.backend.logger import logger


def resolve_check_callable(name: str) -> Callable[..., Any] | None:
    """Look the function up by name without raising on missing entries.

    We rely on ``checks_resolver`` so geo + optional modules are searched
    too, but we don't want a single missing entry to fail the caller:
    the registry is contributed-to by side-effect of imports and the
    optional modules can legitimately be unavailable on some deployments
    (e.g. PII module gated behind a separate install).
    """
    from databricks.labs.dqx.checks_resolver import resolve_check_function

    try:
        return resolve_check_function(name, fail_on_missing=False)
    except Exception as exc:
        # Resolution can fail for reasons beyond "name not found":
        # ``resolve_check_function`` triggers side-effect imports of
        # optional check modules (PII, geo, etc.) that may not be
        # installed on every deployment. A missing optional module
        # raises ImportError; a syntactically-valid but broken module
        # could raise AttributeError, TypeError, or anything its
        # ``@register_rule`` evaluator throws. The contract here is
        # "callers must keep working when one optional module misbehaves" —
        # returning None lets the caller skip the broken entry instead of
        # failing outright. See the BLE001 policy in pyproject.toml.
        logger.warning(f"Failed to resolve DQX check function {name!r}: {exc}")
        return None


@lru_cache(maxsize=256)
def supports_negate(function: str) -> bool:
    """Whether the DQX check *function* takes a ``negate`` argument.

    A function that can't be resolved here (a custom check, an optional
    module that isn't installed) counts as supporting it, so a stored
    polarity is kept and DQX reports the real problem when the check runs.
    The result is cached for the life of the process, including for
    functions that failed to resolve.
    """
    func = resolve_check_callable(function)
    if func is None:
        return True
    try:
        return "negate" in inspect.signature(func).parameters
    except (TypeError, ValueError):
        return True
