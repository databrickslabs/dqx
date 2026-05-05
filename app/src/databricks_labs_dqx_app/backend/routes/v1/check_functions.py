"""Read-only registry of DQX check functions exposed to the UI.

The backend is the single source of truth for which check functions are
selectable in the rule editor. Keeping the canonical list here (rather
than mirroring it as a hardcoded constant on the client) means the UI
automatically picks up new DQX checks on every backend deploy without a
matching frontend change.

The list is built once per process by introspecting
:data:`databricks.labs.dqx.rule.CHECK_FUNC_REGISTRY` plus
:func:`inspect.signature` on each registered callable. We deliberately
omit cross-table dataset checks (``foreign_key``, ``compare_datasets``,
``sql_query``) because they require a reference dataset and therefore
belong to the cross-table-rules editor, not the single-table editor.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import lru_cache
from typing import Any

from fastapi import APIRouter

from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    CheckFunctionDef,
    CheckFunctionParam,
    CheckFunctionsOut,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Filter / categorize / classify helpers
# ---------------------------------------------------------------------------


# Functions that are intentionally hidden from the single-table-rules editor.
# All three need a *reference* dataset (or are handled via the cross-table SQL
# editor) and therefore don't fit the single-table flow.
_HIDDEN_FROM_SINGLE_TABLE: frozenset[str] = frozenset(
    {
        "foreign_key",
        "compare_datasets",
        "sql_query",
    }
)


# Hand-curated mapping from function name to UX category. New DQX checks
# fall back to ``"Other"`` until added here — that's intentional: the UI
# group order is deterministic and we'd rather keep an unexpected new check
# in a clearly-labelled bucket than guess wrong.
_CATEGORIES: dict[str, str] = {
    # Null & Empty
    "is_not_null": "Null & Empty",
    "is_null": "Null & Empty",
    "is_empty": "Null & Empty",
    "is_not_empty": "Null & Empty",
    "is_not_null_and_not_empty": "Null & Empty",
    "is_null_or_empty": "Null & Empty",
    "is_not_null_and_not_empty_array": "Null & Empty",
    # Allowed Values
    "is_in_list": "Allowed Values",
    "is_not_in_list": "Allowed Values",
    "is_not_null_and_is_in_list": "Allowed Values",
    # Numeric & Comparable
    "is_in_range": "Numeric & Comparable",
    "is_not_in_range": "Numeric & Comparable",
    "is_not_less_than": "Numeric & Comparable",
    "is_not_greater_than": "Numeric & Comparable",
    "is_equal_to": "Numeric & Comparable",
    "is_not_equal_to": "Numeric & Comparable",
    # Dates & Times
    "is_valid_date": "Dates & Times",
    "is_valid_timestamp": "Dates & Times",
    "is_data_fresh": "Dates & Times",
    "is_data_fresh_per_time_window": "Dates & Times",
    "is_older_than_n_days": "Dates & Times",
    "is_older_than_col2_for_n_days": "Dates & Times",
    "is_not_in_future": "Dates & Times",
    "is_not_in_near_future": "Dates & Times",
    # Patterns & Regex
    "regex_match": "Patterns & Regex",
    # IP Addresses
    "is_valid_ipv4_address": "IP Addresses",
    "is_valid_ipv6_address": "IP Addresses",
    "is_ipv4_address_in_cidr": "IP Addresses",
    "is_ipv6_address_in_cidr": "IP Addresses",
    # JSON
    "is_valid_json": "JSON",
    "has_json_keys": "JSON",
    "has_valid_json_schema": "JSON",
    # Aggregates
    "is_aggr_not_greater_than": "Aggregates",
    "is_aggr_not_less_than": "Aggregates",
    "is_aggr_equal": "Aggregates",
    "is_aggr_not_equal": "Aggregates",
    "has_no_aggr_outliers": "Aggregates",
    # Outliers
    "has_no_outliers": "Outliers",
    # Uniqueness
    "is_unique": "Uniqueness",
    # Schema
    "has_valid_schema": "Schema",
    # Custom SQL
    "sql_expression": "Custom SQL",
    # Anomaly Detection (optional module)
    "has_no_row_anomalies": "Anomaly Detection",
}


def _category_for(name: str) -> str:
    """Look up the UX bucket; geo-prefixed checks are folded under one bucket."""
    if name in _CATEGORIES:
        return _CATEGORIES[name]
    # Catch-all for the ~25 ``geo.check_funcs`` entries; they all start with
    # ``is_`` and operate on geometry columns.
    if (
        name in {"is_latitude", "is_longitude"}
        or name.startswith(
            (
                "is_geo",
                "is_geom",
                "is_point",
                "is_line",
                "is_polygon",
                "is_multi",
                "is_ogc",
                "is_non_empty",
                "is_not_null_island",
            )
        )
        or "_coordinate_" in name
        or "_area_" in name
        or name.startswith("is_num_points")
        or name.startswith("has_dimension")
    ):
        return "Geospatial"
    return "Other"


def _classify_param_kind(name: str, annotation: object) -> str:
    """Map a Python type annotation to one of the UI-input kinds.

    The mapping is intentionally lossy: the UI only renders a handful of
    input widgets, so we collapse Python's richer type system down to
    ``column`` / ``columns`` / ``boolean`` / ``number`` / ``list`` /
    ``string``. Multi-typed unions (e.g. ``int | float | Decimal | str |
    datetime.date | Column | None``) are classified by the most
    "user-meaningful" widget — for that example we'd pick ``number``
    because the UI's number input also accepts free text for the rare
    column-expression case.
    """
    type_str = "" if annotation is inspect.Parameter.empty else str(annotation)
    lowered = type_str.lower()
    has_list = "list" in lowered
    has_column = "column" in lowered  # captures pyspark Column

    # Composite-key column input (e.g. ``is_unique(columns: list[str | Column])``).
    if name == "columns" and has_list and has_column:
        return "columns"
    # Single column input. DQX uses ``column`` (most checks) or ``column1`` /
    # ``column2`` (e.g. ``is_older_than_col2_for_n_days``); both should be
    # rendered as a column picker.
    if (name == "column" or name.startswith("column")) and has_column and not has_list:
        return "column"

    # ``bool | None`` and friends.
    if "bool" in lowered:
        return "boolean"

    # Anything else that's a list-of-something gets a CSV input.
    if has_list:
        return "list"

    # Numbers cover int / float / Decimal. Many DQX checks accept "number
    # OR datetime OR column-expression"; we still render those as
    # ``number`` since the UI's numeric input falls back to free text for
    # the rarer cases.
    if any(token in lowered for token in ("int", "float", "decimal")):
        return "number"

    # Default: free-form string.
    return "string"


def _serialize_default(value: Any) -> str | None:
    """Render ``inspect.Parameter.default`` as a stable string for the UI."""
    if value is inspect.Parameter.empty or value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _first_doc_line(doc: str | None) -> str:
    if not doc:
        return ""
    for line in doc.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _resolve_callable(name: str) -> Callable[..., Any] | None:
    """Look the function up by name without raising on missing entries.

    We rely on ``checks_resolver`` so geo + optional modules are searched
    too, but we don't want a single missing entry to fail the whole
    endpoint: the registry is contributed-to by side-effect of imports
    and the optional modules can legitimately be unavailable on some
    deployments (e.g. PII module gated behind a separate install).
    """
    from databricks.labs.dqx.checks_resolver import resolve_check_function

    try:
        return resolve_check_function(name, fail_on_missing=False)
    except Exception as exc:  # noqa: BLE001 — defensive
        logger.warning("Failed to resolve DQX check function %r: %s", name, exc)
        return None


def _ensure_optional_modules_loaded() -> None:
    """Import the optional check modules so their ``@register_rule``
    decorators run (and thereby populate ``CHECK_FUNC_REGISTRY``).

    ``checks_resolver._load_optional_check_module`` is the canonical way
    to do this — it caches results and swallows ``ImportError`` for
    deployments where the optional modules aren't installed.
    """
    from databricks.labs.dqx.checks_resolver import _load_optional_check_module  # noqa: PLC0415

    for module_path in (
        "databricks.labs.dqx.anomaly.check_funcs",
        "databricks.labs.dqx.pii.pii_detection_funcs",
    ):
        _load_optional_check_module(module_path)


def _build_param(param: inspect.Parameter) -> CheckFunctionParam:
    annotation = param.annotation
    return CheckFunctionParam(
        name=param.name,
        kind=_classify_param_kind(param.name, annotation),
        required=param.default is inspect.Parameter.empty,
        default=_serialize_default(param.default),
        annotation="" if annotation is inspect.Parameter.empty else str(annotation),
    )


# ---------------------------------------------------------------------------
# Introspection (cached)
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _introspect_check_functions() -> tuple[CheckFunctionDef, ...]:
    """Build the registry response. Cached for the lifetime of the process.

    Returns a tuple (rather than a list) so the cached value is immutable;
    callers always copy it into a fresh list before mutating.
    """
    from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

    _ensure_optional_modules_loaded()

    out: list[CheckFunctionDef] = []
    for name, rule_type in CHECK_FUNC_REGISTRY.items():
        if name in _HIDDEN_FROM_SINGLE_TABLE:
            continue
        func = _resolve_callable(name)
        if func is None:
            # Registered but the module didn't import on this host (e.g.
            # PII gated behind a separate install). Skip silently.
            continue
        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError) as exc:
            logger.warning("inspect.signature failed for %r: %s", name, exc)
            continue
        params: list[CheckFunctionParam] = []
        skip_function = False
        for param_name, param in sig.parameters.items():
            # Filter out internal-only parameters that the engine injects
            # (the rule editor doesn't know how to populate them).
            if param_name in {"row_filter"}:
                continue
            params.append(_build_param(param))
            # Cross-table dataset checks always carry a ref-table parameter;
            # using the parameter name as a defensive escape hatch lets us
            # auto-hide future ``foreign_key``-style additions even if we
            # forget to add them to ``_HIDDEN_FROM_SINGLE_TABLE``.
            if param_name.startswith("ref_") and param_name not in {"ref_columns"}:
                skip_function = True
                break
        if skip_function:
            continue
        out.append(
            CheckFunctionDef(
                name=name,
                rule_type=rule_type,
                category=_category_for(name),
                doc=_first_doc_line(inspect.getdoc(func)),
                params=params,
            )
        )

    # Stable ordering so the UI dropdown is deterministic.
    out.sort(key=lambda f: (f.category, f.name))
    return tuple(out)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=CheckFunctionsOut,
    operation_id="listCheckFunctions",
)
async def list_check_functions() -> CheckFunctionsOut:
    """Return every DQX check function the single-table editor should offer.

    The response is built by introspecting DQX's own registry, so adding
    a new ``@register_rule("row")`` to ``check_funcs.py`` is enough to
    surface it in the UI on the next backend deploy — no frontend change
    required.
    """
    return CheckFunctionsOut(functions=list(_introspect_check_functions()))
