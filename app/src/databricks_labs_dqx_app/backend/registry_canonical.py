"""Behaviour-equivalent forms of a registry rule's polarity and parameters.

Rules that render the same check must dedup as one rule, so identity hashes
(``registry_fingerprint``, ``generic_rule_shape``) compare these canonical
forms instead of the raw stored values:

* polarity: the materializer negates only on ``"fail"``, so a missing polarity
  behaves exactly like ``"pass"``;
* ``dqx_native`` parameters: the materializer omits ``None`` values, so DQX
  falls back to the function's default — an unset parameter, one set to
  ``None`` and one set explicitly to the default value are the same check.
"""

import functools
import inspect
import logging
from types import MappingProxyType
from typing import Any

from databricks_labs_dqx_app.backend.registry_models import RuleDefinition

logger = logging.getLogger(__name__)

__all__ = ["canonical_parameters", "canonical_polarity", "native_parameter_defaults"]

_JSON_SCALARS = (bool, int, float, str)


def canonical_polarity(polarity: str | None) -> str:
    return "fail" if polarity == "fail" else "pass"


@functools.cache
def native_parameter_defaults(function: str) -> MappingProxyType[str, Any]:
    """JSON-scalar keyword defaults of the DQX check function *function* (empty when unknown)."""
    from databricks.labs.dqx.checks_resolver import resolve_check_function  # noqa: PLC0415

    try:
        fn = resolve_check_function(function, fail_on_missing=False)
        signature = inspect.signature(fn) if fn is not None else None
    except Exception as exc:
        logger.warning("Could not read defaults of DQX check function %r: %s", function, exc)
        signature = None
    if signature is None:
        return MappingProxyType({})
    return MappingProxyType(
        {
            name: param.default
            for name, param in signature.parameters.items()
            if param.default is None or type(param.default) in _JSON_SCALARS
        }
    )


def _is_default(value: Any, default: Any) -> bool:
    return value is None or (type(value) is type(default) and value == default)


def canonical_parameters(mode: str, definition: RuleDefinition) -> list[dict[str, Any]]:
    """Parameters sorted by name, minus native parameters left at the function's default."""
    defaults: MappingProxyType[str, Any] = MappingProxyType({})
    body_arguments: dict[str, Any] = {}
    if mode == "dqx_native":
        function = str(definition.body.get("function") or "").strip()
        defaults = native_parameter_defaults(function) if function else defaults
        arguments = definition.body.get("arguments")
        body_arguments = arguments if isinstance(arguments, dict) else {}
    params = [
        {"name": p.name, "type": p.type, "value": p.value}
        for p in definition.parameters
        if not (p.name in defaults and p.name not in body_arguments and _is_default(p.value, defaults[p.name]))
    ]
    return sorted(params, key=lambda p: p["name"])
