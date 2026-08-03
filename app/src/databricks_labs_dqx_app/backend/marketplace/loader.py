"""Discover, parse, validate and cache the bundled marketplace packs.

The loader reads every ``*.yaml`` in :data:`PACKS_DIR` on first request,
validates each rule's normalized check dict through the injected validator
(``DQEngine.validate_checks``), and caches the result. A pack that fails to
parse or whose rules fail validation / vocabulary is logged at WARNING and
skipped — a bad pack never crashes startup or the endpoint.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from databricks.labs.dqx.checks_validator import ChecksValidationStatus

from databricks_labs_dqx_app.backend.marketplace.models import (
    VALID_DIMENSIONS,
    VALID_SEVERITIES,
    MarketplacePack,
    MarketplacePackOut,
    MarketplaceRule,
    MarketplaceRuleOut,
)

PACKS_DIR = Path(__file__).parent / "packs"

_cache: list[MarketplacePackOut] | None = None

logger = logging.getLogger(__name__)

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    """Lowercase, replace non-alphanumerics with single hyphens, strip ends."""
    return _SLUG_RE.sub("-", name.lower()).strip("-")


def normalize_check(rule: MarketplaceRule) -> dict[str, Any]:
    """Produce the same normalized shape ``normalizeImportedCheck`` yields.

    *dimension*/*severity*/*name*/*description* land in reserved
    *user_metadata* keys. A *for_each_column* (if authored on the check) is
    preserved at the top level so the DQX validator sees the real check.
    """
    check_block = dict(rule.check)
    fn = str(check_block.get("function", ""))
    args = check_block.get("arguments")
    arguments = args if isinstance(args, dict) else {}
    result: dict[str, Any] = {
        "criticality": rule.criticality,
        "check": {"function": fn, "arguments": arguments},
        "user_metadata": {
            "name": rule.name,
            "description": rule.description,
            "dimension": rule.dimension,
            "severity": rule.severity,
        },
    }
    for_each = check_block.get("for_each_column")
    if isinstance(for_each, list):
        result["for_each_column"] = for_each
    return result


def _validate_rule(
    rule: MarketplaceRule,
    normalized: dict[str, Any],
    validate_fn: Callable[[list[dict[str, Any]]], ChecksValidationStatus],
) -> str | None:
    """Return an error string if the rule is invalid, else None."""
    if rule.dimension not in VALID_DIMENSIONS:
        return f"invalid dimension {rule.dimension!r}"
    if rule.severity not in VALID_SEVERITIES:
        return f"invalid severity {rule.severity!r}"
    status: ChecksValidationStatus = validate_fn([normalized])
    if status.has_errors:
        return status.to_string()
    return None


def _load_pack_file(
    path: Path,
    validate_fn: Callable[[list[dict[str, Any]]], ChecksValidationStatus],
) -> MarketplacePackOut | None:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        pack = MarketplacePack.model_validate(raw)
    except Exception as exc:
        logger.warning("Skipping malformed marketplace pack %s: %s", path.name, exc)
        return None

    rules_out: list[MarketplaceRuleOut] = []
    for rule in pack.rules:
        normalized = normalize_check(rule)
        err = _validate_rule(rule, normalized, validate_fn)
        if err is not None:
            logger.warning("Skipping marketplace pack %s: rule %r invalid: %s", pack.id, rule.name, err)
            return None
        rules_out.append(
            MarketplaceRuleOut(
                rule_key=f"{pack.id}:{slugify(rule.name)}",
                name=rule.name,
                description=rule.description,
                industries=rule.industries,
                regions=rule.regions,
                dimension=rule.dimension,
                severity=rule.severity,
                check=normalized,
                slot_families=rule.slot_families,
            )
        )
    return MarketplacePackOut(
        id=pack.id, title=pack.title, icon=pack.icon, description=pack.description, rules=rules_out
    )


def load_packs(
    validate_fn: Callable[[list[dict[str, Any]]], ChecksValidationStatus],
) -> list[MarketplacePackOut]:
    """Load (and cache) all valid packs, sorted A-Z by title."""
    global _cache
    if _cache is not None:
        return _cache
    packs: list[MarketplacePackOut] = []
    for path in sorted(PACKS_DIR.glob("*.yaml")):
        pack = _load_pack_file(path, validate_fn)
        if pack is not None:
            packs.append(pack)
    packs.sort(key=lambda p: p.title)
    _cache = packs
    return _cache


def clear_cache() -> None:
    """Reset the module-level cache (tests / reload)."""
    global _cache
    _cache = None
