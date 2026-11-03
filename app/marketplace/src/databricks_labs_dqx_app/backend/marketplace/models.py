"""Pydantic models for the Rules Marketplace pack catalogue.

Two layers:
- ``MarketplaceRule`` / ``MarketplacePack`` model the raw pack YAML on disk.
- ``MarketplaceRuleOut`` / ``MarketplacePackOut`` / ``MarketplacePacksOut`` are
  the API response shape returned by ``GET /marketplace/packs``; each rule
  carries the normalized DQX check dict so the UI can preview + import without
  a second round-trip.
"""

from typing import Any

from pydantic import BaseModel, Field

VALID_DIMENSIONS = {"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"}
VALID_SEVERITIES = {"Low", "Medium", "High", "Critical"}


class MarketplaceRule(BaseModel):
    """A single reusable rule as authored in a pack YAML file."""

    name: str
    description: str
    industries: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)
    criticality: str = "error"
    dimension: str
    severity: str
    check: dict[str, Any]
    # Declared slot families ({{slot}} name -> numeric|temporal|boolean|text),
    # used by the "Try it out" test grid to render the right input control.
    # Only needed for sql_expression rules, where the family can't be inferred
    # from the check function (native checks derive it from the function).
    slot_families: dict[str, str] = Field(default_factory=dict)


class MarketplacePack(BaseModel):
    """A domain-organised bundle of reusable rules (one YAML file)."""

    id: str
    title: str
    icon: str
    description: str
    rules: list[MarketplaceRule]


class MarketplaceRuleOut(BaseModel):
    """A marketplace rule as returned to the frontend (normalized check dict)."""

    rule_key: str
    name: str
    description: str
    industries: list[str]
    regions: list[str]
    dimension: str
    severity: str
    check: dict[str, Any]
    slot_families: dict[str, str] = Field(default_factory=dict)
    # True when a rule with this name already exists in the registry — the UI
    # disables its checkbox so it can't be re-added. Set per-request by the
    # route (the loader itself is registry-agnostic and cached).
    imported: bool = False


class MarketplacePackOut(BaseModel):
    id: str
    title: str
    icon: str
    description: str
    rules: list[MarketplaceRuleOut]


class MarketplacePacksOut(BaseModel):
    packs: list[MarketplacePackOut]
