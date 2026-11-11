"""Pure rule policy evaluation.

Given a stored :class:`RulePolicy`, the rule's slots and a snapshot of one table
(owners, tags, columns), decide which concrete column mappings the policy
produces. No I/O, no logging: everything here is a deterministic function of
its inputs, so it can be used by snapshot readers and orchestration alike.
"""

import itertools
import math
import re
from dataclasses import dataclass
from typing import Literal

from databricks_labs_dqx_app.backend.policy_models import (
    ALL,
    ColumnCondition,
    ConditionRow,
    PolicyScope,
    RulePolicy,
    TableCondition,
)
from databricks_labs_dqx_app.backend.registry_models import ColumnMappingGroup, RuleSlot
from databricks_labs_dqx_app.backend.services.tag_mapping_service import family_for_type, parse_tag

MAX_COMBINATIONS = 10_000
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_TOKEN_SEPARATORS = re.compile(r"[_.\-]+")

MatchStatus = Literal["auto", "ambiguous", "partial"]


@dataclass(frozen=True)
class ColumnSnapshot:
    """A column's name, UC type and tags (``"key"`` or ``"key=value"``)."""

    name: str
    type_name: str
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class TableSnapshot:
    """Everything a policy can look at on one table."""

    table_fqn: str
    owner: str | None
    catalog_owner: str | None
    schema_owner: str | None
    catalog_tags: tuple[str, ...]
    schema_tags: tuple[str, ...]
    table_tags: tuple[str, ...]
    columns: tuple[ColumnSnapshot, ...]


@dataclass(frozen=True)
class PolicyMatch:
    """One proposed application of a policy's rule to a table."""

    policy_id: str
    rule_id: str
    table_fqn: str
    column_mapping: ColumnMappingGroup
    status: MatchStatus
    reason: str


def tags_match(have: tuple[str, ...], want: list[str]) -> bool:
    """Any-overlap tag match: a bare key matches any value, ``key=value`` matches exactly."""
    parsed = [parse_tag(t) for t in have]
    for wanted in want:
        wanted_key, wanted_value = parse_tag(wanted)
        if any(key == wanted_key and (wanted_value is None or wanted_value == value) for key, value in parsed):
            return True
    return False


def like(value: str, pattern: str) -> bool:
    """SQL LIKE (case-insensitive): ``%`` is any run, ``_`` one character, all else literal."""
    regex = "".join(".*" if ch == "%" else "." if ch == "_" else re.escape(ch) for ch in pattern)
    return re.fullmatch(regex, value, flags=re.IGNORECASE | re.DOTALL) is not None


def _row_holds(row: ConditionRow, *, name: str, owner: str | None, tags: tuple[str, ...]) -> bool:
    if row.predicate == "has_tag":
        return tags_match(tags, [row.value])
    if row.predicate == "not_has_tag":
        return not tags_match(tags, [row.value])
    if row.predicate == "has_name":
        return name.lower() == row.value.lower()
    if row.predicate == "name_like":
        return like(name, row.value)
    return owner is not None and owner.lower() == row.value.lower()  # has_owner


def _fold(results: list[tuple[str | None, bool]]) -> bool:
    """Left-to-right AND/OR with AND binding tighter (standard precedence)."""
    groups: list[list[bool]] = [[]]
    for combinator, value in results:
        if combinator == "OR":
            groups.append([])
        groups[-1].append(value)
    return any(all(group) for group in groups if group)


def in_scope(scope: PolicyScope, table_fqn: str) -> bool:
    """Whether *table_fqn* (``cat.sch.tbl``) falls inside the policy scope (case-insensitive)."""
    parts = table_fqn.lower().split(".")
    if len(parts) != 3:
        return False
    catalog, schema = parts[0], ".".join(parts[:2])
    fqn = ".".join(parts)

    def allowed(selection: list[str] | str, candidate: str) -> bool:
        return selection == ALL or candidate in {item.lower() for item in selection}

    return allowed(scope.catalogs, catalog) and allowed(scope.schemas, schema) and allowed(scope.tables, fqn)


def table_condition_holds(cond: TableCondition, snap: TableSnapshot) -> bool:
    """Evaluate the table-level condition against the snapshot."""
    if cond.kind == "none":
        return True
    if cond.kind == "tags":
        return tags_match(snap.table_tags, cond.tags)
    if cond.expr is None:
        return False
    catalog, schema, table = snap.table_fqn.split(".", 2)
    views = {
        "catalog": (catalog, snap.catalog_owner, snap.catalog_tags),
        "schema": (schema, snap.schema_owner, snap.schema_tags),
        "table": (table, snap.owner, snap.table_tags),
    }
    results = []
    for row in cond.expr.rows:
        name, owner, tags = views[row.object]
        results.append((row.combinator, _row_holds(row, name=name, owner=owner, tags=tags)))
    return _fold(results)


def column_condition_holds(cond: ColumnCondition, col: ColumnSnapshot) -> bool:
    """Evaluate a slot's column condition against one column."""
    if cond.kind == "tags":
        return tags_match(col.tags, cond.tags)
    if cond.expr is None:
        return False
    return _fold(
        [(row.combinator, _row_holds(row, name=col.name, owner=None, tags=col.tags)) for row in cond.expr.rows]
    )


def _tokens(name: str) -> list[str]:
    parts: list[str] = []
    for chunk in _TOKEN_SEPARATORS.split(name):
        parts.extend(_CAMEL_BOUNDARY.split(chunk))
    return [p.lower() for p in parts if p]


def _shared_affixes(first: list[str], second: list[str]) -> tuple[int, int]:
    limit = min(len(first), len(second))
    prefix = 0
    while prefix < limit and first[prefix] == second[prefix]:
        prefix += 1
    suffix = 0
    while suffix < limit - prefix and first[-1 - suffix] == second[-1 - suffix]:
        suffix += 1
    return prefix, suffix


def _combo_affixes(combo: tuple[ColumnSnapshot, ...]) -> list[tuple[int, int]]:
    tokenised = [_tokens(c.name) for c in combo]
    return [_shared_affixes(a, b) for a, b in itertools.pairwise(tokenised)]


def _score(combo: tuple[ColumnSnapshot, ...]) -> int:
    return sum(prefix + suffix for prefix, suffix in _combo_affixes(combo))


def _stem(combo: tuple[ColumnSnapshot, ...]) -> str:
    affixes = _combo_affixes(combo)
    prefix = min(p for p, _ in affixes)
    suffix = min(s for _, s in affixes)
    first = _tokens(combo[0].name)
    head = first[:prefix]
    tail = first[len(first) - suffix :] if suffix else []
    return "_".join([*head, "*", *tail])


def _condition_reason(cond: ColumnCondition) -> str:
    return "matched " + ", ".join(cond.tags) if cond.kind == "tags" else "matched condition"


def _pair_by_name(
    policy: RulePolicy, table_fqn: str, slots: list[RuleSlot], candidates: list[list[ColumnSnapshot]]
) -> list[PolicyMatch]:
    """Pair one candidate per slot by shared name stems; ambiguous when the stems don't decide."""
    if math.prod(len(c) for c in candidates) > MAX_COMBINATIONS:
        return [_match(policy, table_fqn, {}, "ambiguous", "too many possible column combinations")]
    remaining = [combo for combo in itertools.product(*candidates) if len({c.name for c in combo}) == len(combo)]
    scored = [(combo, _score(combo)) for combo in remaining]
    matches: list[PolicyMatch] = []
    while scored:
        best_combo, best_score = max(scored, key=lambda item: item[1])
        best_names = {c.name for c in best_combo}
        tied = any(
            score == best_score and combo is not best_combo and best_names & {c.name for c in combo}
            for combo, score in scored
        )
        mapping = {slot.name: col.name for slot, col in zip(slots, best_combo)}
        if best_score >= 1 and not tied:
            matches.append(_match(policy, table_fqn, mapping, "auto", f"paired by name {_stem(best_combo)}"))
        else:
            matches.append(_match(policy, table_fqn, mapping, "ambiguous", "several possible columns"))
        scored = [(c, s) for c, s in scored if not best_names & {col.name for col in c}]
    return matches


def _match(
    policy: RulePolicy, table_fqn: str, mapping: ColumnMappingGroup, status: MatchStatus, reason: str
) -> PolicyMatch:
    return PolicyMatch(policy.policy_id, policy.rule_id, table_fqn, mapping, status, reason)


def evaluate(policy: RulePolicy, slots: list[RuleSlot], snap: TableSnapshot) -> list[PolicyMatch]:
    """Return the matches *policy* proposes for *snap*; empty when out of scope or not applicable."""
    if not in_scope(policy.scope, snap.table_fqn) or not table_condition_holds(policy.table_condition, snap):
        return []
    if not slots:
        return [_match(policy, snap.table_fqn, {}, "auto", "table-level")]

    ordered = sorted(slots, key=lambda s: s.position)
    columns = sorted(snap.columns, key=lambda c: c.name)
    candidates: list[list[ColumnSnapshot]] = []
    for slot in ordered:
        cond = policy.column_conditions.get(slot.name)
        if cond is None:
            return []
        candidates.append(
            [
                col
                for col in columns
                if (slot.family == "any" or family_for_type(col.type_name) == slot.family)
                and column_condition_holds(cond, col)
            ]
        )

    def bound(slot: RuleSlot, cols: list[ColumnSnapshot]) -> str:
        return ",".join(c.name for c in cols)

    empty = next((slot for slot, cols in zip(ordered, candidates) if not cols), None)
    if empty is not None:
        filled = {
            slot.name: bound(slot, cols)
            for slot, cols in zip(ordered, candidates)
            if cols and (slot.cardinality == "many" or len(cols) == 1)
        }
        return [_match(policy, snap.table_fqn, filled, "partial", f"no candidate for {empty.name}")]

    if len(ordered) == 1:
        slot, cols = ordered[0], candidates[0]
        reason = _condition_reason(policy.column_conditions[slot.name])
        if slot.cardinality == "many":
            return [_match(policy, snap.table_fqn, {slot.name: bound(slot, cols)}, "auto", reason)]
        return [_match(policy, snap.table_fqn, {slot.name: c.name}, "auto", reason) for c in cols]

    one_idx = [i for i, s in enumerate(ordered) if s.cardinality == "one"]
    many_idx = [i for i, s in enumerate(ordered) if s.cardinality == "many"]

    if all(len(candidates[i]) == 1 for i in one_idx):
        chosen = [candidates[i][0] for i in one_idx]
        if len({c.name for c in chosen}) == len(chosen):
            used = {c.name for c in chosen}
            mapping = {ordered[i].name: candidates[i][0].name for i in one_idx}
            for i in many_idx:
                rest = [c for c in candidates[i] if c.name not in used]
                if not rest:
                    return [_match(policy, snap.table_fqn, mapping, "partial", f"no candidate for {ordered[i].name}")]
                mapping[ordered[i].name] = bound(ordered[i], rest)
            return [_match(policy, snap.table_fqn, mapping, "auto", "single candidate per input")]

    if len(one_idx) < 2:
        return [_match(policy, snap.table_fqn, {}, "ambiguous", "several possible columns")]

    one_slots = [ordered[i] for i in one_idx]
    paired = _pair_by_name(policy, snap.table_fqn, one_slots, [candidates[i] for i in one_idx])
    if not many_idx:
        return paired
    result: list[PolicyMatch] = []
    for match in paired:
        mapping = dict(match.column_mapping)
        used = set(mapping.values())
        status = match.status
        for i in many_idx:
            rest = [c for c in candidates[i] if c.name not in used]
            if rest and mapping:
                mapping[ordered[i].name] = bound(ordered[i], rest)
            else:
                status = "partial" if status == "auto" else status
        result.append(PolicyMatch(match.policy_id, match.rule_id, match.table_fqn, mapping, status, match.reason))
    return result
