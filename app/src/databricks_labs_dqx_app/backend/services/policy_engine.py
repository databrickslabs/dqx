"""Pure rule policy evaluation.

Given a stored :class:`RulePolicy`, the rule's slots and a snapshot of one table
(owners, tags, columns), decide which concrete column mappings the policy
produces. No I/O, no logging: everything here is a deterministic function of
its inputs, so it can be used by snapshot readers and orchestration alike.
"""

import itertools
import math
import re
from collections.abc import Iterable
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
class SnapshotNeeds:
    """Which optional facts a snapshot must carry (names, columns, types and table owners always come)."""

    column_tags: bool = False
    table_tags: bool = False
    schema_tags: bool = False
    catalog_tags: bool = False
    schema_owner: bool = False
    catalog_owner: bool = False

    def union(self, other: "SnapshotNeeds") -> "SnapshotNeeds":
        return SnapshotNeeds(
            column_tags=self.column_tags or other.column_tags,
            table_tags=self.table_tags or other.table_tags,
            schema_tags=self.schema_tags or other.schema_tags,
            catalog_tags=self.catalog_tags or other.catalog_tags,
            schema_owner=self.schema_owner or other.schema_owner,
            catalog_owner=self.catalog_owner or other.catalog_owner,
        )

    def covers(self, other: "SnapshotNeeds") -> bool:
        """True when a snapshot read with these needs has every fact *other* needs."""
        return self.union(other) == self


ALL_NEEDS = SnapshotNeeds(
    column_tags=True, table_tags=True, schema_tags=True, catalog_tags=True, schema_owner=True, catalog_owner=True
)
_TAG_PREDICATES = ("has_tag", "not_has_tag")


def _row_needs(row: ConditionRow) -> SnapshotNeeds:
    tags = row.predicate in _TAG_PREDICATES
    owner = row.predicate == "has_owner"
    return SnapshotNeeds(
        column_tags=tags and row.object == "column",
        table_tags=tags and row.object == "table",
        schema_tags=tags and row.object == "schema",
        catalog_tags=tags and row.object == "catalog",
        schema_owner=owner and row.object == "schema",
        catalog_owner=owner and row.object == "catalog",
    )


def needs_of(policies: Iterable[RulePolicy]) -> SnapshotNeeds:
    """The facts evaluating *policies* reads, so snapshots can skip the rest."""
    needs = SnapshotNeeds()
    for policy in policies:
        table = policy.table_condition
        if table.kind == "tags":
            needs = needs.union(SnapshotNeeds(table_tags=True))
        elif table.kind == "builder" and table.expr is not None:
            for row in table.expr.rows:
                needs = needs.union(_row_needs(row))
        for cond in policy.column_conditions.values():
            if cond.kind == "tags":
                needs = needs.union(SnapshotNeeds(column_tags=True))
            elif cond.expr is not None:
                for row in cond.expr.rows:
                    needs = needs.union(_row_needs(row))
    return needs


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


def _match(
    policy: RulePolicy, table_fqn: str, mapping: ColumnMappingGroup, status: MatchStatus, reason: str
) -> PolicyMatch:
    return PolicyMatch(policy.policy_id, policy.rule_id, table_fqn, mapping, status, reason)


def _pair_by_name(
    policy: RulePolicy, table_fqn: str, slots: list[RuleSlot], candidates: list[list[ColumnSnapshot]]
) -> list[PolicyMatch]:
    """Pair one candidate per slot by shared name stems.

    A pick is ``auto`` only when it scores at least 1 and no other remaining combination with the
    same score shares a column with it. Otherwise the whole connected group of combinations that
    share columns with the tied ones is emitted as ``ambiguous`` alternatives, so no ``auto`` is
    ever derived from columns involved in a tie.
    """
    if math.prod(len(c) for c in candidates) > MAX_COMBINATIONS:
        return [_match(policy, table_fqn, {}, "ambiguous", "too many possible column combinations")]
    scored = [
        (combo, _score(combo), frozenset(c.name for c in combo))
        for combo in itertools.product(*candidates)
        if len({c.name for c in combo}) == len(combo)
    ]
    matches: list[PolicyMatch] = []
    while scored:
        best = max(scored, key=lambda item: item[1])
        tied = [item for item in scored if item[1] == best[1] and item[2] & best[2]]
        if best[1] >= 1 and len(tied) == 1:
            mapping = {slot.name: c.name for slot, c in zip(slots, best[0])}
            matches.append(_match(policy, table_fqn, mapping, "auto", f"paired by name {_stem(best[0])}"))
            consumed = best[2]
        else:
            group = list(tied)
            grown = True
            while grown:
                names = frozenset().union(*(item[2] for item in group))
                extra = [item for item in scored if item not in group and item[2] & names]
                group.extend(extra)
                grown = bool(extra)
            for combo, _, _ in group:
                mapping = {slot.name: c.name for slot, c in zip(slots, combo)}
                matches.append(_match(policy, table_fqn, mapping, "ambiguous", "several possible columns"))
            consumed = frozenset().union(*(item[2] for item in group))
        scored = [item for item in scored if not item[2] & consumed]
    return matches


def _bind_many_slots(
    match: PolicyMatch, many: list[tuple[RuleSlot, list[ColumnSnapshot]]], one_names: set[str]
) -> PolicyMatch:
    """Bind each many-slot to its candidates not already used by the match or an earlier many-slot."""
    used: set[str] = {name for slot_name, name in match.column_mapping.items() if slot_name in one_names}
    mapping = dict(match.column_mapping)
    status = match.status
    reason = match.reason
    for slot, cols in many:
        rest = [c.name for c in cols if c.name not in used]
        if not rest:
            if status == "auto":
                status, reason = "partial", f"no candidate for {slot.name}"
            continue
        mapping[slot.name] = ",".join(rest)
        used.update(rest)
    return PolicyMatch(match.policy_id, match.rule_id, match.table_fqn, mapping, status, reason)


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

    def bound(cols: list[ColumnSnapshot]) -> str:
        return ",".join(c.name for c in cols)

    empty = next((slot for slot, cols in zip(ordered, candidates) if not cols), None)
    if empty is not None:
        filled = {
            slot.name: bound(cols)
            for slot, cols in zip(ordered, candidates)
            if cols and (slot.cardinality == "many" or len(cols) == 1)
        }
        return [_match(policy, snap.table_fqn, filled, "partial", f"no candidate for {empty.name}")]

    if len(ordered) == 1:
        slot, cols = ordered[0], candidates[0]
        reason = _condition_reason(policy.column_conditions[slot.name])
        if slot.cardinality == "many":
            return [_match(policy, snap.table_fqn, {slot.name: bound(cols)}, "auto", reason)]
        return [_match(policy, snap.table_fqn, {slot.name: c.name}, "auto", reason) for c in cols]

    one = [(s, c) for s, c in zip(ordered, candidates) if s.cardinality == "one"]
    many = [(s, c) for s, c in zip(ordered, candidates) if s.cardinality == "many"]

    if not one:
        return [_bind_many_slots(_match(policy, snap.table_fqn, {}, "auto", "all candidates"), many, set())]

    if len(one) == 1:
        slot, cols = one[0]
        reason = _condition_reason(policy.column_conditions[slot.name])
        return [
            _bind_many_slots(_match(policy, snap.table_fqn, {slot.name: c.name}, "auto", reason), many, {slot.name})
            for c in cols
        ]

    if all(len(cols) == 1 for _, cols in one):
        seen: set[str] = set()
        mapping: ColumnMappingGroup = {}
        for slot, cols in one:
            if cols[0].name in seen:
                return [_match(policy, snap.table_fqn, mapping, "partial", f"no candidate for {slot.name}")]
            seen.add(cols[0].name)
            mapping[slot.name] = cols[0].name
        single = _match(policy, snap.table_fqn, mapping, "auto", "single candidate per input")
        return [_bind_many_slots(single, many, set(mapping))]

    paired = _pair_by_name(policy, snap.table_fqn, [s for s, _ in one], [c for _, c in one])
    one_names = {s.name for s, _ in one}
    return [m if not m.column_mapping else _bind_many_slots(m, many, one_names) for m in paired]
