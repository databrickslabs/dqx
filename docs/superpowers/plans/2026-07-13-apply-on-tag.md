# Apply on Tag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a registry rule's `{{slot}}` placeholders auto-bind to real Unity Catalog `class.*` column tags, so a published rule fans out to every monitored table whose columns carry matching tags — standing, admin-gated.

**Architecture:** Slot→tags map stored in the rule's existing `user_metadata` JSON (`slot_tags` reserved key). A pure `TagMappingResolver` computes column-mapping groups from (slots+tags, table column-tags), enforcing any-overlap tag match AND family compatibility. Three SP-authed reconcile hooks (publish / register / periodic sweep) converge on the existing `ApplyRulesService.apply_rule` + `Materializer`. A global `tag_auto_apply` admin toggle (default OFF) switches between eager auto-attach (Cartesian product) and suggestions-only. Authoring UI adds tag chips to `SlotsPanel`, fed by a lazy/cached `system.information_schema.column_tags` picker.

**Tech Stack:** Python + FastAPI + Pydantic (backend), React + TypeScript + shadcn/ui + TanStack Query (frontend), Databricks SDK, pytest + bun test.

## Global Constraints

- GPG-sign every commit; trailer `Co-authored-by: Isaac`.
- Never stage `app/uv.lock` or `uv.lock` (`git checkout --` them before every commit).
- `git add` only changed files BY PATH — never `git add -A`/`.`. Never stage `.superpowers/`.
- i18n 4-locale parity: every user-facing string via `t()`, added to ALL of `ui/lib/i18n/locales/{en,pt-BR,it,es}.json`; `en` is source of truth.
- No lint-suppression: no `// eslint-disable`, `@ts-ignore`, `@ts-expect-error`, `# type: ignore`, `# noqa`. Fix the code.
- Security: generated/templated SQL must pass `is_sql_query_safe()`; validate/escape UC identifiers (`validate_fqn`/`quote_fqn`/`escape_sql_string`); don't log untrusted values raw.
- Type hints on every param/return (mypy/basedpyright); modern generics (`list[str]`, `str | None`).
- Only `class.*` tag keys are ever surfaced, matched, or persisted.
- Tag value matching: bare key matches any value; `key=value` requires exact value.
- Within a slot: ANY-overlap (a column matches if it carries any one of the slot's tags). Across slots: every slot must be filled for a valid group.
- Family/type compatibility is always enforced — a tag match never overrides it.
- Commands run from the WORKTREE root. Backend tests: `make app-test` or `cd app && .venv/bin/pytest tests/unit/<file> -v`. UI/type check: `make app-check`. After backend response-model changes: `make app-regen-api`.
- Use **opus** for subagents (fable is spend-limited).

---

## File Structure

**Backend (new):**
- `app/src/databricks_labs_dqx_app/backend/services/tag_mapping_service.py` — pure `TagMappingResolver` + `family_for_type` (lifted from rule_suggester) + tag-string parse/match helpers.
- `app/src/databricks_labs_dqx_app/backend/services/tag_reconcile_service.py` — SP-authed orchestrator: reconcile-one-rule, reconcile-one-table, sweep. Reads column tags, calls resolver, calls ApplyRulesService.

**Backend (modified):**
- `backend/registry_models.py` — `RESERVED_SLOT_TAGS_KEY`, `get_slot_tags`/`set_slot_tags`, `ORIGIN_TAG_AUTO`; add slot_tags to reserved set.
- `backend/services/app_settings_service.py` — `get_tag_auto_apply`/`save_tag_auto_apply`.
- `backend/services/discovery.py` — `list_class_column_tags` (system.information_schema, lazy/cached).
- `backend/services/rule_suggester.py` — re-import `family_for_type` from tag_mapping_service (single source of truth).
- `backend/routes/v1/config.py` — `tag_auto_apply` on RulesRegistrySettingsOut/In.
- `backend/routes/v1/discovery.py` — `GET /discovery/class-tags` endpoint.
- `backend/services/registry_service.py` — call reconcile on approve (hook 1).
- `backend/services/monitored_table_service.py` — call reconcile on register/bulk_register (hook 2).
- `backend/services/scheduler_service.py` — periodic sweep tick (hook 3).
- `backend/dependencies.py` — wire the two new services.

**Frontend (new):**
- `app/src/databricks_labs_dqx_app/ui/components/apply-rules/TagPicker.tsx` — `class.*` tag popover list.

**Frontend (modified):**
- `ui/components/RegistryRuleFormDialog.tsx` — tag chips in `SlotsPanel`; thread slot_tags through snapshot/build.
- `ui/lib/i18n/locales/{en,pt-BR,it,es}.json` — new keys.
- `ui/routes/_sidebar/config.tsx` — the toggle Switch.
- `ui/lib/api.ts` (regen) + `ui/lib/api-custom.ts` (if needed).

---

## Task 1: Slot-tags model helpers in `registry_models.py`

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/registry_models.py` (near line 424, the reserved-key block)
- Test: `app/tests/unit/test_registry_models.py`

**Interfaces:**
- Produces: `RESERVED_SLOT_TAGS_KEY: str = "slot_tags"`; `ORIGIN_KEY: str = "origin"`; `ORIGIN_TAG_AUTO: str = "tag_auto"`; `get_slot_tags(user_metadata: dict[str, Any]) -> dict[str, list[str]]`; `set_slot_tags(user_metadata: dict[str, Any], mapping: dict[str, list[str]]) -> dict[str, Any]`.
- `set_slot_tags` drops slots with empty tag lists; removes the key entirely when the mapping is empty. Returns a NEW dict (never mutates input), mirroring `set_reserved_tag`.
- `get_slot_tags` returns `{}` when absent/malformed; coerces to `dict[str, list[str]]`, dropping non-string tags and non-list values defensively.

- [ ] **Step 1: Write failing tests**

```python
# in test_registry_models.py
from databricks_labs_dqx_app.backend.registry_models import (
    RESERVED_SLOT_TAGS_KEY,
    RESERVED_RULE_METADATA_KEYS,
    ORIGIN_KEY,
    ORIGIN_TAG_AUTO,
    get_slot_tags,
    set_slot_tags,
)


def test_slot_tags_key_is_reserved():
    assert RESERVED_SLOT_TAGS_KEY == "slot_tags"
    assert RESERVED_SLOT_TAGS_KEY in RESERVED_RULE_METADATA_KEYS


def test_origin_marker_constants():
    assert ORIGIN_KEY == "origin"
    assert ORIGIN_TAG_AUTO == "tag_auto"


def test_get_slot_tags_absent_returns_empty():
    assert get_slot_tags({}) == {}
    assert get_slot_tags({"name": "x"}) == {}


def test_get_slot_tags_reads_mapping():
    um = {"slot_tags": {"column_1": ["class.pii", "class.name=given"]}}
    assert get_slot_tags(um) == {"column_1": ["class.pii", "class.name=given"]}


def test_get_slot_tags_drops_malformed():
    um = {"slot_tags": {"a": ["class.x", 3, ""], "b": "notalist", "c": []}}
    # non-string tags dropped, non-list values dropped, empty list preserved as []
    assert get_slot_tags(um) == {"a": ["class.x"], "c": []}


def test_set_slot_tags_roundtrip_and_immutability():
    um = {"name": "keep"}
    out = set_slot_tags(um, {"column_1": ["class.pii"]})
    assert out["slot_tags"] == {"column_1": ["class.pii"]}
    assert out["name"] == "keep"
    assert "slot_tags" not in um  # original untouched


def test_set_slot_tags_drops_empty_slots_and_key():
    assert set_slot_tags({}, {"a": []}) == {}  # empty-list slot dropped -> mapping empty -> key removed
    out = set_slot_tags({"slot_tags": {"a": ["class.x"]}}, {})
    assert "slot_tags" not in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && .venv/bin/pytest tests/unit/test_registry_models.py -k "slot_tags or origin_marker" -v`
Expected: FAIL (ImportError — names not defined)

- [ ] **Step 3: Implement in `registry_models.py`**

Add after the `RESERVED_SEVERITY_KEY` line (before `RESERVED_RULE_METADATA_KEYS`):

```python
RESERVED_SLOT_TAGS_KEY = "slot_tags"

# Applied-rule origin marker (in AppliedRule.user_metadata) distinguishing an
# auto-created tag-mapping attachment from a hand-applied one. The tag-reconcile
# engine only ever touches ``tag_auto`` rows; hand-applied rows stay unmarked.
ORIGIN_KEY = "origin"
ORIGIN_TAG_AUTO = "tag_auto"
```

Add `RESERVED_SLOT_TAGS_KEY` into the `RESERVED_RULE_METADATA_KEYS` frozenset.

Add these functions after `set_reserved_tag`:

```python
def get_slot_tags(user_metadata: dict[str, Any]) -> dict[str, list[str]]:
    """Read the reserved ``slot_tags`` map from *user_metadata*.

    Returns a ``{slot_name: [tag, ...]}`` dict; ``{}`` when absent or malformed.
    Non-list slot values are dropped; non-string tags within a list are dropped.
    """
    raw = user_metadata.get(RESERVED_SLOT_TAGS_KEY)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for slot_name, tags in raw.items():
        if not isinstance(slot_name, str) or not isinstance(tags, list):
            continue
        out[slot_name] = [t for t in tags if isinstance(t, str) and t]
    return out


def set_slot_tags(user_metadata: dict[str, Any], mapping: dict[str, list[str]]) -> dict[str, Any]:
    """Return a *new* ``user_metadata`` with ``slot_tags`` set to *mapping*.

    Slots with an empty tag list are dropped; when the resulting map is empty
    the key is removed entirely. Never mutates *user_metadata* in place.
    """
    cleaned = {slot: list(tags) for slot, tags in mapping.items() if tags}
    updated = dict(user_metadata)
    if cleaned:
        updated[RESERVED_SLOT_TAGS_KEY] = cleaned
    else:
        updated.pop(RESERVED_SLOT_TAGS_KEY, None)
    return updated
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd app && .venv/bin/pytest tests/unit/test_registry_models.py -k "slot_tags or origin_marker" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/registry_models.py app/tests/unit/test_registry_models.py
git commit -S -m "$(printf 'apply-on-tag: slot_tags user_metadata helpers + origin marker\n\nCo-authored-by: Isaac')"
```

---

## Task 2: `TagMappingResolver` (pure match engine)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/services/tag_mapping_service.py`
- Test: `app/tests/unit/test_tag_mapping_service.py`

**Interfaces:**
- Consumes: `ColumnMappingGroup` (`= dict[str, str]`, slot_name→column_name) and `RuleSlot` from `registry_models`.
- Produces:
  - `CLASS_TAG_PREFIX: str = "class."`
  - `family_for_type(type_name: str) -> str` (moved here from rule_suggester; classifies UC type → numeric/text/temporal/boolean/array/any).
  - `parse_tag(tag: str) -> tuple[str, str | None]` — splits `"class.pii=email"` → `("class.pii", "email")`; `"class.pii"` → `("class.pii", None)`.
  - `column_matches_slot(col_type: str, col_tags: list[str], slot_families_ok: bool, slot_tags: list[str]) -> bool` — internal helper (kept module-private is fine).
  - `@dataclass ColumnInfo: name: str; type_name: str; tags: list[str]` — one candidate column (tags are raw `"key"` / `"key=value"` strings).
  - `resolve(slots: list[RuleSlot], slot_tags: dict[str, list[str]], columns: list[ColumnInfo], *, single: bool = False) -> list[ColumnMappingGroup]` — returns valid mapping groups. `single=True` returns at most one representative group (first in deterministic order).

**Match rules (from Global Constraints):** a column matches a slot iff (family OK) AND (it carries ≥1 tag overlapping the slot's tags, with bare-key=any-value / key=value=exact). Only `class.*` tags participate — filter both the column's tags and defensively ignore any non-`class.` slot tag. Group valid iff every slot has ≥1 candidate. Cartesian product across slots; deterministic order (slot position order preserved; within a slot, columns sorted by name). No column used twice within a single group.

- [ ] **Step 1: Write failing tests**

```python
# test_tag_mapping_service.py
from databricks_labs_dqx_app.backend.registry_models import RuleSlot
from databricks_labs_dqx_app.backend.services.tag_mapping_service import (
    ColumnInfo,
    family_for_type,
    parse_tag,
    resolve,
)


def _slot(name, family="any", position=0):
    return RuleSlot(name=name, family=family, position=position, cardinality="one")


def test_family_for_type():
    assert family_for_type("INT") == "numeric"
    assert family_for_type("DECIMAL(10,2)") == "numeric"
    assert family_for_type("STRING") == "text"
    assert family_for_type("TIMESTAMP") == "temporal"
    assert family_for_type("BOOLEAN") == "boolean"
    assert family_for_type("ARRAY<INT>") == "array"
    assert family_for_type("MAP<STRING,INT>") == "any"


def test_parse_tag():
    assert parse_tag("class.pii") == ("class.pii", None)
    assert parse_tag("class.name=given") == ("class.name", "given")


def test_single_slot_any_overlap():
    slots = [_slot("c1", "any")]
    slot_tags = {"c1": ["class.pii", "class.sensitive"]}
    cols = [
        ColumnInfo("email", "STRING", ["class.pii"]),
        ColumnInfo("age", "INT", ["class.other"]),
        ColumnInfo("ssn", "STRING", ["class.sensitive", "class.pii"]),
    ]
    groups = resolve(slots, slot_tags, cols)
    # email + ssn match (any overlap); age has no overlapping class tag
    assert groups == [{"c1": "email"}, {"c1": "ssn"}]


def test_family_still_enforced():
    slots = [_slot("c1", "text")]
    slot_tags = {"c1": ["class.pii"]}
    cols = [
        ColumnInfo("email", "STRING", ["class.pii"]),   # text OK
        ColumnInfo("pii_id", "INT", ["class.pii"]),      # tag matches but family text != numeric
    ]
    assert resolve(slots, slot_tags, cols) == [{"c1": "email"}]


def test_value_optional_vs_exact():
    slots = [_slot("c1", "any")]
    cols = [
        ColumnInfo("a", "STRING", ["class.name=given"]),
        ColumnInfo("b", "STRING", ["class.name=family"]),
    ]
    # bare key matches any value
    assert resolve(slots, {"c1": ["class.name"]}, cols) == [{"c1": "a"}, {"c1": "b"}]
    # key=value requires exact
    assert resolve(slots, {"c1": ["class.name=given"]}, cols) == [{"c1": "a"}]


def test_non_class_tags_ignored():
    slots = [_slot("c1", "any")]
    cols = [ColumnInfo("a", "STRING", ["pii", "owner=finance"])]  # no class.* tag
    assert resolve(slots, {"c1": ["class.pii"]}, cols) == []


def test_multi_slot_cartesian_and_all_filled():
    slots = [_slot("c1", "any", 0), _slot("c2", "any", 1)]
    slot_tags = {"c1": ["class.a"], "c2": ["class.b"]}
    cols = [
        ColumnInfo("x1", "STRING", ["class.a"]),
        ColumnInfo("x2", "STRING", ["class.a"]),
        ColumnInfo("y1", "STRING", ["class.b"]),
    ]
    groups = resolve(slots, slot_tags, cols)
    assert groups == [{"c1": "x1", "c2": "y1"}, {"c1": "x2", "c2": "y1"}]


def test_multi_slot_missing_slot_yields_nothing():
    slots = [_slot("c1", "any", 0), _slot("c2", "any", 1)]
    slot_tags = {"c1": ["class.a"], "c2": ["class.b"]}
    cols = [ColumnInfo("x1", "STRING", ["class.a"])]  # nothing matches c2
    assert resolve(slots, slot_tags, cols) == []


def test_no_column_used_twice_in_a_group():
    slots = [_slot("c1", "any", 0), _slot("c2", "any", 1)]
    slot_tags = {"c1": ["class.a"], "c2": ["class.a"]}
    cols = [ColumnInfo("x1", "STRING", ["class.a"]), ColumnInfo("x2", "STRING", ["class.a"])]
    groups = resolve(slots, slot_tags, cols)
    # (x1,x1) and (x2,x2) excluded; only cross pairings
    assert {"c1": "x1", "c2": "x2"} in groups
    assert {"c1": "x1", "c2": "x1"} not in groups


def test_single_flag_returns_one_group():
    slots = [_slot("c1", "any")]
    cols = [ColumnInfo("a", "STRING", ["class.pii"]), ColumnInfo("b", "STRING", ["class.pii"])]
    assert resolve(slots, {"c1": ["class.pii"]}, cols, single=True) == [{"c1": "a"}]


def test_slot_with_no_declared_tags_yields_nothing():
    slots = [_slot("c1", "any")]
    cols = [ColumnInfo("a", "STRING", ["class.pii"])]
    assert resolve(slots, {}, cols) == []
```

- [ ] **Step 2: Run to verify fail**

Run: `cd app && .venv/bin/pytest tests/unit/test_tag_mapping_service.py -v`
Expected: FAIL (module missing)

- [ ] **Step 3: Implement `tag_mapping_service.py`**

```python
"""Pure tag→column mapping resolver for the apply-on-tag feature.

Given a rule's declared slots (each with a family) and its slot→tags map, plus a
table's columns (each with a UC type and its ``class.*`` tags), compute the
valid slot→column mapping GROUPS. A column matches a slot when it is
family-compatible AND carries at least one of the slot's declared tags
(any-overlap; bare key matches any value, ``key=value`` requires an exact
value). A group is valid only when every slot is filled; the full result is the
Cartesian product across slots (no column reused within one group).

No I/O — unit-testable in isolation. The orchestrating reads/writes live in
``tag_reconcile_service``.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from databricks_labs_dqx_app.backend.registry_models import ColumnMappingGroup, RuleSlot

CLASS_TAG_PREFIX = "class."

_TYPE_FAMILY: dict[str, str] = {
    "TINYINT": "numeric", "SMALLINT": "numeric", "INT": "numeric", "INTEGER": "numeric",
    "BIGINT": "numeric", "LONG": "numeric", "FLOAT": "numeric", "DOUBLE": "numeric", "DECIMAL": "numeric",
    "STRING": "text", "VARCHAR": "text", "CHAR": "text",
    "DATE": "temporal", "TIMESTAMP": "temporal", "TIMESTAMP_NTZ": "temporal",
    "BOOLEAN": "boolean", "ARRAY": "array",
}


def family_for_type(type_name: str) -> str:
    """Classify a UC column ``type_name`` into a registry slot family."""
    head = (type_name or "").upper().split("(")[0].split("<")[0].strip()
    return _TYPE_FAMILY.get(head, "any")


def parse_tag(tag: str) -> tuple[str, str | None]:
    """Split a tag string into ``(key, value|None)``. ``class.x=v`` -> ``("class.x","v")``."""
    key, sep, value = tag.partition("=")
    return (key, value if sep else None)


@dataclass
class ColumnInfo:
    name: str
    type_name: str
    tags: list[str] = field(default_factory=list)


def _class_tags(tags: list[str]) -> list[tuple[str, str | None]]:
    return [parse_tag(t) for t in tags if t.startswith(CLASS_TAG_PREFIX)]


def _column_matches(col: ColumnInfo, slot: RuleSlot, slot_tags: list[str]) -> bool:
    if slot.family != "any" and family_for_type(col.type_name) != slot.family:
        return False
    col_tags = _class_tags(col.tags)
    for want in slot_tags:
        if not want.startswith(CLASS_TAG_PREFIX):
            continue
        want_key, want_val = parse_tag(want)
        for have_key, have_val in col_tags:
            if have_key != want_key:
                continue
            if want_val is None or want_val == have_val:
                return True
    return False


def resolve(
    slots: list[RuleSlot],
    slot_tags: dict[str, list[str]],
    columns: list[ColumnInfo],
    *,
    single: bool = False,
) -> list[ColumnMappingGroup]:
    """Return valid slot→column mapping groups (see module docstring)."""
    if not slots:
        return []
    ordered = sorted(slots, key=lambda s: s.position)
    sorted_cols = sorted(columns, key=lambda c: c.name)
    per_slot: list[list[str]] = []
    for slot in ordered:
        tags = slot_tags.get(slot.name, [])
        if not tags:
            return []  # a slot with no declared tags can never be filled by this feature
        matches = [c.name for c in sorted_cols if _column_matches(c, slot, tags)]
        if not matches:
            return []
        per_slot.append(matches)

    groups: list[ColumnMappingGroup] = []
    for combo in itertools.product(*per_slot):
        if len(set(combo)) != len(combo):
            continue  # a column can't fill two slots of one group
        groups.append({slot.name: col for slot, col in zip(ordered, combo)})
        if single:
            break
    return groups
```

- [ ] **Step 4: Run to verify pass**

Run: `cd app && .venv/bin/pytest tests/unit/test_tag_mapping_service.py -v`
Expected: PASS (all)

- [ ] **Step 5: Point `rule_suggester` at the shared classifier**

In `backend/services/rule_suggester.py`, replace the local `_TYPE_FAMILY` dict and `_family_for_type` def with:

```python
from databricks_labs_dqx_app.backend.services.tag_mapping_service import family_for_type as _family_for_type
```

Delete the now-dead `_TYPE_FAMILY` literal and `def _family_for_type`. Keep the `_family_for_type` name so existing call sites are unchanged.

- [ ] **Step 6: Run rule_suggester tests**

Run: `cd app && .venv/bin/pytest tests/unit/test_rule_suggester.py -v`
Expected: PASS (no behavior change)

- [ ] **Step 7: Commit**

```bash
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/services/tag_mapping_service.py app/tests/unit/test_tag_mapping_service.py app/src/databricks_labs_dqx_app/backend/services/rule_suggester.py
git commit -S -m "$(printf 'apply-on-tag: pure TagMappingResolver + shared family_for_type\n\nCo-authored-by: Isaac')"
```

---

## Task 3: `tag_auto_apply` admin setting

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/app_settings_service.py` (near the `_DEFAULT_AUTO_UPGRADE_KEY` block ~line 352)
- Test: `app/tests/unit/test_app_settings_service.py`

**Interfaces:**
- Produces: `AppSettingsService.get_tag_auto_apply() -> bool` (default `False`); `save_tag_auto_apply(enabled: bool, *, user_email: str | None = None) -> bool`.

- [ ] **Step 1: Write failing tests** (mirror the existing `default_auto_upgrade` tests in this file — find them for the exact fixture/mocking style, then add:)

```python
def test_tag_auto_apply_defaults_false(app_settings_service):
    assert app_settings_service.get_tag_auto_apply() is False


def test_tag_auto_apply_roundtrip(app_settings_service):
    assert app_settings_service.save_tag_auto_apply(True, user_email="a@b.c") is True
    assert app_settings_service.get_tag_auto_apply() is True
    app_settings_service.save_tag_auto_apply(False, user_email="a@b.c")
    assert app_settings_service.get_tag_auto_apply() is False
```

(Use whatever fixture the neighbouring tests use — likely a service built over a fake/in-memory SQL executor. Match it exactly; do not invent a new fixture.)

- [ ] **Step 2: Run to verify fail**

Run: `cd app && .venv/bin/pytest tests/unit/test_app_settings_service.py -k tag_auto_apply -v`
Expected: FAIL

- [ ] **Step 3: Implement** (mirror `get_default_auto_upgrade`/`save_default_auto_upgrade` exactly):

```python
    _TAG_AUTO_APPLY_KEY = "tag_auto_apply"

    def get_tag_auto_apply(self) -> bool:
        """Whether tag-mapped rules eagerly auto-attach (True) vs. only feed suggestions (False, default)."""
        raw = self.get_setting(self._TAG_AUTO_APPLY_KEY)
        return raw == "true"

    def save_tag_auto_apply(self, enabled: bool, *, user_email: str | None = None) -> bool:
        self.save_setting(self._TAG_AUTO_APPLY_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled
```

- [ ] **Step 4: Run to verify pass**

Run: `cd app && .venv/bin/pytest tests/unit/test_app_settings_service.py -k tag_auto_apply -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/services/app_settings_service.py app/tests/unit/test_app_settings_service.py
git commit -S -m "$(printf 'apply-on-tag: tag_auto_apply admin setting (default off)\n\nCo-authored-by: Isaac')"
```

---

## Task 4: `tag_auto_apply` on the settings route

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/config.py` (`RulesRegistrySettingsOut`/`In`, `_rules_registry_settings_out`, `save_rules_registry_settings`, ~lines 1116-1185)
- Test: `app/tests/unit/test_rules_registry_settings_route.py`

**Interfaces:**
- Produces: `tag_auto_apply: bool` field on GET response; `tag_auto_apply: bool | None = None` on PUT payload; PUT persists it when provided.

- [ ] **Step 1: Write failing tests** (mirror existing tests in the file — read them first for the client/fixture pattern, then add):

```python
def test_get_includes_tag_auto_apply(client):
    resp = client.get("/api/v1/rules-registry-settings")
    assert resp.status_code == 200
    assert resp.json()["tag_auto_apply"] is False


def test_put_sets_tag_auto_apply(client):
    resp = client.put("/api/v1/rules-registry-settings", json={"tag_auto_apply": True})
    assert resp.status_code == 200
    assert resp.json()["tag_auto_apply"] is True
```

- [ ] **Step 2: Run to verify fail**

Run: `cd app && .venv/bin/pytest tests/unit/test_rules_registry_settings_route.py -k tag_auto_apply -v`
Expected: FAIL

- [ ] **Step 3: Implement**

- Add to `RulesRegistrySettingsOut`:
  ```python
  tag_auto_apply: bool = Field(
      description="Tag-mapping apply behaviour: eagerly auto-attach tag-mapped rules "
      "across monitored tables (True) vs. only surface them as suggestions (False, default)."
  )
  ```
- Add to `RulesRegistrySettingsIn`: `tag_auto_apply: bool | None = None`
- In `_rules_registry_settings_out`: add `tag_auto_apply=svc.get_tag_auto_apply()`.
- In `save_rules_registry_settings`: update the guard so any of the three being non-None is accepted; and add:
  ```python
  if body.tag_auto_apply is not None:
      svc.save_tag_auto_apply(body.tag_auto_apply, user_email=email)
  ```
  (Update the 400-guard condition to include `and body.tag_auto_apply is None`.)

- [ ] **Step 4: Run to verify pass**

Run: `cd app && .venv/bin/pytest tests/unit/test_rules_registry_settings_route.py -v`
Expected: PASS (existing + new)

- [ ] **Step 5: Regen API client**

Run: `make app-regen-api`
Then verify `RulesRegistrySettings` types in `ui/lib/api.ts` now carry `tag_auto_apply`.

- [ ] **Step 6: Commit**

```bash
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/routes/v1/config.py app/tests/unit/test_rules_registry_settings_route.py app/src/databricks_labs_dqx_app/ui/lib/api.ts
git commit -S -m "$(printf 'apply-on-tag: expose tag_auto_apply on rules-registry-settings route\n\nCo-authored-by: Isaac')"
```

---

## Task 5: Class-tag discovery (backend service + route)

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/discovery.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/discovery.py`
- Test: `app/tests/unit/test_discovery_class_tags.py` (new)

**Interfaces:**
- Produces: `DiscoveryService.list_class_column_tags() -> list[str]` — sorted distinct `class.*` tag strings (`"class.pii"` or `"class.pii=email"` when a value exists) from `system.information_schema.column_tags`, permission-filtered by OBO. Falls back to the configured catalog's `information_schema.column_tags` when `system` is inaccessible; returns `[]` on total failure (never raises). Cached (`@app_cache.cached("discovery:{_user}:class_tags", ttl=_TAGS_TTL)` async wrapper `list_class_column_tags_async`).
- Route: `GET /api/v1/discovery/class-tags` → `ClassTagsOut(tags: list[str])`.

- [ ] **Step 1: Write failing test** (unit — inject a fake SQL/query seam). The DiscoveryService currently wraps `WorkspaceClient`; class-tag listing needs to run SQL. Read how other services run `information_schema` SQL (e.g. `scheduler_service` uses a `SqlExecutor`). For the unit test, construct `DiscoveryService` with a `create_autospec(WorkspaceClient)` whose `.statement_execution` (or the injected executor) returns canned rows. **Match the existing DiscoveryService construction/injection pattern** — if it only holds a `WorkspaceClient`, run the query via `ws.statement_execution.execute_statement` and autospec that. Test:

```python
def test_list_class_column_tags_filters_and_formats(discovery_with_rows):
    # rows: (catalog, schema, table, column, tag_name, tag_value)
    # given class.pii/'' , class.name/'given', non-class 'owner'/'x'
    svc = discovery_with_rows([
        ("c","s","t","col1","class.pii",""),
        ("c","s","t","col2","class.name","given"),
        ("c","s","t","col3","owner","x"),
    ])
    assert svc.list_class_column_tags() == ["class.name=given", "class.pii"]
```

(Adapt the fixture to however DiscoveryService runs SQL. If injecting an executor is cleaner, add an optional `sql_executor` param to `DiscoveryService.__init__` defaulting to one built from `ws` — dependency injection, matching AGENTS.md.)

- [ ] **Step 2: Run to verify fail**

Run: `cd app && .venv/bin/pytest tests/unit/test_discovery_class_tags.py -v`
Expected: FAIL

- [ ] **Step 3: Implement service method**

In `discovery.py`, add a SQL-backed lister. Use `escape_sql_string` for the `class.%` literal; the query is a fixed string (no user input interpolated beyond the constant prefix) and must pass `is_sql_query_safe()` before execution. Sketch:

```python
def list_class_column_tags(self) -> list[str]:
    """Distinct ``class.*`` column tags visible to the caller (OBO), sorted.

    Reads metastore-wide from ``system.information_schema.column_tags`` (UC
    permission-filters rows to the caller). Falls back to the configured
    catalog on failure; returns ``[]`` if nothing is readable. Never raises.
    """
    like = escape_sql_string(f"{CLASS_TAG_PREFIX}%")
    query = (
        "SELECT DISTINCT tag_name, tag_value "
        "FROM system.information_schema.column_tags "
        f"WHERE tag_name LIKE '{like}'"
    )
    try:
        if not is_sql_query_safe(query):
            return []
        rows = self._run_sql(query)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("class-tag discovery failed: %s", e)
        return []
    seen: set[str] = set()
    for tag_name, tag_value in rows:
        if not tag_name:
            continue
        seen.add(f"{tag_name}={tag_value}" if tag_value else tag_name)
    return sorted(seen)
```

Add `CLASS_TAG_PREFIX` import from `tag_mapping_service`; add `_run_sql` helper (or reuse injected executor). Add the cached async wrapper `list_class_column_tags_async`.

- [ ] **Step 4: Add the route** in `routes/v1/discovery.py` (mirror `get_table_tags`):

```python
class ClassTagsOut(BaseModel):
    tags: list[str] = Field(default_factory=list, description="Distinct class.* column tags")


@router.get("/discovery/class-tags", response_model=ClassTagsOut, operation_id="listClassTags")
async def list_class_tags(
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> ClassTagsOut:
    return ClassTagsOut(tags=await discovery.list_class_column_tags_async())
```

(Confirm the exact router prefix/paths used by neighbouring discovery routes and match them.)

- [ ] **Step 5: Run to verify pass**

Run: `cd app && .venv/bin/pytest tests/unit/test_discovery_class_tags.py -v`
Expected: PASS

- [ ] **Step 6: Regen API + commit**

```bash
make app-regen-api
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/services/discovery.py app/src/databricks_labs_dqx_app/backend/routes/v1/discovery.py app/tests/unit/test_discovery_class_tags.py app/src/databricks_labs_dqx_app/ui/lib/api.ts
git commit -S -m "$(printf 'apply-on-tag: class.* column-tag discovery service + route\n\nCo-authored-by: Isaac')"
```

---

## Task 6: `TagReconcileService` (orchestrator)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/services/tag_reconcile_service.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/dependencies.py` (wire it)
- Test: `app/tests/unit/test_tag_reconcile_service.py`

**Interfaces:**
- Consumes: `RegistryService` (list published rules + get slots/slot_tags), `MonitoredTableService` (list monitored tables), `ApplyRulesService.apply_rule`, `AppSettingsService.get_tag_auto_apply`, a column-tag reader (SP-authed — a callable `read_column_tags(table_fqn) -> list[ColumnInfo]`), and `Materializer` (optional, mirror how routes call it after apply).
- Produces:
  - `reconcile_rule(rule_id: str, user_email: str) -> int` — attach matches for one rule across all monitored tables; returns count attached. No-op (returns 0) when `get_tag_auto_apply()` is False.
  - `reconcile_table(binding_id: str, table_fqn: str, user_email: str) -> int` — attach matches of all published tag-mapped rules to one table. No-op when setting off.
  - `sweep(user_email: str) -> int` — reconcile all published tag-mapped rules × all monitored tables. No-op when setting off.
  - Every attach uses `apply_rule(..., tags={ORIGIN_KEY: ORIGIN_TAG_AUTO})` and is idempotent (identical mapping_hash → existing UPDATE). Failures per (rule,table) are logged and skipped.

- [ ] **Step 1: Write failing tests** — construct with `create_autospec` for each dependency; a fake column-tag reader returning `ColumnInfo` lists; assert:
  - setting OFF → `reconcile_rule`/`reconcile_table`/`sweep` all return 0 and never call `apply_rule`.
  - setting ON, one rule with `slot_tags={"c1":["class.pii"]}`, one table with a `class.pii` STRING column → `apply_rule` called once with a `{"c1": <col>}` group and `tags={"origin":"tag_auto"}`.
  - setting ON, rule with slot_tags but table with no matching column → `apply_rule` NOT called.
  - an `apply_rule` raising for one table does not abort the others (call it with 2 tables, first raises).

Write these against the real resolver (don't mock `resolve`) so the wiring is exercised end-to-end.

- [ ] **Step 2: Run to verify fail** → module missing.

- [ ] **Step 3: Implement** the orchestrator. Pull each published rule's slots (`rule.definition.slots`) and `get_slot_tags(rule.user_metadata)`; skip rules with empty slot_tags. For each table, call the SP column-tag reader, run `resolve(slots, slot_tags, columns, single=False)`, and `apply_rule` per group. Guard each (rule,table) in try/except with `logger.warning`. Return counts.

- [ ] **Step 4: Run to verify pass.**

- [ ] **Step 5: Wire in `dependencies.py`** — a `get_tag_reconcile_service(...)` provider composing SP-authed executors (mirror `get_rule_embeddings_service`'s SP pattern) + the OBO/SP discovery read. The column-tag reader should use the **SP** client (reconcile runs without a user). Reuse `DiscoveryService.get_table_tags` shape but SP-authed, mapping its `column_tags` dict + column types into `ColumnInfo` (you'll need column types too — fetch via `ws.tables.get`, mapping `type_name`).

- [ ] **Step 6: Commit** (service + deps + test; checkout uv.locks; GPG-sign; `Co-authored-by: Isaac`).

---

## Task 7: Wire reconcile hooks (publish / register / sweep)

**Files:**
- Modify: `backend/services/registry_service.py` (`approve`) — hook 1
- Modify: `backend/services/monitored_table_service.py` (`register`, `bulk_register`) — hook 2
- Modify: `backend/services/scheduler_service.py` (tick loop) — hook 3
- Modify: relevant route wiring so the hook has a `TagReconcileService` handle
- Test: extend `test_registry_service.py`, `test_monitored_table_service.py`, `test_scheduler_service.py`

**Interfaces:**
- Consumes: `TagReconcileService.reconcile_rule` / `reconcile_table` / `sweep`.
- Design note: to avoid a circular dependency (RegistryService ← TagReconcileService ← RegistryService), do NOT inject the reconcile service into these low-level services. Instead trigger reconcile at the **route layer** after `approve` / `register` succeeds (mirroring how routes call `Materializer` after `apply_rule` today), OR pass an optional callback. Prefer the route-layer trigger — read `routes/v1/registry_rules.py` (approve endpoint) and `routes/v1/monitored_tables.py` (register endpoint) and call `reconcile_rule`/`reconcile_table` there. The scheduler sweep is internal to `SchedulerService` — inject the reconcile service (or a thin callable) at scheduler construction in `dependencies.py`/app startup.

- [ ] **Step 1: Tests** — route-level: approving a tag-mapped rule with `tag_auto_apply` ON attaches to a monitored table (integration-style with the app's test client + fakes already used in `test_registry_rules_routes.py`); registering a table attaches matching published rules; with setting OFF neither attaches. Scheduler: `_tick` (or the new sweep tick) calls `reconcile.sweep` when due. Match existing route/scheduler test patterns.
- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement** the three triggers. Keep them best-effort: wrap in try/except, log, never fail the originating request/tick. For the scheduler, add a low-frequency cadence (reuse an existing interval constant style; e.g. every N ticks) — read how `_maybe_gc_orphan_views` is gated and mirror it.
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit.**

---

## Task 8: `TagPicker` component

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/components/apply-rules/TagPicker.tsx`
- Modify: i18n locales (4) — keys `tagPickerPlaceholder`, `tagPickerEmpty`, `tagPickerSearch`
- Test: `app/src/databricks_labs_dqx_app/ui/components/apply-rules/TagPicker.test.tsx` (if a colocated test pattern exists — check neighbouring `.test.ts(x)`)

**Interfaces:**
- Produces: `TagPicker({ selected, onSelect, onClose }: { selected: string[]; onSelect: (tag: string) => void; onClose?: () => void })` — renders a searchable list of `class.*` tags from `useListClassTagsSuspense`/`useListClassTags` (generated hook from Task 5's route), lazy-fetched (query enabled only when mounted, i.e. popover open). Filters out already-`selected` tags. Mirrors `ColumnDropdownList` layout (search box + scrollable list + count). Clicking a tag calls `onSelect` then `onClose`. No free-text entry.

- [ ] **Step 1: Write failing test** (if colocated UI tests exist — see `ai-suggestion-utils.test.ts` for the runner; a component test may need `@testing-library/react`. If component-level RTL isn't used in this repo, instead write a pure helper `filterTags(all, selected, query)` in a `tag-picker-utils.ts` and unit-test THAT, keeping the component thin). Check first; prefer testing a pure helper.

```ts
// tag-picker-utils.test.ts
import { filterTags } from "./tag-picker-utils";
test("filters selected and by query", () => {
  expect(filterTags(["class.pii", "class.name", "class.other"], ["class.pii"], "nam"))
    .toEqual(["class.name"]);
});
```

- [ ] **Step 2: Run to verify fail** → `make app-check` (bun test) or `cd app/.../ui && bun test tag-picker-utils`.
- [ ] **Step 3: Implement** `tag-picker-utils.ts` (`filterTags`) + `TagPicker.tsx` using it and the generated query hook.
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** (component + utils + test + 4 locales; checkout uv.locks).

---

## Task 9: Tag chips in `SlotsPanel`

**Files:**
- Modify: `ui/components/RegistryRuleFormDialog.tsx` — `SlotsPanel` (add tag-chip region on the slot header row); thread `slotTags` state through `snapshotFromRule`/build/`PRISTINE_NEW_SNAPSHOT`.
- Modify: i18n locales (4) — `applyToTagButton` ("+ Apply to a tag"), `applyToAnotherTagButton` ("+ Apply to another tag"), `applyToTagTooltip` ("Apply tags to this column to auto-apply this rule to any column of a monitored table with matching tag(s)"), `slotTagChipRemove`.
- Test: extend an existing RegistryRuleFormDialog test or add a pure helper test for slot_tags snapshot round-trip.

**Interfaces:**
- Consumes: `TagPicker` (Task 8); `get_slot_tags`/`set_slot_tags` equivalent on the TS side — read/write `user_metadata.slot_tags` in the rule snapshot. The form already round-trips `user_metadata` (reserved tags); add `slotTags: Record<string, string[]>` to `RuleEditSnapshot`, hydrate from `user_metadata.slot_tags` in `snapshotFromRule`, and write it back into `user_metadata` via the reserved-tag serialization path on save.
- `SlotsPanel` gains props: `slotTags: Record<string, string[]>`, `onSlotTagsChange: (next: Record<string, string[]>) => void`. Chips render right after `{{slot.name}}`, before the family badge, on the always-visible header row. Editable exactly when `!disabled` (inherits the existing gate). Tooltip on the add button.

- [ ] **Step 1: Write failing test** — a pure helper `slotTagsFromUserMetadata(um)` / `userMetadataWithSlotTags(um, map)` in a small `registry-rule-conversion.ts` (that file already exists per grep) mirroring the backend, unit-tested for round-trip + `class.` filtering. Then the panel uses it.

```ts
test("slot_tags round-trip via user_metadata", () => {
  const um = userMetadataWithSlotTags({ name: "x" }, { c1: ["class.pii"] });
  expect(um.slot_tags).toEqual({ c1: ["class.pii"] });
  expect(slotTagsFromUserMetadata(um)).toEqual({ c1: ["class.pii"] });
  expect(slotTagsFromUserMetadata({})).toEqual({});
});
```

- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement** helpers + thread through the form + render chips (reuse the chip visual from `MappingChips` — a small local chip or import the styling) + wire `TagPicker` in a `Popover` on the "+ Apply to a tag" button + tooltip. Ensure it renders for all three modes (the panel already shows for SQL/lowcode/native-with-slots).
- [ ] **Step 4: Run to verify pass** (`make app-check`).
- [ ] **Step 5: Commit** (dialog + conversion + test + 4 locales; checkout uv.locks).

---

## Task 10: Suggestions surface (setting OFF) + admin toggle UI

**Files:**
- Modify: `ui/routes/_sidebar/config.tsx` — add the `tag_auto_apply` Switch (mirror `auto_upgrade_without_approval`, ~line 1881).
- Modify: i18n locales (4) — `config.tagAutoApplyLabel`, `config.tagAutoApplyHint`.
- Modify: the Apply Rules suggestions path (`monitored-tables.$bindingId.tsx` + backend suggester or a new light endpoint) so tag-matched rules appear as suggestions when the setting is OFF.
- Test: config toggle covered by existing settings tests; suggestions covered by a resolver-backed unit test (reuse Task 2) + a route test if a new endpoint is added.

**Design note:** The cheapest correct implementation for the OFF/suggestions path is a backend endpoint `GET /monitored-tables/{binding_id}/tag-suggestions` that runs the resolver (`single=True`) over the table's columns for all published tag-mapped rules NOT already applied, returning suggested (rule, mapping) pairs. Surface them in the existing suggestions area. If time-constrained, the toggle + auto-apply (ON) path is the primary deliverable; the OFF suggestions surface can reuse the same endpoint the ON path's reconcile uses in "dry-run" mode.

- [ ] **Step 1: Toggle first** — add the Switch, wire to `saveRulesRegistrySettings({tag_auto_apply})`, 4 locales. Verify `make app-check`.
- [ ] **Step 2: Commit the toggle.**
- [ ] **Step 3: Suggestions endpoint** — implement `tag-suggestions` route calling the resolver `single=True`, excluding already-applied rules. Unit-test the exclusion + single-group behavior.
- [ ] **Step 4: Wire suggestions into the apply screen** — surface in the existing suggestion list; accept-to-attach reuses the existing apply mutation. `make app-check`.
- [ ] **Step 5: Commit.**

---

## Task 11: Full green + deploy

- [ ] **Step 1:** `make app-check` → require `EXIT 0`, 381+ bun tests pass (new UI tests added).
- [ ] **Step 2:** `make app-test` → require `EXIT 0`, 2937+ backend tests pass (new tests added).
- [ ] **Step 3:** i18n parity check — `cd app && .venv/bin/pytest tests/unit/test_i18n_locale_parity.py -v` (every new `en` key present in pt-BR/it/es).
- [ ] **Step 4:** Deploy:
  ```bash
  make app-deploy PROFILE=fe-sandbox-dq-demo TARGET=dev > /tmp/deploy.log 2>&1; echo "EXIT: $?"; tail -30 /tmp/deploy.log
  ```
  Require `EXIT: 0` + "Deployment complete!" + "App started successfully".
- [ ] **Step 5:** Final commit if any regen/lockfile drift (checkout uv.locks first).

---

## Self-Review notes

- **Spec coverage:** model (T1) · resolver w/ family+any-overlap+cartesian (T2) · setting (T3) · route (T4) · discovery `class.*` lazy/cached (T5) · standing reconcile 3 hooks (T6,T7) · authoring chips (T8,T9) · toggle + suggestions (T10) · deploy (T11). All §2 decisions mapped.
- **`class.*` filter** enforced in resolver (T2), discovery query (T5), and TS conversion (T9).
- **Family compat** in `_column_matches` (T2), never overridden by tags.
- **Origin marker** written on every auto-attach (T6), enabling future two-way sync.
- **Circular-dep risk** addressed in T7 (trigger at route layer, not inside low-level services).
- **OBO vs SP:** picker discovery = OBO (T5); reconcile column reads = SP (T6).
