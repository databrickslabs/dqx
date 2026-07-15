# Apply on Tag — tag-based rule auto-mapping

> Design spec for the `dqx/apply-on-tag` stream. Lets a registry rule's
> `{{slot}}` placeholders auto-bind to real Unity Catalog **column tags** (the
> `class.*` namespace), so a published rule fans out to every monitored table
> whose columns carry matching tags — instead of being mapped table-by-table,
> column-by-column, by hand.

Date: 2026-07-13
Branch: `dqx/apply-on-tag` (off `dqx-dqlake-integration` @ `fe7efa6a`)

---

## 1. Problem & goal

Today, applying a registry rule to a monitored table means opening that table's
**Apply Rules** screen and hand-mapping each of the rule's declared slots
(`{{slot}}`) to a real column (`apply-rules/MappingChips.tsx` +
`ApplyRulesService.apply_rule`). Every table is done one at a time.

**Goal:** on the rule itself (in the Registry "Columns used" panel), an author
can declare, per slot, one or more **UC column tags**. A published rule then
auto-maps to any monitored table whose columns carry those tags — a
**standing** relationship that keeps matching new tables and newly-tagged
columns over time, gated by a workspace admin toggle.

This is purely additive: a rule with no slot-tags behaves exactly as today.

## 2. Scope decisions (locked with the user)

| Decision | Choice |
|---|---|
| Tag source | **Real Unity Catalog column tags** (not app labels, no free text). |
| Tag namespace | **Only `class.*` keys** (e.g. `class.name`, `class.pii`). Everything else is invisible to this feature. |
| Match — within a slot | **ANY-overlap**: a column matches a slot if it carries *any one* of that slot's declared tags. Not "all of them". |
| Match — tag value | Match on tag **key**; a bare key matches any value, an explicit `key=value` requires an exact value match. |
| Match — across slots (multi-slot rule) | A mapping group is valid only when **every** slot is filled by some column. |
| Type/family compatibility | **Still enforced** — a tag match never overrides family. A column qualifies only if it carries a matching tag **AND** is family-compatible (`slot.family == "any"` or `family_for_type(col) == slot.family`). |
| Multiplicity when auto-apply ON | **Cartesian product** of each slot's matching columns. |
| Multiplicity when auto-apply OFF | Collapse to **one** representative group (surfaced as a suggestion). |
| Standing vs one-time | **Standing** — reconciles on publish, on new monitored table, and on a periodic sweep. |
| Admin setting | **One global boolean, default OFF.** |
| Removal semantics | **Add-only** (option a): auto-apply never detaches. Auto-created rows are stamped `origin: "tag_auto"` so true two-way sync can be added later without migration. |
| Editability | Slot-tags are **core rule logic**, gated by the existing role model (see §7). Not a separate read-only concept. |

## 3. Data model

### 3.1 Slot→tags binding lives in `user_metadata`

Rather than a new top-level field, the slot→tags mapping is stored in the
rule's existing `user_metadata` JSON dict (the same place reserved keys like
`dimension`/`severity` live — see `registry_models.py`), under a new reserved
key:

```python
user_metadata["slot_tags"] = {
    "<slot_name>": ["class.pii", "class.name=given"],
    ...
}
```

Consequences:
- It's part of the rule **definition/logic**: versioned into `dq_rule_versions`
  snapshots, travels through the JSON dialog, and edits to an approved rule go
  through the normal revision/approval path.
- **No new table, no migration** — `user_metadata` is an existing JSON column.
- New helpers next to `get_reserved_tag`/`set_reserved_tag`:
  `get_slot_tags(user_metadata) -> dict[str, list[str]]` and
  `set_slot_tags(user_metadata, mapping) -> dict[str, Any]`.
- `slot_tags` is added to the reserved-key set so it's excluded from the
  free-text tags UI.

### 3.2 Validation guard

`set_slot_tags` (and the create/update rule path) validate that:
- every key names a real declared slot on the rule,
- every tag string begins with `class.` (rejects any non-`class.` namespace,
  so a hand-edited JSON dialog can't inject one),
- tag strings are well-formed (`key` or `key=value`).

### 3.3 Auto-created attachment marker

When the engine auto-attaches a rule to a table, the resulting `AppliedRule`
carries `user_metadata["origin"] = "tag_auto"`. Hand-applied rows stay
unmarked and are never touched by the engine. This is the seam for a future
two-way-sync (detach-on-no-longer-match) without a migration.

## 4. Matching engine — `TagMappingResolver`

New module `backend/services/tag_mapping_service.py`. Kept free of I/O so the
match logic is unit-testable in isolation.

```
resolve(slots_with_tags, table_columns_with_tags) -> list[ColumnMappingGroup]
```

Algorithm:
1. Restrict every column's tag set to `class.*` keys.
2. For each slot, candidate columns = columns that (a) carry **any one** of the
   slot's declared tags (bare key → any value; `key=value` → exact) **AND**
   (b) are family-compatible with the slot.
3. A valid mapping group requires **every** slot to have ≥1 candidate.
   Produce the **Cartesian product** across slots' candidate sets.
4. Deterministic ordering (slot position, then column name) so re-runs are
   stable and `compute_mapping_hash` is idempotent.
5. Caller collapses to one representative group when in suggestions mode.

Reuses `compute_mapping_hash` (existing) so re-attaching an identical group is
the existing no-op UPDATE in `ApplyRulesService.apply_rule`, never a duplicate.

**Family classification** reuses the single canonical `family_for_type`
classifier currently in `rule_suggester.py` — lifted to a shared location
(e.g. `registry_models.py` or a small `slot_families.py`) so there is one
source of truth, not a duplicated type→family map.

## 5. Standing reconcile — hook points

All reconcile runs are **SP-authed** (service principal — column-tag discovery
must run without a user in the loop) and **idempotent + best-effort** (a failed
table/rule is logged and skipped, never crashes the caller). They all converge
on `TagMappingResolver` → `ApplyRulesService.apply_rule` → `Materializer`, as
the manual path does today.

1. **On publish/approve** — `RegistryService.approve` (and the edit-republish
   path): scan all monitored tables, attach matches for that one rule.
2. **On monitored-table registration** — `MonitoredTableService.register` /
   `bulk_register`: scan all published tag-mapped rules, attach matches for the
   new table.
3. **Periodic sweep** — in `SchedulerService` (already has the SP client + an
   idempotent tick loop): catches tag changes on already-monitored tables (a
   column tagged after both rule and table already exist). Low-frequency.

Reconcile reads each monitored table's column tags **per table** (bounded to
tables the app already governs — reusing `DiscoveryService.get_table_tags`'s
SDK read, or a `WHERE catalog_name=…`-scoped `information_schema` query). No
metastore-wide sweep on the engine path.

## 6. Admin toggle

One global boolean in `AppSettingsService`, mirroring
`auto_upgrade_without_approval` exactly:

```python
_TAG_AUTO_APPLY_KEY = "tag_auto_apply"          # default False
def get_tag_auto_apply(self) -> bool: ...
def save_tag_auto_apply(self, enabled, *, user_email) -> bool: ...
```

Surfaced as a `Switch` in the admin **Config** screen
(`routes/_sidebar/config.tsx`), default **OFF**.

- **OFF** → tag maps never auto-attach. They feed the **suggestions** surface
  on a table's Apply Rules screen (§8), collapsed to one representative group,
  where a steward accepts/rejects.
- **ON** → tag maps eagerly auto-attach across monitored tables (Cartesian
  product), via the §5 reconcile hooks.

## 7. Authoring UI

Entry point: the Registry rule form's **"Columns used"** panel (`SlotsPanel` in
`RegistryRuleFormDialog.tsx`). Works for **all** rule types (SQL / Low-Code /
DQX Native) and shows **regardless of expanded/collapsed** state (the tag
affordance is on the always-visible slot header row).

Layout — tag chips sit **immediately after the `{{slot_name}}`**, before the
family badge:

```
{{franchiseID}}  [ class.pii ▾ × ]  [ + Apply to a tag ]   [TEXT]   ×
```

- Each declared tag → a removable chip, reusing `MappingChips.tsx` chip styling.
- **`+ Apply to a tag`** / **`+ Apply to another tag`** — dashed button (same as
  MappingChips' "+ Apply to another column"), opens the `TagPicker` popover.
- Hover tooltip, verbatim: *"Apply tags to this column to auto-apply this rule
  to any column of a monitored table with matching tag(s)."*

New `TagPicker.tsx` (sibling to `ColumnPicker.tsx`), mirroring
`ColumnDropdownList` look. Lists **only real `class.*` UC tags** (§9). A
search/filter box narrows the real list; **no free-text entry** — every
selectable item is a real tag. Click-to-commit closes the popover.

**Editability = core rule logic, gated by role** (`common/authorization.py`):
- ADMIN / RULE_APPROVER / RULE_AUTHOR (hold `edit_rules`) → chips
  add/removable, inheriting `SlotsPanel`'s existing `disabled={!canEdit}` gate
  (`canEdit = (draft||approved) && perms.canCreateRules`).
- VIEWER / RUNNER → whole form is `viewingRule` (read-only); chips render as
  static labels, no `×`/add — same as every other field.
- Editing an approved rule follows the existing revision→approval path; the
  slot-tags ride along in the definition.

## 8. Suggestions path (toggle OFF)

The resolver runs live on the Apply Rules screen for the viewed table, and
tag-matched rules surface in the **existing suggestions surface** on
`monitored-tables.$bindingId.tsx` (alongside profiler/AI suggestions),
collapsed to one representative group, with accept-to-attach. No new UI
paradigm — plugs into the suggestion list already there.

## 9. Tag discovery (for the picker)

- **Author picker** queries **`system.information_schema.column_tags`** once,
  metastore-wide, **OBO** (on-behalf-of the author). UC row-filters this to
  objects the author can access, so there's no per-catalog fan-out and no
  over-exposure:
  ```sql
  SELECT DISTINCT tag_name, tag_value
  FROM system.information_schema.column_tags
  WHERE tag_name LIKE 'class.%'
  ```
  (`class.` literal properly escaped.) **Lazy** — fires only when the picker
  popover opens (TanStack Query `enabled` gated on open), never on page load or
  form open. **Cached** — server-side `@app_cache.cached(ttl=…)` (shared across
  authors) + client stale-time (one fetch per authoring session). Graceful
  degrade: if `system` isn't accessible, fall back to the app's configured
  catalog's `information_schema.column_tags`.
- Column shape (per docs): `CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME,
  TAG_NAME, TAG_VALUE` — all permission-filtered by UC.

## 10. Security

- All generated SQL passes `is_sql_query_safe()`; identifiers validated/escaped
  (`validate_fqn` / `quote_fqn` / `escape_sql_string`). The `class.%` LIKE
  literal is escaped.
- The `system.information_schema` read is OBO, so it can't leak tags beyond the
  author's own UC visibility.
- Reconcile reads are SP-authed but bounded to already-governed monitored
  tables; no arbitrary user input drives a metastore-wide scan.
- `slot_tags` validation rejects non-`class.` namespaces (defense against a
  hand-edited JSON dialog).

## 11. Wiring, i18n, tests

- New backend response models → `make app-regen-api` (regen `ui/lib/api.ts`;
  hand-update `api-custom.ts` if a type is imported from there).
- New i18n keys (`applyToTagButton`, `applyToAnotherTagButton`,
  `applyToTagTooltip`, `tagPickerPlaceholder`, config toggle label/hint, …) in
  **all 4 locales** (`en`, `pt-BR`, `it`, `es`); `en` is source of truth.
- Tests:
  - **Unit** — `TagMappingResolver` (tag×family matrix, any-overlap, cartesian,
    value-optional matching, `class.*` filtering, idempotency); `slot_tags`
    get/set + validation; `tag_auto_apply` getter/setter; the discovery query
    string (safety + `class.%` filter).
  - **Integration** — the three reconcile hooks (publish / register / sweep),
    add-only + `origin` marker, no-duplicate re-apply.
  - **UI** — chip flow, tooltip copy, role-gated editability, picker lists only
    real `class.*` tags, no free-text.

## 12. Non-goals (v1)

- Two-way sync / detach-on-no-longer-match (deferred; seam preserved via the
  `origin: "tag_auto"` marker).
- Table-level tag matching (`table_tags`) — this feature is column-tag /
  slot-mapping only.
- Tag namespaces other than `class.*`.
- Per-rule auto-apply override (single global toggle only).
