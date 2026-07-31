# Rules Marketplace — Design

**Date:** 2026-07-31
**Branch:** `dqx-studio/rules-marketplace` (worktree off `dqx-studio/dqlake-integration`)
**Status:** Approved design, ready for implementation plan

## Summary

An **admin-only Marketplace** page in DQX Studio where an admin browses curated
**content packs** — domain-organised bundles of reusable data-quality rules —
selects **individual rules** across packs, and imports them into the Rules
Registry as reusable rule templates (unbound, column `{{slots}}` mapped later via
the existing Apply Rules flow). The page also hosts the existing **Deploy demo
content** action (relocated from Settings) as its first row.

Packs are DQX-YAML files bundled in the app wheel. The import path reuses the
existing `POST /registry-rules/batch-import` endpoint (fingerprint dedup +
publish). No new import/validation logic is introduced.

## Goals

- Give admins a discovery-first catalogue of ready-made DQ rules, grouped by
  **domain** (Pricing & Money, Contacts & People, Standard checks, …).
- Rule-level selection: tick individual rules across any packs, then one
  **"Import N selected"** action.
- **Industry** is a per-rule tag and the sole list filter.
- Imported rules are **reusable templates** that publish immediately (dedup skips
  already-imported rules).
- Relocate the demo-content deployer into the Marketplace as its first row.
- Fix a latent bug in the shared read-only rule-logic view (missing polarity)
  that this feature depends on.

## Non-goals

- No per-table binding at import time (mapping stays in Apply Rules).
- No new check functions — every rule uses existing DQX built-ins or
  `sql_expression`.
- No user-authored/custom packs, no external pack registry, no versioning UI
  (packs are versioned in git).
- No "recommended for this table" / profiler integration (future work).
- No approval-workflow step in the import (publishes directly).

## Users & access

**Admin only.** Enforced in three places, consistent with existing patterns:
- Sidebar item renders only when `usePermissions().isAdmin`.
- Route component redirects non-admins (mirrors `ConfigPage`'s
  `useEffect`-redirect).
- Backend `marketplace` router is hard-gated with `require_role(UserRole.ADMIN)`.

## UX

### Placement

New sidebar item **"Marketplace"** in the bottom-pinned group in
`routes/_sidebar/route.tsx`, positioned **just above "Documentation"**. Icon:
`Store` (lucide). Gated on `isAdmin`.

### Page structure (top to bottom)

1. **Toolbar** — free-text search (matches rule name/description across all
   packs; auto-expands packs with hits) + **"Import N selected"** button
   (disabled with 0 selected; label shows the live count, e.g. "Import 3
   selected").
2. **Industry filter** — chip row (`All`, `Banking`, `Retail`, `Healthcare`,
   `Telco`, `Insurance`, `Logistics`, …). Selecting an industry narrows visible
   rules to those tagged with it; rules tagged `general` (or untagged) always
   show. This is the only filter (no dimension filter).
3. **Deploy demo content** — the first row, in a distinct amber-tinted box.
   Clicking the row opens the demo **confirm modal** (wipe-first checkbox,
   existing `useDeployDemoContent` flow). No separate "Deploy" button — the row
   itself is the trigger.
4. **Pack groups**, sorted **A–Z** by title. Each pack is a collapsible group:
   - Header: tri-state checkbox (none/some/all in pack), icon, title, "N / M
     selected" count, chevron.
   - Rules: each rule row has a **checkbox** (toggles selection), the **rule
     name**, its **dimension** and **severity** badges beside the name, the
     **industry tag(s)**, and the **description** beneath the name.

### Interactions

- **Checkbox** toggles rule selection (row-level) or whole-pack selection
  (header, tri-state). The click target for selection is the checkbox.
- **Clicking a rule row** (anywhere other than the checkbox) **expands it
  inline** to reveal the rule logic. **Accordion: one rule open per pack** —
  opening another rule in the same pack closes the previous.
- Selection state is client-side until Import.
- **Import** sends the selected rules' check dicts through
  `importChecksAsRegistryDrafts` → `batch-import` with `skip_duplicates: true`,
  `also_submit: false` (publishes directly). Toast reports saved / reused /
  failed counts. Selection clears on success.
- Industry filter + search operate on the already-loaded catalogue (all
  filtering client-side; the catalogue is small).

### Rule-logic inline view (reuses existing component)

The expanded rule reuses the exported **`RuleLogicDisclosure`** from
`components/apply-rules/RuleConfigCard.tsx` (already used by `AddRulesDialog`),
which renders:
- **lowcode** rules → the read-only `LowcodeBuilder` "IF …" structured builder,
- **dqx_native** rules → friendly function label + read-only `RuleParametersView`,
- **sql** rules → read-only SQL `<pre>`.

Because the marketplace preview passes a synthesised `RegistryRuleOut`-shaped
object (from the pack's check dict) into this component, it renders identically
to Apply Rules — no bespoke preview code.

### Bundled bug-fix: polarity in `RuleLogicDisclosure`

**Bug:** `RuleLogicDisclosure` renders the condition but omits the rule's
**polarity** — the editor (`RegistryRuleFormDialog` via
`PredicatePolaritySwitch`, i18n `rulesRegistry.polarityPass/Fail`) lets an author
say a true predicate is a pass or a fail, but the read-only disclosure never
shows it. A viewer can't tell whether "IF `{{number}}` >= 0" passing means the
row is good or bad.

**Fix:** append a read-only polarity line to `RuleLogicBody` driven by
`registryRule.polarity`, reading exactly **"THEN THE RULE PASSES"** or **"THEN
THE RULE FAILS"** — a single consistent font/treatment, no "when true" suffix.
Rendered for all three modes. This fix lands in the shared component, so **Apply
Rules → by-rule cards benefit too**; note it in the What's-new entry and cover it
with a UI test. New i18n keys in all four locales under the namespace the
disclosure already reads from (`monitoredTables.*`, alongside
`ruleLogicLabel`): `monitoredTables.ruleLogicThenPasses` = "THEN THE RULE
PASSES", `monitoredTables.ruleLogicThenFails` = "THEN THE RULE FAILS".

### Animations

- Page container uses the existing `FadeIn`.
- Pack rows stagger in on load.
- Rule inline expand/collapse uses the existing grid-rows height transition +
  chevron rotate already in `RuleLogicDisclosure`.
- Checkbox / row-selected states transition (background/border).

## Architecture

### Pack storage & format

Packs are DQX-YAML files bundled in the wheel under a new package resource dir:

```
app/src/databricks_labs_dqx_app/backend/marketplace/
├── __init__.py
├── loader.py            # discovers + parses + validates pack YAML at import time
├── models.py            # MarketplacePack, MarketplaceRule pydantic models
└── packs/
    ├── addresses_and_geo.yaml
    ├── codes_and_classifications.yaml
    ├── contacts_and_people.yaml
    ├── dates_and_freshness.yaml
    ├── pricing_and_money.yaml
    ├── standard_checks.yaml
    └── transactions_and_amounts.yaml
```

Each pack YAML:

```yaml
id: pricing-and-money
title: Pricing & Money
icon: DollarSign                 # lucide icon name
description: >
  Sentence describing the domain the pack covers.
rules:
  - name: Valid credit card (Luhn)
    description: Card number passes the Luhn checksum after non-digits are removed.
    industries: [banking]        # [] / omitted => general (always shown)
    criticality: error           # DQX execution field
    user_metadata:
      dimension: Validity
      severity: High
    check:
      function: sql_expression
      arguments:
        expression: "luhn_check(regexp_replace({{card_number}}, '[^0-9]', ''))"
```

- Rules are authored in their most natural mode: **dqx_native** for built-in
  check funcs (`is_not_null`, `is_valid_email`, `regex_match`, `is_in_range`,
  `is_unique`, `has_no_outliers`, `is_not_in_future`, `is_data_fresh`, …),
  **sql** for checksum/cross-field logic (Luhn, structuring, cost≤price),
  **lowcode** where a simple comparison reads best in the IF builder.
- Column arguments use `{{slot}}` placeholders → the existing conversion path
  turns them into reusable rule slots.
- `dimension` and `severity` go in `user_metadata` reserved keys and render via
  the app's standard `TagBadge` / `SeverityBadge`. Values must match the seeded
  label definitions: dimensions ∈ {Validity, Completeness, Accuracy,
  Consistency, Uniqueness, Timeliness}; severities ∈ {Low, Medium, High,
  Critical}.

`loader.py` reads every YAML at first request (cached), validates each rule
through the existing check validator, and logs+skips a malformed pack rather
than failing startup.

### Backend API (new `routes/v1/marketplace.py`, admin-gated)

- `GET /api/v1/marketplace/packs` → list of packs, each with `id`, `title`,
  `icon`, `description`, and its rules. Each rule carries: stable `rule_key`
  (pack id + slug), `name`, `description`, `industries[]`, `dimension`,
  `severity`, and the **normalised check dict** (same shape
  `normalizeImportedCheck` produces) so the UI can both preview (via
  `RuleLogicDisclosure`) and import (via `importChecksAsRegistryDrafts`) without
  a second round-trip.

No new import endpoint. The UI collects the selected rules' check dicts and
calls the existing `batch-import` path. Filtering/search are client-side.

### Frontend

- New route `routes/_sidebar/marketplace.tsx` (admin-gated; redirect otherwise).
- New components under `components/marketplace/`:
  - `MarketplacePage` — toolbar, industry filter, demo row, pack list.
  - `PackGroup` — collapsible pack with tri-state header checkbox.
  - `MarketplaceRuleRow` — checkbox + name + dimension/severity badges +
    industry tags + description + inline `RuleLogicDisclosure` (accordion).
  - `DeployDemoRow` — the amber demo row + confirm dialog.
- Extract the existing `DeployDemoCard` logic (`settings.tsx` ~L3023) into a
  reusable hook/component so both the (removed) settings entry and the new demo
  row share it. **Remove** the `deployDemo` entry from `settings.tsx` (L3200) —
  the demo action now lives only in the Marketplace.
- `api.ts` regenerated via `make app-regen-api` after the backend route lands.

## Rule catalogue

Legend: **name** — dimension / severity — mode/check. Slots in `{{ }}`.
All rules import as reusable templates. Industry tags in (parens); packs with no
industry note are `general`.

### Pricing & Money  *(icon: DollarSign)*
1. Amount must be non-zero — Validity / Medium — `is_not_equal_to({{amount}}, 0)` (banking, retail)
2. Price cannot be negative — Validity / High — `is_not_less_than({{price}}, 0)` (retail)
3. At most two decimal places — Consistency / Low — sql `{{amount}} = round({{amount}}, 2)`
4. Valid ISO-4217 currency code — Validity / Medium — `regex_match({{currency}}, '^[A-Za-z]{3}$')`
5. Valid numeric currency code — Validity / Low — `regex_match({{currency_code}}, '^[0-9]{3}$')`
6. Cost cannot exceed price — Consistency / Medium — sql `{{cost}} <= {{price}}` (retail)
7. Margin not extreme — Accuracy / Medium — sql margin bounds (retail)
8. Valid credit card (Luhn) — Validity / High — sql `luhn_check(regexp_replace({{card_number}}, '[^0-9]', ''))` (banking)
9. Valid IBAN format — Validity / Medium — `regex_match({{iban}}, '^[A-Za-z]{2}\d{2}[A-Za-z0-9]{1,30}$')` (banking)

### Contacts & People  *(icon: User)*
1. Valid email — Validity / High — `is_valid_email({{email}})`
2. Valid phone (E.164) — Validity / High — `regex_match({{phone}}, '^\+[1-9]\d{1,14}$')`
3. Phone must include country code — Validity / Medium — sql leading `+` and 1–3 digit code
4. Valid US phone (NANP) — Validity / Low — `regex_match` NANP
5. Full name must be present — Completeness / Medium — `is_not_null_and_not_empty` over `{{first_name}}`,`{{last_name}}` (for_each)
6. Valid MSISDN — Validity / Medium — `regex_match({{msisdn}}, '^\+?[1-9]\d{1,14}$')` (telco)
7. Valid IMSI — Validity / Medium — `regex_match({{imsi}}, '^\d{15}$')` (telco)

### Addresses & Geo  *(icon: MapPin)*
1. Valid UK postcode — Validity / Medium — regex (GDS)
2. Valid Canadian postal code — Validity / Low — regex
3. Valid Netherlands postcode — Validity / Low — regex
4. Valid German postcode — Validity / Low — `^\d{5}$`
5. Valid French postcode — Validity / Low — `^\d{5}$`
6. Valid Australian postcode — Validity / Low — `^\d{4}$`
7. Valid postcode (generic) — Validity / Low — permissive regex
8. Valid ISO-2 country code — Validity / Medium — `^[A-Za-z]{2}$`
9. Valid ISO-3 country code — Validity / Medium — `^[A-Za-z]{3}$`
10. Valid latitude — Validity / Medium — `is_in_range({{lat}}, -90, 90)`
11. Valid longitude — Validity / Medium — `is_in_range({{lon}}, -180, 180)`

### Dates & Freshness  *(icon: CalendarClock)*
1. Must not be in the future — Validity / Medium — `is_not_in_future({{ts}})`
2. End must not precede start — Consistency / High — sql `{{end_ts}} >= {{start_ts}}`
3. Must be fresh (within SLA) — Timeliness / Medium — `is_data_fresh({{ts}}, {{max_age_minutes}})`
4. Admission before discharge — Consistency / High — sql `{{admission_ts}} <= {{discharge_ts}}` (healthcare)

### Standard checks  *(icon: SquareCheck)*  (merged Identifiers + Completeness Basics)
1. Must not be null — Completeness / High — `is_not_null({{column}})`
2. Must not be empty — Completeness / High — `is_not_empty({{column}})`
3. Must have no surrounding whitespace — Validity / Low — sql `{{column}} = trim({{column}})`
4. Must be unique — Uniqueness / Critical — `is_unique([{{column}}])`
5. Valid UUID — Validity / Medium — `regex_match({{uuid}}, '^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$')`
6. Must have no statistical outliers — Accuracy / Medium — `has_no_outliers({{column}})` (MAD, median ± 3.5·MAD)

### Codes & Classifications  *(icon: FileBadge)*
1. Valid ICD-10-CM code — Validity / High — regex (healthcare)
2. Valid CPT code — Validity / Medium — regex (healthcare)
3. Valid FHIR administrative gender — Validity / Medium — `is_in_list({{gender}}, [male,female,other,unknown])` (healthcare)
4. Valid hex colour — Validity / Low — `regex_match({{hex}}, '^#[0-9A-Fa-f]{6}$')` (retail)

### Transactions & Amounts  *(icon: ShieldAlert)*  (banking)
1. Round-amount structuring — Validity / High — sql round-number pattern
2. Amount just below reporting threshold — Validity / High — sql `{{amount}} between 9950 and 9999`
3. Credit must be positive — Consistency / High — sql sign-vs-type
4. Debit must be negative — Consistency / High — sql sign-vs-type
5. Duplicate transaction — Uniqueness / High — `is_unique([{{account}}, {{amount}}, {{reference}}])`

**Totals:** 7 packs, ~48 rules. Exact regexes/thresholds are drafts validated
against sourced research; final values live in the pack YAML and are covered by
tests.

### Reusability principle (why some rules were cut)

Domain packs contain only **reusable domain logic** — no rule that bakes in a
table-specific allow-list or arbitrary bounds, and no bare null/uniqueness
duplicate of a Standard check. This removed (from earlier drafts): "valid
transaction status", "valid product category", "weight in bounds", "valid
tracking number", "reading within spec", "valid machine ID", "SKU format",
"currency is a real national currency", "amount within system bounds", plus
per-pack "present"/"unique" duplicates. The Logistics and Manufacturing industry
packs dissolved once their non-reusable rules were removed; their surviving
generic rules (date ordering, non-negativity) live in Dates/Standard.

## Testing

- **Backend unit** (`app/.../backend/.../test_marketplace_*`):
  - Every bundled pack YAML loads, parses, and each rule passes the existing
    check validator (guards against typos in check names / arguments).
  - Every rule's `dimension`/`severity` ∈ the seeded label vocabularies.
  - Loader skips a malformed pack with a WARNING (inject a bad temp pack).
  - `GET /marketplace/packs` returns the catalogue; route rejects non-admin
    (403) via `require_role`.
- **Frontend unit** (vitest):
  - Client filtering: industry chip narrows rules; `general` rules always show;
    search matches name/description.
  - Tri-state pack-header checkbox logic (none/some/all) and select/deselect.
  - "Import N selected" disabled at 0; count reflects selection.
  - `RuleLogicDisclosure` polarity line renders "THEN THE RULE PASSES/FAILS" for
    each polarity (regression test for the bundled bug-fix).
- **Integration** (existing patterns): import a pack's rules via `batch-import`
  and assert they land as reusable drafts, publish, and re-import dedupes
  (`reused` > 0, no duplicates).
- Manual visual QA on the sandbox after deploy (English-only), per project
  conventions.

## Rollout / conventions

- Worktree `dqx/rules-marketplace` off `dqx-studio/dqlake-integration`.
- i18n: all new strings in `en`, `pt-BR`, `it`, `es`.
- GPG-signed commits with `Co-authored-by: Isaac` trailer; never stage
  `*/uv.lock`; no lint suppression; real-exit-code deploy capture.
- What's-new entry in `docs/dqx/docs/studio/whats-new/index.mdx` covering the
  Marketplace and the polarity fix.
- `make app-check` (tsc + basedpyright) + `make app-test` green before merge.

## Open risks

- **Regex accuracy** (UK postcode, ICD-10, CPT, phone) — drafts from sourced
  research; validate final strings in pack tests. Keep quantifiers bounded
  (ReDoS) per repo security rules, especially the generic postcode pattern.
- **`luhn_check`** requires DBR 13.3+/Spark 3.5+ — acceptable for the Studio's
  execution targets; note in the rule description.
- **IBAN checksum** (mod-97) intentionally **not** shipped — only structure
  regex — because it can't be a clean SQL expression; a future `is_valid_iban`
  core check function is the right home.
