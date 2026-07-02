# DQX Rules Registry — Design Spec

**Date:** 2026-07-02
**Status:** Approved to implement (owner delegated autonomous build)
**Scope:** Merge the "Rules Registry" concept and surrounding features from `databricks-dqwatch` ("dqlake")
into DQX Studio (`app/`), staying native to DQX and **without changing how jobs run** or the core
persistence/versioning mechanism.

---

## 1. Goals

- A **Rules Registry**: reusable, versioned, governed rule definitions decoupled from any single table.
- OOTB **dimensions** and **severity** taxonomies, admin-managed.
- First-class **rule polarity** (PASS/FAIL) for SQL / low-code rules.
- Consolidate today's two authoring tabs (single-table, cross-table) into the registry.
- Seed **all ~78 existing DQX built-in checks** as OOTB registry rules, with typed slots/params.
- **Monitored tables**: apply registry rules to tables (slot→column mapping) and hold profiling.
- **AI**: build-with-AI rule authoring, per-field suggestions, and an AI **rule-mapping suggester**.
- Preserve existing features: import rules, RBAC + approval lifecycle, reviews, schedules, insights,
  quarantine, free-text tags.

## 2. Non-goals / hard constraints

- **Do NOT change how jobs run.** Execution stays `jobs.run_now` on the pre-provisioned serverless wheel
  task (`DQX_JOB_ID`); the task runner keeps consuming plain DQX check dicts.
- **Keep DQX's persistence/versioning mechanism** (Lakebase Postgres OLTP + Delta fallback; integer
  `version` + append-only history). New tables are additive.
- Stay native to DQX: tags via `user_metadata`, `negate`, `{{variable}}` substitution.
- Core DQX library (`src/databricks/labs/dqx/`) is not regressed; app-side changes only, except additive
  seed metadata.

## 3. Architecture — three layers

```
LAYER 1  Rules Registry (NEW)          table-agnostic rule definitions (slots, dimension, severity,
                                        polarity, mode, versioning)
             │  apply to a monitored table → map slots → real columns
LAYER 2  Monitored tables + mapping (NEW)
             │  publish → materialize
LAYER 3  dq_quality_rules (EXISTING)   concrete check dicts, per-table — the ONLY thing the runner reads
             ▼
         jobs.run_now wheel task (UNCHANGED)
```

The registry is the **authoring/governance** layer; `dq_quality_rules` remains the **materialized/applied**
layer. Publishing a rule bumps its version and writes a frozen snapshot; applied tables that *follow latest*
re-materialize, *pinned* tables stay frozen. The runner never learns the registry exists.

### 3.1 Data model (new tables)

All new OLTP tables get a Postgres migration **and** a mirrored Delta `oltp_fallback` migration (append-only,
never edit existing entries). Column names indicative.

```sql
dq_dimensions(id PK, name UNIQUE, description, color, is_builtin, is_enabled, audit)
dq_dimension_examples(id PK, dimension_id FK, position, text, UNIQUE(dimension_id, position))
dq_severities(id PK, name UNIQUE, description, color, rank UNIQUE, dqx_criticality, is_builtin, is_enabled, audit)

dq_rules(                                  -- registry template (LIVE)
  rule_id uuid PK, name, description,
  mode text,            -- 'dqx_native' | 'lowcode' | 'sql'
  status text,          -- 'draft' | 'pending_approval' | 'approved'(published) | 'rejected' | 'deprecated'
  version int,          -- 0 until first publish
  dimension_id FK NULL, severity_id FK NULL,
  polarity text,        -- 'pass' | 'fail'  (meaningful for lowcode/sql only)
  author_kind text,     -- human | ai_generated | ai_assisted
  definition jsonb,     -- native: {function, arguments-with-{{slots}}}; lowcode: {lowcode_ast, predicate};
                        -- sql: {predicate}; slots+param types declared inline
  user_metadata jsonb,  -- free-text tags (preserved, in ADDITION to dimension)
  fingerprint char(64), -- dedup over canonical definition + slots
  steward text, is_builtin bool, source text, audit)

dq_rule_versions(                          -- FROZEN snapshot on publish (pinnable artifact + audit)
  id PK, rule_id FK, version int, definition jsonb, dimension_id, severity_id, polarity,
  user_metadata jsonb, created_by, created_at, UNIQUE(rule_id, version))

dq_monitored_tables(                       -- thin binding; profiling reuses dq_profiling_results
  binding_id uuid PK, table_fqn text UNIQUE, steward, status text, -- draft | published
  last_profiled_at, created/updated audit)

dq_applied_rules(                          -- the LIVE LINK
  id PK, binding_id FK, rule_id FK,
  pinned_version int NULL,       -- NULL = follow latest published (auto-upgrade)
  severity_override_id FK NULL,
  column_mapping jsonb,          -- [{slot, column}] — one mapping group = one materialized check
  user_metadata jsonb,           -- per-application free-text tags
  audit, UNIQUE(binding_id, rule_id, mapping-hash))
```

Existing `dq_quality_rules` gains provenance columns: `registry_rule_id uuid NULL`, `registry_version int NULL`,
`applied_rule_id uuid NULL`. Mechanism otherwise unchanged.

### 3.2 Slots & typing (carried from DQX)

- A rule declares **slots** (the `{{columns}}`), each with a `family`: `numeric|text|temporal|boolean|any`.
- Non-column arguments are **parameters** with types: `number|string|list|boolean|regex|ref_table|ref_column`.
- Derived from DQX's typed check-function signatures via the existing `listCheckFunctions`; a small seed map
  refines slot family where `str | Column` is too loose.
- Slot family drives the family-filtered column picker when mapping; param types drive value inputs.

## 4. Authoring: three rule types (all compile to DQX check dicts)

| Type | User authors | Serializes to | Polarity |
|---|---|---|---|
| **DQX Native** | pick a built-in check func + fill args | that function directly | via paired funcs / per-func `negate` only — no universal toggle |
| **Low-Code SQL** | condition/aggregation builder (ported) | `sql_expression` (row) / `sql_query` (dataset) | universal PASS/FAIL toggle → `negate` |
| **SQL** | raw predicate/query (CodeMirror) | `sql_expression` / `sql_query` | universal PASS/FAIL toggle → `negate` |

Cross-table logic is expressed as a subquery in the predicate (`sql_query`) or the reference-table checks
(`foreign_key`, `has_valid_schema`) — same as DQX today. Single vs cross-table is real-FQN vs synthetic
`__sql_check__/<name>` under the hood; both live in one registry.

## 5. Approval model (two-tier, reuses existing states/roles)

1. **Registry rule gate:** RULE_AUTHOR `draft` → submit → `pending_approval` → RULE_APPROVER → `approved`
   (published) → `deprecated`. Only a published rule can be applied.
2. **Per-table application gate:** applying a published rule materializes a `dq_quality_rules` row that runs
   the existing `draft → pending_approval → approved` per-table flow (no auto-inherit). Direct/ad-hoc per-table
   authoring is unchanged.
3. **Version-upgrade behaviour = admin setting** `auto_upgrade_without_approval` (default **false = Behaviour B**):
   republishing a rule pushes each *following* table's materialized check back to `pending_approval`. When true
   (Behaviour A) followers silently re-materialize as `approved` (central rule approval suffices). Pinned tables
   never change; surface a "vN available" nudge.

## 6. Seeding

- Seed **all ~78** built-in checks as pre-published, `is_builtin=true` OOTB registry rules (definition locked;
  disable/re-tag allowed). Column args → slots; other args → apply-time parameters. Each pre-assigned a
  dimension + default severity via a seed map (admin-editable). Full function list also available when
  authoring a new native rule.
- Seed dimensions: Validity, Completeness, Accuracy, Consistency, Uniqueness, Timeliness.
- Seed severities: Low, Medium, High, Critical (→ dqx_criticality warn/warn/error/error). Drop `jira_priority`.

## 7. Monitored tables + apply/map + materialization

- **Monitored Tables** screen: list + per-table detail with **Profile**, **Apply Rules**, **Results** tabs.
- **Apply Rules**: by-column / by-rule lenses; slot→column mapping via family-filtered picker; per-application
  **version pin** and **severity override**; add published rules; **AI "Suggest rules"**; and the requested
  **"＋ Create new rule"** entry point that opens the registry create-rule modal inline and returns to mapping.
- **Profiling** reuses existing `dq_profiling_results` + profiler job; displayed per column.
- **Materialization**: on publishing a monitored table, each applied rule × mapping group renders a
  `dq_quality_rules` row (slots substituted with real columns; params filled; dimension/severity/polarity/
  provenance stamped into `user_metadata`). This is the only thing that reaches the runner.

## 8. AI (dqwatch-native substrate, DQX-native validation)

- **AIGateway** (serving-endpoint calls; kill-switch; per-user rate limit; audit log; author_kind provenance).
  Purpose calls: suggest / explain / improve / generate-rule (two-pass low-code→SQL) / suggest-naming /
  suggest-severity. The app's existing `aiAssistedChecksGeneration` is reworked to route through the gateway.
- **DQX-native validation of generated JSON:** reuse DQX's `RuleValidator`, `_filter_unsafe_sql_rules`,
  `LLMRuleCompiler`, `DQEngine.validate_checks`, and PK detection to validate/repair generated check dicts
  before surfacing. (Core-library DSPy engine stays intact for CLI/library users.)
- **Rule-mapping suggester:** **Databricks Vector Search** index over rule embeddings (embedding serving
  endpoint), behind a swappable `RuleRetriever` seam → top-K retrieve → LLM judge → filter/dedup/exclude-applied
  → prefetched suggestion dialog with slot→column mappings. Re-embed on rule publish (fire-and-forget).
- VS endpoint + index + embedding endpoint provisioned in `app/databricks.yml` + post-deploy grants.

## 9. Reviews / workflows / insights integration

- Dimension, severity, polarity, and provenance ride into results **via `user_metadata`** on the materialized
  check — the runner already aggregates check `user_metadata` into `dq_metrics.user_metadata`, so results,
  Runs History, and the Insights dashboard light up with **no runner change**.
- Runs History gains dimension/severity breakdowns and per-failure rule provenance; run-review-status + comments
  unchanged. Schedules unchanged (dimension-scoped scheduling is a cheap future add).

## 10. Navigation / IA

```
Rules Registry      browse + create (3 types) + govern
Monitored Tables    Profile · Apply Rules · Results
Drafts & Review     approvals for registry rules AND per-table applications
──────
Run Rules           RUNNER (unchanged)
Runs History        + run review status (unchanged)
Insights            embedded dashboard (unchanged)
```
Config (header/admin) gains **Dimensions**, **Severity**, and **Import rules** sections. Single-table +
cross-table tabs merge into the Registry; Discovery + Profile&Generate fold into Monitored Tables;
Active Rules removed.

## 11. Migrations plan

- New OLTP tables (`dq_dimensions`, `dq_dimension_examples`, `dq_severities`, `dq_rules`, `dq_rule_versions`,
  `dq_monitored_tables`, `dq_applied_rules`) + `dq_quality_rules` provenance columns: one appended
  `PgMigration` and one mirrored appended `DeltaMigration(oltp_fallback=True)` each. Analytical-only changes
  Delta-only. Follow all DDL invariants (no `;`/placeholders in literals, `IF NOT EXISTS`, `ADD COLUMN` w/o
  `IF NOT EXISTS` on Delta, CHECK constraints separate). Add `test_migration_runner` / `test_pg_migration_runner`
  coverage.
- Seed data applied idempotently at startup (existing rows never overwritten), mirroring dqwatch seeds.

## 12. Phased decomposition (each phase: spec-ref → plan → TDD → review)

1. **Taxonomies + admin** — dimensions & severities tables, seeds, CRUD endpoints, Config UI cards; move Import
   into Settings.
2. **Registry core + authoring** — `dq_rules` + `dq_rule_versions`, registry service (CRUD, publish→snapshot,
   deprecate, fingerprint dedup), seed all 78 built-ins, slot/param model, registry list + create modal (3
   types, polarity), two-tier approval at the registry.
3. **Monitored tables + apply/map + materialization** — `dq_monitored_tables` + `dq_applied_rules`, apply-rules
   UI (lenses, mapping, pin, override, ＋create-rule), materialization into `dq_quality_rules` on publish,
   auto-upgrade setting, per-table approval, profiling tab.
4. **AI** — AIGateway, build-with-AI + per-field suggest (DQX-validated), Vector Search embeddings + mapping
   suggester.
5. **Reviews / insights / polish** — dimension/severity into metrics + Runs History breakdowns, nav
   consolidation cleanup, copy pass, i18n across all 4 locales.

## 13. Testing & quality

- Backend: unit tests per new service/route/migration/dispatcher (`make app-test`, no Databricks).
- Type safety: `make app-check` (tsc + basedpyright) green.
- i18n parity across en / pt-BR / it / es (enforced by `test_i18n_locale_parity`).
- No lint disables (`no-cheat` / `test_lint_policy`). GPG-signed commits. orval regen after backend changes
  (never hand-edit `api.ts`).
- **DQ-steward reviewer subagent** validates that a non-expert is railroaded through author→approve→apply→run.
- Copy: plain, confident, non-cringe.

## 14. Risks / open

- **Local end-to-end run** needs a Databricks profile + `app/.env` (none present in the worktree). Offline
  verification (`make app-test`, `make app-check`, build) is the primary gate; live launch attempted against a
  DQ sandbox profile if safe.
- Vector Search adds deploy-time infra (endpoint/index/embedding endpoint) — provisioned via DAB.
- Materialization fan-out (rule × mappings) and auto-upgrade re-materialization must be idempotent and keyed by
  provenance to avoid orphaned/duplicate `dq_quality_rules` rows.
