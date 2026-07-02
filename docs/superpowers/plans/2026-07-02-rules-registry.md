# Rules Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Each phase's fine-grained steps are authored just-in-time against the live codebase before that phase executes.

**Goal:** Merge the dqwatch Rules Registry (reusable versioned rules, dimensions/severity, monitored tables, AI) into DQX Studio without changing how jobs run.

**Architecture:** Three layers — registry (new `dq_rules`/`dq_rule_versions`) → monitored tables + mapping (new `dq_monitored_tables`/`dq_applied_rules`) → existing `dq_quality_rules` (materialized, runner-facing, unchanged). See `docs/superpowers/specs/2026-07-02-rules-registry-design.md`.

**Tech Stack:** FastAPI + Pydantic 2 backend, hybrid Lakebase Postgres / Delta store, React 19 + TanStack + shadcn/ui frontend (orval-generated client), Databricks Vector Search, serving-endpoint AIGateway.

## Global Constraints

- Execution path (`jobs.run_now` on `DQX_JOB_ID` wheel task; runner consumes DQX check dicts) MUST NOT change.
- New OLTP tables/columns: append one `PgMigration` AND one mirrored `DeltaMigration(oltp_fallback=True)`; never edit/reorder existing migrations. Delta DDL: `IF NOT EXISTS`; `ADD COLUMN` (no `IF NOT EXISTS`); CHECK constraints separate; no `;` or `{catalog}`/`{schema}` inside string literals.
- Every user-facing string via `t("key")`; add keys to ALL 4 locales (en, pt-BR, it, es); en is source of truth.
- Never edit generated `ui/lib/api.ts` / `routeTree.gen.ts`; run `make app-regen-api` after backend model/route changes.
- No lint disables (`# noqa`/`# type: ignore`/`# pylint: disable`). Full type hints; Pydantic 2 models; DI via `Depends`; wrap sync SDK calls in `asyncio.to_thread`.
- Portable SQL only via executor dialect helpers (`q()`, `json_literal_expr()`, `ts_text()`, `upsert()`, `select_json_text()`).
- RBAC via `require_role(...)`; SQL identifiers via `validate_fqn`/`quote_fqn`; user SQL via `is_sql_query_safe()`.
- Verify each phase: `make app-test` green, `make app-check` green, `test_i18n_locale_parity` green.
- Commits: conventional messages, `Co-authored-by: Isaac`. (GPG unavailable in build env — commits unsigned locally, MUST be re-signed before push.)
- Copy: plain, confident, non-cringe.

---

## PHASE 1 — Taxonomies + Admin (dimensions & severities)

Reference patterns to mirror: `RunReviewStatusesSettings` (value+description+color-token+default) and
`LabelDefinitionsSettings` in `ui/routes/_sidebar/config.tsx`; `app_settings_service.py` seed-on-startup;
`migrations/postgres.py` + `migrations/__init__.py`.

### Task 1.1: Migrations for dimensions + severities

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/migrations/postgres.py` (append `PgMigration` v4)
- Modify: `app/src/databricks_labs_dqx_app/backend/migrations/__init__.py` (append `DeltaMigration` vN, `oltp_fallback=True`)
- Test: `app/tests/test_pg_migration_runner.py`, `app/tests/test_migration_runner.py`

**Produces:** tables `dq_dimensions(id,name,description,color,is_builtin,is_enabled,created_by/at,updated_by/at)`,
`dq_dimension_examples(id,dimension_id,position,text)`, `dq_severities(id,name,description,color,rank,dqx_criticality,is_builtin,is_enabled,audit)`.

- [ ] Step 1: Write failing test asserting the new migration version numbers exist and are monotonic in both runners.
- [ ] Step 2: Run tests, verify fail.
- [ ] Step 3: Append the Postgres migration (DDL with `{schema}` placeholder, CHECK on `dqx_criticality IN ('warn','error')`, UNIQUE on name/rank).
- [ ] Step 4: Append mirrored Delta `oltp_fallback=True` migration (respect Delta DDL invariants).
- [ ] Step 5: Run migration-runner tests, verify pass.
- [ ] Step 6: Commit.

### Task 1.2: Seed data (idempotent, at startup)

**Files:** Create `app/src/databricks_labs_dqx_app/backend/services/taxonomy_seeds.py`; modify startup lifespan to call it (find where `MigrationRunner.run_all()` / settings seeding runs).

**Produces:** `seed_taxonomies(executor)` — inserts 6 dimensions + 4 severities if absent (never overwrites existing names).

- [ ] Test: seeding twice is idempotent; existing edited rows preserved. → implement → pass → commit.

### Task 1.3: Backend models + services

**Files:** Create `services/dimension_service.py`, `services/severity_service.py`; add Pydantic models to `models.py`; DI in `dependencies.py`.

**Produces:** `DimensionService.list/create/update/delete`, `SeverityService.list/create/update/delete/reorder` (two-phase negative-rank swap for reorder; built-ins can't be renamed/deleted, can be recolored/disabled).

- [ ] TDD each method (service tests with in-memory/fake executor per existing test patterns) → commit.

### Task 1.4: Routes

**Files:** Create `routes/v1/dimensions.py`, `routes/v1/severities.py`; register in `routes/v1/__init__.py`.

**Produces:** `/dimensions` + `/severities` routers. Reads VIEWER+, mutations ADMIN. operation_ids: `listDimensions`, `createDimension`, `updateDimension`, `deleteDimension`, `listSeverities`, `createSeverity`, `updateSeverity`, `deleteSeverity`, `reorderSeverities`.

- [ ] TDD routes (route tests mirror existing) → `make app-regen-api` → commit.

### Task 1.5: UI — Config cards + move Import into Settings

**Files:** Modify `ui/routes/_sidebar/config.tsx` (add `DimensionsSettings`, `SeveritySettings` cards + an Import card/section); create editor components under `ui/components/admin/`; add i18n keys to all 4 locales; add generated-hook usage.

**Produces:** admin Dimensions card (name/desc/color/examples/enable-disable; built-ins locked) and Severity card (drag-reorder rank via dnd-kit, color, dqx_criticality select, enable-disable); Import rules relocated from the Create-Rules nav group into Config.

- [ ] Build cards (mirror `RunReviewStatusesSettings`) → `make app-check` → i18n parity test → commit.

### Task 1.6: Phase 1 verification + reviews

- [ ] `make app-test` green; `make app-check` green.
- [ ] DQ-steward reviewer subagent: can a non-expert admin understand dimensions vs severity and configure them?
- [ ] Code review subagent on the diff. Fix findings. Commit.

---

## PHASE 2 — Registry core + 3 authoring types + seed 78

- **2.1** Migrations: `dq_rules`, `dq_rule_versions` (+ indexes on status/fingerprint/steward). PG + Delta mirror.
- **2.2** Rule domain model + fingerprint (reuse core `rule_fingerprint` ideas) + slot/param model; derive slot family/param types from `listCheckFunctions` + seed-map refinement.
- **2.3** `RegistryService`: create/update(draft)/submit/approve/reject/publish(→version+snapshot)/deprecate/delete; fingerprint dedup warning; reuse existing status machine + roles.
- **2.4** Seed all ~78 built-ins as `is_builtin` published rules (definition locked; slots+families+dimension+default severity from seed map). Idempotent.
- **2.5** Routes `/registry-rules` (+ reuse import validation). orval regen.
- **2.6** UI: Rules Registry list (filters: status/dimension/severity/steward/tag) + create/edit modal with 3-type toggle (DQX Native picker / Low-Code builder / SQL CodeMirror), polarity switch for lowcode+sql, dimension/severity selects, free-text tags editor, About/Implementation/Sharing structure adapted to DQX RBAC. Merge single-table + cross-table authoring here.
- **2.7** Verify + steward review + code review.

## PHASE 3 — Monitored tables + apply/map + materialization

- **3.1** Migrations: `dq_monitored_tables`, `dq_applied_rules`; add provenance cols to `dq_quality_rules` (PG + Delta mirror).
- **3.2** `MonitoredTableService` (register/list/get/publish) + profiling read (reuse `dq_profiling_results`, profiler job).
- **3.3** `ApplyRulesService` + **materializer**: render each applied rule × mapping group → `dq_quality_rules` row (substitute slots→columns, fill params, stamp dimension/severity/polarity/provenance into `user_metadata`); idempotent, keyed by `applied_rule_id`+mapping hash; per-table approval; auto-upgrade admin setting (Behaviour A/B) driving re-materialization + re-approval.
- **3.4** Routes `/monitored-tables` (list/detail/apply/publish/profile). orval regen.
- **3.5** UI: Monitored Tables list + detail (Profile / Apply Rules / Results). Apply-Rules: by-column & by-rule lenses, family-filtered slot→column picker, version pin, severity override, add published rules, **＋ Create new rule** inline modal (returns to mapping). Fold Discovery + Profile&Generate here.
- **3.6** Verify + steward review + code review. **Confirm materialized dicts are byte-identical to today's for equivalent checks (runner untouched).**

## PHASE 4 — AI (gateway + Vector Search suggester)

- **4.1** `AIGateway` service (serving_endpoints.query; kill-switch; per-user rate limit; audit log; author_kind). Rework app `aiAssistedChecksGeneration` to route through it.
- **4.2** Build-with-AI (full-form generate, two-pass lowcode→sql) + per-field suggest (name/desc/dimension/severity), validating/repairing generated DQX JSON via core `RuleValidator`/`_filter_unsafe_sql_rules`/`DQEngine.validate_checks`/PK detection.
- **4.3** Rule embeddings + Databricks Vector Search index (behind `RuleRetriever` seam) + embedding serving endpoint; re-embed on publish; DAB bundle + grants for VS/embedding infra.
- **4.4** Mapping suggester `/monitored-tables/{id}/suggest-rules`: VS retrieve top-K → LLM judge → filter/dedup/exclude-applied. Prefetched suggestion dialog UI (by rule/by column, slot→column chips, add N).
- **4.5** Admin AI settings card (endpoint, enable toggle, rate limit). Verify + reviews.

## PHASE 5 — Reviews / insights / polish

- **5.1** Ensure dimension/severity/provenance flow into `dq_metrics.user_metadata`; Runs History dimension/severity breakdowns + per-failure rule provenance.
- **5.2** Nav consolidation cleanup (remove single-table/cross-table/Active Rules routes; sidebar per spec §10).
- **5.3** Full copy pass (non-cringe); i18n parity across all 4 locales; docs update.
- **5.4** Full `make app-test` + `make app-check`; steward end-to-end walkthrough; final code review.

---

## Self-review notes
- Spec coverage: every spec section maps to a phase/task (taxonomies→P1, registry+types+seed→P2, monitored/apply/materialize→P3, AI→P4, reviews/nav/copy→P5).
- Phases are independently testable and build in dependency order (P2 needs P1 taxonomies; P3 needs P2 rules; P4 needs P2/P3; P5 integrates).
- Fine-grained TDD steps for P2–P5 authored just-in-time before each phase (code written against live source).
