# Rules Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Each phase's fine-grained steps are authored just-in-time against the live codebase before that phase executes.

**Goal:** Merge the dqwatch Rules Registry (reusable versioned rules, dimensions/severity, monitored tables, AI) into DQX Studio without changing how jobs run.

**Architecture:** Three layers — registry (new `dq_rules`/`dq_rule_versions`) → monitored tables + mapping (new `dq_monitored_tables`/`dq_applied_rules`) → existing `dq_quality_rules` (materialized, runner-facing, unchanged). See `docs/superpowers/specs/2026-07-02-rules-registry-design.md`.

**Tech Stack:** FastAPI + Pydantic 2 backend, hybrid Lakebase Postgres / Delta store, React 19 + TanStack + shadcn/ui frontend (orval-generated client), Databricks Vector Search, serving-endpoint AIGateway.

## Global Constraints

- Execution path (`jobs.run_now` on `DQX_JOB_ID` wheel task; runner consumes DQX check dicts) MUST NOT change.
- GREENFIELD schema (from scratch): do NOT write incremental migrations, mirror ceremony, or migration tests. Define new tables/columns directly in the baseline schema (Postgres baseline + Delta-fallback baseline) so a fresh deploy creates them. Still honour Delta DDL invariants (`IF NOT EXISTS`; `ADD COLUMN` w/o `IF NOT EXISTS`; CHECK constraints separate; no `;`/`{catalog}`/`{schema}` in string literals) so baseline creation succeeds. Dimensions/severity/name/description are TAGS in `user_metadata` (reserved label keys) — no tables.
- Every user-facing string via `t("key")`; add keys to ALL 4 locales (en, pt-BR, it, es); en is source of truth.
- Never edit generated `ui/lib/api.ts` / `routeTree.gen.ts`; run `make app-regen-api` after backend model/route changes.
- No lint disables (`# noqa`/`# type: ignore`/`# pylint: disable`). Full type hints; Pydantic 2 models; DI via `Depends`; wrap sync SDK calls in `asyncio.to_thread`.
- Portable SQL only via executor dialect helpers (`q()`, `json_literal_expr()`, `ts_text()`, `upsert()`, `select_json_text()`).
- RBAC via `require_role(...)`; SQL identifiers via `validate_fqn`/`quote_fqn`; user SQL via `is_sql_query_safe()`.
- Verify each phase: `make app-test` green, `make app-check` green, `test_i18n_locale_parity` green.
- Commits: conventional messages, `Co-authored-by: Isaac`. (GPG unavailable in build env — commits unsigned locally, MUST be re-signed before push.)
- Copy: plain, confident, non-cringe.

---

## PHASE 1 — Taxonomies-as-tags + Admin (dimensions & severity as pre-built label keys)

**Model (decided 2026-07-02):** dimensions & severity are **reserved label keys** (`dimension`, `severity`)
in the EXISTING `label_definitions` admin catalog — NOT new tables. Stored in `user_metadata` like all tags.
DQX `criticality` (warn/error) stays the separate execution field. Reference: `LabelDefinition` /
`LabelDefinitionsSettings` and `getLabelDefinitions`/`saveLabelDefinitions` in
`app_settings_service.py` + `routes/v1/config.py` + `ui/routes/_sidebar/config.tsx`; the reserved `weight`
key (`RESERVED_WEIGHT_KEY`) as the pattern for a reserved key.

### Task 1.1: Extend LabelDefinition model (per-value color + reserved flag)

**Files:** Modify `backend/config.py` (`LabelDefinition` model), `backend/services/app_settings_service.py`
(defaults/validation), tests under `app/tests/`.

**Produces:** `LabelDefinition` gains optional `value_colors: dict[str,str] | None` (value→hex) and
`is_builtin: bool = False` (reserved keys can't be deleted/renamed; values can be edited/recolored). Backward
compatible (both optional). Validate hex colors.

- [ ] Write failing test for the extended model + validation → run (fail) → implement → run (pass) → commit.

### Task 1.2: Seed reserved dimension + severity label keys (idempotent, at startup)

**Files:** Modify `backend/services/app_settings_service.py` seed path (where `label_definitions` /
`run_review_statuses_v1` are seeded at startup).

**Produces:** on startup, ensure `label_definitions` contains reserved keys `dimension`
(values Validity, Completeness, Accuracy, Consistency, Uniqueness, Timeliness; `allow_custom_values=true`) and
`severity` (values Low, Medium, High, Critical), each `is_builtin=true` with per-value colors. Idempotent:
never overwrite an admin-edited entry; only add if absent.

- [ ] Test: seeding twice idempotent; admin edits preserved; reserved keys present → implement → pass → commit.

### Task 1.3: Guard reserved keys in save endpoint

**Files:** Modify `backend/routes/v1/config.py` `saveLabelDefinitions` handler + `app_settings_service.py`.

**Produces:** saving label definitions cannot delete or rename a `is_builtin` reserved key (dimension/severity)
or remove the key itself; values within may be added/edited/recolored. ADMIN only (unchanged).

- [ ] TDD the guard (reject delete/rename of reserved key; allow value edits) → `make app-regen-api` if the
  response model changed → commit.

### Task 1.4: UI — Label Definitions card handles colors + reserved keys; move Import into Settings

**Files:** Modify `ui/routes/_sidebar/config.tsx` (`LabelDefinitionsSettings`): render reserved keys as locked
(no delete/rename, value editing + color swatch allowed); add optional per-value color swatch editor. Add an
**Import rules** section/card to Config (relocate from the Create-Rules sidebar group — update
`routes/_sidebar/route.tsx` nav). Add/adjust i18n keys in ALL 4 locales.

**Produces:** admins manage dimension/severity values + colors inside the existing Label Definitions card;
Import rules reachable from Config.

- [ ] Build → `make app-check` → i18n parity test green → commit.

### Task 1.5: Phase 1 verification + reviews

- [ ] `make app-test` green; `make app-check` green.
- [ ] DQ-steward reviewer subagent: can a non-expert admin understand dimensions vs severity and configure them?
- [ ] Code review subagent on the diff. Fix findings. Commit.

---

## PHASE 2 — Registry core + 3 authoring types + seed 78

- **2.1** Baseline schema (greenfield, no migrations): define `dq_rules`, `dq_rule_versions` in the Postgres + Delta-fallback baseline table definitions (+ indexes on status/fingerprint/steward).
- **2.2** Rule domain model + fingerprint (reuse core `rule_fingerprint` ideas) + slot/param model; derive slot family/param types from `listCheckFunctions` + seed-map refinement. Descriptive metadata (name, description, dimension, severity) lives in `user_metadata` as reserved TAG keys alongside arbitrary free-text tags — not columns.
- **2.3** `RegistryService`: create/update(draft)/submit/approve/reject/publish(→version+snapshot)/deprecate/delete; fingerprint dedup warning; reuse existing status machine + roles.
- **2.4** Seed all ~78 built-ins as `is_builtin` published rules (definition locked; slots+families+dimension+default severity from seed map). Idempotent.
- **2.5** Routes `/registry-rules` (+ reuse import validation). orval regen.
- **2.6** UI: Rules Registry list (filters: status/dimension/severity/steward/tag) + create/edit modal with 3-type toggle (DQX Native picker / Low-Code builder / SQL CodeMirror), polarity switch for lowcode+sql, dimension/severity selects, free-text tags editor, About/Implementation/Sharing structure adapted to DQX RBAC. Merge single-table + cross-table authoring here.
- **2.7** Verify + steward review + code review.

## PHASE 3 — Monitored tables + apply/map + materialization

- **3.1** Baseline schema (greenfield, no migrations): define `dq_monitored_tables`, `dq_applied_rules` + `dq_quality_rules` provenance cols in the Postgres + Delta-fallback baseline table definitions.
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
