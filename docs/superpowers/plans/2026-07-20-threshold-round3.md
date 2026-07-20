# Threshold Round 3 + AI-Suggest Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** (A) Make AI rule suggestions match dqlake quality by fixing the embedding feature gap + retrieval breadth; (B) fix the threshold-freeze correctness bug so changing a threshold never re-judges past runs; (C) fix 6 UI issues in Apply Rules / rule-logic display / the Mixed pill.

**Architecture:** The AI-suggest gap is a *feature* gap, not an algorithm gap — the rule embedding omits slot family/cardinality + check-function name that the query carries, so cosine has nothing to match. Fix the embed text + re-embed (keep pure-Python cosine; no vector store). The threshold-freeze fix is cheap: the resolved effective threshold is ALREADY frozen per-run in `dq_validation_runs.checks_json.user_metadata.pass_threshold` — just plumb it through the results views into breach evaluation instead of recomputing from live settings. The UI fixes are localized to `RuleConfigCard.tsx` and `ThresholdPill.tsx`.

**Tech Stack:** FastAPI + Pydantic 2, Databricks SQL (Delta shaping views), React 19 + TanStack Query + shadcn/ui, orval `api.ts`, react-i18next (en/pt-BR/it/es), pytest + bun test.

## Global Constraints

- **Do NOT touch the tag-suggestion feature** (user: it works well). AI-suggest changes are confined to `rule_suggester.py`, `rule_embeddings.py`, and the backfill route. `tag_suggestion_service.py` and tag-mapping code stay untouched.
- **No vector store.** Keep `CosineRuleRetriever` (pure-Python cosine over the JSON-column corpus). Do not wire `VectorSearchRetriever`.
- **Threshold precedence unchanged** (per-column → per-rule → registry → admin), `resolve_pass_threshold` is the source of truth, nullish not truthy (0 is real).
- **Freeze reads the per-RUN value, never the version snapshot** — the version snapshot's `get_checks` re-renders against the live admin default and is NOT frozen. Source the frozen threshold from the run's `checks_json.user_metadata.pass_threshold` only.
- **Changing embed text requires re-embedding the corpus** — every embed-text change must be followed by the existing `POST /registry-rules/backfill-embeddings` (admin) to re-embed all approved rules, and the plan's deploy step must run it. Old embeddings built from the old text otherwise linger.
- i18n: new/changed strings in all four locales; parity holds. After backend model/route change: `make app-regen-api`. Always `git checkout app/uv.lock uv.lock` before staging; never stage `.superpowers/`.
- No lint suppressions; ruff clean on changed backend files.
- Gates per task: `cd app && node_modules/.bin/tsc -b --incremental`, `bun test src/databricks_labs_dqx_app/ui`, relevant `uv run --group test pytest`, and `pytest tests/test_i18n_locale_parity.py` when locales change.
- **Deploy from `app/`** (`databricks.yml` lives there; repo-root deploy fails "bundle root not found"); `databricks bundle deploy -p fe-sandbox-dq-demo-2 -t dev --auto-approve` then `bundle run`.

---

## Task 1: AI-suggest embed-text feature overhaul (backend) + re-embed

**Files:**
- Modify: `backend/services/rule_embeddings.py` (`build_rule_embed_text` ~48-90)
- Test: `app/tests/test_rule_embeddings.py` (or wherever `build_rule_embed_text` is tested — `grep -rl build_rule_embed_text app/tests`)

**Interfaces:** `build_rule_embed_text(rule) -> str` unchanged signature; richer text. `RuleSlot` (registry_models.py) has `.name`, `.family`, `.cardinality`.

- [ ] **Step 1: Failing tests** — assert the embed text for a rule with a slot of family `textual`/cardinality single now CONTAINS the family token and cardinality; contains the check-function name when the body has one (`is_not_null`, `regex_match`); and does NOT contain a `severity:` line. Mirror existing `build_rule_embed_text` tests.
- [ ] **Step 2: Run → fail.**
- [ ] **Step 3: Implement** — in `build_rule_embed_text`:
  - Replace the `slots: <names>` line with dqlake's shape: `f"input columns: " + ", ".join(f"{s.name} ({s.family}, {s.cardinality})" for s in rule.definition.slots)` (guard empty; use the enum `.value` if cardinality is an enum).
  - Add a deterministic check-function line: derive the function name (from `body["function"]` for dqx_native; for lowcode/sql, the operator/check identity if available) and always emit `f"check: {fn}"` when known. Keep `_extract_predicate` for the predicate line.
  - Remove the `severity: <x>` line (low discrimination). Keep `dimension`.
  - Keep name, description, predicate, tags.
- [ ] **Step 4: Run → pass**; `uv run ruff check` the file.
- [ ] **Step 5: Commit** — `feat(ai-suggest): embed slot family/cardinality + check function, drop severity noise`.

---

## Task 2: AI-suggest — bump per-column top-K (backend)

**Files:** `backend/services/rule_suggester.py` (`DEFAULT_TOP_K` ~49). Test: `app/tests/test_rule_suggester.py`.

- [ ] **Step 1** — Raise `DEFAULT_TOP_K` from 8 to a larger value for the per-column path so each column gets a generous candidate set (e.g. 20). Keep the per-column union cap proportional (`_retrieve_per_column` already caps at `max(top_k, top_k*3)`). Consider making it a module constant with a comment tying it to corpus size. Do NOT change the tag suggester.
- [ ] **Step 2** — Add/adjust a test asserting the per-column retrieval requests the higher K (the `FakeRetriever` records `top_k`). Keep the existing 25 suggester tests green.
- [ ] **Step 3 (reject same-column multi-slot mappings)** — In the suggester's post-processing (`_post_process` / wherever a judged suggestion's `column_mapping` is validated before becoming a suggestion), DISCARD any suggestion whose mapping binds two or more DISTINCT slots to the SAME column (e.g. `{{start_ts}}: order_ts, {{end_ts}}: order_ts`). This is the exact bad mapping from the screenshots — a 2-slot comparison rule forced onto one column. Deterministic guard, no judge change. A single-slot rule mapping its one slot to a column is fine; only reject when ≥2 distinct slot keys share the same mapped column value. Add a test: a judged suggestion mapping `{a: col1, b: col1}` is dropped; `{a: col1, b: col2}` is kept; `{a: col1}` is kept.
- [ ] **Step 4** — Run suggester tests; ruff. **Commit** — `feat(ai-suggest): raise per-column top-K; reject same-column multi-slot mappings`.

---

## Task 3: Threshold freeze — plumb the per-run frozen threshold into breach eval (backend)

**Files:**
- Modify: `backend/services/score_view_service.py` (`attribution_view_ddl` ~206-214 extract; `shaping_view_ddl` ~298-320 propagate)
- Modify: `backend/services/dq_results_service.py` (`CheckResultRow` ~120-146 + `parse_check_rows` ~182-217 add `pass_threshold`)
- Modify: `backend/routes/v1/dq_results.py` (`_build_threshold_resolver.resolve()` ~409-419 prefer `row.pass_threshold`)
- Test: `app/tests/test_dq_results_service.py`, and any score-view DDL test.

**Interfaces:** `CheckResultRow` gains `pass_threshold: int | None = None`. The resolver, given a `CheckResultRow`, returns `row.pass_threshold` when not None, else the existing live chain (legacy runs).

- [ ] **Step 1: Failing test** — in `test_dq_results_service.py`: a `CheckResultRow` carrying `pass_threshold=90` breaches/doesn't-breach based on 90 REGARDLESS of the live admin default passed to the resolver (prove the frozen value wins). A row with `pass_threshold=None` falls back to the live chain. This is the correctness guarantee: changing the live default does not move a stamped run's verdict.
- [ ] **Step 2: Run → fail.**
- [ ] **Step 3: Views** — `attribution_view_ddl`: extract `user_metadata['pass_threshold']` as an int column (mirror how `criticality`/`severity` are extracted ~206-214; cast to int, NULL when absent). `shaping_view_ddl` (`v_dq_check_results`): add `pass_threshold` to the projected columns (~298-320). Update any DDL snapshot/test.
- [ ] **Step 4: Parse** — `CheckResultRow` add `pass_threshold: int | None`; `parse_check_rows` read `row.get("pass_threshold")` → int|None (tolerate str).
- [ ] **Step 5: Resolver** — in `dq_results.py` `_build_threshold_resolver`, make `resolve(row)` return `row.pass_threshold` when not None, else the current per-column→rule→registry→admin live computation. (This makes stamped runs immutable; legacy runs keep today's behavior.) Confirm ALL breach-eval call sites go through this resolver (they do — Round 2).
- [ ] **Step 6: Run → pass**; `pytest tests/test_dq_results_service.py` + score-view tests; ruff. `make app-regen-api` only if a model changed (CheckResultRow is internal, likely no API change — verify GroupRowOut unchanged).
- [ ] **Step 7: Commit** — `fix(threshold): freeze breach at per-run threshold so changing the setting never re-judges past runs`.
- [ ] **Step 8 (note for deploy):** the shaping/attribution views are recreated on deploy/startup (confirm `score_view_service` re-runs its DDL on app start — it does via the provisioner). No manual view rebuild needed beyond redeploy.

---

## Task 4: Rule-logic display fixes — args leak, compiled SQL, low-code group-by/filter (frontend)

**Files:** `ui/components/apply-rules/RuleConfigCard.tsx` (`RuleLogicBody` ~157-190, `LowcodeLogicBody` ~110-155); reuse `ui/components/rules/lowcode/GroupByField.tsx` + `FilterBuilder.tsx`; i18n.

- [ ] **Step 1 (args leak, Bug 1)** — line ~181: `const text = sql ?? predicate ?? \`${fnLabel}(${args ? JSON.stringify(args) : ""})\`;` → drop the args: render `\`${fnLabel}()\`` (or better, `fnLabel` alone) for the function case. The parameters are already shown separately via `RuleParametersView` (~80-102), so args in the label are redundant. Result: `Is Not In Future` not `Is Not In Future({"column":"{{ts}}"})`.
- [ ] **Step 2 (compiled SQL, Bug 2)** — remove the raw compiled-SQL `<pre>` surfaces: `LowcodeLogicBody` no-AST fallback (~124-125) and the always-on "Compiled SQL" disclosure (~145-152); and the `sql`-mode raw `text` path (~181) so custom rules render via low-code, not raw SQL. If a rule genuinely has only SQL (sql-mode cross-table), show a neutral summary, not the compiled predicate. Keep the `ruleLogicCompiledSql` i18n key only if still referenced elsewhere (grep; remove if now unused).
- [ ] **Step 3 (low-code group-by + filter, Bug 3)** — in `LowcodeLogicBody`:
  - Replace the `<code>{groupBy}</code>` block (~137-144) with a read-only `<GroupByField value={groupBy} onChange={() => {}} declaredColumns={declaredColumns.filter(c => !c.name.includes("."))} disabled />`.
  - Add a read-only `<FilterBuilder ast={filterAst} onChange={() => {}} declaredColumns={declaredColumns} readOnly />` where `filterAst` comes from `body.filter_ast` guarded by `isV2Ast` (mirror RegistryRuleFormDialog ~1876-1877). Import both from `@/components/rules/lowcode/*`.
  - Verify slot-derived `declaredColumns` (~116-119) binds the `{{slot}}` tokens/badges correctly read-only; if joined-table columns are needed, note the limitation.
- [ ] **Step 4** — tsc clean, `bun test`, parity. **Commit** — `fix(apply-rules): rule-logic shows clean function label + low-code group-by/filter, no raw compiled SQL`.

---

## Task 5: Mixed pill — per-column editor + consistent size (frontend)

**Files:** `ui/components/apply-rules/ThresholdPill.tsx`; the by-rule pill host `RuleConfigCard.tsx` + `monitored-tables.$bindingId.tsx` (needs the per-column data + a change handler); i18n.

**Design:** When `mixed`, clicking the pill opens a popover listing EACH column with its current per-column threshold, editable inline (per-column number inputs), instead of a single rule-level override. And the pill keeps a **consistent width** whether it shows a number or "Mixed".

- [ ] **Step 1 (consistent size, Bug 6)** — give the pill trigger a fixed min-width (e.g. `min-w-[N]` sized to the widest of "Warn < 100%" / "Mixed") so switching to "Mixed" doesn't shrink it. Verify both states render same width.
- [ ] **Step 2 (per-column editor, Bug 5)** — extend `ThresholdPill` (or a Mixed-specific variant) so that when `mixed`, the popover renders a row per column: `columnName — <number input>` bound to that column's current threshold, calling a new `onColumnThresholdChange(column, value)` prop. Non-mixed behaviour unchanged (single rule-level input).
  - The by-rule pill needs the per-column map + column list. `rule.column_pass_thresholds` is already on the `rule` prop; the column list comes from the rule's mapped columns (derive from `rule.column_mapping`). Pass a `columns` list + `columnThresholds` map + `onColumnThresholdChange` down from `RuleConfigCard`. The host already has `handleColumnThresholdChange(ruleId, column, value)` (Round 2, Task 6) — wire the by-rule pill to it (it currently only exists in the by-column view).
  - "no blanks" still applies (each per-column input follows the no-blank rule from Round 2 Task 4).
- [ ] **Step 3** — i18n for any new labels (e.g. `thresholdMixedPopoverTitle` = "Per-column thresholds"). All 4 locales.
- [ ] **Step 4** — tsc clean, `bun test`, parity. **Commit** — `feat(threshold): Mixed pill edits per-column thresholds inline; consistent pill width`.

---

## Task 6: Duplicate "Advanced" section in rule Implementation tab (frontend regression fix)

**Files:** `ui/components/RegistryRuleFormDialog.tsx`.

**Context:** Round 2 Task 7 added a single shared Advanced disclosure at the bottom of `implementationTabContent` for the threshold field. The Implementation tab ALSO has per-mode Advanced disclosures (lowcode ~3441, dqx_native ~3612, sql ~3691). Result: two "Advanced" sections show. Fix to ONE.

- [ ] **Step 1: Investigate the exact duplication** — open `implementationTabContent`; confirm whether the per-mode Advanced disclosure AND the new threshold Advanced disclosure both render for a given mode (they do — the threshold one is unconditional at the tab bottom). Decide the consolidation: put the threshold field INSIDE the existing per-mode Advanced disclosure (so there's one Advanced per mode) OR keep one shared Advanced at the bottom and remove the per-mode ones (riskier — they hold filter/group-by/joins). SAFEST: move the threshold field into each per-mode Advanced disclosure's content and remove the separate shared one, so each mode shows exactly one "Advanced" containing filter/group-by/joins + threshold.
- [ ] **Step 2: Implement** the chosen consolidation. Preserve the threshold field's gating (`usePassThresholdEnabled`) and monospace default from Round 2 Task 7. Preserve all 6 round-trip touch points (snapshotFromRule, PRISTINE, hydration, currentSnapshot, buildUserMetadata, applyParsedToForm) — JSX move only.
- [ ] **Step 3** — tsc clean, `bun test`, parity. Verify (reason through) only ONE Advanced section per mode now, threshold field still round-trips. **Commit** — `fix(rule-form): single Advanced section per Implementation mode (was duplicated)`.

---

## Task 7: Whole-branch review + redeploy + re-embed + re-verify

- [ ] Broad review of the Round-3 diff (opus); fix Critical/Important.
- [ ] Full gates: tsc, bun, backend pytest suites, ruff, parity.
- [ ] Build + `bundle deploy --auto-approve` from `app/` + `bundle run`; verify RUNNING/302/startup-complete.
- [ ] **Re-embed the corpus:** `POST /api/v1/registry-rules/backfill-embeddings` (admin) so all approved rules are re-embedded with the new Task-1 text. Confirm via the app API that the corpus re-embedded.
- [ ] **Re-verify with the user's cases:** AI suggestions for `orders`/`sales_customers` (expect not-null on id/amount, unique on id, email rule on email — matching dqlake); and the threshold-freeze (change the default, confirm PAST runs' breach verdicts DON'T move); and re-check drill-down breach now that freeze is in.

---

## Notes for the executor
- **One variable at a time:** embed-text (T1) is the AI-suggest root-cause fix; judge tuning is deliberately deferred until after re-test.
- **Freeze targets the run, not the version** — read `checks_json.user_metadata.pass_threshold`, never `get_checks` (which re-renders live).
- **Re-embed is mandatory after T1** — old embeddings are stale until backfilled.
- Minor per-task findings → ledger; final review triages.
