# DQ Score, Severity Mapping & Results UI — Design

**Date:** 2026-07-10
**Branch:** `dq-score-results` (off `rules-registry`)
**Status:** Approved for planning

## Goal

Port dqlake's DQ score and results-viewing experience into DQX Studio: a severity
taxonomy mapped to DQX's native `warn`/`error` criticality, a DQ score computed
from passing/failing test counts (not just pass/fail rows), and results UI at
table, data-product, rule, and global scope — all while strictly respecting each
user's own Unity Catalog SELECT permissions on the underlying source tables.

**Explicitly out of scope:** Genie integration (Genie Space provisioning + chat
UI). This is a separate follow-on spec, once the results/score data this spec
builds actually exists for Genie to point at.

**Frozen, must not change:** `app/tasks/`, `backend/services/job_service.py`,
the `dq_quality_rules` table spec, and the frozen `metrics_observer.py`/quality
checker job behavior. All new logic here is additive, in new or existing
non-frozen backend services.

## 1. Severity taxonomy → DQX criticality mapping

Today, severity is a free-text tag (`Low`/`Medium`/`High`/`Critical`) inside
`dq_rules.user_metadata`, governed by a generic, reusable `label_definitions`
admin system (also used for `dimension` and custom tags). The severity→DQX
criticality mapping already exists, but only as a hardcoded Python dict
(`SEVERITY_TO_CRITICALITY` in `registry_models.py`) — not stored, not
admin-editable.

**Design:** extend the existing `severity` label definition rather than
introduce a new table:

- Add `value_criticality: dict[str, Literal["warn", "error"]]` alongside the
  existing `value_colors` field on the `LabelDefinition` model, scoped to the
  reserved `severity` key.
- Seed `value_criticality` from today's hardcoded `SEVERITY_TO_CRITICALITY`
  dict, so behavior is unchanged immediately after this ships.
- `resolve_criticality()` reads the mapping from stored config instead of the
  hardcoded dict, falling back to today's `DEFAULT_CRITICALITY = "warn"` for
  unmapped/unknown values.
- Rank is already implicit in `values`' array order — no new field.
- Admin UI: extend the existing severity editor on `config.tsx` with a
  warn/error dropdown per value, next to the existing color picker. No new
  page, no new table, no migration of `severity_override`/`user_metadata`
  usage — those keep resolving through `resolve_criticality()` exactly as
  today, just against an editable mapping instead of a hardcoded one.

## 2. DQ score

**Formula** (per table, per run):

```
score = 1 − (Σ failed_tests / Σ input_row_count)
```

summed across all rules applied to that table in the run. `failed_tests` per
rule is already emitted today in `dq_metrics.check_metrics[].error_count` /
`.warning_count` (a genuine, already-filter-aware per-rule failed-row count).
`input_row_count` is the existing table-wide row count already emitted per
run.

**Known, accepted approximation:** per-rule `filter` scoping is not accounted
for in the denominator — a rule scoped to a subset of rows via `filter` will
show an optimistic score relative to its true failure rate within that subset.
This is an explicit trade-off to avoid new instrumentation in the frozen
pipeline; revisit only if filtered rules become a common pattern and the
approximation proves misleading in practice.

**Data product (table space) score:** unweighted mean of member tables' latest
scores — a tiny table and a large table count equally, matching dqlake.

**No changes to the frozen job/task.** All of this reads entirely from the
existing `dq_metrics` table already written today by the frozen pipeline. New
work is a non-frozen backend service (e.g. `services/score_service.py`) that
reads `dq_metrics` and computes table/product/rule/global scores, exposed via
new API endpoints.

## 3. Permissions

The crux constraint: a user must never see quality results — especially
sample failing rows — for a source table they don't have live UC SELECT on,
even in an aggregated/global view. Unity Catalog views (including metric
views) execute with the view owner's privileges on the underlying tables
("definer's rights"), not the querying user's — so a shared view/metric view
over all monitored tables' results would leak data regardless of the viewer's
own grants, unless paired with a hand-maintained ACL-mirror row filter (which
duplicates UC's own grant model and can drift). That approach is rejected.

DQX Studio already runs a proven OBO (on-behalf-of-user) auth path
(`get_obo_ws`/`get_obo_sql_executor`, wired via `X-Forwarded-Access-Token` and
declared `user_api_scopes` in `databricks.yml`) for exactly this kind of
enforcement (e.g. source-table preview). We reuse that pattern:

- **Aggregate scores** (table, product, rule-level, global) are computed from
  the SP-owned, shared `dq_metrics` table — low sensitivity (counts only, no
  row values). Before returning aggregate results, the backend uses the
  requesting user's own OBO token to determine which monitored tables they
  currently have SELECT on (reusing the existing OBO catalog/schema/table
  listing pattern in `dependencies.py`), and filters the aggregate query to
  that set. This check is live per request (not synced/cached beyond a short
  TTL), so there is no meaningful permission-drift window on the aggregate
  path.

- **Row-level failing-sample data** (the per-cell-highlight drill-down): no
  new UC grants are introduced anywhere. `_dq_output`/`_dq_quarantine` tables
  remain owned and queried only by the app's service principal (which already
  has access as their creator). Before returning any row-level sample, the
  backend uses the *requesting user's own* OBO token to self-check their
  current SELECT on the *source* table (a self-check requiring no elevated
  privilege — checking your own access never requires `MANAGE`/ownership).
  Only on success does the SP fetch and return the precomputed sample rows
  from the last run's quarantine table.

  This was chosen over two alternatives considered and rejected:
  - *Grant-mirroring* (sync UC grants from source table onto the quarantine
    table): requires the SP to read arbitrary source tables' effective
    grants, which needs `MANAGE`/ownership/admin — a new, heavier grant this
    project's existing deploy model deliberately avoids (confirmed in
    `DEPLOYMENT.md`: the bundle grants only `USE CATALOG`, never table-level
    `SELECT`, specifically to avoid over-provisioning SP access; and no
    onboarding path in this codebase issues any UC grant on a source table
    today).
  - *Live re-query* (reconstruct the rule's failure predicate and execute it
    live via OBO against the current source table): truly UC-native with
    zero drift, but not chosen — added engineering risk translating
    PySpark `Column` checks to portable SQL, and risks showing a sample that
    doesn't match what actually failed in the specific historical run if the
    source has since mutated.

- **Row/column-security suppression (universal safety net):** before serving
  any row-level sample, check the source table's metadata —
  `WorkspaceClient().tables.get(full_name).row_filter` and each
  `ColumnInfo.mask` — both cheap metadata reads, no data query. If either is
  present, suppress the sample entirely ("row-level detail unavailable — this
  table has fine-grained access controls") rather than risk bypassing a
  policy our sample can't faithfully replicate. This applies regardless of
  which table/user is asking.

## 4. Results UI

Four surfaces, extending existing routes where they already exist:

1. **Table results tab** — new tab on `monitored-tables.$bindingId`. Score
   box, per-rule breakdown, and a failing-records table with per-cell
   highlighting, reproducing dqlake's `BindingIssuesTab`/`FailingRecordsTable`.
   Per-cell highlighting maps directly onto DQX's existing `_errors`/
   `_warnings` struct's `columns: array<string>` field
   (`dq_result_schema.py`) — no schema changes needed, only an API-layer
   transform into the `row_values` + `failures[].columns` shape the frontend
   component expects.

2. **Data product results tab** — new tab on `table-spaces.$productId`. Score
   box (unweighted average across member tables) + trend chart, reproducing
   dqlake's `ProductResultsTab`.

3. **Rule-level results page** (new route) — pick a rule from the Rules
   Registry; see its aggregate pass/fail stats and a per-table breakdown
   across every monitored table it's currently applied to, with OBO-gated
   drill-down into any table's failing sample. **If the rule has zero current
   applications, the results view is disabled with a tooltip** explaining the
   rule must be applied to at least one monitored table before results are
   available — avoids a confusing empty state.

4. **Global results page** (new top-level route, dqlake's `issues.tsx`
   equivalent) — org-wide score summary across every table the user can see,
   permission-filtered per section 3.

## 5. Testing

- **Unit tests:** score formula (including the accepted filter approximation,
  documented via test cases showing the known skew), severity→criticality
  resolution (default seed + custom admin-edited mappings), and the
  OBO-permission-filtering logic (`create_autospec(WorkspaceClient)`, no live
  Spark/workspace).
- **Integration tests:** new endpoints against a live workspace covering: a
  user with source-table access sees results; a user without access sees an
  empty/filtered result (never an error that leaks the table's existence);
  row-filter/column-mask suppression triggers correctly on a table configured
  with either; rule-level page disabled state when a rule has no
  applications.

## Open items carried forward (unchanged by this spec)

- SELECT-privilege enforcement ruling for other, unrelated Rules Registry
  surfaces (pre-existing open item, not part of this spec).
- Genie integration — follow-on spec, once this spec's data model ships.
