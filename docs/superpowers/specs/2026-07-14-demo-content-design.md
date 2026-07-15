# Demo content for DQX Studio — design

> Adapts the dqlake "real-runs-first" demo generator to DQX Studio's data model
> and APIs, delivered as a **shared seeding module** driven by an **ADMIN-only
> in-app "Deploy demo content" action** (with a committed CLI wrapper for local
> iteration). Produces a rich, believable e-commerce DQ demo — breadth of rules,
> real seeded issues, a multi-week quality story, and tag-based auto-apply — so a
> skeptical DQ steward is wowed.

Date: 2026-07-14
Branch: `dqx/demo` (off `dqx-dqlake-integration` @ `c3942a4e`, which includes apply-on-tag)

---

## 1. Goal & the buyer's lens

The sandbox install is near-empty. This builds demo content that makes DQX Studio
look like a mature, weeks-old deployment. Faithful to the dqlake north star
(`databricks-dqwatch/docs/superpowers/specs/2026-06-17-demo-content.md`), "wow"
to a skeptical DQ steward means:

- **Breadth** — several monitored tables in a believable domain, grouped into 2
  data products; ~15 rules spanning all six quality dimensions (Completeness,
  Validity, Accuracy, Consistency, Uniqueness, Timeliness) and all severities.
- **Realism** — large tables (tens-to-hundreds of thousands of rows) with genuine,
  explainable issues (malformed values, nulls, dup keys, out-of-range amounts,
  future/stale dates, cross-field inconsistencies), and real failed records.
- **A story over time** — a **9-week** run history where scores move believably:
  an incident dip+recover on orders, a steady improvement on customers, a new rule
  added mid-history on shipments, a rule tightened on payments.
- **Diagnosis** — the homepage stats, trends, per-table drilldowns, data-product
  rollups, and failed records all return crisp, correct, grounded answers.
- **Tag-based governance** — a few demo columns carry governed UC tags and a few
  rules declare matching `slot_tags`, so tag-based auto-apply / suggestions
  (the just-shipped apply-on-tag feature) is demonstrable.

## 2. Decisions locked with the user

| Decision | Choice |
|---|---|
| Deliverable shape | **Shared seeding module** + **thin ADMIN-only in-app trigger** (source of truth); a committed CLI wrapper (`app/scripts/seed_demo.py`) reuses the same module for local-dev iteration. |
| Confirmation modal | The admin action opens a modal that (a) **offers to wipe the database first**, and (b) warns: *"This takes about an hour to populate. Don't interact with the app while it runs."* |
| Access control | **ADMIN only** — router-level `require_role(UserRole.ADMIN)` (mirrors the `admin` router) **and** the UI card only renders for admins. |
| Source data location | A dedicated **`dqx.dqx_studio_demo`** schema in the app's own catalog, created + owned by the app **service principal** (no extra catalog grants; runs read it as SP). |
| Domain | **E-commerce** (reuse dqlake): `customers / orders / payments / products / shipments` → data products **Customer 360** + **Fulfillment**. |
| Trend construction | **Real-runs-first + re-date** — genuine engine runs per binding validated against seeded rates, then re-dated to weekly instants. Every number on screen is engine-computed. |
| History span | **9 weeks** (dqlake default; ~45 real runs; fits the ~1-hour budget). |
| Rule naming | Authored to the AI-naming contract, **minimal phrasing, no leading articles** ("Email is present", not "The email must not be null"). Names/descriptions only — logic untouched. |

## 3. Architecture

Because the admin trigger runs **in-process**, the seeder calls the backend's
internal **service layer directly** (not the HTTP API as dqlake did) — cleaner,
no auth token juggling, and it can approve objects directly rather than toggling
approvals mode.

```
app/src/databricks_labs_dqx_app/backend/
  demo/                              ← new package (all demo content lives here)
    __init__.py
    manifest.py                      ← the demo definition (pure data): tables,
                                        seeded issue rates, rules (name/desc/logic),
                                        bindings + slot→column mappings, data
                                        products, slot_tags, column tags, the
                                        9-week story (lifecycle + per-week targets)
    datagen.py                       ← deterministic source-table SQL (seeded RNG
                                        via pmod(hash(pk,salt),1000)); ALTER…SET TAGS
    seed_service.py                  ← DemoSeedService: the orchestrator
    redate.py                        ← re-date SQL builders (Delta + OLTP), all
                                        identifiers/literals validated/escaped
    status.py                        ← settings-backed job status (dq_app_settings)
  routes/v1/admin.py                 ← + POST /admin/demo/deploy, GET /admin/demo/status
  services/database_reset_service.py ← reused as-is for the optional wipe
app/scripts/seed_demo.py             ← thin CLI wrapper (constructs deps from a profile)
app/src/databricks_labs_dqx_app/ui/
  routes/_sidebar/config.tsx         ← + "Deploy demo content" card (ADMIN-only)
                                        + confirmation modal + status polling
  lib/i18n/locales/{en,pt-BR,it,es}.json  ← new keys (4-locale parity)
```

`DemoSeedService` is constructed from injected dependencies (dependency injection,
per house rules) so it is testable and reusable:
- SP `SqlExecutor` bound to `dqx.dqx_studio_demo` (datagen + Delta re-date),
- the SP OLTP executor (`dq_score_history` / `dq_score_cache` re-date),
- the internal services: `RegistryService`, `MonitoredTableService` /
  `ApplyRulesService`, `DataProductService`, `BindingRunService`,
  `ScoreCacheService`, and (optionally) `DatabaseResetService`.

## 4. Data flow — the seeding pipeline

Idempotent throughout (upsert by display-name / fingerprint; deterministic datagen).

0. **(Optional) wipe** — if the admin ticked "wipe first", call
   `DatabaseResetService.reset_all_data(performed_by=<admin>)` (clears app `dq_*`
   tables, preserves admin roles, re-seeds fresh-install defaults). Then also drop
   & rebuild the `dqx_studio_demo` source schema.
1. **Source data** — `CREATE SCHEMA IF NOT EXISTS dqx.dqx_studio_demo`; build the 5
   tables from `range(N)` with deterministic seeded issues (adapted from
   `demo_datagen.py`). Apply table/column comments. `ALTER TABLE … ALTER COLUMN …
   SET TAGS` on a few columns with governed tags drawn from `SHOW GOVERNED TAGS`.
2. **Registry** — create + approve ~15 rules (via `RegistryService`), each carrying
   its authored name/description/dimension/severity in `user_metadata` and 2–3 with
   `slot_tags` matching the tagged columns.
3. **Bindings** — register the 5 monitored tables; batch-apply each table's rule set
   with slot→column `column_mapping`; submit + approve to materialize.
4. **Data products** — create Customer 360 + Fulfillment, add approved bindings as
   members, submit + approve.
5. **Validation gate** — reset source columns to a week-0 baseline, do one real run
   per binding, compare engine `failed_tests` vs the manifest's seeded rate;
   **hard-fail** a misfiring rule (the "calculations are provably correct" gate)
   before any history is built.
6. **9-week history** — for each week: apply that week's rule-lifecycle changes
   (add/retire), deterministically mutate the source columns to the week's target
   rates, trigger real runs per binding, wait for terminal state.
7. **Re-date** — UPDATE that batch's `dq_metrics` + `dq_validation_runs` (Delta) and
   upsert/append `dq_score_history` + `dq_score_cache` (OLTP) to the target weekly
   instant; the final week is one shared recent instant for a clean headline.
8. **Refresh + verify** — `ScoreCacheService` refresh; assert trend point counts,
   dimension/severity coverage, product rollups, and a populated latest slice.

Progress + terminal status is written to `dq_app_settings` at each phase so the UI
can poll it; the run itself executes on a **named daemon thread** (fire-and-forget),
so the HTTP request returns immediately.

## 5. Identity & the real-run path (key design point)

Runs normally create the temp view **OBO** (as the logged-in user) and execute the
Job as the **SP**. The background seed has no live user token for a full hour. Since
the demo source tables live in the **SP-owned** `dqx_studio_demo` schema, the seed
path creates its view and reads source data **as the SP** — no OBO, no token expiry.
Planning must confirm `BindingRunService` can run the seed with an SP-created view
(add a seed/SP code path if it currently hardcodes OBO view creation). This is the
apply-on-tag lesson applied up front: never assume the SP has grants on arbitrary
user catalogs — so the demo data lives where the SP already has them.

## 6. Rule set & naming (task #2)

~15 reusable, table-agnostic `dqx_native` rules built on real DQX check functions
(`is_not_null`, `is_not_null_and_not_empty`, `is_not_negative`, `is_in_range`,
`is_in_list`, `is_not_in_future`, `is_unique`, `regex_match`, plus multi-column SQL
predicates for cross-field consistency). Rule *logic* mirrors dqlake's proven,
correctness-gated set (uniqueness uses the canonical aggregated shape, not
count=count_distinct; foreign-key rules are **out** — the same known DQX limitation
dqlake documented). **Email validation is deliberately excluded** — no
`is_valid_email` rule — because email validation is reserved for a separate
user-led demo flow, not the seeded content.

Names/descriptions are authored to the AI contract — short human-readable name
(≤80 chars), one declarative sentence — with **minimal phrasing and no leading
articles** ("A"/"The"). They are stored via `set_reserved_tag(user_metadata, …)`;
`definition`, `mode`, `polarity`, and `slots` are byte-identical to keep the
structural fingerprint stable. Representative sample:

| Function / intent | Name | Description |
|---|---|---|
| `is_not_null` | Value is present | Value is not null. |
| `is_not_negative` | Amount is not negative | Numeric amount is zero or greater. |
| `is_in_range` 0–100 | Discount is 0 to 100 | Discount percentage falls between 0 and 100 inclusive. |
| `is_in_list` countries | Country is a known ISO code | Country code is one of the supported ISO codes. |
| `is_not_in_future` | Timestamp is not in the future | Event timestamp is no later than now. |
| `is_unique` | Key is unique | Key value appears at most once in the table. |
| `regex_match` | Card last-four is four digits | Stored last four card digits are exactly four digits. |
| multi-col SQL | End is not before start | End timestamp is on or after start timestamp. |

(The prior "poor names" the brief referenced don't exist in a demo branch —
`origin/feature_555_dqx_insurance_demo` is an empty stale upstream ref. The
terse-echo style to avoid is the builtin seed's `humanize_function_name` output,
e.g. "Is not null" with a raw docstring as its description.)

## 7. Tags (task #3)

Datagen tags a few demo columns with governed tags that read naturally for
e-commerce (e.g. `class.name` on `customers.first_name`/`last_name`,
`class.country` on `customers.country_code`, `class.pii` where apt) — chosen from
what `SHOW GOVERNED TAGS` returns in the sandbox (~375 available). **No email tag
is used** (email validation is out of scope — user-led flow). 2–3 registry rules
declare matching `slot_tags` (`{slot: [class.*]}` via `set_slot_tags`). The demo
ships with the `tag_auto_apply` toggle **OFF** by default so tag matches surface as
**suggestions** on the Apply Rules screen (the safe, reversible showcase); the demo
script can optionally flip it ON to show eager auto-apply. Tag strings validate as
`class.*` (existing guard).

## 8. Admin UI (task #1b) — ADMIN only

A "Deploy demo content" card added to the **Config** page danger/tools area,
rendered **only when the user is ADMIN** (the card is gated in the UI, and the
backend routes are `require_role(UserRole.ADMIN)` at the router level — defence in
depth). Mirrors the existing Reset-database `DangerZoneCard` pattern:

- **Button** → confirmation modal (shadcn `AlertDialog`): a checkbox **"Wipe existing
  data first (recommended)"**, the ~1-hour / don't-interfere warning, and a typed
  confirmation. On confirm, calls `useDeployDemoContent()` (orval hook) →
  `POST /api/v1/admin/demo/deploy` with `{ wipe_first: bool }`.
- **Status** → `GET /api/v1/admin/demo/status` polled while a job is running
  (phase label + started-at), reading the `dq_app_settings` status blob. A second
  deploy is rejected (409) while one is in progress.
- **i18n** — all new strings via `t()` and present in all four locales
  (`en` source of truth); the parity test (`tests/test_i18n_locale_parity.py`) stays
  green.

## 9. Security

- All datagen/re-date SQL: identifiers via `validate_fqn`/`quote_fqn`/`quote_ident`,
  literals via `escape_sql_string`, and any templated body through
  `is_sql_query_safe()`. Governed-tag keys are validated against the `class.*` guard
  and escaped before use in `SET TAGS`.
- The demo schema is SP-owned inside the app catalog; the seed never reads or writes
  arbitrary user catalogs.
- No secrets in the CLI or the module (profile / SP creds only). Untrusted values are
  not logged raw.
- The deploy action is ADMIN-gated at the router (not just the UI).

## 10. Testing

- **Unit** (no Spark/workspace): manifest integrity (every rule has a name +
  one-sentence description within the contract and with no leading article; every
  binding slot resolves to a real declared slot + column; `slot_tags` are valid
  `class.*`); `datagen` and `redate` SQL builders produce safe, escaped SQL (assert
  `validate_fqn`/`escape_sql_string` are applied, no raw interpolation); the
  status store round-trips; the admin route enforces ADMIN and rejects a concurrent
  deploy (with `create_autospec`'d services, no real workspace).
- **Backend** `make app-test` and **`make app-check`** stay green (3033 backend +
  395 UI baseline).
- **End-to-end**: validated by deploying to the sandbox and running the demo — the
  homepage shows the rich demo (rules/tables/products, 9-week trends that move
  realistically, real failed records, tag suggestions). Visual QA is owed to the
  user (agents can't drive the authed browser).

## 11. Risks

- **1-hour in-process job** survives only while the app process does; an app restart
  mid-seed aborts it. Mitigations: the modal warns "don't interfere"; status is
  persisted so a restart is detectable; the seed is idempotent and re-runnable; the
  CLI is the robust out-of-process fallback for dev.
- **Real-run wait time** depends on serverless Job scheduling; the ~1-hour estimate
  assumes warm serverless. The seed waits on terminal state with generous timeouts
  and continues best-effort per binding.
- **Governed tags must exist** in the target metastore and the chosen tag keys must
  be assignable to columns; datagen picks from `SHOW GOVERNED TAGS` and degrades
  gracefully (skips tagging, logs) if a key is unavailable.
- **`BindingRunService` OBO assumption** (see §5) — if the run path can't create an
  SP view for the seed, planning adds a minimal SP-view seed path.

## 12. Non-goals

- Foreign-key / referential rules (known DQX engine limitation — dqlake documented
  it; left out rather than ship a rule that fails the correctness gate).
- Genie / NL diagnosis wiring (dqlake had it; out of scope here unless DQX already
  exposes it — not required for the "wow").
- A general-purpose long-job framework — the seed uses the existing daemon-thread +
  settings-status idiom, nothing new.
- Two-way tag sync (inherited apply-on-tag non-goal).
