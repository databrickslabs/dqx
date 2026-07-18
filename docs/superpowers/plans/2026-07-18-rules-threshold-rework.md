# Pass-Threshold Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-implement the DQX Studio "pass threshold" as a warn-if-`<X%`-of-rows-pass quality gate, with a 4-level resolution chain (admin default → registry-rule default → apply-time per-rule → apply-time per-column) and threshold-breach indicators surfaced across the results drill-downs, run picker, and trend chart.

**Architecture:** The threshold is **policy metadata**, and a **breach is evaluated app-side at results-query time** — DQX core has no native per-row-check pass-rate parameter (`criticality` is binary warn/error; the only native expression rewrites a row check as a dataset aggregate, which destroys per-row quarantine detail — rejected). Each check result already carries `error_count`/`warning_count`/`input_row_count`, so per-check `pass_rate = 1 − (error+warning)/input_rows`; a check **breaches** when `pass_rate < threshold/100`. Breaches roll up into every drill-down group ("any child breached") and carry the breaching check's DQX **criticality** so the UI can show a warn (amber) vs error (red) symbol. **No DB migrations:** admin default is a `dq_app_settings` key/value row; the registry-rule default is a reserved `user_metadata` key (exactly like `dimension`/`severity`); the per-rule override reuses the **existing** `dq_applied_rules.pass_threshold` column (today persisted+materialized but unenforced); the per-column override is a reserved `user_metadata` key (`column_pass_thresholds`: a `{column: int}` map) on the applied rule.

**Tech Stack:** FastAPI + Pydantic 2 backend, React 19 + TanStack Query + shadcn/ui frontend, orval-generated `api.ts`, react-i18next (en/pt-BR/it/es), pytest + bun test.

## Global Constraints

- **Threshold is an integer percent in [0, 100].** Semantics: it is a **minimum pass rate**. A check breaches when `pass_rate*100 < threshold`. `0` = never breaches; `100` = breaches unless every row passes.
- **Resolution order (first non-null wins):** per-column override → per-rule override → registry-rule default → admin default (compiled fallback `70`). This order is fixed and must be identical everywhere it's computed (frontend placeholder display AND backend breach evaluation).
- **No DB migrations.** Reuse existing columns and reserved `user_metadata` keys only.
- **Reserved `user_metadata` keys** must go through helper accessors, never raw dict lookups — mirror `get_rule_dimension`/`get_rule_severity` in `backend/registry_models.py`. New keys: registry rule `pass_threshold` (int), applied-rule `column_pass_thresholds` (`dict[str,int]`).
- **i18n:** every new user-facing string added to all four locales — `ui/lib/i18n/locales/{en,pt-BR,it,es}.json` (en is source of truth + fallback). Pluralize with `_one`/`_other` + `{{count}}` — never a hard-coded `"s"`.
- **Rule mode ids unchanged:** `dqx_native`, `lowcode`, `sql`.
- **After any backend model/route change** run `make app-regen-api` (never hand-edit `ui/lib/api.ts`). Always `git checkout app/uv.lock uv.lock` before staging — test runs churn them.
- **"Pass threshold" is admin/policy language.** Apply-time UI copy should read as "warning threshold" (the ⚠ pill) — same underlying value, framed as "warn when pass rate drops below X%".
- Backend paths are under `app/src/databricks_labs_dqx_app/backend/`, frontend under `app/src/databricks_labs_dqx_app/ui/`. Tests under `app/tests/`. Run backend tests with `cd app && uv run --group test pytest <file> -q`; UI with `cd app && bun test src/databricks_labs_dqx_app/ui`; typecheck `cd app && node_modules/.bin/tsc -b --incremental`.

---

## File Structure

**Backend**
- `backend/services/app_settings_service.py` — add `default_pass_threshold` get/save (Task 1).
- `backend/routes/v1/config.py` — add `DefaultPassThresholdOut`/`In` + GET/PUT (Task 1).
- `backend/registry_models.py` — reserved-key constants + `get_rule_pass_threshold` / `get_applied_column_pass_thresholds` accessors + a pure `resolve_pass_threshold(...)` helper (Task 2).
- `backend/services/dq_results_service.py` — thread `criticality` through `CheckResultRow`; compute per-check breach + roll-ups; new breach fields on group/run/trend outputs (Task 4).
- `backend/models.py` — new optional breach fields on `GroupRowOut`, `RunRowOut`, `TrendPointOut` (Task 4).
- `backend/routes/v1/monitored_tables.py` — carry `column_pass_thresholds` in the save/apply payload mapping (Task 3).

**Frontend**
- `ui/routes/_sidebar/settings.tsx` — new `DefaultPassThresholdSettings` card + `entries` registration (Task 5).
- `ui/components/RegistryRuleFormDialog.tsx` — registry-rule default threshold field in the advanced section (Task 6).
- `ui/components/apply-rules/RuleConfigCard.tsx` — replace the number-input threshold sub-card with a `⚠ <pct>` pill beside the severity dropdown; resolution-aware placeholder (Task 7).
- `ui/components/apply-rules/RulesByColumn.tsx` + `monitored-tables.$bindingId.tsx` — per-column threshold override editing + staging (Task 8).
- `ui/components/apply-rules/shared.tsx` — carry `column_pass_thresholds` through `buildDesiredApplications`/`desiredApplicationsKey` (Task 8).
- `ui/components/results/DimensionBreakdown.tsx`, `RunPicker.tsx`, `ScoreTrendChart.tsx`, `MultiTableResults.tsx` — breach indicators (Task 9).
- `ui/lib/i18n/locales/*.json` — strings (each task adds its own).

---

## Task 1: Admin setting — `default_pass_threshold`

Mirror the `draft-run-sample-limit` setting exactly.

**Files:**
- Modify: `backend/services/app_settings_service.py`
- Modify: `backend/routes/v1/config.py`
- Test: `app/tests/test_app_settings_service.py`, `app/tests/test_config_routes.py` (or the existing settings-route test file — locate with `grep -rl draft_run_sample_limit app/tests`)

**Interfaces:**
- Produces: `AppSettingsService.get_default_pass_threshold() -> int` (returns `70` when unset), `AppSettingsService.save_default_pass_threshold(value: int, *, user_email: str) -> int`. Constant `DEFAULT_PASS_THRESHOLD_DEFAULT = 70`. Route `operation_id`s `getDefaultPassThreshold` (GET `/api/v1/config/default-pass-threshold`) and `saveDefaultPassThreshold` (PUT). `DefaultPassThresholdOut{default_pass_threshold: int, default_pass_threshold_default: int}`, `DefaultPassThresholdIn{default_pass_threshold: int = Field(ge=0, le=100)}`.

- [ ] **Step 1: Write the failing service test**

In `app/tests/test_app_settings_service.py` (mirror the existing `draft_run_sample_limit` / `ai_rate_limit` tests — find one and copy its fixture setup):

```python
def test_default_pass_threshold_defaults_to_70(app_settings_service):
    assert app_settings_service.get_default_pass_threshold() == 70

def test_save_and_get_default_pass_threshold(app_settings_service):
    app_settings_service.save_default_pass_threshold(85, user_email="a@x")
    assert app_settings_service.get_default_pass_threshold() == 85
```

- [ ] **Step 2: Run to verify fail**

Run: `cd app && uv run --group test pytest tests/test_app_settings_service.py -k default_pass_threshold -q`
Expected: FAIL (`AttributeError: ... get_default_pass_threshold`).

- [ ] **Step 3: Implement the service methods**

In `backend/services/app_settings_service.py`, near the `ai_rate_limit` block (~line 756/804), add the constant and methods (mirror `get_ai_rate_limit_per_user_per_hour` which returns a default rather than `None`):

```python
DEFAULT_PASS_THRESHOLD_DEFAULT = 70

# inside class AppSettingsService:
_DEFAULT_PASS_THRESHOLD_KEY = "default_pass_threshold"

def get_default_pass_threshold(self) -> int:
    """Org-wide default minimum pass rate (%) below which a check warns.

    Returns the compiled default (70) when unset or unparseable. Clamped to
    [0, 100] defensively so a hand-edited row can never escape the range.
    """
    value = self._get_int_setting(self._DEFAULT_PASS_THRESHOLD_KEY)
    if value is None:
        return DEFAULT_PASS_THRESHOLD_DEFAULT
    return max(0, min(100, value))

def save_default_pass_threshold(self, value: int, *, user_email: str) -> int:
    clamped = max(0, min(100, int(value)))
    self.save_setting(self._DEFAULT_PASS_THRESHOLD_KEY, str(clamped), user_email=user_email)
    return clamped
```

- [ ] **Step 4: Run service test to pass**

Run: `cd app && uv run --group test pytest tests/test_app_settings_service.py -k default_pass_threshold -q`
Expected: PASS.

- [ ] **Step 5: Write the failing route test**

In the config-routes test file, mirror the `draft_run_sample_limit` route test: GET returns `{default_pass_threshold: 70, default_pass_threshold_default: 70}`; PUT with `{default_pass_threshold: 90}` returns `90`; PUT with `150` returns 422 (Pydantic bound). Assert `require_role(ADMIN)` via the same `_route_required_roles` helper used by other admin settings tests.

- [ ] **Step 6: Run to verify fail**

Run: `cd app && uv run --group test pytest tests/<config_routes_test>.py -k default_pass_threshold -q`
Expected: FAIL (route 404 / attribute missing).

- [ ] **Step 7: Implement the routes**

In `backend/routes/v1/config.py`, mirror the `draft-run-sample-limit` GET/PUT (~lines 421-469). Add the models near the other `*Out`/`*In` pairs:

```python
class DefaultPassThresholdOut(BaseModel):
    default_pass_threshold: int
    default_pass_threshold_default: int

class DefaultPassThresholdIn(BaseModel):
    default_pass_threshold: int = Field(ge=0, le=100)

@router.get("/default-pass-threshold", response_model=DefaultPassThresholdOut,
            operation_id="getDefaultPassThreshold", dependencies=[require_role(UserRole.ADMIN)])
def get_default_pass_threshold(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DefaultPassThresholdOut:
    from databricks_labs_dqx_app.backend.services.app_settings_service import DEFAULT_PASS_THRESHOLD_DEFAULT
    return DefaultPassThresholdOut(
        default_pass_threshold=svc.get_default_pass_threshold(),
        default_pass_threshold_default=DEFAULT_PASS_THRESHOLD_DEFAULT,
    )

@router.put("/default-pass-threshold", response_model=DefaultPassThresholdOut,
            operation_id="saveDefaultPassThreshold", dependencies=[require_role(UserRole.ADMIN)])
def save_default_pass_threshold(
    body: DefaultPassThresholdIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> DefaultPassThresholdOut:
    from databricks_labs_dqx_app.backend.services.app_settings_service import DEFAULT_PASS_THRESHOLD_DEFAULT
    saved = svc.save_default_pass_threshold(body.default_pass_threshold, user_email=email)
    return DefaultPassThresholdOut(default_pass_threshold=saved, default_pass_threshold_default=DEFAULT_PASS_THRESHOLD_DEFAULT)
```

(Match the file's existing import style for `get_user_email`, `require_role`, `UserRole`, `get_app_settings_service`, `Field`, `Annotated`, `Depends`. Prefer a top-of-file import for `DEFAULT_PASS_THRESHOLD_DEFAULT` if the file imports other AppSettings constants that way.)

- [ ] **Step 8: Run route test to pass**

Run: `cd app && uv run --group test pytest tests/<config_routes_test>.py -k default_pass_threshold -q`
Expected: PASS.

- [ ] **Step 9: Regenerate API client & typecheck**

Run: `cd /Users/oliver.gordon/Documents/Code/Other/dqx && make app-regen-api && cd app && node_modules/.bin/tsc -b --incremental`
Expected: orval regenerates `useGetDefaultPassThreshold`/`useSaveDefaultPassThreshold`; tsc clean.

- [ ] **Step 10: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx && git checkout app/uv.lock uv.lock
git add app/src/databricks_labs_dqx_app/backend app/src/databricks_labs_dqx_app/ui/lib/api.ts app/tests
git commit -m "feat(threshold): admin default_pass_threshold setting (backend + API)"
```

---

## Task 2: Threshold resolution helpers (registry-rule default + resolver)

Add the reserved-key accessors and the single source-of-truth resolver used by breach evaluation.

**Files:**
- Modify: `backend/registry_models.py`
- Test: `app/tests/test_registry_models.py` (or wherever `get_rule_dimension` is tested — `grep -rl get_rule_dimension app/tests`)

**Interfaces:**
- Consumes: existing `RESERVED_*_KEY` pattern and `get_rule_dimension` (registry_models.py:454-569).
- Produces:
  - `RESERVED_PASS_THRESHOLD_KEY = "pass_threshold"` (registry rule `user_metadata`)
  - `RESERVED_COLUMN_PASS_THRESHOLDS_KEY = "column_pass_thresholds"` (applied-rule `user_metadata`)
  - `get_rule_pass_threshold(user_metadata: dict[str, Any]) -> int | None`
  - `get_applied_column_pass_thresholds(user_metadata: dict[str, Any]) -> dict[str, int]`
  - `resolve_pass_threshold(*, column_override: int | None, rule_override: int | None, registry_default: int | None, admin_default: int) -> int` — first non-null of (column, rule, registry) else admin. Every consumer must call this.

- [ ] **Step 1: Write failing tests**

```python
from databricks_labs_dqx_app.backend.registry_models import (
    get_rule_pass_threshold, get_applied_column_pass_thresholds, resolve_pass_threshold,
)

def test_get_rule_pass_threshold_reads_reserved_key():
    assert get_rule_pass_threshold({"pass_threshold": 80}) == 80
    assert get_rule_pass_threshold({"pass_threshold": "80"}) == 80  # tolerate string
    assert get_rule_pass_threshold({}) is None
    assert get_rule_pass_threshold({"pass_threshold": 999}) == 100  # clamp
    assert get_rule_pass_threshold({"pass_threshold": "bad"}) is None

def test_get_applied_column_pass_thresholds():
    assert get_applied_column_pass_thresholds({"column_pass_thresholds": {"a": 90, "b": 50}}) == {"a": 90, "b": 50}
    assert get_applied_column_pass_thresholds({}) == {}
    assert get_applied_column_pass_thresholds({"column_pass_thresholds": {"a": 999, "b": "bad"}}) == {"a": 100}  # clamp + drop bad

def test_resolve_pass_threshold_precedence():
    assert resolve_pass_threshold(column_override=95, rule_override=80, registry_default=60, admin_default=70) == 95
    assert resolve_pass_threshold(column_override=None, rule_override=80, registry_default=60, admin_default=70) == 80
    assert resolve_pass_threshold(column_override=None, rule_override=None, registry_default=60, admin_default=70) == 60
    assert resolve_pass_threshold(column_override=None, rule_override=None, registry_default=None, admin_default=70) == 70
    # zero is a real value, not "unset"
    assert resolve_pass_threshold(column_override=0, rule_override=80, registry_default=60, admin_default=70) == 0
```

- [ ] **Step 2: Run to verify fail**

Run: `cd app && uv run --group test pytest tests/test_registry_models.py -k pass_threshold -q`
Expected: FAIL (import error).

- [ ] **Step 3: Implement**

In `backend/registry_models.py`, near the other reserved keys (~454) and accessors (~554):

```python
RESERVED_PASS_THRESHOLD_KEY = "pass_threshold"
RESERVED_COLUMN_PASS_THRESHOLDS_KEY = "column_pass_thresholds"


def _coerce_threshold(value: object) -> int | None:
    """Parse a stored threshold to a clamped int in [0, 100], or None if invalid."""
    try:
        return max(0, min(100, int(value)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def get_rule_pass_threshold(user_metadata: dict[str, Any]) -> int | None:
    """Registry-rule default minimum pass rate (%), or None if unset."""
    return _coerce_threshold(user_metadata.get(RESERVED_PASS_THRESHOLD_KEY))


def get_applied_column_pass_thresholds(user_metadata: dict[str, Any]) -> dict[str, int]:
    """Per-column threshold overrides on an applied rule ({column: pct}). Invalid entries dropped."""
    raw = user_metadata.get(RESERVED_COLUMN_PASS_THRESHOLDS_KEY)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for col, val in raw.items():
        coerced = _coerce_threshold(val)
        if coerced is not None:
            out[str(col)] = coerced
    return out


def resolve_pass_threshold(
    *, column_override: int | None, rule_override: int | None,
    registry_default: int | None, admin_default: int,
) -> int:
    """First non-null of (column, rule, registry) else the admin default. See plan Global Constraints."""
    for candidate in (column_override, rule_override, registry_default):
        if candidate is not None:
            return candidate
    return admin_default
```

(Confirm `Any` is already imported in the file — it is, used by `get_rule_dimension`.)

- [ ] **Step 4: Run to pass**

Run: `cd app && uv run --group test pytest tests/test_registry_models.py -k pass_threshold -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx && git checkout app/uv.lock uv.lock
git add app/src/databricks_labs_dqx_app/backend/registry_models.py app/tests
git commit -m "feat(threshold): reserved-key accessors + resolve_pass_threshold precedence helper"
```

---

## Task 3: Persist per-column threshold overrides (backend)

Carry `column_pass_thresholds` through the apply/save payload into the applied-rule `user_metadata`. The per-rule `pass_threshold` column already round-trips (verified) — no change needed there.

**Files:**
- Modify: `backend/models.py` (`DesiredAppliedRuleIn`, `AppliedRuleOut`)
- Modify: `backend/routes/v1/monitored_tables.py` (save/apply mapping ~772-782)
- Modify: `backend/services/apply_rules_service.py` (`DesiredAppliedRule` dataclass ~115; ensure `user_metadata` persists the key)
- Test: `app/tests/test_apply_rules_service.py` (or the applied-rules service test file — `grep -rl save_applied_rules app/tests`)

**Interfaces:**
- Consumes: `get_applied_column_pass_thresholds` (Task 2).
- Produces: `DesiredAppliedRuleIn.column_pass_thresholds: dict[str, int] | None`, threaded into the applied rule's `user_metadata[RESERVED_COLUMN_PASS_THRESHOLDS_KEY]`. `AppliedRuleOut` exposes it back (read from `user_metadata` via the accessor) as `column_pass_thresholds: dict[str, int]`.

**Design note:** `user_metadata` on the applied rule is already persisted as JSON (the `tags` payload). Store the map under the reserved key inside that same JSON blob — no new column. The apply/save handler already forwards `tags` → `user_metadata`; merge the column map into it server-side so the client sends it as a first-class field rather than smuggling it in free-text tags.

- [ ] **Step 1: Write failing service test**

Assert that saving an application with `column_pass_thresholds={"email": 90}` persists it and `AppliedRuleOut`/domain read-back returns `{"email": 90}` (and that invalid values are dropped/clamped via the accessor). Mirror the existing `pass_threshold` persistence test in the same file.

- [ ] **Step 2: Run to verify fail** — `pytest tests/test_apply_rules_service.py -k column_pass_threshold -q` → FAIL.

- [ ] **Step 3: Implement models**

In `backend/models.py`, on `DesiredAppliedRuleIn` (~780-806) add:

```python
column_pass_thresholds: dict[str, int] | None = Field(
    default=None,
    description="Per-column minimum-pass-rate overrides ({column: pct 0-100}); merged into user_metadata.",
)
```

On `AppliedRuleOut` (~692) add `column_pass_thresholds: dict[str, int] = {}` and populate it in `from_summary`/`from_domain` via `get_applied_column_pass_thresholds(<user_metadata>)`.

- [ ] **Step 4: Implement the mapping**

In `backend/routes/v1/monitored_tables.py` `save_applied_rules` (~772-782), when building each `DesiredAppliedRule`, merge the column map into the tags/user_metadata dict under `RESERVED_COLUMN_PASS_THRESHOLDS_KEY` (only when non-empty; omit the key entirely when the client sends `None`/`{}` so it doesn't clutter metadata). In `backend/services/apply_rules_service.py`, ensure the `DesiredAppliedRule.user_metadata` is what gets persisted to the `user_metadata` JSON column (it already is for tags) — add the dataclass field if the column map is passed separately, else rely on the merged user_metadata.

- [ ] **Step 5: Run to pass** — `pytest tests/test_apply_rules_service.py -k column_pass_threshold -q` → PASS.

- [ ] **Step 6: Regenerate API + typecheck** — `make app-regen-api && cd app && node_modules/.bin/tsc -b --incremental`.

- [ ] **Step 7: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx && git checkout app/uv.lock uv.lock
git add app/src/databricks_labs_dqx_app/backend app/src/databricks_labs_dqx_app/ui/lib/api.ts app/tests
git commit -m "feat(threshold): persist per-column threshold overrides in applied-rule metadata"
```

---

## Task 4: Breach evaluation in the results service

Thread `criticality` through the per-check aggregation (currently dropped at `parse_check_rows`, dq_results_service.py:135) and compute per-check breach + group/run/trend roll-ups.

**Files:**
- Modify: `backend/services/dq_results_service.py` (`CheckResultRow`:65-86, `parse_check_rows`:135, `_GroupAcc`, `_group_rows`:210, `_by_rule_rows`:235, `_by_column_rows`:267, `_trend_*`, `compute_entity_results`:501)
- Modify: `backend/services/score_view_service.py` only if `criticality` isn't already selectable per-check row (it IS projected in `v_dq_check_results` at :315 — confirm `_fetch_check_rows` in dq_results.py selects it; add to the SELECT if missing)
- Modify: `backend/models.py` (`GroupRowOut`:1847, `RunRowOut`:1946, `TrendPointOut`:1873 — add optional breach fields)
- Modify: `backend/routes/v1/dq_results.py` (`_fetch_check_rows`:188 select `criticality`; pass admin default + resolved thresholds into the service)
- Test: `app/tests/test_dq_results_service.py`

**Interfaces:**
- Consumes: `resolve_pass_threshold`, `get_rule_pass_threshold`, `get_applied_column_pass_thresholds` (Task 2); `AppSettingsService.get_default_pass_threshold` (Task 1); applied-rule + registry `user_metadata` fetched for the binding(s) under evaluation.
- Produces on each `GroupRowOut`: `breached: bool` and `breach_criticality: str | None` (`"error"` if any breaching child check is error-criticality, else `"warn"`, else `None`). Same two fields on `RunRowOut` and `TrendPointOut`. Per-check breach: `pass_rate_pct < resolved_threshold` where `pass_rate_pct = (1 - failed/total)*100`, `failed = error_count + warning_count`, `total = input_row_count` (guard `total == 0` → not breached).

**Design note (threshold lookup at query time):** the results service aggregates check rows that carry `registry_rule_id` and `columns`. Build a resolver closure once per request: fetch the admin default (1 call), and for the binding(s) in scope fetch applied-rule rows (rule_id → per-rule `pass_threshold`, per-column map) and the registry rule `user_metadata` (rule_id → registry default). For a check row, resolve with `resolve_pass_threshold(column_override=<map.get(column)>, rule_override=<applied.pass_threshold>, registry_default=<get_rule_pass_threshold(registry_md)>, admin_default=<admin>)`. When a check spans multiple columns, use the **strictest** applicable column override (max threshold) so a single lax column can't hide a breach. Global (org-wide) results with no single binding: resolve with rule/registry/admin only (skip column overrides).

- [ ] **Step 1: Write failing tests** — construct `CheckResultRow`s with known error/warning/input counts + criticality, feed a fixed resolver (admin=70, one rule override=90), assert: a check at 80% pass breaches under the 90 override (warn if warn-crit); a by-rule group with one breaching check has `breached=True`, `breach_criticality` reflecting the worst; a clean group has `breached=False, breach_criticality=None`; `total==0` never breaches.

- [ ] **Step 2: Run to verify fail** — `pytest tests/test_dq_results_service.py -k breach -q` → FAIL.

- [ ] **Step 3: Add `criticality` to `CheckResultRow`** and stop collapsing it — keep `error_count`/`warning_count` separate through aggregation (they already are separate columns; the sum happens only for `failed`). Add `criticality: str | None` field (default None) and populate from the row (view column exists).

- [ ] **Step 4: Implement per-check breach + roll-up** — add a `resolve_threshold(check_row) -> int` callback param to `compute_entity_results` (and the run/trend builders). For each check compute breach; a group breaches if any child check breaches; `breach_criticality` = `"error"` if any breaching child is error-crit else `"warn"`. Thread the two new fields into `GroupRowOut`, per-run into `RunRowOut`, per-trend-point into `TrendPointOut`.

- [ ] **Step 5: Add model fields** — `breached: bool = False`, `breach_criticality: str | None = None` on the three output models (optional/defaulted so nothing else breaks).

- [ ] **Step 6: Wire the routes** — in `dq_results.py`, build the resolver (fetch admin default + applied/registry thresholds) and pass it into `compute_entity_results` and the run/trend queries; ensure `_fetch_check_rows` selects `criticality`.

- [ ] **Step 7: Run to pass** — `pytest tests/test_dq_results_service.py -k breach -q` → PASS. Then full file: `pytest tests/test_dq_results_service.py -q`.

- [ ] **Step 8: Regenerate API + typecheck** — `make app-regen-api && cd app && node_modules/.bin/tsc -b --incremental`.

- [ ] **Step 9: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx && git checkout app/uv.lock uv.lock
git add app/src/databricks_labs_dqx_app/backend app/src/databricks_labs_dqx_app/ui/lib/api.ts app/tests
git commit -m "feat(threshold): per-check breach evaluation + criticality-aware roll-ups in results service"
```

---

## Task 5: Admin settings card (frontend)

**Files:**
- Modify: `ui/routes/_sidebar/settings.tsx` (new `DefaultPassThresholdSettings` component + `entries` registration ~2912, `governance` tab)
- Modify: `ui/lib/i18n/locales/*.json` (`config` block)
- Test: add/extend a component test if the settings file has one (`grep -rl DraftRunSampleLimit app/src/databricks_labs_dqx_app/ui/**/*.test.*`); otherwise rely on tsc + manual.

**Interfaces:** Consumes `useGetDefaultPassThreshold`/`useSaveDefaultPassThreshold` (Task 1).

- [ ] **Step 1** — Add i18n keys to all four locales under `config`: `defaultPassThresholdTitle`, `defaultPassThresholdLabel`, `defaultPassThresholdHint` ("Checks warn when fewer than this % of rows pass. Overridable per rule and per column."), `defaultPassThresholdSaved`, `failedSaveDefaultPassThreshold`, `defaultPassThresholdAdminOnlyHint`, `kwDefaultPassThreshold` (search keywords: "threshold pass warning quality gate").

- [ ] **Step 2** — Write `DefaultPassThresholdSettings` mirroring `DraftRunSampleLimitSettings` (settings.tsx:1210-1288): `useGetDefaultPassThreshold()` → `resp?.data`; number `<Input type="number" min={0} max={100} step={1}>` with a trailing `%`; clamp `[0,100]` on blur; `useSaveDefaultPassThreshold()` with `onSuccess` invalidate + toast, `onError` toast; `disabled={!isAdmin || saveMutation.isPending}` + admin-only hint. Register one `entries` object `{ id: "default-pass-threshold", tab: "governance", title: t("config.defaultPassThresholdTitle"), keywords: t("config.kwDefaultPassThreshold"), render: () => <DefaultPassThresholdSettings /> }`.

- [ ] **Step 3** — `cd app && node_modules/.bin/tsc -b --incremental` (clean) + `uv run --group test pytest tests/test_i18n_locale_parity.py -q` (parity).

- [ ] **Step 4: Commit** — `feat(threshold): admin default pass-threshold settings card`.

---

## Task 6: Registry-rule default threshold (rule form advanced section)

**Files:**
- Modify: `ui/components/RegistryRuleFormDialog.tsx` (advanced section; snapshot/build user_metadata)
- Modify: `ui/lib/i18n/locales/*.json`
- Test: extend `app/src/.../ui` form tests if present.

**Interfaces:** persists the value into the rule's `user_metadata.pass_threshold` (read back by `get_rule_pass_threshold` server-side, Task 2). Must round-trip through the same `buildUserMetadata`/`snapshotFromRule` path that handles `dimension`/`severity` (RegistryRuleFormDialog.tsx ~1564-1590, 1839, 2073-2074).

- [ ] **Step 1** — i18n (4 locales) under `rulesRegistry`: `defaultPassThresholdLabel` ("Default pass threshold"), `defaultPassThresholdHint` ("Optional. Checks from this rule warn when fewer than this % of rows pass, unless overridden when applied. Leave blank to use the workspace default."), `defaultPassThresholdPlaceholder` ("Workspace default").

- [ ] **Step 2** — Add `passThreshold: number | null` to the form snapshot type (~1539) and default (~1634); read from `user_metadata.pass_threshold` in `snapshotFromRule` (~1564-1590) via a null-tolerant parse; include it in `buildUserMetadata` (~2073) writing the reserved `pass_threshold` key (omit the key when null). Render a number `<Input min={0} max={100} step={1}>` (with `%` suffix + placeholder = workspace default) inside an existing `AdvancedDisclosure` block (the metadata/advanced one — reuse the section that already holds dimension/severity or the filter advanced block ~3384-3423). Wire value/onChange to the snapshot state.

- [ ] **Step 3** — tsc clean + locale parity.

- [ ] **Step 4: Commit** — `feat(threshold): registry-rule default threshold in rule form advanced section`.

---

## Task 7: Apply-time per-rule threshold pill (by-rule)

Replace the number-input threshold sub-card with a `⚠ <pct>%` pill beside the severity dropdown. Reuses the existing `pass_threshold` staged field + `handlePassThresholdChange` (monitored-tables.$bindingId.tsx:2321-2323) — only the control changes.

**Files:**
- Modify: `ui/components/apply-rules/RuleConfigCard.tsx` (remove sub-card 800-829; add pill in the header badge row at ~695 beside `SeverityDropdown`)
- Modify: `ui/lib/i18n/locales/*.json`
- Test: extend RuleConfigCard tests if present.

**Interfaces:** Consumes `onPassThresholdChange` (already a prop). Needs the **resolved default** for placeholder display: pass a new prop `resolvedDefaultThreshold: number` (computed in `ApplyRulesTab` as registry-rule default ?? admin default — fetch admin default via `useGetDefaultPassThreshold`, registry default from the rule's `rule_pass_threshold` if surfaced on `AppliedRuleOut`; if not surfaced, use admin default only and note the limitation). The pill shows the **effective** value: `rule.pass_threshold ?? resolvedDefaultThreshold`, with a subtle "overridden" marker (like severity's `*`) when `rule.pass_threshold != null`.

- [ ] **Step 1** — i18n (4 locales) under `monitoredTables`: `thresholdPillLabel` ("Warn < {{pct}}%"), `thresholdPillAria` ("Warning threshold: warn when fewer than {{pct}}% of rows pass"), `thresholdPopoverTitle` ("Warning threshold"), `thresholdPopoverHint` ("Warn when fewer than this % of rows pass this check. Blank = use default ({{pct}}%)."), `thresholdResetToDefault` ("Use default"). Remove the now-unused `ruleThresholdLabel/Placeholder/Hint` keys from all four (or repurpose).

- [ ] **Step 2** — Build a `ThresholdPill` (a `Badge`-triggered `Popover` with a number input + "Use default" button, mirroring `SeverityDropdown`'s badge-trigger pattern at RuleConfigCard.tsx:444-480). Effective value + overridden marker as above. On change → `onPassThresholdChange(clampedIntOrNull)`; "Use default" → `onPassThresholdChange(null)`. Render it in the header badge row (the `<div className="flex items-center gap-1 shrink-0 ml-2">` at ~695) right after `SeverityDropdown`. Delete the old sub-card (800-829).

- [ ] **Step 3** — In `ApplyRulesTab` (monitored-tables.$bindingId.tsx), compute `resolvedDefaultThreshold` and pass it to each `RuleConfigCard` (~2498-2521). Remove the old sub-card wiring path if any state is now dead.

- [ ] **Step 4** — tsc clean + `bun test src/databricks_labs_dqx_app/ui` + locale parity.

- [ ] **Step 5: Commit** — `feat(threshold): warning-threshold pill beside severity in by-rule apply view`.

---

## Task 8: Apply-time per-column threshold override (by-column)

Add per-column threshold editing to the by-column view and stage/persist it as `column_pass_thresholds` (Task 3 backend).

**Files:**
- Modify: `ui/components/apply-rules/RulesByColumn.tsx` (add a per-column threshold pill on each column card / rule entry)
- Modify: `ui/routes/_sidebar/monitored-tables.$bindingId.tsx` (staging handler `handleColumnThresholdChange`; seed from `AppliedRuleOut.column_pass_thresholds`)
- Modify: `ui/components/apply-rules/shared.tsx` (`buildDesiredApplications` ~199 emit `column_pass_thresholds`; `desiredApplicationsKey` ~225 include it; `newStagedRow` default)
- Modify: `ui/lib/i18n/locales/*.json`
- Test: extend shared.tsx / RulesByColumn tests if present; add a staging→payload unit test for `buildDesiredApplications` carrying the column map.

**Interfaces:** Consumes `AppliedRuleOut.column_pass_thresholds` (Task 3). Per-column override lives on the applied rule keyed by column name. Because `buildDesiredApplications` collapses rows to `rows[0]`, store the merged `{column: pct}` map on the rule application (union across its columns) — a per-column, per-rule-application value.

- [ ] **Step 1** — i18n (4 locales) under `monitoredTables`: `columnThresholdPillLabel` ("Warn < {{pct}}%"), `columnThresholdPopoverHint` ("Warn when fewer than this % of rows pass this rule for this column. Blank = use the rule's threshold ({{pct}}%)."), `columnThresholdResetToDefault` ("Use rule default").

- [ ] **Step 2** — Extend `RuleEntry` (RulesByColumn.tsx:36-43) to carry the current per-column threshold + the resolved rule-level default for placeholder. Render a small `ThresholdPill` (reuse the component from Task 7 — extract it to `ui/components/apply-rules/ThresholdPill.tsx` in Task 7 so both views share it) on each rule entry. On change call `onColumnThresholdChange(ruleId, column, pctOrNull)`.

- [ ] **Step 3** — Add `handleColumnThresholdChange` in `ApplyRulesTab`: update `stagedRows` for the `rule_id`, merging `{column: pct}` into that row's `column_pass_thresholds` (delete the key when null). Seed staged rows' `column_pass_thresholds` from `AppliedRuleOut` in `normalizeStagedRows`/`newStagedRow`.

- [ ] **Step 4** — `shared.tsx`: `buildDesiredApplications` emits `column_pass_thresholds: mergeColumnThresholds(rows)` (union of all rows' maps for the rule_id); `desiredApplicationsKey` includes a stable-sorted serialization of it.

- [ ] **Step 5** — tsc clean + `bun test src/databricks_labs_dqx_app/ui` (incl. new buildDesiredApplications test) + locale parity.

- [ ] **Step 6: Commit** — `feat(threshold): per-column warning-threshold override in by-column apply view`.

---

## Task 9: Results breach indicators

Surface breach warnings across drill-downs, run picker, and trend chart, colored by criticality (Task 4 fields).

**Files:**
- Modify: `ui/components/results/DimensionBreakdown.tsx` (row render 305-368 — warning glyph when `row.breached`)
- Modify: `ui/components/results/MultiTableResults.tsx` (`toRows` 485-496 carry `breached`/`breach_criticality`; pass a `breachMarkers` prop to the trend chart built from breaching trend points)
- Modify: `ui/components/results/RunPicker.tsx` (`Run` type 12-19 + `formatRun` 27-47 + item render 99-113 — icon when the run breached)
- Modify: `ui/components/results/ScoreTrendChart.tsx` (per-run marker — mirror the `versionMarkers`→`ReferenceLine` pattern 1544-1563, or a variant `scoreDot` drawing a warning glyph when a per-point breach flag is set)
- Modify: `ui/lib/i18n/locales/*.json`
- Test: extend results component tests if present.

**Interfaces:** Consumes `GroupRowOut.breached/breach_criticality`, `RunRowOut.breached/breach_criticality`, `TrendPointOut.breached/breach_criticality` (Task 4). `BreakdownRow` (DimensionBreakdown.tsx:67-79) gains `breached?: boolean; breachCriticality?: "warn" | "error" | null`.

- [ ] **Step 1** — i18n (4 locales) under `resultsUi`: `breachWarnTooltip` ("Warning: pass rate below threshold"), `breachErrorTooltip` ("Error: pass rate below threshold"), `runBreachTooltip` ("This run has a threshold breach").

- [ ] **Step 2** — Shared glyph: a small `AlertTriangle` (lucide) in amber for `warn`, red for `error`, wrapped in a `Tooltip`. Add to `BreakdownRow` render (before/after the label) when `row.breached`. Extend `BreakdownRow` + `toRows` to carry the two fields for by-dimension/severity/rule/column/table.

- [ ] **Step 3** — `RunPicker`: extend `Run` with `breached`/`breach_criticality`; render the glyph in each item (99-113) and in `formatRun`.

- [ ] **Step 4** — `ScoreTrendChart`: add a `breachMarkers`-style overlay (a `ReferenceDot`/custom `scoreDot` variant) drawing the warning glyph at each breaching run's point, colored by criticality. Build the marker list in `MultiTableResults.tsx` from breaching trend points.

- [ ] **Step 5** — tsc clean + `bun test src/databricks_labs_dqx_app/ui` + locale parity.

- [ ] **Step 6: Commit** — `feat(threshold): breach indicators in drill-downs, run picker, and trend chart`.

---

## Notes for the executor

- **This plan intentionally avoids DB migrations.** If a task seems to need one, stop and re-read the storage design in the Architecture header — you're probably missing a reserved `user_metadata` key.
- **`resolve_pass_threshold` is the single source of truth** for precedence. The frontend placeholder logic (Tasks 7/8) must produce the same effective value the backend breach evaluator (Task 4) uses. If they diverge, the pill will lie about when a breach fires.
- **Tasks 1-2 are foundation** (no UI). Tasks 3-4 are the backend spine. Tasks 5-9 are UI. If runway runs short, a coherent shippable slice is Tasks 1+2+5+6 (admin + registry defaults, settable but not yet breach-evaluated) — but breach indicators (the user's headline ask) need Task 4 + Task 9, so prioritize the 1→2→3→4→7→9 spine before the by-column refinement (Task 8).
- **Open decision flagged for the user (do not block on it):** breach is a **results-display** concept, not enforced in the actual run's `_errors`/`_warnings` (DQX can't express per-row-check pass-rate natively without rewriting checks as dataset aggregates and losing quarantine detail). If the user wants the run itself to emit a warn based on threshold, that's a materializer/executor change — a separate, larger effort. Called out in the handoff.
