# DQ Score, Severity Mapping & Results UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **REQUIRED: use the `fable` model for every implementer subagent dispatched against this plan.**

**Goal:** Add an admin-editable severity→DQX-criticality mapping, a DQ score computed from existing `dq_metrics` data, and results UI (table, data product, rule, and global scope) to DQX Studio — all gated so a user only ever sees results for source tables they currently have live Unity Catalog SELECT on.

**Architecture:** Extend the existing `label_definitions` system (no new severity table) for criticality mapping. Add a new `dq_score.py` backend service + router that reads the existing, unmodified `dq_metrics` table to compute scores — no changes to the frozen job/task pipeline. Reuse the existing `get_user_catalog_names` OBO-filtering pattern (already used by `metrics.py`) for aggregate score endpoints, and add a stricter per-table OBO self-check for the row-level failing-sample endpoint, since quarantine rows for all monitored tables live in one shared `dq_quarantine_records` table filtered by `source_table_fqn` (there is no per-table quarantine object).

**Tech Stack:** FastAPI + Pydantic backend, React + TypeScript + TanStack Router + shadcn/ui frontend, Orval-generated API client, pytest for backend tests.

## Global Constraints

- Do not modify `app/tasks/`, `backend/services/job_service.py`, or the `dq_quality_rules` table spec — all frozen.
- Do not change frozen pipeline files: `src/databricks/labs/dqx/metrics_observer.py`, `src/databricks/labs/dqx/quality_checker/*`.
- All new UI strings go through `t()` in all 4 locales (see `app/src/databricks_labs_dqx_app/ui/CLAUDE.md` for the i18n workflow) — English-only for manual testing per project convention, but every string must still be wired through `t()` and present in all locale files.
- Run `make app-check` (bun tsc -b + basedpyright) and `make app-test` after each task; run `make app-regen-api` after any backend route/model change, before touching frontend code that consumes it.
- No new frontend charting dependency — build the score/trend display with existing shadcn primitives (`badge`, `card`) and plain SVG/CSS, not a new chart library (YAGNI; no chart lib is installed today).
- Format with `make fmt` before every commit.
- GPG-sign every commit (already configured repo-locally); never `--no-gpg-sign`.
- (Added 2026-07-10, user requirement) Score/results aggregate data shown in the UI must be backed by a Unity Catalog **metric view** (see Task 14), not ad hoc SQL over `dq_metrics`.
- (Added 2026-07-10, user requirement) Results queries in the UI must never auto-refetch (no `refetchOnWindowFocus`, no polling interval, `staleTime: Infinity`); they are invalidated **only** when a relevant run finishes (see Tasks 9-12).

---

### Task 1: Severity → criticality mapping (backend)

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/config.py` (`LabelDefinition` model, ~lines 78-129)
- Modify: `app/src/databricks_labs_dqx_app/backend/services/app_settings_service.py` (`_RESERVED_LABEL_DEFINITION_SEEDS`, ~lines 561-605)
- Modify: `app/src/databricks_labs_dqx_app/backend/registry_models.py` (`resolve_criticality`, ~lines 504-543)
- Test: `app/tests/unit/backend/test_registry_models.py` (or the existing test file covering `resolve_criticality` — search for it; create alongside if none exists)
- Test: `app/tests/unit/backend/routes/test_config.py` (existing file covering `label-definitions` — extend it)

**Interfaces:**
- Produces: `LabelDefinition.value_criticality: dict[str, Literal["warn", "error"]] | None` (new Pydantic field)
- Produces: `resolve_criticality(severity: str | None, app_settings_service: AppSettingsService) -> str` (signature changes — now takes the settings service instead of using the hardcoded dict; every existing caller in `materializer.py` must be updated to pass it through)
- Consumes: existing `AppSettingsService.get_setting`/`save_setting` (unchanged)

- [ ] **Step 1: Write the failing test for the new field**

```python
# app/tests/unit/backend/routes/test_config.py
def test_label_definition_accepts_value_criticality():
    from databricks_labs_dqx_app.backend.routes.v1.config import LabelDefinition

    definition = LabelDefinition(
        key="severity",
        values=["Low", "Critical"],
        value_criticality={"Low": "warn", "Critical": "error"},
        is_builtin=True,
    )
    assert definition.value_criticality == {"Low": "warn", "Critical": "error"}


def test_label_definition_rejects_invalid_criticality_value():
    from databricks_labs_dqx_app.backend.routes.v1.config import LabelDefinition
    import pytest

    with pytest.raises(ValueError):
        LabelDefinition(
            key="severity",
            values=["Low"],
            value_criticality={"Low": "not-a-real-criticality"},
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest tests/unit/backend/routes/test_config.py -k value_criticality -v`
Expected: FAIL — `value_criticality` is not a recognized field / no validation error raised.

- [ ] **Step 3: Add the field + validator to `LabelDefinition`**

In `config.py`, add alongside `value_colors`:

```python
    value_criticality: dict[str, str] | None = None

    @field_validator("value_criticality")
    @classmethod
    def _validate_value_criticality(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is None:
            return None
        for label_value, criticality in value.items():
            if criticality not in ("warn", "error"):
                raise ValueError(
                    f"Invalid criticality {criticality!r} for value {label_value!r}; expected 'warn' or 'error'."
                )
        return value
```

Also prune `value_criticality` to keys present in `values` in `save_label_definitions`, next to the existing `cleaned_colors`/`cleaned_descriptions` pruning:

```python
        cleaned_criticality = {
            v: c for v, c in (d.value_criticality or {}).items() if v in seen_values
        } or None
```

and pass it into the constructed `LabelDefinition(...)` alongside `value_colors=cleaned_colors`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest tests/unit/backend/routes/test_config.py -k value_criticality -v`
Expected: PASS

- [ ] **Step 5: Seed `value_criticality` on the `severity` reserved definition**

In `app_settings_service.py`, add to the `"severity"` entry in `_RESERVED_LABEL_DEFINITION_SEEDS`:

```python
            "value_criticality": {
                "Low": "warn",
                "Medium": "warn",
                "High": "error",
                "Critical": "error",
            },
```

- [ ] **Step 6: Write the failing test for `resolve_criticality` reading stored config**

```python
# app/tests/unit/backend/test_registry_models.py
from unittest.mock import create_autospec
from databricks_labs_dqx_app.backend.registry_models import resolve_criticality
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


def test_resolve_criticality_reads_admin_edited_mapping():
    svc = create_autospec(AppSettingsService, instance=True)
    svc.get_setting.return_value = (
        '[{"key": "severity", "values": ["Low", "Critical"], '
        '"value_criticality": {"Low": "error", "Critical": "warn"}}]'
    )
    # Admin flipped the default mapping — resolve_criticality must honor it.
    assert resolve_criticality("Low", svc) == "error"
    assert resolve_criticality("Critical", svc) == "warn"


def test_resolve_criticality_falls_back_to_default_for_unmapped_value():
    svc = create_autospec(AppSettingsService, instance=True)
    svc.get_setting.return_value = "[]"
    assert resolve_criticality("SomeCustomValue", svc) == "warn"
    assert resolve_criticality(None, svc) == "warn"
```

- [ ] **Step 7: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest tests/unit/backend/test_registry_models.py -v`
Expected: FAIL — `resolve_criticality` doesn't accept a second argument yet / still uses the hardcoded dict.

- [ ] **Step 8: Update `resolve_criticality` to read stored config**

In `registry_models.py`, replace the hardcoded-dict lookup:

```python
DEFAULT_CRITICALITY = "warn"

_SEVERITY_LABEL_KEY = "severity"


def resolve_criticality(severity: str | None, app_settings_service: "AppSettingsService") -> str:
    """Map a registry ``severity`` tag value to a DQX ``criticality`` value.

    Reads the admin-editable ``value_criticality`` map on the ``severity``
    label definition (falls back to :data:`DEFAULT_CRITICALITY` if the
    definition, or this specific value within it, isn't present — e.g. a
    custom severity value with no explicit mapping).
    """
    if severity is None:
        return DEFAULT_CRITICALITY
    definitions = app_settings_service.get_label_definitions()
    for definition in definitions:
        if definition.get("key") == _SEVERITY_LABEL_KEY:
            mapping = definition.get("value_criticality") or {}
            return mapping.get(severity, DEFAULT_CRITICALITY)
    return DEFAULT_CRITICALITY
```

Add a small `AppSettingsService.get_label_definitions() -> list[dict]` helper if one doesn't already exist (it likely already has the parsing logic inline in `seed_reserved_label_definitions_if_absent` — extract that JSON-parse-with-fallback logic into a reusable method both call).

Update the one call site in `materializer.py:273-277` to pass the service through:

```python
    check_dict: dict[str, Any] = {
        "criticality": resolve_criticality(effective_severity, self._app_settings_service),
        "check": check_inner,
        "user_metadata": user_metadata,
    }
```

(Check `MaterializerService.__init__` for how it already holds/injects `AppSettingsService` — it must, since it needs it for other settings; wire this through the constructor if not already available.)

- [ ] **Step 9: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest tests/unit/backend/test_registry_models.py -v`
Expected: PASS

- [ ] **Step 10: Run full backend unit suite + regen API**

Run: `cd app && .venv/bin/pytest tests/unit -q && make -C .. app-regen-api`
Expected: all green; `lib/api.ts` regenerated with the new `value_criticality` field.

- [ ] **Step 11: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/routes/v1/config.py \
        app/src/databricks_labs_dqx_app/backend/services/app_settings_service.py \
        app/src/databricks_labs_dqx_app/backend/registry_models.py \
        app/src/databricks_labs_dqx_app/backend/services/materializer.py \
        app/src/databricks_labs_dqx_app/ui/lib/api.ts \
        app/tests/unit/backend/test_registry_models.py \
        app/tests/unit/backend/routes/test_config.py
git commit -m "Make severity->criticality mapping admin-editable via label definitions"
```

---

### Task 2: Admin UI for severity→criticality mapping

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx` (severity editor section)
- Modify: locale files under `app/src/databricks_labs_dqx_app/ui/locales/` (all 4 locales — find via `grep -rl "severity" app/src/databricks_labs_dqx_app/ui/locales/*/`)
- Test: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.test.tsx` (extend existing, or find the actual test file for this route)

**Interfaces:**
- Consumes: `LabelDefinition.value_criticality` (Task 1)
- Produces: nothing consumed by later tasks (leaf UI feature)

- [ ] **Step 1: Read the existing severity editor section in `config.tsx` to find the per-value row component** (it renders the color picker per value today — find that JSX block, likely a `.map` over `definition.values` rendering a color `<input type="color">` or shadcn color-swatch component per value).

- [ ] **Step 2: Write the failing component test**

```tsx
// config.test.tsx (add near existing severity-editor tests)
it("lets an admin change a severity's DQX criticality mapping", async () => {
  const user = userEvent.setup();
  render(<ConfigPage />, { wrapper: TestProviders });

  const criticalRow = await screen.findByText("Critical");
  const criticalitySelect = within(criticalRow.closest("[data-testid='severity-row']")!).getByRole("combobox", {
    name: /criticality/i,
  });
  await user.selectOptions(criticalitySelect, "warn");

  await user.click(screen.getByRole("button", { name: /save/i }));

  expect(mockSaveLabelDefinitions).toHaveBeenCalledWith(
    expect.objectContaining({
      definitions: expect.arrayContaining([
        expect.objectContaining({
          key: "severity",
          value_criticality: expect.objectContaining({ Critical: "warn" }),
        }),
      ]),
    }),
  );
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app && bun test ui/routes/_sidebar/config.test.tsx`
Expected: FAIL — no criticality select exists yet.

- [ ] **Step 4: Add the criticality dropdown to the severity row**

Add a `data-testid="severity-row"` wrapper (if not present) and, next to the color picker, a shadcn `Select` bound to `value_criticality[value]`:

```tsx
<Select
  value={definition.value_criticality?.[value] ?? "warn"}
  onValueChange={(next) =>
    updateValueCriticality(definition.key, value, next as "warn" | "error")
  }
>
  <SelectTrigger aria-label={t("config.severity.criticalityLabel")} className="w-28">
    <SelectValue />
  </SelectTrigger>
  <SelectContent>
    <SelectItem value="warn">{t("config.severity.criticalityWarn")}</SelectItem>
    <SelectItem value="error">{t("config.severity.criticalityError")}</SelectItem>
  </SelectContent>
</Select>
```

Add the `updateValueCriticality` handler next to the existing `updateValueColor`-style handler already in the file, following the same immer/setState pattern.

- [ ] **Step 5: Add the new translation keys to all 4 locale files**

Add `config.severity.criticalityLabel`, `config.severity.criticalityWarn`, `config.severity.criticalityError` to every locale JSON under `app/src/databricks_labs_dqx_app/ui/locales/*/` (find the exact directory structure via `ls app/src/databricks_labs_dqx_app/ui/locales/`) with translated values for non-English locales — do not leave them English-only.

- [ ] **Step 6: Run test to verify it passes**

Run: `cd app && bun test ui/routes/_sidebar/config.test.tsx`
Expected: PASS

- [ ] **Step 7: Run i18n check and full frontend check**

Run: `cd app && make app-check` (per `AGENTS.md`, `K=i18n` per project convention — check `Makefile`/`app/Makefile` for the exact i18n-completeness target name and run it)
Expected: no missing-key errors across locales.

- [ ] **Step 8: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx \
        app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.test.tsx \
        app/src/databricks_labs_dqx_app/ui/locales
git commit -m "Add admin UI for editing severity->DQX criticality mapping"
```

---

### Task 3: Score computation service + table-level score endpoint

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/services/score_service.py`
- Create: `app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/__init__.py` (register the new router)
- Modify: `app/src/databricks_labs_dqx_app/backend/models.py` (new `TableScoreOut` model)
- Test: `app/tests/unit/backend/services/test_score_service.py`
- Test: `app/tests/unit/backend/routes/test_dq_score.py`

**Interfaces:**
- Produces: `ScoreService.compute_table_score(check_metrics: list[CheckMetricBreakdown], input_row_count: int) -> float | None` — pure function, no I/O, easily unit-testable
- Produces: `TableScoreOut(source_table_fqn: str, score: float | None, latest_run_id: str | None, total_tests: int, failed_tests: int)` Pydantic model
- Produces: `GET /api/v1/dq-score/table/{table_fqn:path}` endpoint
- Consumes: `MetricSnapshotOut`/`CheckMetricBreakdown` (existing, from `metrics.py`/`models.py`), `get_user_catalog_names` (existing OBO dependency in `dependencies.py`)

- [ ] **Step 1: Write the failing unit test for the pure score formula**

```python
# app/tests/unit/backend/services/test_score_service.py
from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown
from databricks_labs_dqx_app.backend.services.score_service import ScoreService


def test_compute_table_score_faithful_to_row_weighted_formula():
    # 100 rows, rule A fails 10, rule B fails 30 -> total failed_tests=40, total_tests=200
    check_metrics = [
        CheckMetricBreakdown(check_name="rule_a", error_count=10, warning_count=0),
        CheckMetricBreakdown(check_name="rule_b", error_count=20, warning_count=10),
    ]
    score = ScoreService.compute_table_score(check_metrics, input_row_count=100)
    assert score == pytest.approx(1 - (40 / 200))


def test_compute_table_score_returns_none_when_no_rows():
    score = ScoreService.compute_table_score([], input_row_count=0)
    assert score is None


def test_compute_table_score_perfect_when_no_failures():
    check_metrics = [CheckMetricBreakdown(check_name="rule_a", error_count=0, warning_count=0)]
    assert ScoreService.compute_table_score(check_metrics, input_row_count=50) == 1.0
```

(Add `import pytest` at top.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest tests/unit/backend/services/test_score_service.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement `ScoreService`**

```python
# app/src/databricks_labs_dqx_app/backend/services/score_service.py
"""Computes DQ scores from the existing dq_metrics data.

Score formula (row-weighted, faithful port of dqlake's approach, minus
per-rule `filter` scoping — an accepted approximation, see
docs/superpowers/specs/2026-07-10-dq-score-results-design.md §2):

    score = 1 - (sum(failed_tests) / sum(total_tests))

where, per rule in a run: total_tests = input_row_count (table-wide,
not filter-scoped), failed_tests = error_count + warning_count for
that rule.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown


class ScoreService:
    @staticmethod
    def compute_table_score(
        check_metrics: list[CheckMetricBreakdown],
        input_row_count: int,
    ) -> float | None:
        if input_row_count <= 0 or not check_metrics:
            return None
        total_tests = input_row_count * len(check_metrics)
        failed_tests = sum(m.error_count + m.warning_count for m in check_metrics)
        return 1.0 - (failed_tests / total_tests)

    @staticmethod
    def compute_product_score(table_scores: list[float]) -> float | None:
        """Unweighted mean of member tables' latest scores."""
        if not table_scores:
            return None
        return sum(table_scores) / len(table_scores)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest tests/unit/backend/services/test_score_service.py -v`
Expected: PASS

- [ ] **Step 5: Add `TableScoreOut` to `models.py`**

```python
class TableScoreOut(BaseModel):
    source_table_fqn: str
    score: float | None = None
    latest_run_id: str | None = None
    total_tests: int = 0
    failed_tests: int = 0
```

- [ ] **Step 6: Write the failing route test**

```python
# app/tests/unit/backend/routes/test_dq_score.py
from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def test_get_table_score_returns_403_for_inaccessible_catalog(client, monkeypatch):
    # Follows the same pattern as existing test_metrics.py catalog-gating tests —
    # find and mirror it exactly (dependency override for get_user_catalog_names
    # returning a frozenset that excludes the requested table's catalog).
    ...


def test_get_table_score_computes_from_latest_run(client, monkeypatch):
    sql = create_autospec(SqlExecutor, instance=True)
    sql.query_dicts.return_value = [
        {"run_id": "r1", "metric_name": "input_row_count", "metric_value": "100"},
        {
            "run_id": "r1",
            "metric_name": "check_metrics",
            "metric_value": '[{"check_name": "rule_a", "error_count": 10, "warning_count": 0}]',
        },
    ]
    # override get_sp_sql_executor + get_user_catalog_names dependencies to return
    # this mock and an accessible-catalog set, following the existing test_metrics.py
    # dependency-override pattern exactly.
    ...
```

Read `app/tests/unit/backend/routes/test_metrics.py` first (it must exist, since `metrics.py` has existing catalog-gating tests) and copy its exact dependency-override / test-client fixture pattern rather than inventing a new one — this ensures consistency and that the `403` and success paths are asserted the same way.

- [ ] **Step 7: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest tests/unit/backend/routes/test_dq_score.py -v`
Expected: FAIL — router doesn't exist.

- [ ] **Step 8: Implement the router**

```python
# app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py
"""DQ score read API.

Computes table-level DQ scores from the existing dq_metrics table —
no changes to the frozen metrics-emission pipeline. Aggregate scores
are low-sensitivity (counts only, no row values), so access is gated
at catalog granularity via the same get_user_catalog_names OBO pattern
already used by metrics.py, not a full per-table live check (that
stricter check is reserved for the row-level sample endpoint in
quarantine_samples.py, since that returns actual row values).
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_sp_sql_executor,
    get_user_catalog_names,
)
from databricks_labs_dqx_app.backend.models import TableScoreOut
from databricks_labs_dqx_app.backend.routes.v1.metrics import _catalog_of, _parse_check_metrics, _safe_int
from databricks_labs_dqx_app.backend.services.score_service import ScoreService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/table/{table_fqn:path}", operation_id="getTableScore")
def get_table_score(
    table_fqn: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> TableScoreOut:
    try:
        validate_fqn(table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if _catalog_of(table_fqn) not in user_catalogs:
        raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")

    metrics_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    e_fqn = escape_sql_string(table_fqn)
    stmt = (
        f"WITH latest_run AS ("
        f"  SELECT run_id FROM {metrics_table} WHERE input_location = '{e_fqn}' "  # noqa: S608
        f"  ORDER BY run_time DESC LIMIT 1"
        f") "
        f"SELECT m.run_id, m.metric_name, m.metric_value FROM {metrics_table} m "
        f"JOIN latest_run lr ON lr.run_id = m.run_id"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception as exc:
        logger.exception("Failed to compute score for %s", table_fqn)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not rows:
        return TableScoreOut(source_table_fqn=table_fqn)

    metrics = {r["metric_name"]: r["metric_value"] for r in rows}
    input_row_count = _safe_int(metrics.get("input_row_count")) or 0
    check_metrics = _parse_check_metrics(metrics.get("check_metrics")) or []
    score = ScoreService.compute_table_score(check_metrics, input_row_count)
    failed = sum(m.error_count + m.warning_count for m in check_metrics)

    return TableScoreOut(
        source_table_fqn=table_fqn,
        score=round(score, 4) if score is not None else None,
        latest_run_id=rows[0]["run_id"],
        total_tests=input_row_count * len(check_metrics),
        failed_tests=failed,
    )
```

Check `metrics.py` for the exact names/visibility of `_parse_check_metrics`/`_safe_int`/`_catalog_of` — if they're module-private (leading underscore) and not meant for cross-module import, move them to a shared `metrics_utils.py` instead and update `metrics.py` to import from there too, rather than reaching into another route module's private helpers.

- [ ] **Step 9: Register the router**

In `routes/v1/__init__.py`, add:

```python
from .dq_score import router as dq_score_router
...
v1_router.include_router(dq_score_router, prefix="/dq-score", tags=["dq-score"])
```

- [ ] **Step 10: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest tests/unit/backend/routes/test_dq_score.py -v`
Expected: PASS

- [ ] **Step 11: Regen API and run full backend suite**

Run: `cd app && make -C .. app-regen-api && .venv/bin/pytest tests/unit -q`
Expected: all green.

- [ ] **Step 12: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/score_service.py \
        app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py \
        app/src/databricks_labs_dqx_app/backend/routes/v1/__init__.py \
        app/src/databricks_labs_dqx_app/backend/models.py \
        app/src/databricks_labs_dqx_app/ui/lib/api.ts \
        app/tests/unit/backend/services/test_score_service.py \
        app/tests/unit/backend/routes/test_dq_score.py
git commit -m "Add DQ score computation service and table-level score endpoint"
```

---

### Task 4: Data product score endpoint

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/models.py` (`ProductScoreOut`)
- Test: `app/tests/unit/backend/routes/test_dq_score.py` (extend)

**Interfaces:**
- Consumes: `ScoreService.compute_product_score` (Task 3), existing `DataProductService`/whatever service lists a product's member table FQNs — read `app/src/databricks_labs_dqx_app/backend/services/data_product_service.py` first to find the exact method name (e.g. `list_member_tables(product_id) -> list[str]`) and use it, don't reinvent.
- Produces: `GET /api/v1/dq-score/product/{product_id}` → `ProductScoreOut(product_id, score, member_table_scores: list[TableScoreOut])`

- [ ] **Step 1: Read `data_product_service.py` to find the exact member-table-listing method signature before writing anything.**

- [ ] **Step 2: Write the failing test**

```python
def test_get_product_score_averages_member_tables(client, monkeypatch):
    # Mock the member-table listing to return 2 tables, mock get_table_score's
    # underlying SQL query to return different scores for each, assert the
    # response is the unweighted mean. Follow the exact fixture pattern from
    # Task 3's test_dq_score.py tests.
    ...
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest tests/unit/backend/routes/test_dq_score.py -k product -v`
Expected: FAIL

- [ ] **Step 4: Implement, reusing the per-table scoring logic from Task 3 as a shared helper**

Refactor Task 3's inline scoring logic in `get_table_score` into a reusable `_compute_score_for_table(table_fqn, sql, app_conf) -> TableScoreOut` module-level function, call it from both endpoints:

```python
@router.get("/product/{product_id}", operation_id="getProductScore")
def get_product_score(
    product_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    data_products: Annotated["DataProductService", Depends(get_data_product_service)],
) -> ProductScoreOut:
    member_tables = data_products.list_member_table_fqns(product_id)
    accessible = [t for t in member_tables if _catalog_of(t) in user_catalogs]

    table_scores = [_compute_score_for_table(t, sql, app_conf) for t in accessible]
    scored = [s.score for s in table_scores if s.score is not None]
    return ProductScoreOut(
        product_id=product_id,
        score=round(ScoreService.compute_product_score(scored), 4) if scored else None,
        member_table_scores=table_scores,
    )
```

(Import `get_data_product_service` from wherever it's already defined — check `dependencies.py` or `data_product_service.py` itself for the existing FastAPI dependency provider function.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest tests/unit/backend/routes/test_dq_score.py -k product -v`
Expected: PASS

- [ ] **Step 6: Regen API, run full suite, commit**

```bash
cd app && make -C .. app-regen-api && .venv/bin/pytest tests/unit -q
git add app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py \
        app/src/databricks_labs_dqx_app/backend/models.py \
        app/src/databricks_labs_dqx_app/ui/lib/api.ts \
        app/tests/unit/backend/routes/test_dq_score.py
git commit -m "Add data product DQ score endpoint (unweighted mean of member tables)"
```

---

### Task 5: Rule-level aggregate endpoint + "not applied anywhere" signal

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/apply_rules_service.py` (or wherever `ApplyRulesService` lives — confirmed name from research, exact file may differ; grep for `class ApplyRulesService`)
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/models.py` (`RuleScoreOut`)
- Test: extend `test_dq_score.py`
- Test: extend the existing `ApplyRulesService` test file

**Interfaces:**
- Produces: `ApplyRulesService.list_bindings_for_rule(rule_id: str) -> list[AppliedRuleOut]` (new reverse-lookup method — none exists today per research)
- Produces: `GET /api/v1/dq-score/rule/{rule_id}` → `RuleScoreOut(rule_id, applied_to_count: int, overall_score: float | None, per_table: list[TableScoreOut])`

- [ ] **Step 1: Find the exact `ApplyRulesService` class and its existing `list_applied`/`get_applied` methods, and the underlying `dq_applied_rules` table's columns (needs `rule_id`, `binding_id`).**

- [ ] **Step 2: Write the failing test for the reverse lookup**

```python
def test_list_bindings_for_rule_returns_empty_when_unapplied(apply_rules_service):
    assert apply_rules_service.list_bindings_for_rule("rule-not-applied-anywhere") == []


def test_list_bindings_for_rule_returns_applications(apply_rules_service, seeded_applied_rule):
    result = apply_rules_service.list_bindings_for_rule(seeded_applied_rule.rule_id)
    assert len(result) == 1
    assert result[0].id == seeded_applied_rule.id
```

(Use whatever fixture pattern the existing `ApplyRulesService` test file already uses for seeding — Lakebase test session or in-memory equivalent; do not invent a new one.)

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest -k list_bindings_for_rule -v`
Expected: FAIL — method doesn't exist.

- [ ] **Step 4: Implement `list_bindings_for_rule`**

Add a query mirroring the existing `list_applied(binding_id)` method but filtering on `rule_id` instead of `binding_id`:

```python
    def list_bindings_for_rule(self, rule_id: str) -> list[AppliedRuleOut]:
        """Every monitored-table application of *rule_id*, across all bindings."""
        rows = self._session.execute(
            select(AppliedRuleRow).where(AppliedRuleRow.rule_id == rule_id)
        ).scalars().all()
        return [self._to_out(row) for row in rows]
```

(Match whatever ORM/session pattern `list_applied` actually uses — SQLAlchemy Core vs raw SQL vs the app's own query builder; read that method's real body first and mirror it exactly, including its existing `_to_out` row-mapping helper.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest -k list_bindings_for_rule -v`
Expected: PASS

- [ ] **Step 6: Write the failing route test**

```python
def test_get_rule_score_reports_zero_applications(client, monkeypatch):
    # Mock list_bindings_for_rule to return [] -> expect applied_to_count=0,
    # overall_score=None, per_table=[]
    ...

def test_get_rule_score_aggregates_across_tables(client, monkeypatch):
    # Mock list_bindings_for_rule to return 2 bindings with different table_fqns,
    # mock per-table score computation, assert overall_score is the unweighted mean
    # and per_table has 2 entries.
    ...
```

- [ ] **Step 7: Run test to verify it fails**

- [ ] **Step 8: Implement the endpoint**

```python
@router.get("/rule/{rule_id}", operation_id="getRuleScore")
def get_rule_score(
    rule_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    apply_rules: Annotated["ApplyRulesService", Depends(get_apply_rules_service)],
    monitored_tables: Annotated["MonitoredTableService", Depends(get_monitored_table_service)],
) -> RuleScoreOut:
    applications = apply_rules.list_bindings_for_rule(rule_id)
    table_fqns = [monitored_tables.get(a.binding_id).table_fqn for a in applications]
    accessible = [t for t in table_fqns if _catalog_of(t) in user_catalogs]

    per_table = [_compute_score_for_table(t, sql, app_conf) for t in accessible]
    scored = [s.score for s in per_table if s.score is not None]

    return RuleScoreOut(
        rule_id=rule_id,
        applied_to_count=len(applications),
        overall_score=round(ScoreService.compute_product_score(scored), 4) if scored else None,
        per_table=per_table,
    )
```

`applied_to_count` uses the *total* application count (not just the accessible-to-this-user subset) so the frontend's "not applied anywhere" disabled state (Task 8) is correct regardless of the viewer's own permissions — a rule genuinely applied to 3 tables the current viewer can't see is still "applied," just with an empty/filtered `per_table`.

- [ ] **Step 9: Run test to verify it passes**

- [ ] **Step 10: Regen API, run full suite, commit**

```bash
cd app && make -C .. app-regen-api && .venv/bin/pytest tests/unit -q
git add -A -- app/src/databricks_labs_dqx_app/backend app/src/databricks_labs_dqx_app/ui/lib/api.ts app/tests
git commit -m "Add rule-level DQ score aggregation endpoint"
```

---

### Task 6: Global results endpoint

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/models.py` (`GlobalScoreOut`)
- Test: extend `test_dq_score.py`

**Interfaces:**
- Consumes: existing `get_metrics_summary`-equivalent query logic (Task 3's `metrics.py` reference), `get_user_catalog_names`
- Produces: `GET /api/v1/dq-score/global` → `GlobalScoreOut(overall_score: float | None, table_count: int, tables: list[TableScoreOut])`

- [ ] **Step 1: Write the failing test**

```python
def test_get_global_score_filters_to_accessible_catalogs(client, monkeypatch):
    # 3 tables in dq_metrics across 2 catalogs; user_catalogs only includes one ->
    # assert `tables` in the response only contains that catalog's table(s), and
    # overall_score is the unweighted mean of just those.
    ...
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement, reusing `get_metrics_summary`'s existing latest-run-per-table SQL as the base query**

```python
@router.get("/global", operation_id="getGlobalScore")
def get_global_score(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> GlobalScoreOut:
    metrics_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    stmt = (
        f"WITH latest AS ("
        f"  SELECT input_location, run_id, "
        f"         ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC) AS rn "
        f"  FROM {metrics_table}"  # noqa: S608
        f") SELECT DISTINCT input_location, run_id FROM latest WHERE rn = 1"
    )
    try:
        latest_runs = sql.query_dicts(stmt)
    except Exception as exc:
        logger.exception("Failed to load global score")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    accessible = [r for r in latest_runs if _catalog_of(r["input_location"]) in user_catalogs]
    tables = [_compute_score_for_table(r["input_location"], sql, app_conf) for r in accessible]
    scored = [t.score for t in tables if t.score is not None]

    return GlobalScoreOut(
        overall_score=round(ScoreService.compute_product_score(scored), 4) if scored else None,
        table_count=len(tables),
        tables=tables,
    )
```

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Regen API, run full suite, commit**

```bash
cd app && make -C .. app-regen-api && .venv/bin/pytest tests/unit -q
git add -A -- app/src/databricks_labs_dqx_app/backend app/src/databricks_labs_dqx_app/ui/lib/api.ts app/tests
git commit -m "Add global cross-table DQ score endpoint"
```

---

### Task 7: Row-level failing-sample endpoint (OBO self-check + suppression)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/services/quarantine_sample_service.py`
- Create: `app/src/databricks_labs_dqx_app/backend/routes/v1/quarantine_samples.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/__init__.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/models.py` (`FailingRecordOut`, `FailingRecordsOut`)
- Test: `app/tests/unit/backend/services/test_quarantine_sample_service.py`
- Test: `app/tests/unit/backend/routes/test_quarantine_samples.py`

**Interfaces:**
- Produces: `QuarantineSampleService.has_fine_grained_access_control(obo_ws: WorkspaceClient, table_fqn: str) -> bool`
- Produces: `QuarantineSampleService.user_can_select(obo_ws: WorkspaceClient, table_fqn: str) -> bool`
- Produces: `FailingRecordOut(record_key: str, row_values: dict[str, str | None], failed_columns: list[str], failures: list[dict])`
- Produces: `GET /api/v1/quarantine-samples/{table_fqn:path}?limit=50`

- [ ] **Step 1: Write the failing unit tests for the two permission-check helpers**

```python
# app/tests/unit/backend/services/test_quarantine_sample_service.py
from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, TableRowFilter

from databricks_labs_dqx_app.backend.services.quarantine_sample_service import QuarantineSampleService


def test_has_fine_grained_access_control_true_when_row_filter_present():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.tables.get.return_value = TableInfo(row_filter=TableRowFilter(function_name="mask_fn"))
    assert QuarantineSampleService.has_fine_grained_access_control(ws, "cat.schema.tbl") is True


def test_has_fine_grained_access_control_true_when_column_mask_present():
    ws = create_autospec(WorkspaceClient, instance=True)
    masked_col = MagicMock(mask=MagicMock())
    ws.tables.get.return_value = TableInfo(row_filter=None, columns=[masked_col])
    assert QuarantineSampleService.has_fine_grained_access_control(ws, "cat.schema.tbl") is True


def test_has_fine_grained_access_control_false_when_neither_present():
    ws = create_autospec(WorkspaceClient, instance=True)
    plain_col = MagicMock(mask=None)
    ws.tables.get.return_value = TableInfo(row_filter=None, columns=[plain_col])
    assert QuarantineSampleService.has_fine_grained_access_control(ws, "cat.schema.tbl") is False


def test_user_can_select_true_when_obo_query_succeeds():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.statement_execution.execute_statement.return_value = MagicMock()
    assert QuarantineSampleService.user_can_select(ws, "cat.schema.tbl") is True


def test_user_can_select_false_when_obo_query_raises():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.statement_execution.execute_statement.side_effect = Exception("PERMISSION_DENIED")
    assert QuarantineSampleService.user_can_select(ws, "cat.schema.tbl") is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && .venv/bin/pytest tests/unit/backend/services/test_quarantine_sample_service.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement `QuarantineSampleService`**

```python
# app/src/databricks_labs_dqx_app/backend/services/quarantine_sample_service.py
"""Row-level failing-sample access, gated by a live per-request OBO check.

No new UC grants anywhere: the shared dq_quarantine_records table is
always read via the app's service principal (it already owns that
table). Before returning any row, the requesting user's own OBO token
self-checks their current SELECT on the *source* table — a check that
needs no elevated privilege, since checking your own access never
requires MANAGE/ownership. See docs/superpowers/specs/
2026-07-10-dq-score-results-design.md §3.
"""

from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logger = logging.getLogger(__name__)


class QuarantineSampleService:
    @staticmethod
    def has_fine_grained_access_control(obo_ws: WorkspaceClient, table_fqn: str) -> bool:
        """True if the source table has a row filter or any column mask."""
        table_info = obo_ws.tables.get(table_fqn)
        if table_info.row_filter is not None:
            return True
        return any(col.mask is not None for col in (table_info.columns or []))

    @staticmethod
    def user_can_select(obo_ws: WorkspaceClient, table_fqn: str) -> bool:
        """Live self-check: can the calling user currently SELECT this table.

        Uses a zero-row query (no data returned, cheap) via the user's own
        OBO-scoped SQL execution — never an elevated/service-principal call.
        """
        try:
            statement = obo_ws.statement_execution.execute_statement(
                warehouse_id=obo_ws.config.warehouse_id,
                statement=f"SELECT 1 FROM {table_fqn} LIMIT 0",  # noqa: S608 -- table_fqn is UC-validated, not user text
            )
            return statement.status is None or statement.status.state != StatementState.FAILED
        except Exception:
            logger.info("OBO self-check denied for %s", table_fqn)
            return False
```

Check `dependencies.py`/existing OBO SQL callers (`table_data_service.py`) for the established way to get a warehouse ID and issue a statement — mirror that exact mechanism instead of hardcoding `obo_ws.config.warehouse_id` if a different helper already exists (e.g. `get_obo_sql_executor` might already wrap this).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app && .venv/bin/pytest tests/unit/backend/services/test_quarantine_sample_service.py -v`
Expected: PASS

- [ ] **Step 5: Write the failing route test**

```python
def test_get_quarantine_sample_returns_empty_when_user_lacks_access(client, monkeypatch):
    # Override QuarantineSampleService.user_can_select to return False ->
    # expect 200 with an empty records list (never a 403 that would confirm
    # the table's existence to an unauthorized caller), or a documented 403 —
    # pick one behavior and assert it; recommend empty-200 to avoid existence
    # leakage, matching the design doc's stated goal.
    ...

def test_get_quarantine_sample_suppressed_when_row_filter_present(client, monkeypatch):
    # Override has_fine_grained_access_control to return True -> expect the
    # response's `suppressed: true` flag and no row_values.
    ...

def test_get_quarantine_sample_returns_rows_with_per_cell_failures(client, monkeypatch):
    # Mock user_can_select True, has_fine_grained_access_control False, and
    # sql.query_dicts to return quarantine rows with _errors/_warnings-derived
    # columns -> assert the transform into FailingRecordOut.failed_columns.
    ...
```

- [ ] **Step 6: Run test to verify it fails**

- [ ] **Step 7: Implement the endpoint**

```python
# app/src/databricks_labs_dqx_app/backend/routes/v1/quarantine_samples.py
"""Row-level failing-sample read API — see quarantine_sample_service.py
for the permission model this enforces."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_conf, get_obo_ws, get_sp_sql_executor
from databricks_labs_dqx_app.backend.models import FailingRecordOut, FailingRecordsOut
from databricks_labs_dqx_app.backend.services.quarantine_sample_service import QuarantineSampleService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/{table_fqn:path}", operation_id="getQuarantineSample")
def get_quarantine_sample(
    table_fqn: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    limit: int = Query(50, ge=1, le=200),
) -> FailingRecordsOut:
    try:
        validate_fqn(table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not QuarantineSampleService.user_can_select(obo_ws, table_fqn):
        # Empty, not 403 -- never confirm/deny table existence to a caller
        # without access to it.
        return FailingRecordsOut(source_table_fqn=table_fqn, records=[], suppressed=False)

    if QuarantineSampleService.has_fine_grained_access_control(obo_ws, table_fqn):
        return FailingRecordsOut(source_table_fqn=table_fqn, records=[], suppressed=True)

    quarantine_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    e_fqn = escape_sql_string(table_fqn)
    stmt = (
        f"SELECT * FROM {quarantine_table} WHERE source_table_fqn = '{e_fqn}' "  # noqa: S608
        f"ORDER BY created_at DESC LIMIT {limit}"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception as exc:
        logger.exception("Failed to load quarantine sample for %s", table_fqn)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return FailingRecordsOut(
        source_table_fqn=table_fqn,
        records=[_to_failing_record(r) for r in rows],
        suppressed=False,
    )


def _to_failing_record(row: dict) -> FailingRecordOut:
    """Transform a dq_quarantine_records row into the UI's failure-highlight shape.

    `dq_quarantine_records` stores the DQX-native `_errors`/`_warnings`
    struct-array columns (see `src/databricks/labs/dqx/schema/dq_result_schema.py`)
    alongside the quarantined row's own source columns. Databricks SQL
    returns struct/array columns as JSON-encoded strings via `query_dicts`
    (consistent with how `metrics.py`'s `check_metrics` column is parsed),
    so `_errors`/`_warnings` arrive as JSON text here too.
    """
    import json

    reserved = {
        "quarantine_id", "run_id", "source_table_fqn", "requesting_user",
        "created_at", "_errors", "_warnings",
    }
    row_values = {k: (str(v) if v is not None else None) for k, v in row.items() if k not in reserved}

    failures: list[dict] = []
    for col_name in ("_errors", "_warnings"):
        raw = row.get(col_name)
        if not raw:
            continue
        try:
            entries = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            continue
        for entry in entries or []:
            failures.append(
                {
                    "rule_name": entry.get("name"),
                    "message": entry.get("message"),
                    "columns": entry.get("columns") or [],
                }
            )

    failed_columns = sorted({c for f in failures for c in f["columns"]})
    return FailingRecordOut(
        record_key=str(row.get("quarantine_id")),
        row_values=row_values,
        failed_columns=failed_columns,
        failures=failures,
    )
```

Before running Step 8, read `quarantine.py`'s `_query_quarantine` SELECT list and `dq_quarantine_records`' actual column list (not fully captured in research — only `quarantine_id, run_id, source_table_fqn, requesting_user, ...` was confirmed, the rest was elided) to confirm the `_errors`/`_warnings` column names and JSON-vs-native-array return shape match this implementation exactly; adjust `reserved`/parsing above if the real schema differs (e.g. if row data is nested under a single JSON column rather than spread as top-level columns).

- [ ] **Step 8: Run test to verify it passes** (after filling in `_to_failing_record` per the real quarantine row shape)

- [ ] **Step 9: Register the router, regen API, run suite, commit**

```python
# routes/v1/__init__.py
from .quarantine_samples import router as quarantine_samples_router
...
v1_router.include_router(quarantine_samples_router, prefix="/quarantine-samples", tags=["quarantine-samples"])
```

```bash
cd app && make -C .. app-regen-api && .venv/bin/pytest tests/unit -q
git add -A -- app/src/databricks_labs_dqx_app/backend app/src/databricks_labs_dqx_app/ui/lib/api.ts app/tests
git commit -m "Add OBO-gated row-level failing-sample endpoint with fine-grained-control suppression"
```

---

### Task 8: Shared frontend components — ScoreBox and FailingRecordsTable

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/components/results/ScoreBox.tsx`
- Create: `app/src/databricks_labs_dqx_app/ui/components/results/FailingRecordsTable.tsx`
- Test: `app/src/databricks_labs_dqx_app/ui/components/results/ScoreBox.test.tsx`
- Test: `app/src/databricks_labs_dqx_app/ui/components/results/FailingRecordsTable.test.tsx`

**Interfaces:**
- Produces: `<ScoreBox score={number | null} label={string} totalTests={number} failedTests={number} />`
- Produces: `<FailingRecordsTable records={FailingRecordOut[]} suppressed={boolean} />`
- Consumes: `TableScoreOut`/`FailingRecordOut` types (generated by Orval from Tasks 3/7)

- [ ] **Step 1: Write the failing test for ScoreBox**

```tsx
// ScoreBox.test.tsx
it("renders the score as a percentage with a color keyed to severity band", () => {
  render(<ScoreBox score={0.942} label="Table score" totalTests={1000} failedTests={58} />);
  expect(screen.getByText("94.2%")).toBeInTheDocument();
  expect(screen.getByText("58 of 1000 tests failed")).toBeInTheDocument();
});

it("renders a placeholder when score is null (no runs yet)", () => {
  render(<ScoreBox score={null} label="Table score" totalTests={0} failedTests={0} />);
  expect(screen.getByText(/no runs yet/i)).toBeInTheDocument();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && bun test ui/components/results/ScoreBox.test.tsx`
Expected: FAIL — component doesn't exist.

- [ ] **Step 3: Implement `ScoreBox`**

```tsx
// app/src/databricks_labs_dqx_app/ui/components/results/ScoreBox.tsx
import { Card, CardContent } from "@/components/ui/card";
import { useTranslation } from "react-i18next";

interface ScoreBoxProps {
  score: number | null;
  label: string;
  totalTests: number;
  failedTests: number;
}

function bandColor(score: number): string {
  if (score >= 0.95) return "text-emerald-600 dark:text-emerald-400";
  if (score >= 0.8) return "text-amber-600 dark:text-amber-400";
  return "text-red-600 dark:text-red-400";
}

export function ScoreBox({ score, label, totalTests, failedTests }: ScoreBoxProps) {
  const { t } = useTranslation();
  return (
    <Card>
      <CardContent className="flex flex-col items-center gap-1 py-6">
        <span className="text-sm text-muted-foreground">{label}</span>
        {score === null ? (
          <span className="text-lg text-muted-foreground">{t("results.noRunsYet")}</span>
        ) : (
          <>
            <span className={`text-4xl font-semibold ${bandColor(score)}`}>
              {(score * 100).toFixed(1)}%
            </span>
            <span className="text-sm text-muted-foreground">
              {t("results.testsFailedOfTotal", { failed: failedTests, total: totalTests })}
            </span>
          </>
        )}
      </CardContent>
    </Card>
  );
}
```

Add `results.noRunsYet` and `results.testsFailedOfTotal` (with `{{failed}}`/`{{total}}` interpolation) to all 4 locale files.

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Write the failing test for FailingRecordsTable**

```tsx
// FailingRecordsTable.test.tsx
it("highlights failed cells", () => {
  const records = [
    {
      record_key: "1",
      row_values: { id: "42", email: "not-an-email" },
      failed_columns: ["email"],
      failures: [{ rule_name: "is_valid_email", message: "bad format", columns: ["email"] }],
    },
  ];
  render(<FailingRecordsTable records={records} suppressed={false} />);
  const cell = screen.getByText("not-an-email");
  expect(cell.closest("td")).toHaveClass("bg-red-50");
});

it("shows a suppression notice instead of rows when suppressed", () => {
  render(<FailingRecordsTable records={[]} suppressed />);
  expect(screen.getByText(/fine-grained access controls/i)).toBeInTheDocument();
});
```

- [ ] **Step 6: Run test to verify it fails**

- [ ] **Step 7: Implement `FailingRecordsTable`**

```tsx
// app/src/databricks_labs_dqx_app/ui/components/results/FailingRecordsTable.tsx
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useTranslation } from "react-i18next";
import type { FailingRecordOut } from "@/lib/api";

interface FailingRecordsTableProps {
  records: FailingRecordOut[];
  suppressed: boolean;
}

export function FailingRecordsTable({ records, suppressed }: FailingRecordsTableProps) {
  const { t } = useTranslation();
  if (suppressed) {
    return <p className="text-sm text-muted-foreground">{t("results.suppressedFineGrainedControls")}</p>;
  }
  if (records.length === 0) {
    return <p className="text-sm text-muted-foreground">{t("results.noFailingRecords")}</p>;
  }
  const columns = Object.keys(records[0].row_values);
  return (
    <Table>
      <TableHeader>
        <TableRow>
          {columns.map((col) => (
            <TableHead key={col}>{col}</TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {records.map((record) => (
          <TableRow key={record.record_key}>
            {columns.map((col) => (
              <TableCell
                key={col}
                className={record.failed_columns.includes(col) ? "bg-red-50 dark:bg-red-950" : undefined}
                title={
                  record.failed_columns.includes(col)
                    ? record.failures.find((f) => f.columns?.includes(col))?.message
                    : undefined
                }
              >
                {record.row_values[col] ?? "—"}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
```

Add `results.suppressedFineGrainedControls` and `results.noFailingRecords` to all 4 locale files.

- [ ] **Step 8: Run test to verify it passes**

Run: `cd app && bun test ui/components/results/`
Expected: both PASS

- [ ] **Step 9: Run i18n check, commit**

```bash
cd app && make app-check
git add app/src/databricks_labs_dqx_app/ui/components/results app/src/databricks_labs_dqx_app/ui/locales
git commit -m "Add ScoreBox and FailingRecordsTable shared results components"
```

---

### Task 9: Wire real content into the existing monitored-table Results tab

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/monitored-tables.$bindingId.tsx` (`ResultsTab` component)
- Test: extend the existing test file for this route

**Interfaces:**
- Consumes: `ScoreBox`, `FailingRecordsTable` (Task 8), `useGetTableScoreSuspense`, `useGetQuarantineSample` (Orval-generated from Tasks 3/7)

- [ ] **Step 1: Read the current `ResultsTab({ tableFqn, status })` placeholder implementation in full before changing it.**

- [ ] **Step 2: Write the failing test**

```tsx
it("shows the table's DQ score and failing records in the Results tab", async () => {
  mockUseGetTableScore.mockReturnValue({ data: { score: 0.9, total_tests: 100, failed_tests: 10 } });
  mockUseGetQuarantineSample.mockReturnValue({ data: { records: [], suppressed: false } });

  render(<MonitoredTableDetailPage />, { wrapper: TestProviders, initialEntries: ["/monitored-tables/b1?tab=results"] });

  expect(await screen.findByText("90.0%")).toBeInTheDocument();
});
```

- [ ] **Step 3: Run test to verify it fails**

- [ ] **Step 4: Replace the placeholder `ResultsTab` body**

```tsx
function ResultsTab({ tableFqn }: { tableFqn: string }) {
  const { data: score } = useGetTableScoreSuspense(tableFqn);
  const { data: sample } = useGetQuarantineSampleSuspense(tableFqn);
  return (
    <div className="flex flex-col gap-4">
      <ScoreBox
        score={score.score}
        label={t("monitoredTable.results.scoreLabel")}
        totalTests={score.total_tests}
        failedTests={score.failed_tests}
      />
      <FailingRecordsTable records={sample.records} suppressed={sample.suppressed} />
    </div>
  );
}
```

Wrap in the same `QueryErrorResetBoundary`/`ErrorBoundary`/`Suspense` pattern every other tab in this file already uses — copy it exactly from a neighboring tab, don't invent a new error-handling shape.

- [ ] **Step 5: Run test to verify it passes**

- [ ] **Step 6: Add translation keys, run i18n + full frontend check, commit**

```bash
cd app && make app-check
git add app/src/databricks_labs_dqx_app/ui/routes/_sidebar/monitored-tables.\$bindingId.tsx app/src/databricks_labs_dqx_app/ui/locales
git commit -m "Wire DQ score and failing-records data into the monitored table Results tab"
```

---

### Task 10: Data product Results tab

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/components/products/ProductResultsTab.tsx`
- Modify: `app/src/databricks_labs_dqx_app/ui/components/products/ProductTabsShell.tsx` (or wherever the tab list is assembled — read it first)
- Test: `app/src/databricks_labs_dqx_app/ui/components/products/ProductResultsTab.test.tsx`

**Interfaces:**
- Consumes: `ScoreBox` (Task 8), `useGetProductScoreSuspense` (Orval, Task 4)

- [ ] **Step 1: Read `ProductTabsShell.tsx` and one sibling tab component (e.g. `ProductRunsTab.tsx`) in full to copy the exact registration pattern.**

- [ ] **Step 2: Write the failing test**

```tsx
it("shows the product's average score and per-table breakdown", async () => {
  mockUseGetProductScore.mockReturnValue({
    data: { score: 0.88, member_table_scores: [{ source_table_fqn: "c.s.t1", score: 0.9 }, { source_table_fqn: "c.s.t2", score: 0.86 }] },
  });
  render(<ProductResultsTab productId="p1" />, { wrapper: TestProviders });
  expect(await screen.findByText("88.0%")).toBeInTheDocument();
  expect(screen.getByText("c.s.t1")).toBeInTheDocument();
});
```

- [ ] **Step 3: Run test to verify it fails**

- [ ] **Step 4: Implement `ProductResultsTab`**

```tsx
// app/src/databricks_labs_dqx_app/ui/components/products/ProductResultsTab.tsx
import { ScoreBox } from "@/components/results/ScoreBox";
import { useGetProductScoreSuspense } from "@/lib/api";
import { useTranslation } from "react-i18next";

export function ProductResultsTab({ productId }: { productId: string }) {
  const { t } = useTranslation();
  const { data } = useGetProductScoreSuspense(productId);
  return (
    <div className="flex flex-col gap-4">
      <ScoreBox
        score={data.score}
        label={t("productResults.averageScoreLabel")}
        totalTests={data.member_table_scores.reduce((sum, s) => sum + s.total_tests, 0)}
        failedTests={data.member_table_scores.reduce((sum, s) => sum + s.failed_tests, 0)}
      />
      <ul className="flex flex-col gap-2">
        {data.member_table_scores.map((s) => (
          <li key={s.source_table_fqn} className="flex justify-between text-sm">
            <span>{s.source_table_fqn}</span>
            <span>{s.score !== null ? `${(s.score * 100).toFixed(1)}%` : t("results.noRunsYet")}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

- [ ] **Step 5: Register the tab in `ProductTabsShell.tsx`**, mirroring how the existing tabs (`ProductRunsTab`, `ProductHistoryTab`, etc.) are wired in — same `Tabs`/`TabsTrigger`/`TabsContent` structure as Task 9.

- [ ] **Step 6: Run test to verify it passes**

- [ ] **Step 7: Add translation keys, run i18n + full frontend check, commit**

```bash
cd app && make app-check
git add app/src/databricks_labs_dqx_app/ui/components/products app/src/databricks_labs_dqx_app/ui/locales
git commit -m "Add data product Results tab with average score and per-table breakdown"
```

---

### Task 11: Rule-level results page (with disabled/tooltip state)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/registry-rules.$ruleId.results.tsx` (match the existing rule-detail route's naming convention — check `registry-rules.*` files first for the exact pattern)
- Test: sibling `.test.tsx`

**Interfaces:**
- Consumes: `ScoreBox`, `FailingRecordsTable` (Task 8), `useGetRuleScoreSuspense` (Orval, Task 5)

- [ ] **Step 1: Read the existing `registry-rules.$ruleId.*` route file(s) to find the tab/nav structure a rule-detail page already uses, so this "Results" surface is added consistently (either a new tab there, or a new route — follow whichever pattern the codebase already establishes for rule detail sub-pages).**

- [ ] **Step 2: Write the failing test for the disabled state**

```tsx
it("disables the Results view with a tooltip when the rule has zero applications", async () => {
  mockUseGetRuleScore.mockReturnValue({ data: { applied_to_count: 0, overall_score: null, per_table: [] } });
  render(<RuleResultsView ruleId="r1" />, { wrapper: TestProviders });

  const trigger = screen.getByRole("tab", { name: /results/i });
  expect(trigger).toHaveAttribute("aria-disabled", "true");

  await userEvent.hover(trigger);
  expect(await screen.findByText(/must be applied to at least one monitored table/i)).toBeInTheDocument();
});
```

- [ ] **Step 3: Run test to verify it fails**

- [ ] **Step 4: Write the failing test for the populated state**

```tsx
it("shows per-table breakdown when the rule has applications", async () => {
  mockUseGetRuleScore.mockReturnValue({
    data: { applied_to_count: 2, overall_score: 0.93, per_table: [{ source_table_fqn: "c.s.t1", score: 0.95 }, { source_table_fqn: "c.s.t2", score: 0.91 }] },
  });
  render(<RuleResultsView ruleId="r1" />, { wrapper: TestProviders });
  expect(await screen.findByText("93.0%")).toBeInTheDocument();
});
```

- [ ] **Step 5: Run both tests to verify they fail**

- [ ] **Step 6: Implement**

```tsx
// app/src/databricks_labs_dqx_app/ui/routes/_sidebar/registry-rules.$ruleId.results.tsx
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ScoreBox } from "@/components/results/ScoreBox";
import { useGetRuleScoreSuspense } from "@/lib/api";
import { useTranslation } from "react-i18next";

export function RuleResultsView({ ruleId }: { ruleId: string }) {
  const { t } = useTranslation();
  const { data } = useGetRuleScoreSuspense(ruleId);

  if (data.applied_to_count === 0) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <span role="tab" aria-disabled="true" className="cursor-not-allowed text-muted-foreground">
            {t("ruleResults.tabLabel")}
          </span>
        </TooltipTrigger>
        <TooltipContent>{t("ruleResults.notAppliedTooltip")}</TooltipContent>
      </Tooltip>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      <ScoreBox
        score={data.overall_score}
        label={t("ruleResults.overallScoreLabel", { count: data.applied_to_count })}
        totalTests={data.per_table.reduce((sum, s) => sum + s.total_tests, 0)}
        failedTests={data.per_table.reduce((sum, s) => sum + s.failed_tests, 0)}
      />
      <ul className="flex flex-col gap-2">
        {data.per_table.map((s) => (
          <li key={s.source_table_fqn} className="flex justify-between text-sm">
            <span>{s.source_table_fqn}</span>
            <span>{s.score !== null ? `${(s.score * 100).toFixed(1)}%` : t("results.noRunsYet")}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

Wire this into the actual TanStack Router route file structure discovered in Step 1 — the file path above is a best guess at the naming convention; adjust to match whatever `registry-rules.*` already establishes.

- [ ] **Step 7: Run both tests to verify they pass**

- [ ] **Step 8: Add translation keys (`ruleResults.tabLabel`, `ruleResults.notAppliedTooltip`, `ruleResults.overallScoreLabel`) to all 4 locales, run i18n + full frontend check, commit**

```bash
cd app && make app-check
git add -A -- app/src/databricks_labs_dqx_app/ui
git commit -m "Add rule-level results view, disabled with tooltip when unapplied"
```

---

### Task 12: Global results page

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/results.tsx` (new top-level sidebar route)
- Modify: sidebar navigation config (find the file listing nav items — likely near `ui/routes/_sidebar.tsx` or a `nav-items.ts`/`sidebar-config.ts`)
- Test: sibling `.test.tsx`

**Interfaces:**
- Consumes: `ScoreBox` (Task 8), `useGetGlobalScoreSuspense` (Orval, Task 6)

- [ ] **Step 1: Read `_sidebar.tsx` (or equivalent) to find the nav-item list structure and add "Results"/"Issues" as a new entry, matching an existing entry's shape exactly (icon, label key, path).**

- [ ] **Step 2: Write the failing test**

```tsx
it("shows the org-wide score and a list of tables", async () => {
  mockUseGetGlobalScore.mockReturnValue({
    data: { overall_score: 0.91, table_count: 2, tables: [{ source_table_fqn: "c.s.t1", score: 0.95 }, { source_table_fqn: "c.s.t2", score: 0.87 }] },
  });
  render(<GlobalResultsPage />, { wrapper: TestProviders });
  expect(await screen.findByText("91.0%")).toBeInTheDocument();
  expect(screen.getByText("c.s.t1")).toBeInTheDocument();
});
```

- [ ] **Step 3: Run test to verify it fails**

- [ ] **Step 4: Implement the route**

```tsx
// app/src/databricks_labs_dqx_app/ui/routes/_sidebar/results.tsx
import { createFileRoute } from "@tanstack/react-router";
import { ScoreBox } from "@/components/results/ScoreBox";
import { useGetGlobalScoreSuspense } from "@/lib/api";
import { useTranslation } from "react-i18next";

export const Route = createFileRoute("/_sidebar/results")({
  component: GlobalResultsPage,
});

function GlobalResultsPage() {
  const { t } = useTranslation();
  const { data } = useGetGlobalScoreSuspense();
  return (
    <div className="flex flex-col gap-4 p-6">
      <h1 className="text-xl font-semibold">{t("globalResults.title")}</h1>
      <ScoreBox
        score={data.overall_score}
        label={t("globalResults.orgWideScoreLabel", { count: data.table_count })}
        totalTests={data.tables.reduce((sum, s) => sum + s.total_tests, 0)}
        failedTests={data.tables.reduce((sum, s) => sum + s.failed_tests, 0)}
      />
      <ul className="flex flex-col gap-2">
        {data.tables.map((s) => (
          <li key={s.source_table_fqn} className="flex justify-between text-sm">
            <span>{s.source_table_fqn}</span>
            <span>{s.score !== null ? `${(s.score * 100).toFixed(1)}%` : t("results.noRunsYet")}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

Match the actual TanStack Router file-based route export convention used by sibling top-level routes (e.g. `issues.tsx`/`home.tsx` if they exist) exactly — confirm `createFileRoute` path string matches this file's location.

- [ ] **Step 5: Add the nav entry** in the sidebar config file found in Step 1, following the exact shape of an existing entry (e.g. the entry for `home` or `monitored-tables`).

- [ ] **Step 6: Run test to verify it passes**

- [ ] **Step 7: Add translation keys (`globalResults.title`, `globalResults.orgWideScoreLabel`, plus the new nav-item label key) to all 4 locales, run i18n + full frontend check**

Run: `cd app && make app-check`
Expected: PASS, no missing i18n keys.

- [ ] **Step 8: Commit**

```bash
git add -A -- app/src/databricks_labs_dqx_app/ui
git commit -m "Add global cross-table results page"
```

---

### Task 13: Full verification pass

**Files:** none (verification only)

- [ ] **Step 1: Run the full backend suite**

Run: `cd app && .venv/bin/pytest tests/unit -q`
Expected: all green (should already be 1879+ tests per the existing baseline, plus this plan's new tests).

- [ ] **Step 2: Run the full frontend suite + typecheck + i18n check**

Run: `cd app && make app-check`
Expected: all green, no missing translation keys across any of the 4 locales.

- [ ] **Step 3: Manually exercise the golden path in English, per project convention**

Start the dev servers (`make app-start-dev`), and in the browser:
- Open a monitored table with existing runs → confirm the Results tab shows a real score and failing-record samples (not the old placeholder).
- Open a data product → confirm its Results tab shows the averaged score and per-table breakdown.
- Open a rule that has never been applied anywhere → confirm its Results view is disabled with the correct tooltip. Apply it to a table, re-check → confirm it becomes enabled and shows data.
- Open the new global Results page → confirm it lists only tables you have live UC access to (test with a table you know you lack SELECT on, if one exists in the test workspace — confirm it's silently excluded, not shown as an error).
- In `config.tsx`, change a severity's criticality mapping, save, confirm a newly-materialized rule using that severity gets the new criticality.

Stop the dev servers: `make app-stop-dev`.

- [ ] **Step 4: Report results to the user** — do not mark this plan complete until the manual pass above has actually been run and observed, per project verification standards; typecheck and unit tests confirm code correctness, not feature correctness.

---

### Task 14: Metric-view backing for the score endpoints (user addition, 2026-07-10)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/services/score_view_service.py` (DDL management: shaping view + metric view, idempotent create-or-replace at startup)
- Modify: wherever the app runs its startup Delta DDL/migrations (find the existing startup migration path in `app.py`/migrations and register the new DDL step there, after the `dq_metrics` table exists)
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/dq_score.py` (`_compute_score_for_table` and the `/global` latest-run query refactored onto the metric view)
- Test: extend `app/tests/test_dq_score_route.py`; new `app/tests/test_score_view_service.py`

**Interfaces:**
- Consumes: existing `dq_metrics` table (unchanged, frozen pipeline still writes it).
- Produces: `<catalog>.<schema>.v_dq_check_results` — a plain UC view over `dq_metrics` that explodes the per-rule `check_metrics` JSON into one row per (run_id, input_location, check_name) with `error_count`, `warning_count`, `input_row_count`, `run_time`, and an `is_latest_run` flag (window function per input_location).
- Produces: `<catalog>.<schema>.mv_dq_scores` — a UC **metric view** (YAML `WITH METRICS LANGUAGE YAML`) over the shaping view with dimensions (`input_location`, `run_id`, `run_time`, `is_latest_run`, `check_name`) and measures `failed_tests = SUM(error_count + warning_count)`, `total_tests = SUM(input_row_count)`, `score = 1 - SUM(error_count + warning_count) / SUM(input_row_count)`.
- Produces: dq_score endpoints querying `SELECT input_location, MEASURE(score), MEASURE(failed_tests), MEASURE(total_tests) FROM mv_dq_scores WHERE is_latest_run GROUP BY input_location` (and per-table/per-check variants), replacing the raw `dq_metrics` SQL. Response models and the app-layer OBO catalog filtering are UNCHANGED — the metric view is SP-owned (definer's rights) and is not itself the permission boundary.

Implementation notes:
- Consult the `databricks-metric-views` skill for exact YAML/DDL syntax before writing the DDL.
- The score formula must remain numerically identical to `ScoreService.compute_table_score` (same approved filter approximation); keep the ScoreService unit tests as the formula's specification and add a test asserting the MEASURE-based path and the pure-Python path agree on the same fixture data.
- DDL must be idempotent (`CREATE OR REPLACE`) and tolerant of `dq_metrics` not existing yet on first boot (same ordering guarantees as the app's existing startup DDL).
- The row-level failing-sample endpoint (Task 7) is NOT part of this — row-level data cannot and must not come from a metric view.

### Refresh-behavior requirement for Tasks 9-12 (user addition, 2026-07-10)

All results/score React Query hooks used in Tasks 9-12 must be configured so they never auto-refetch: `staleTime: Infinity`, `refetchOnWindowFocus: false`, no `refetchInterval`. The ONLY refresh trigger is query invalidation fired when a relevant run finishes: find the existing run-status polling/completion signal in the frontend (run history / run sets UI already tracks run state) and, on a run's transition to a terminal state for a given table/product, invalidate the dq-score and quarantine-sample query keys scoped to that table/product (and the global score key). Task 9's implementer establishes the invalidation helper; Tasks 10-12 reuse it.

---

# PHASE 2 (user course-correction, 2026-07-10): Faithful dqlake results-UI port

Tasks 8-12 built a minimal approximation (ScoreBox + flat lists). The user's standing invariant is that dqlake ports COPY the original faithfully. Phase 2 replaces the minimal surfaces with dqlake's actual results UI — trend charts, dimension/severity/rule/column breakdowns, drilldown filters, inline failing-records sample with per-cell severity tinting, CSV/XLSX export — hooked to the Phase 1 backend + metric views. The authoritative port manifest (file inventory, props, endpoint shapes, dependency list) is recorded in `.superpowers/sdd/progress.md` and re-derivable from `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch/src/dqlake/ui/`. The dqlake source code is the spec: when in doubt, match it.

**User-mandated adaptations (the ONLY intended deviations from dqlake):**
1. Global "Results" page = the full results UI over ALL tables the viewer can access (dqlake's product-tab-style composition spanning everything), not just ScoreBox + dashboard embed.
2. Rules-registry results = the same UI with the rule facet locked to that single rule.
3. Genie stripped (GenieChatProvider wrappers deleted — separate follow-on spec).
4. All user-facing strings wrapped in t() with real translations in all 4 locales (dqlake has no i18n; DQX Studio's hard invariant wins).
5. NO idle polling: dqlake's resultsPolling (5s/15s intervals) is NOT ported as-is — queries stay `RESULTS_QUERY_OPTIONS` (staleTime Infinity) with refresh ONLY on run-completion invalidation (the user explicitly called dqlake's constant self-refreshing a defect). The activity hooks may be ported to drive a "run in progress" banner and to trigger the completion invalidation, but never an idle refetch loop.

**New dependencies authorized:** `recharts ^3.8.1` and `xlsx ^0.18.5` (dqlake's own stack). Add via the app's yarn workflow + `make lock-app-dependencies`; committed lockfiles must stay public-URL-only.

**Permission model unchanged:** all new backend endpoints follow Phase 1 rules — aggregate data catalog-filtered via `get_user_catalog_names` (filtered, not 403), row-level samples only through the Task 7 OBO-gated path (the new filtered failed-rows endpoint must route through the same `QuarantineSampleService` gates: OBO self-check first, fine-grained suppression, SP fetch last).

### Task P2.1: Backend — results query API (dqlake shapes over the metric views)

New router `routes/v1/dq_results.py` (mounted `/api/v1/dq-results`) reproducing the response shapes dqlake's UI consumes (recorded in the manifest): 

- `GET /runs/{binding_or_table}` → `RunsOut {rows: [{run_id, run_ts, pass_rate, failed_tests, total_tests}]}` — per-run rollup from `mv_dq_scores` (`GROUP BY run_id`).
- `GET /table/{table_fqn}` with `axes=trend|breakdown` + repeatable filter params (`dimension`, `severity`, `rule`, `column`, `run_id`) → `EntityResultsOut` with `by_dimension/by_severity/by_rule/by_column` GroupRows, `trend`, `trend_by_dimension/_by_severity`, `trend_counts`, `trend_failures`. Per-check rows come from `v_dq_check_results`; dimension/severity/column attributes come from joining check_name → the binding's applied-rule metadata (severity tag, dimension tag, column mapping) in the app DB — join in Python, group per axis.
- `GET /product/{product_id}` (+ `/product/{id}/runs`) → same shapes plus `by_table` and `tables`, aggregated over member tables.
- `GET /global` → same shapes over ALL accessible tables (adaptation #1) — reuse the product aggregation with the accessible-table universe.
- `GET /rule/{rule_id}` → same shapes filtered to one rule across its applied tables (adaptation #2).
- `GET /failed-rows/{table_fqn}?limit=&dimension=&severity=&rule=&column=` → `FailedRowsOut` — the Task 7 quarantine-samples path EXTENDED with server-side failure filters (rule_name direct; severity/dimension via rule-metadata join; column via failures[].columns); all Task 7 security invariants apply unchanged.
- `GET /registries/severities` + `/registries/dimensions` → `SeverityOut[]`/`DimensionOut[]` with `{name, color, rank}` derived from the existing label definitions (rank = array order; colors from value_colors).
- `pass_rate` fields may be emitted as numbers (frontend `toNum()` tolerates strings; prefer numbers).

TDD with the established mocked-SqlExecutor route-test pattern; catalog filtering tests per endpoint; failed-rows security tests re-asserting the Task 7 invariant order.

### Task P2.2: Frontend — copy dqlake pure components + add deps

Add `recharts` + `xlsx` (yarn + `make lock-app-dependencies`; verify lockfiles stay public-URL-only). Copy from dqlake `ui/components/results/` essentially verbatim into `app/src/databricks_labs_dqx_app/ui/components/results/`: `ScoreTrendChart.tsx`, `DimensionBreakdown.tsx`, `FailingRecordsTable.tsx` (dqlake's — REPLACES our Task 8 version; port its per-cell severity tinting via `severityRank.ts` + severityColors/Ranks props), `severityRank.ts`, `failedRecordsExport.ts` + `DownloadFailedRecordsMenu.tsx`, `CollapseRegion.tsx`, `CollapsibleSection.tsx`, `FilterChips.tsx`, `RunPicker.tsx`, `countSeries.ts`, dqlake's `ScoreBox.tsx` (merge with/replace ours — keep dqlake's richer props: trend arrow, info tooltip). Wrap strings in t() (4 locales); verify the `--chart-1..5` CSS vars exist in the Studio theme (add if missing); re-point the one `@/lib/api` type import in failedRecordsExport. Keep existing pure-helper bun tests where still relevant; add bun tests for `severityRank`, `countSeries`, `pivot`/`pivotCounts` (dqlake exports them — test against manifest semantics).

### Task P2.3: Port BindingIssuesTab → monitored-table Results tab

Copy dqlake `components/bindings/BindingIssuesTab.tsx` (~769 ln) as the new Results tab body (replacing Task 9's minimal tab): RunInProgressBanner, ScoreBox + RunPicker, "Over time" collapsible (overall trend chart, score-by-dimension/severity, count charts), "Drilldown" collapsible (facet filters + FilterChips, By dimension/severity/rule/column breakdowns, Failed Records inline table + Download menu). Re-point hooks to P2.1 endpoints; strip Genie wrapper; i18n; polling per adaptation #5 (activity hooks feed the existing run-completion invalidation + a banner only). Keep exported helpers (`Facet`, `MultiFilters`, `toggleFacet`, `toNum`, `ApplicableToggle`, `COUNT_INFO`) since P2.4-P2.6 reuse them.

### Task P2.4: Port ProductResultsTab → table-space Results tab

Copy dqlake `components/products/ProductResultsTab.tsx` (~740 ln) replacing Task 10's tab: Average ScoreBox, overall/average trend mode with per-table dull lines, By dimension/severity, By rule, By table + By column, by-table row selection revealing that table's invalid-samples FailingRecordsTable (row-level fetch via the Task 7-gated endpoint). Re-point, strip Genie, i18n, no idle polling.

### Task P2.5: Global Results page = full UI over all tables

Replace Task 12's minimal page with the product-style composition spanning ALL accessible tables (adaptation #1), backed by `GET /api/v1/dq-results/global`. Keep the sidebar entry. By-table drilldown/selection reveals per-table sample (Task 7 gates).

### Task P2.6: Rule results = same UI locked to one rule

Replace Task 11's minimal tab content with the shared composition filtered to the rule (adaptation #2), backed by `GET /api/v1/dq-results/rule/{rule_id}`. The disabled-tab + tooltip behavior for `applied_to_count === 0` (already reviewed) is KEPT exactly as-is. Also FIX the Task 13 finding: applying/unapplying a rule must invalidate the dq-score AND dq-results query keys so the tab enables without a reload.

### Task P2.7: Verification pass (repeat Task 13 protocol)

Full suites + browser pass with the dqlake-fidelity lens: charts render with real trend data, breakdowns filter, drilldown chips work, per-cell severity tinting uses admin-configured severity colors, exports download, rule tab enables immediately after applying a rule, no idle refetch, global page spans all tables. Side-by-side eyeball against dqlake screenshots where possible.

---

# PHASE 3 (user directives, 2026-07-10 evening): follow-ups, draft semantics, score cache, homepage, Genie

Research ground truth (recorded in ledger): binding status lifecycle is `draft|pending_approval|approved|rejected`; a draft binding CAN run today (`source="draft"` renders live unapproved rules, run-set member gets `binding_version=None`); `add_member` checks only existence, no status; `dq_validation_runs.run_type` ∈ {dryrun, scheduled, preview} reaches UC (runner promotes dryrun→scheduled when sample_size==0); `binding_version` lives only in Postgres run-set members; the frozen runner's `_aggregate_rule_labels` intersection preserves any tag that has the SAME value on every check → a uniform per-check `run_mode`/`binding_version` tag added at run-assembly time lands in `dq_metrics.user_metadata` in UC. ScoreBox counts are missing on the 3 MultiTableResults surfaces because `totalTests={0}` is hardcoded (a faithfully-ported dqlake bug); `by_table` rows already carry `total_tests`. DimensionBreakdown has no pagination anywhere (nor in dqlake).

### Task P3.1: Backend hardening batch (final-review follow-ups)

- Quote metric-view/shaping-view/dq_metrics/dq_quarantine_records READ FQNs (backtick-quote parts) consistently with the DDL side, so hyphenated catalogs work end-to-end (`score_view_service.metric_view_fqn`, `dq_results._shaping_view_fqn`, and the config-FQN builders in dq_score/dq_results/metrics/quarantine paths). Tests: a hyphenated-catalog fixture through each query builder.
- Re-validate FQNs (`validate_fqn`) on the product/rule score paths before interpolation (defense-in-depth; app-DB-sourced FQNs).
- Add `binding_id` to `by_table` GroupRows (join available in the service: table_fqn → binding via monitored-table service, already loaded for product/rule scoping; for global, one batched lookup) so the UI can link rows. Shape change is additive (`binding_id: str | None`).

### Task P3.2: Draft tables cannot join table spaces

- `data_product_service.add_member`: reject bindings whose status is not `approved` (or version==0/never-approved) with a clear 400; test both rejection and the approved-path success. Consider existing members: do NOT retroactively evict, but `run()` fan-out already skips never-approved under source="approved".
- UI: the add-tables dialog filters out (or disables with a tooltip, matching how the app elsewhere disables ineligible options) non-approved bindings. i18n 4 locales.

### Task P3.3: Run version / draft-run tracking

- At run-assembly (binding_run_service, both draft and approved paths): stamp every check's `user_metadata` with uniform `run_mode` ("draft" | "published") and `binding_version` (int as string; absent/omitted for draft) tags — same value across all checks in the run so the frozen runner's intersection carries them into `dq_metrics.user_metadata` in UC. Do NOT touch frozen files; this happens in the checks payload the app already builds.
- Views: `v_dq_check_results` (or the attribution join) exposes `run_mode` + `binding_version` columns read from `dq_metrics.user_metadata` map; `mv_dq_scores` gains `run_mode` as a dimension. Legacy runs without the tag: derive fallback from the runs join — `run_type='scheduled'` → published, `dryrun`/`preview` → draft (document the heuristic).
- dq-results + dq-score endpoints: new `include_drafts: bool = False` query param — default excludes draft runs everywhere (trend, breakdowns, runs list, failed-rows, scores). Route tests for both modes + the legacy heuristic.

### Task P3.4: Lakebase score cache + list-page score columns

- New Lakebase table `dq_score_cache` (migration in the app's Postgres migrations + Delta fallback): `scope_type` ('table'|'product'|'global'), `scope_key` (fqn / product_id / 'global'), `score`, `failed_tests`, `total_tests`, `latest_run_id`, `run_time`, `computed_at`. PK (scope_type, scope_key). PUBLISHED-only scores (the cache backs list columns + homepage which follow the default view).
- `ScoreCacheService`: `refresh_for_tables(fqns)` recomputes from the metric view (one warehouse query batched over the fqns) and upserts; `refresh_product(product_id)` + `refresh_global()` derive from cached table rows (unweighted means — no extra warehouse hit); `get_many(scope_type, keys)` fast read.
- Refresh triggers: (a) server-side — a new lightweight `POST /api/v1/dq-results/refresh-scores` called by the frontend at the exact run-completion moments that already fire invalidation (results-invalidation.ts gains the POST); (b) lazy staleness — list endpoints return cached values as-is (no blocking recompute) plus `computed_at` so the UI can show staleness subtly if ever needed. No polling, no cron.
- List endpoints: monitored-tables list + data-products list responses gain `score`, `failed_tests`, `total_tests`, `score_computed_at` joined from the cache in the same Postgres round-trip (LEFT JOIN — null when never computed). Catalog-filtering unchanged (rows already filtered).
- UI: score column in the monitored-tables table and table-spaces table, styled like dqlake's list score badge/column (check dqlake's `DataProductsTable.tsx` and bindings table for the exact cell rendering — copy it). Sortable if the list tables support sorting. i18n 4 locales.

### Task P3.5: dqlake-style homepage

- Faithful port of dqlake's `routes/_sidebar/home.tsx` + `components/home/HomeStats.tsx` + `HomeGrid.tsx` (read the originals): "at a glance" overall score + workspace-wide stats + nav cards, adapted to our nav targets (Rules Registry, Monitored Tables, Table Spaces, Results, Config).
- Backend `GET /api/v1/home/stats`: overall score from `dq_score_cache` (scope 'global'), counts from the app DB (rules, monitored tables, products — cheap Postgres counts), all in one response, no warehouse touch. dqlake's `home_stats_cache.py` semantics (short TTL in-process cache) may be layered if the Postgres counts are ever hot, but Postgres-only should be fast enough — implementer's call, note it.
- Make it the default landing route (whatever `/` currently does — check and preserve deliberate behavior; if home already exists, replace its body faithfully).

### Task P3.6: Frontend batch — run-mode dropdown, counts, pagination, small fixes

- **Run-mode dropdown**: next to the run picker on the monitored-table tab (and where MultiTableResults surfaces would show it — global/product/rule get it too since drafts affect them): default "Published only", other option "Published + Draft"; wires `include_drafts` into every dq-results query on that surface. i18n 4 locales.
- **ScoreBox counts fix**: MultiTableResults passes `totalTests` summed from `by_table[].total_tests` (and failedTests consistently from the same rows) so the failed/total subtitle renders on rule/table-space/global ScoreBoxes. (Fixes the ported dqlake `totalTests={0}` bug — deliberate, documented deviation from dqlake.)
- **Pagination**: `DimensionBreakdown` gains optional `pageSize?: number`; when set and rows > pageSize, paginate client-side (slice + compact pager, mirroring FailingRecordsTable's existing pager idiom). Apply `pageSize={8}` to the "By table" and "By rule" boxes on all surfaces. Pure-helper tests for the pager math.
- **Small fixes**: rule Results trigger shows a distinct tooltip on score-fetch error ("couldn't check applicability — retry") instead of silent disabled; global page by_table rows link to the monitored-table Results tab via the new `binding_id` (P3.1); RunInProgressBanner: when THIS page triggers a run, poll that run's status until terminal (active-run-scoped polling is not idle polling), show dqlake's banner while active, fire the invalidation helper on terminal — closing the Task 9 gap; draft-binding results tab shows a lightweight notice when the binding has never been approved (restores the lost pending-approval context without breaking fidelity).
- **xlsx follow-up**: attempt migration to the official SheetJS dist (https://cdn.sheetjs.com tarball pin) IF the lockfile stays public-URL-clean and tests pass; otherwise document the risk acceptance (write-only usage) in a code comment and leave the npm pin. Either outcome is acceptable — report which.
- **Drilldown cross-filtering bug (user report)**: clicking a breakdown row does not filter/hide the OTHER drilldown boxes correctly, and behavior differs between the "All" and "Applicable" toggle modes. Compare our facet wiring in BindingResultsTab/MultiTableResults line-by-line against dqlake's original (which queries feed which box — dqlake uses a filtered query for other boxes and a base query so the clicked box keeps all its rows; the ApplicableToggle changes which of trend/base feeds the counts). Find the divergence, fix to match dqlake exactly, and add pure-helper tests for the facet->params mapping per box.
- **Failed-records hover text bug (user report)**: tooltips over failing rows often show blanks. Investigate the real payload chain: dq_quarantine_records VARIANT failures → `parse_failures`/`_to_failing_record` transform → FailingRecordsTable's cursor tooltip content. Suspects: `message` lost/nulled in the VARIANT parse or legacy `{check_name: message}` coercion; whole-row failures (empty `columns`) whose messages never attach to any cell; empty-string messages rendering as blank tooltips instead of falling back to rule name. Fix at the true source (transform or tooltip fallback: always show at least rule name + message-or-"no message"), with a regression test against a realistic payload fixture that reproduces the blank.

### Task P3.7: Phase 3 verification pass

Full suites + browser pass: draft table cannot be added to a table space (UI + API); a draft run is excluded by default everywhere and appears when the dropdown flips; new runs carry run_mode/binding_version in dq_metrics.user_metadata (inspect a real run); score columns render on both list pages instantly (no warehouse call on page load — verify via network/log timing); homepage renders with overall score; ScoreBox counts show on all four surfaces; >8-row breakdowns paginate; global rows navigate to the table's results; banner appears during a page-triggered run and the tab refreshes on completion.

### Task P3.8: Genie — research + spec

Read dqlake's Genie implementation end-to-end (backend/materialiser/genie_space.py, routers/ai.py + ai_gateway.py, the ~15 Genie UI files, genieSuggestedQuestions.ts, docs/superpowers/specs/2026-06-17-genie-space-rework-design.md) and produce a port manifest in the ledger: space provisioning over OUR objects (mv_dq_scores, v_dq_check_results/attribution, dq_quarantine_records), chat UI wiring, suggested questions, conversation store. Deliverable: manifest + task split for P3.9/P3.10.

### Task P3.9: Genie backend — space provisioning + chat proxy (faithful port, our data objects)

### Task P3.10: Genie UI — AskGenieButton/GenieChatSidebar/GenieResult* port (faithful; i18n; no idle polling)

### Task P3.11: Genie space instructions — REWRITE, not port

User: dqlake's space instructions read wrong ("something's not quite right in the way it talks"). Write fresh instructions for the space: plain, precise, steward-appropriate tone (no cringe, no over-enthusiasm — the user's standing copy preference); grounded in our actual schema objects and score semantics (published-only default, as-of-run attribution); include the sample-question set re-grounded on our objects. Review the drafted instructions against dqlake's to confirm coverage parity even though the voice is new.

### Task P3.12: Genie verification + final whole-branch re-review of Phase 3

---

# PHASE 4 (user-approved 2026-07-11): Genie row-level access via OBO + entitlement cache

Design approved by the user: Genie conversations run as the calling user (OBO), and row-level access flows through a dynamic view gated by a self-verified entitlement cache — no MANAGE grants, no ACL mirroring, drift bounded by TTL. Cold start solved by pre-verifying the tables already on the user's screen (context table / product members / global by_table). Genie load unaffected (verification is fire-and-forget async).

### Task P4.1: Entitlement cache + gated dynamic view (backend)

- New UC Delta table `<catalog>.<schema>.dq_user_table_entitlements(user_email STRING, table_fqn STRING, verified_at TIMESTAMP)` — created idempotently in the same startup ensure step as the score views (it must be a UC object: the dynamic view references it). SP-written only.
- New dynamic view `v_dq_failing_rows` over `dq_quarantine_records` with `WHERE EXISTS (... e.user_email = current_user() AND e.table_fqn = q.source_table_fqn AND e.verified_at > current_timestamp() - INTERVAL 24 HOURS)` — fail-safe empty. Created in the ensure step.
- Startup grants (same precedent as the existing `GRANT USE CATALOG` in app.py): `GRANT SELECT` on `mv_dq_scores`, `v_dq_check_results`, `v_dq_check_attribution`, `v_dq_failing_rows` + `USE SCHEMA` to `account users` (required once Genie runs OBO). The entitlement table itself gets NO user grants (SP-only; the dynamic view reads it with definer's rights).
- `EntitlementService`: `verify_and_record(obo_sql, user_email, fqns)` — for each fqn (validate_fqn first), skip if a fresh cache row exists (one SP read), else run the existing `QuarantineSampleService.user_can_select` OBO probe concurrently (bounded parallelism, e.g. 5), upsert successes SP-side. Never raises to callers; returns per-fqn outcome.
- `POST /api/v1/genie/verify-entitlements {table_fqns: [...]}` — viewer+, cap 50, fire-and-forget-friendly (fast 202-style response; verification inline but bounded — implementer judgment, note it).
- Piggyback: the dq-results failed-rows path already runs `user_can_select` — on success, upsert the entitlement row there too.
- Tests: probe-skip-on-fresh-row, TTL expiry, concurrent bounded verification, fail-closed on probe error, DDL pins for table+view+grants, endpoint cap/validation/RBAC.

### Task P4.2: OBO Genie chat + space row-source restoration

- Chat proxy (`genie_chat_service` + routes): conversation start/poll/ask/feedback switch from the SP client to the caller's OBO `WorkspaceClient` (provisioning stays SP). Verify live on the dev workspace whether the OBO token's scopes permit the Genie conversation API — if blocked, KEEP SP chat, report the exact error, and stop (the user decides on scope expansion; do not silently ship a broken switch).
- Space update: add `v_dq_failing_rows` as a fourth data source (description grounded in real columns; note VARIANT payload columns); restore the row-source instructions element REWRITTEN in the established voice (one short paragraph: query v_dq_failing_rows for failing-record requests, return whole records via to_json(row_data) style if applicable to our VARIANT schema — check the real quarantine columns; explain empty results may mean the table hasn't been opened in DQX Studio); restore the two dropped sample questions + curated SQLs for them (verify against real columns; run_mode-filtered via the run_id subselect pattern from P3.3).
- config-hash change self-PATCHes the existing space on next startup (established mechanism).
- Tests: serialized_space pins updated deliberately; OBO-identity pin on chat calls (mocked); instructions coverage test if one exists.

### Task P4.3: UI pre-verification + E2E verify + deploy

- Fire-and-forget POST to verify-entitlements when: the Genie sidebar opens (context table / product member FQNs — the provider already has them), and when a results surface renders its by_table list (global page: cap the first 25). No UI blocking, silent failure.
- E2E on the deployed app: entitlement row appears after opening a table's results; Genie (OBO) answers a "show failing rows" question for a verified table; returns empty for an unverified one; aggregate questions still work.
- Deploy + install-contract check.

---

# PHASE 5 (user reports, 2026-07-11): trend semantics, rule identity, score-cache correctness

### Task P5.1: As-of average trendline (data-side, Genie-teachable)

The product/global "Average" trendline misbehaves with multiple runs across multiple tables: our trend is per-run-instant (each point = one table's run), while dqlake's product trend is AS-OF carry-forward (at each timestamp, every member contributes its most recent score at-or-before that instant; average those). Read dqlake's product trend construction (mv_product_results / as_of semantics) closely. Fix DATA-SIDE: compute the as-of average series in the backend service SQL (window functions over the shaping view: per member table, last score at-or-before each distinct run_time across the member set; then mean per instant) so the API's `trend` for product/global scope carries the correct average line; the frontend's client-side computeOverallPoints then consumes server truth (simplify/remove the client math where superseded — keep per-table dull lines). Teach Genie the same as-of pattern: curated SQL (parameterized by the preamble's member-table list where applicable) + a short instructions addition. Tests: multi-table misaligned-run fixtures pinning the carry-forward math.

### Task P5.2: Rule-identity grouping in By rule (version-aware)

By rule currently groups by check_name — a rule renamed between versions shows as two rows. Group by `registry_rule_id` (as-of attribution carries it; fall back to check_name when absent), label each group with the NAME FROM THE NEWEST RUN in scope (name changes across versions collapse into one row with the latest display name), and include the rule description where shown consistently with how names resolve. Applies to monitored-table, table-space, rule, and global scopes. Tests: renamed-rule-across-runs fixture -> one row, newest label.

### Task P5.3: Score-cache correctness batch

- LIVE-DIAGNOSE first (deployed app): product-scope cache rows (why table-space score column is null/broken while monitored-tables works) — check scope_key/product-id typing, refresh_product member lookup, and the list JOIN for products.
- Homepage global score: verify refresh_global truly averages per-table cache rows (user observed "just the latest run" — likely only one table had a non-null cached score). Fix whatever diagnosis shows.
- STARTUP RECONCILE: on app start (best-effort, after views ensured), refresh scores for ALL monitored tables in one batched warehouse query + derive products/global — heals stale/null cache rows after semantic changes (like the run_mode hotfix) and cold deployments. Keep it bounded + logged.
- Tables tab in table spaces: per-member DQ score column (ScoreBarCell, cache-fed — extend the product-detail/members response additively with cached score fields).
- Residual gap from the hotfix review: manually-triggered runs with the tab closed miss refresh — the scheduler tick's completion sweep should ALSO track run-sets created by non-scheduler paths (read run-set creation records server-side) if cheap; otherwise document.

### Task P5.4: Phase 5 verification + deploy

## Deferred (explicitly out of scope)

- Product-level batch run ids in dq_metrics (would restore full product run-picker parity) — requires run-submission changes; backlog.
