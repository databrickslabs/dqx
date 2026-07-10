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

## Deferred (explicitly out of scope)

- Genie Space provisioning + chat UI — separate follow-on spec, once this plan's data model has shipped and has real data for Genie to query.
