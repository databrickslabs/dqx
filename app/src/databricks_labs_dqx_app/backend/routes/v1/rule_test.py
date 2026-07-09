"""Rule Test routes (P22-E) — test a registry rule's SQL predicate against sample data.

Ports dqlake's "Test rule" backend, adapted to DQX:

- ``POST /rule-tests/run`` evaluates a ``sql`` / ``lowcode`` rule's effective SQL
  predicate over an inline VALUES grid (manual test) or a real UC table sample,
  returning per-row pass/fail. Runs OBO on the configured SQL warehouse. DQX
  Native (``dqx_native``) rules are NOT testable and are rejected here (the UI
  shows the "not available" notice and never calls this).
- ``POST /rule-tests/generate-data`` asks the AI gateway (OBO) for a mix of
  passing/failing rows for the manual grid; degrades cleanly when AI is off.
- ``POST /rule-tests/warehouse/prewarm`` fire-and-forget starts the configured
  warehouse so the first test run isn't stuck cold-starting.

Security: the rule predicate must pass DQX's ``is_sql_query_safe`` (in the
service); errors are sanitized so no raw warehouse/LLM text reaches the client.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from databricks_labs_dqx_app.backend.common.authorization import CurrentUser, UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_obo_ws,
    get_rule_test_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.rule_test_sql import AdhocSource, TableSource, TestRunResult
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import resolve_warehouse_id
from databricks_labs_dqx_app.backend.services.rule_test_service import RuleTestService

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]

router = APIRouter(dependencies=[require_role(*_AUTHORS_AND_ABOVE)])


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


class SlotIn(BaseModel):
    name: str
    family: str = "any"


class AdhocRunIn(BaseModel):
    columns: list[str]
    rows: list[list[Any]]


class TableRunIn(BaseModel):
    table_fqn: str = Field(min_length=1, max_length=512)
    column_mapping: dict[str, str] = Field(default_factory=dict)
    sample_kind: Literal["records", "percent", "full"] = "records"
    sample_value: int = Field(default=10000, ge=1, le=10_000_000)


class RuleTestRunIn(BaseModel):
    mode: Literal["dqx_native", "lowcode", "sql"]
    predicate: str = Field(min_length=1)
    polarity: Literal["pass", "fail"] = "pass"
    slots: list[SlotIn] = Field(default_factory=list)
    source_kind: Literal["adhoc", "table"]
    adhoc: AdhocRunIn | None = None
    table: TableRunIn | None = None
    display_cap: int = Field(default=5000, ge=1, le=50_000)


class TestRowOut(BaseModel):
    cells: dict[str, str | None]
    passed: bool
    row_idx: int | None = None


class RuleTestRunOut(BaseModel):
    columns: list[str]
    rows: list[TestRowOut]
    truncated: bool


def _to_out(result: TestRunResult) -> RuleTestRunOut:
    return RuleTestRunOut(
        columns=result.columns,
        rows=[TestRowOut(cells=r.cells, passed=r.passed, row_idx=r.row_idx) for r in result.rows],
        truncated=result.truncated,
    )


@router.post("/run", response_model=RuleTestRunOut, operation_id="runRuleTest")
async def run_rule_test(
    body: RuleTestRunIn,
    svc: Annotated[RuleTestService, Depends(get_rule_test_service)],
) -> RuleTestRunOut:
    """Run a rule's SQL predicate against manual rows or a UC table sample."""
    if body.mode == "dqx_native":
        raise HTTPException(status_code=400, detail="Rule tests aren't available for DQX Native rules.")
    try:
        if body.source_kind == "adhoc":
            if body.adhoc is None:
                raise ValueError("Manual test rows are required.")
            families = {s.name: s.family for s in body.slots}
            mapping = {c: c for c in body.adhoc.columns}
            source = AdhocSource(
                columns=body.adhoc.columns,
                rows=body.adhoc.rows,
                families=families,
                column_mapping=mapping,
                display_cap=body.display_cap,
            )
            result = await svc.run_adhoc(predicate=body.predicate, polarity=body.polarity, source=source)
        else:
            if body.table is None:
                raise ValueError("A table and column mapping are required.")
            missing = [s.name for s in body.slots if s.name not in body.table.column_mapping]
            if missing:
                raise ValueError(f"Map a column for: {', '.join(missing)}")
            table_source = TableSource(
                table=body.table.table_fqn,
                column_mapping=body.table.column_mapping,
                sample_kind=body.table.sample_kind,
                sample_value=body.table.sample_value,
                display_cap=body.display_cap,
            )
            result = await svc.run_table(predicate=body.predicate, polarity=body.polarity, source=table_source)
    except UnsafeSqlQueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Never relay raw warehouse errors — they can echo schema/data.
        logger.error("Failed to run rule test: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail="Could not run the test. Check the SQL warehouse and your access.")
    return _to_out(result)


# ---------------------------------------------------------------------------
# AI generate test data
# ---------------------------------------------------------------------------


class GenerateDataIn(BaseModel):
    predicate: str = Field(min_length=1)
    polarity: Literal["pass", "fail"] = "pass"
    columns: list[SlotIn] = Field(default_factory=list)
    row_count: int = Field(default=8, ge=5, le=20)


class GenerateDataOut(BaseModel):
    columns: list[str]
    rows: list[list[str | None]]


@router.post("/generate-data", response_model=GenerateDataOut, operation_id="generateRuleTestData")
async def generate_rule_test_data(
    body: GenerateDataIn,
    svc: Annotated[RuleTestService, Depends(get_rule_test_service)],
    user_email: CurrentUser,
) -> GenerateDataOut:
    """Generate a passing/failing mix of manual test rows via the AI gateway."""
    try:
        result = await svc.generate_test_data(
            predicate=body.predicate,
            polarity=body.polarity,
            columns=[(c.name, c.family) for c in body.columns],
            row_count=body.row_count,
            user_email=user_email,
        )
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        # Treat AI output as untrusted — never relay raw model/exception text.
        logger.error("Failed to generate rule test data: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail="Could not generate test data. Try again.")
    return GenerateDataOut(columns=result.columns, rows=result.rows)


# ---------------------------------------------------------------------------
# Warehouse prewarm
# ---------------------------------------------------------------------------


class PrewarmIn(BaseModel):
    start: bool = True


class PrewarmOut(BaseModel):
    warehouse_id: str
    state: str
    running: bool


@router.post("/warehouse/prewarm", response_model=PrewarmOut, operation_id="prewarmRuleTestWarehouse")
async def prewarm_rule_test_warehouse(
    body: PrewarmIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> PrewarmOut:
    """Fire-and-forget start the configured SQL warehouse so the first run is warm."""
    import asyncio

    warehouse_id = resolve_warehouse_id(app_settings)
    if not warehouse_id:
        raise HTTPException(status_code=503, detail="No SQL warehouse is configured. Set one in Configuration.")
    try:
        warehouse = await asyncio.to_thread(obo_ws.warehouses.get, id=warehouse_id)
        state = getattr(getattr(warehouse, "state", None), "value", None) or str(getattr(warehouse, "state", ""))
        running = state == "RUNNING"
        if body.start and state not in ("RUNNING", "STARTING"):
            # Fire-and-forget: don't await .result() (would block on cold start).
            try:
                await asyncio.to_thread(obo_ws.warehouses.start, id=warehouse_id)
            except Exception:
                # Surface via state; a failed start shouldn't fail the request.
                logger.warning("Warehouse prewarm start failed; surfacing current state", exc_info=True)
        return PrewarmOut(warehouse_id=warehouse_id, state=state, running=running)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to prewarm test warehouse: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail="Could not reach the SQL warehouse.")
