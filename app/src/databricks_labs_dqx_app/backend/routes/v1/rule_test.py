"""Rule Test routes (P22-E) — test a registry rule's SQL predicate against sample data.

Ports dqlake's "Test rule" backend, adapted to DQX:

- ``POST /rule-tests/run`` evaluates a rule's effective SQL predicate over an
  inline VALUES grid (manual test) or a real UC table sample, returning per-row
  pass/fail. Runs OBO on the configured SQL warehouse. ``sql`` / ``lowcode``
  rules send a predicate directly; ``dqx_native`` rules send ``function`` +
  ``native_arguments`` which are compiled to a row-level SQL predicate here
  (dataset / geo / UDF checks are rejected).
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
from databricks_labs_dqx_app.backend.native_test_predicate import (
    NativeTestCompileError,
    NativeTestNotSupportedError,
    compile_native_test_predicate,
)
from databricks_labs_dqx_app.backend.rule_test_sql import (
    INPUT_VIEW_SLOT,
    AdhocGrid,
    AdhocSource,
    TableSource,
    TestRunResult,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import resolve_warehouse_id
from databricks_labs_dqx_app.backend.services.rule_test_service import RuleTestService

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]

# Slot family naming a reference TABLE rather than a column (see RuleSlot).
_TABLE_FAMILY = "table"


def _reference_slots(table_slots: list[str]) -> list[str]:
    """Table slots the author has to supply, i.e. excluding ``{{input_view}}``.

    A slot named ``input_view`` collides with DQX's reserved token for the data
    being checked, which the builders resolve themselves — demanding a binding for
    it would block a test that needs none.
    """
    return [n for n in table_slots if n != INPUT_VIEW_SLOT]

router = APIRouter(dependencies=[require_role(*_AUTHORS_AND_ABOVE)])


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


class SlotIn(BaseModel):
    name: str
    family: str = "any"


class AdhocGridIn(BaseModel):
    """One inline grid standing in for a reference table in the manual test."""

    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    families: dict[str, str] = Field(default_factory=dict, description="Grid column name -> slot family, for typing.")


class AdhocRunIn(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    ref_grids: dict[str, AdhocGridIn] = Field(
        default_factory=dict,
        description="Cross-table rules: family='table' slot name -> the grid standing in for that reference table.",
    )


class TableRunIn(BaseModel):
    table_fqn: str = Field(min_length=1, max_length=512)
    column_mapping: dict[str, str] = Field(default_factory=dict)
    table_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Cross-table rules only: family='table' slot name -> reference table FQN.",
    )
    sample_kind: Literal["records", "percent", "full"] = "records"
    sample_value: int = Field(default=10000, ge=1, le=10_000_000)


class RuleTestRunIn(BaseModel):
    mode: Literal["dqx_native", "lowcode", "sql"]
    predicate: str = ""
    function: str | None = None
    native_arguments: dict[str, Any] | None = None
    polarity: Literal["pass", "fail"] = "pass"
    slots: list[SlotIn] = Field(default_factory=list)
    source_kind: Literal["adhoc", "table"]
    adhoc: AdhocRunIn | None = None
    table: TableRunIn | None = None
    display_cap: int = Field(default=5000, ge=1, le=50_000)
    # Set by the Low-Code editor when the rule folds joins and/or group-by into a
    # dataset-level ``sql_query`` (see ``lib/lowcodeCompile.compileLowcodeBody``).
    # Only the row predicate reaches this route, so testing such a rule would
    # yield a MISLEADING verdict — the UI hides the test surface and the route
    # rejects it (belt-and-braces) rather than silently testing the wrong thing.
    lowcode_advanced: bool = False


def _resolve_predicate(body: RuleTestRunIn) -> str:
    if body.mode == "dqx_native":
        if not body.function:
            raise ValueError("A check function is required for DQX Native rule tests.")
        return compile_native_test_predicate(body.function, body.native_arguments or {})
    if not body.predicate.strip():
        raise ValueError("A predicate is required.")
    return body.predicate


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
    if body.lowcode_advanced:
        raise HTTPException(
            status_code=400,
            detail="Rule tests aren't available for rules with joins or grouping yet.",
        )
    try:
        predicate = _resolve_predicate(body)
        if body.source_kind == "adhoc":
            if body.adhoc is None:
                raise ValueError("Manual test rows are required.")
            # Each table slot stands in as its own inline grid, so a cross-table
            # rule is testable here too — but only if the author actually supplied
            # rows for it; joining an empty grid would silently "pass" everything.
            table_slots = [s.name for s in body.slots if s.family == _TABLE_FAMILY]
            unfilled = [n for n in _reference_slots(table_slots) if not (body.adhoc.ref_grids.get(n) or AdhocGridIn()).columns]
            if unfilled:
                raise ValueError(f"Add columns and rows for the reference table: {', '.join(unfilled)}")
            families = {s.name: s.family for s in body.slots}
            # Column slots map to the identically-named grid column; a table slot
            # resolves to a CTE instead, so it is dropped from the input grid —
            # by INDEX, so each remaining row keeps its cells aligned even if a
            # client still sends a column for it.
            keep = [i for i, c in enumerate(body.adhoc.columns) if c not in set(table_slots)]
            kept_columns = [body.adhoc.columns[i] for i in keep]
            mapping = {c: c for c in kept_columns}
            source = AdhocSource(
                columns=kept_columns,
                rows=[[row[i] if i < len(row) else None for i in keep] for row in body.adhoc.rows],
                families=families,
                column_mapping=mapping,
                display_cap=body.display_cap,
                ref_grids={
                    name: AdhocGrid(columns=g.columns, rows=g.rows, families=g.families)
                    for name, g in body.adhoc.ref_grids.items()
                },
            )
            result = await svc.run_adhoc(predicate=predicate, polarity=body.polarity, source=source)
        else:
            if body.table is None:
                raise ValueError("A table and column mapping are required.")
            # Column slots bind to a column of the sampled table; table slots bind
            # to a reference table's FQN. Each is looked up in its own map, so a
            # table slot is never asked for (nor matched against) a column.
            column_slots = [s.name for s in body.slots if s.family != _TABLE_FAMILY]
            table_slots = [s.name for s in body.slots if s.family == _TABLE_FAMILY]
            missing = [n for n in column_slots if n not in body.table.column_mapping]
            if missing:
                raise ValueError(f"Map a column for: {', '.join(missing)}")
            missing_tables = [n for n in _reference_slots(table_slots) if n not in body.table.table_mapping]
            if missing_tables:
                raise ValueError(f"Pick a table for: {', '.join(missing_tables)}")
            table_source = TableSource(
                table=body.table.table_fqn,
                column_mapping={k: v for k, v in body.table.column_mapping.items() if k not in set(table_slots)},
                table_mapping=body.table.table_mapping,
                sample_kind=body.table.sample_kind,
                sample_value=body.table.sample_value,
                display_cap=body.display_cap,
            )
            result = await svc.run_table(predicate=predicate, polarity=body.polarity, source=table_source)
    except (NativeTestNotSupportedError, NativeTestCompileError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except UnsafeSqlQueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # The test runs the user's OWN OBO query against a table THEY chose, so the
        # underlying SQL/warehouse failure (permission denied, unknown column, syntax
        # error) is theirs to see — surfacing it is what makes a failed run actionable.
        # Only the error text from that query reaches the client; nothing else leaks.
        logger.error("Failed to run rule test: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Could not run the test: {e}") from e
    return _to_out(result)


# ---------------------------------------------------------------------------
# AI generate test data
# ---------------------------------------------------------------------------


class GenerateDataIn(BaseModel):
    predicate: str = ""
    function: str | None = None
    native_arguments: dict[str, Any] | None = None
    polarity: Literal["pass", "fail"] = "pass"
    columns: list[SlotIn] = Field(default_factory=list)
    row_count: int = Field(default=8, ge=5, le=20)
    ref_tables: list[str] = Field(
        default_factory=list,
        description="family='table' slot names the rule joins; asks the model for a consistent "
        "cross-table mix (some input rows matching a reference row, some deliberately not).",
    )


class GeneratedGridOut(BaseModel):
    columns: list[SlotIn]
    rows: list[list[str | None]]


class GenerateDataOut(BaseModel):
    columns: list[str]
    rows: list[list[str | None]]
    refs: dict[str, GeneratedGridOut] = Field(default_factory=dict)


@router.post("/generate-data", response_model=GenerateDataOut, operation_id="generateRuleTestData")
async def generate_rule_test_data(
    body: GenerateDataIn,
    svc: Annotated[RuleTestService, Depends(get_rule_test_service)],
    user_email: CurrentUser,
) -> GenerateDataOut:
    """Generate a passing/failing mix of manual test rows via the AI gateway."""
    try:
        predicate = _resolve_predicate(
            RuleTestRunIn(
                mode="dqx_native" if body.function else "sql",
                predicate=body.predicate,
                function=body.function,
                native_arguments=body.native_arguments,
                source_kind="adhoc",
            )
        )
        result = await svc.generate_test_data(
            predicate=predicate,
            polarity=body.polarity,
            columns=[(c.name, c.family) for c in body.columns],
            row_count=body.row_count,
            user_email=user_email,
            ref_tables=body.ref_tables,
        )
    except (NativeTestNotSupportedError, NativeTestCompileError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
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
    return GenerateDataOut(
        columns=result.columns,
        rows=result.rows,
        refs={
            name: GeneratedGridOut(
                columns=[SlotIn(name=col, family=family) for col, family in grid.columns],
                rows=grid.rows,
            )
            for name, grid in result.refs.items()
        },
    )


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
