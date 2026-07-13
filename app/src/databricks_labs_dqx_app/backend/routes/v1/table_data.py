"""View Data routes (P22-B, item 7) — table preview + pragmatic AI query.

- ``POST /table-data/preview`` returns the first 500 rows of a table via a SQL
  warehouse using the caller's OBO token (Unity Catalog perms enforced). The
  response carries ``ai_available`` so the UI knows whether to offer the
  ask-a-question box.
- ``POST /table-data/query`` translates a natural-language question into a safe
  read-only SELECT via the app's AI gateway, runs it, and returns the rows.
  Degrades cleanly (503/429/502/400) exactly like the other AI routes so the
  tab can fall back to the plain preview.
- ``GET /table-data/sample-questions`` asks the AI gateway for 3 short,
  schema-grounded example questions to seed the ask-a-question chips. Purely
  decorative, so every failure mode (AI off, rate limit, malformed output,
  schema fetch error) returns an empty list — never an error status — and the
  UI falls back to its static prompts.

See ``services/table_data_service.py`` for the Genie-vs-LLM decision rationale.
"""

from __future__ import annotations

from typing import Annotated

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import get_discovery_service, get_table_data_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.services.table_data_service import PreviewResult, TableDataService

# Non-VIEWER by default is not required; UC OBO perms are the real data boundary.
# Any authenticated user who can see the monitored table can preview its data
# subject to their own UC grants (the query runs as them).
router = APIRouter()


class TablePreviewIn(BaseModel):
    table_fqn: str


class TableQueryIn(BaseModel):
    table_fqn: str
    question: str


class SampleQuestionsOut(BaseModel):
    questions: list[str]


class TableDataOut(BaseModel):
    columns: list[str]
    rows: list[dict[str, str | None]]
    row_count: int
    truncated: bool
    generated_sql: str | None = None
    ai_available: bool = False


def _to_out(result: PreviewResult, *, ai_available: bool) -> TableDataOut:
    return TableDataOut(
        columns=result.columns,
        rows=result.rows,
        row_count=len(result.rows),
        truncated=result.truncated,
        generated_sql=result.generated_sql,
        ai_available=ai_available,
    )


@router.post("/preview", response_model=TableDataOut, operation_id="previewTableData")
async def preview_table_data(
    body: TablePreviewIn,
    svc: Annotated[TableDataService, Depends(get_table_data_service)],
) -> TableDataOut:
    """Return the first 500 rows of a table (OBO — UC permissions enforced)."""
    try:
        result = await svc.preview(body.table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to preview table data: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail="Could not load table data. Check the SQL warehouse and your access.")
    return _to_out(result, ai_available=svc.ai_available())


@router.get("/sample-questions", response_model=SampleQuestionsOut, operation_id="getSampleQuestions")
async def get_sample_questions(
    table_fqn: Annotated[str, Query(description="Fully qualified table name (catalog.schema.table)")],
    svc: Annotated[TableDataService, Depends(get_table_data_service)],
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> SampleQuestionsOut:
    """Suggest 3 schema-grounded example questions for the ask-a-question chips.

    Same access model as the ask endpoint above: any authenticated user; the
    schema read runs OBO so Unity Catalog permissions are the real boundary.
    Decorative endpoint — every failure degrades to an empty list (the UI then
    shows its static prompts), and raw errors are never relayed (OWASP LLM06).
    """
    if not svc.ai_available():
        return SampleQuestionsOut(questions=[])
    try:
        parts = table_fqn.split(".")
        if len(parts) != 3:
            return SampleQuestionsOut(questions=[])
        # Same OBO service path the About tab's schema section uses — cached,
        # and enforcing the caller's own UC grants on the metadata read.
        columns = await discovery.get_table_columns_async(*parts)
        questions = await svc.sample_questions(table_fqn, columns, user_email)
    except Exception as e:
        logger.warning("Sample-question generation failed: %s", e, exc_info=True)
        return SampleQuestionsOut(questions=[])
    return SampleQuestionsOut(questions=questions)


@router.post("/query", response_model=TableDataOut, operation_id="queryTableData")
async def query_table_data(
    body: TableQueryIn,
    svc: Annotated[TableDataService, Depends(get_table_data_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> TableDataOut:
    """Answer a natural-language question by generating + running a safe SELECT."""
    try:
        result = await svc.query(body.table_fqn, body.question, user_email)
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except UnsafeSqlQueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Never relay raw errors — they can echo schema/data (OWASP LLM06).
        logger.error("Failed to run View Data query: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail="Could not run that query. Try rephrasing your question.")
    return _to_out(result, ai_available=True)
