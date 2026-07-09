"""AI-assisted rule authoring routes (Rules Registry Phase 4A) — aiGenerateRule / aiSuggestField.

Both routes go through :class:`~databricks_labs_dqx_app.backend.services.ai_gateway.AIGateway`
(via :class:`~databricks_labs_dqx_app.backend.services.ai_rules_service.AiRulesService`), so
they degrade cleanly when AI is disabled or unconfigured: 503 (unavailable), 429 (rate
limit), 502 (unparsable model output), 422 (no valid/safe rule could be produced) — never a
bare 500 for those expected conditions.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import get_ai_rules_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    AiExplainSqlIn,
    AiExplainSqlOut,
    AiGenerateRuleIn,
    AiGenerateRuleOut,
    AiImproveSqlIn,
    AiSqlOut,
    AiSuggestFieldIn,
    AiSuggestFieldOut,
    AiWriteSqlIn,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.ai_rules_service import AiRulesService

router = APIRouter()

# Rule authoring roles — data stewards are the RULE_AUTHOR role in this app's RBAC model
# (see app/CLAUDE.md's persona table); approvers and admins can author too.
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.post(
    "/generate-rule",
    response_model=AiGenerateRuleOut,
    operation_id="aiGenerateRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def ai_generate_rule(
    body: AiGenerateRuleIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> AiGenerateRuleOut:
    """Generate a full, DQX-validated Rules Registry rule proposal from a description."""
    try:
        proposal = await service.generate_rule(
            description=body.description,
            user_email=user_email,
            table_fqn=body.table_fqn,
            columns=body.columns,
            sample_rows=body.sample_rows,
        )
        return AiGenerateRuleOut(**proposal)
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        # Never relay the raw exception to the client — it can echo back prompt,
        # schema, or data-sample content, or internal identifiers/stack detail
        # (OWASP LLM06). Log the detail server-side; return a generic message.
        logger.error(f"Failed to generate AI rule proposal: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate AI rule proposal.")


@router.post(
    "/suggest-field",
    response_model=AiSuggestFieldOut,
    operation_id="aiSuggestField",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def ai_suggest_field(
    body: AiSuggestFieldIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> AiSuggestFieldOut:
    """Suggest a value for a single rule field (name/description/dimension/severity)."""
    try:
        value = await service.suggest_field(field=body.field, context=body.context, user_email=user_email)
        return AiSuggestFieldOut(value=value)
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        # See ai_generate_rule above — same OWASP LLM06 rationale.
        logger.error(f"Failed to generate AI field suggestion: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate AI field suggestion.")


@router.post(
    "/write-sql",
    response_model=AiSqlOut,
    operation_id="aiWriteSql",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def ai_write_sql(
    body: AiWriteSqlIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> AiSqlOut:
    """Write a SQL predicate for a rule from a natural-language description (validated safe)."""
    try:
        result = await service.write_sql(
            description=body.description,
            user_email=user_email,
            columns=body.columns,
            table_fqn=body.table_fqn,
        )
        return AiSqlOut(**result)
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        # See ai_generate_rule above — same OWASP LLM06 rationale.
        logger.error(f"Failed to write AI SQL predicate: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to write AI SQL predicate.")


@router.post(
    "/improve-sql",
    response_model=AiSqlOut,
    operation_id="aiImproveSql",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def ai_improve_sql(
    body: AiImproveSqlIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> AiSqlOut:
    """Refine an existing SQL predicate per a free-text instruction (validated safe)."""
    try:
        result = await service.improve_sql(
            predicate=body.predicate,
            instruction=body.instruction,
            user_email=user_email,
            columns=body.columns,
        )
        return AiSqlOut(**result)
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        # See ai_generate_rule above — same OWASP LLM06 rationale.
        logger.error(f"Failed to improve AI SQL predicate: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to improve AI SQL predicate.")


@router.post(
    "/explain-sql",
    response_model=AiExplainSqlOut,
    operation_id="aiExplainSql",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def ai_explain_sql(
    body: AiExplainSqlIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> AiExplainSqlOut:
    """Explain a SQL predicate in plain language."""
    try:
        explanation = await service.explain_sql(predicate=body.predicate, user_email=user_email)
        return AiExplainSqlOut(explanation=explanation)
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AIResponseParseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        # See ai_generate_rule above — same OWASP LLM06 rationale.
        logger.error(f"Failed to explain AI SQL predicate: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to explain AI SQL predicate.")
