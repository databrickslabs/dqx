from typing import Annotated

import yaml
from databricks.labs.dqx.engine import DQEngine
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import get_ai_rules_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import GenerateChecksIn, GenerateChecksOut
from databricks_labs_dqx_app.backend.services.ai_gateway import AIRateLimitExceededError, AIUnavailableError
from databricks_labs_dqx_app.backend.services.ai_rules_service import AiRulesService

router = APIRouter()

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.post(
    "/generate-checks",
    response_model=GenerateChecksOut,
    operation_id="aiAssistedChecksGeneration",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def ai_generate_checks(
    body: GenerateChecksIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
    user_email: Annotated[str, Depends(get_user_email)],
) -> GenerateChecksOut:
    """Generate data quality checks from natural language using AI-assisted generation.

    Routed through :class:`~databricks_labs_dqx_app.backend.services.ai_gateway.AIGateway`
    (kill-switch, per-user rate limit, audit log — Rules Registry design spec §8) rather than
    calling the model directly. Degrades cleanly: AI disabled/unconfigured returns a 503, and
    an exhausted per-user quota returns a 429 — never a 500.
    """
    try:
        checks = await service.generate_checks_via_gateway(
            user_input=body.user_input, user_email=user_email, table_fqn=body.table_fqn
        )
        yaml_output = yaml.dump(checks, default_flow_style=False, sort_keys=False)

        validation_errors: list[str] = []
        validation = DQEngine.validate_checks(checks)
        if validation.has_errors:
            validation_errors = validation.errors

        return GenerateChecksOut(
            yaml_output=yaml_output,
            checks=checks,
            validation_errors=validation_errors,
        )
    except AIUnavailableError as e:
        raise HTTPException(status_code=503, detail=e.reason)
    except AIRateLimitExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to generate checks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate checks: {str(e)}")
