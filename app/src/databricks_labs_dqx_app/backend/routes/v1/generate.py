from typing import Annotated

import yaml
from databricks.labs.dqx.engine import DQEngine
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_ai_rules_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import GenerateChecksIn, GenerateChecksOut
from databricks_labs_dqx_app.backend.services.ai_rules_service import AiRulesService

router = APIRouter()

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.post(
    "/generate-checks",
    response_model=GenerateChecksOut,
    operation_id="ai_assisted_checks_generation",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def ai_generate_checks(
    body: GenerateChecksIn,
    service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
) -> GenerateChecksOut:
    """Generate data quality checks from natural language using AI-assisted generation."""
    try:
        checks = service.generate(user_input=body.user_input, table_fqn=body.table_fqn)
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
    except Exception as e:
        logger.error(f"Failed to generate checks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate checks: {str(e)}")
