from typing import Annotated

import yaml
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.dependencies import get_generator
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import GenerateChecksIn, GenerateChecksOut

router = APIRouter()


@router.post("/generate-checks", response_model=GenerateChecksOut, operation_id="ai_assisted_checks_generation")
def ai_generate_checks(
    body: GenerateChecksIn,
    generator: Annotated[DQGenerator, Depends(get_generator)],
) -> GenerateChecksOut:
    """Generate data quality checks from natural language using AI-assisted generation."""
    try:
        input_config = InputConfig(location=body.table_fqn) if body.table_fqn else None
        checks = generator.generate_dq_rules_ai_assisted(
            user_input=body.user_input,
            input_config=input_config,
        )
        yaml_output = yaml.dump(checks, default_flow_style=False, sort_keys=False)

        # Validate generated checks
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
