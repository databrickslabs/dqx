from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_check_validator,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ValidateChecksIn,
    ValidateChecksOut,
)

router = APIRouter()

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.post(
    "/validate-checks",
    response_model=ValidateChecksOut,
    operation_id="validateChecks",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def validate_checks(
    body: ValidateChecksIn,
    validate_fn: Annotated[Callable[[list[Any]], ChecksValidationStatus], Depends(get_check_validator)],
) -> ValidateChecksOut:
    """Validate a list of check definitions without saving them."""
    try:
        result = validate_fn(body.checks)
        return ValidateChecksOut(valid=not result.has_errors, errors=result.errors if result.has_errors else [])
    except Exception as e:
        logger.error("Check validation failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")
