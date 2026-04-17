from __future__ import annotations

import json
from collections.abc import Callable
from typing import Annotated, Any

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_check_validator,
    get_obo_ws,
    get_rules_catalog_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ImportFromTableIn,
    ImportFromTableOut,
    RuleCatalogEntryOut,
    ValidateChecksIn,
    ValidateChecksOut,
)
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

router = APIRouter()

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


_SQL_CHECK_PREFIX = "__sql_check__/"


def _entry_to_out(entry) -> RuleCatalogEntryOut:
    fqn = entry.table_fqn
    return RuleCatalogEntryOut(
        table_fqn=fqn,
        display_name=fqn[len(_SQL_CHECK_PREFIX) :] if fqn.startswith(_SQL_CHECK_PREFIX) else fqn,
        checks=entry.checks,
        version=entry.version,
        status=entry.status,
        source=entry.source,
        rule_id=entry.rule_id,
        created_by=entry.created_by,
        created_at=entry.created_at,
        updated_by=entry.updated_by,
        updated_at=entry.updated_at,
    )


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


@router.post(
    "/import-from-table",
    response_model=ImportFromTableOut,
    operation_id="importRulesFromTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def import_rules_from_table(
    body: ImportFromTableIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    validate_fn: Annotated[Callable[[list[Any]], ChecksValidationStatus], Depends(get_check_validator)],
) -> ImportFromTableOut:
    """Read rules from a Delta table (same schema as dq_quality_rules) and import them."""
    from databricks_labs_dqx_app.backend.sql_utils import validate_fqn

    user = obo_ws.current_user.me()
    user_email = user.user_name or "unknown"

    try:
        validate_fqn(body.source_table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        rows = svc.read_external_rules_table(body.source_table_fqn)
    except Exception as e:
        logger.error("Failed to read source table %s: %s", body.source_table_fqn, e, exc_info=True)
        raise HTTPException(status_code=400, detail=f"Failed to read source table: {e}")

    imported = 0
    skipped = 0
    errors: list[dict[str, str]] = []

    for row in rows:
        table_fqn = row.get("table_fqn", "")
        checks_raw = row.get("checks", "[]")

        if not table_fqn:
            skipped += 1
            continue

        try:
            checks = json.loads(checks_raw) if isinstance(checks_raw, str) else checks_raw
        except json.JSONDecodeError as e:
            skipped += 1
            errors.append({"table_fqn": table_fqn, "error": f"Invalid JSON in checks column: {e}"})
            continue

        validation = validate_fn(checks)
        if validation.has_errors:
            skipped += 1
            errors.append({"table_fqn": table_fqn, "error": f"Validation errors: {validation.errors}"})
            continue

        try:
            svc.save(table_fqn, checks, user_email, source="imported")
            imported += 1
        except Exception as e:
            skipped += 1
            errors.append({"table_fqn": table_fqn, "error": str(e)})

    return ImportFromTableOut(imported=imported, skipped=skipped, errors=errors)
