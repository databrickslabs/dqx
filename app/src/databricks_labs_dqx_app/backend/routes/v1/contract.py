"""ODCS data-contract → DQX rule generation endpoint.

Mirrors the AI-assisted generation router (`/v1/ai`) shape but for
contract-based generation. Read-only — the user picks a UC target per
schema in the UI and then saves through the existing
``POST /v1/rules`` flow.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_contract_rules_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ContractMetadataOut,
    ContractSchemaRulesOut,
    GenerateRulesFromContractIn,
    GenerateRulesFromContractOut,
)
from databricks_labs_dqx_app.backend.services.contract_rules_service import (
    ContractGenerationResult,
    ContractRulesService,
)

router = APIRouter()

_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.post(
    "/generate-rules",
    response_model=GenerateRulesFromContractOut,
    operation_id="generateRulesFromContract",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def generate_rules_from_contract(
    body: GenerateRulesFromContractIn,
    service: Annotated[ContractRulesService, Depends(get_contract_rules_service)],
) -> GenerateRulesFromContractOut:
    """Parse an ODCS v3.x contract and return generated DQX rules grouped per schema."""
    if body.default_criticality not in ("error", "warn"):
        raise HTTPException(
            status_code=400,
            detail="default_criticality must be 'error' or 'warn'",
        )
    try:
        result = service.generate(
            contract_text=body.contract_text,
            generate_predefined_rules=body.generate_predefined_rules,
            process_text_rules=body.process_text_rules,
            generate_schema_validation=body.generate_schema_validation,
            strict_schema_validation=body.strict_schema_validation,
            default_criticality=body.default_criticality,
        )
        return _to_out(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        # Missing extras or generator runtime errors — surface as 500.
        logger.error("Contract generation runtime error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:  # pragma: no cover - defensive guard
        # Don't relay the raw exception text — it may carry LLM/SDK detail or
        # internal structure (AGENTS.md LLM06 / CWE-209). Log it server-side.
        logger.error("Failed to generate rules from contract: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate rules from contract.")


def _to_out(result: ContractGenerationResult) -> GenerateRulesFromContractOut:
    return GenerateRulesFromContractOut(
        metadata=ContractMetadataOut(
            contract_id=result.metadata.contract_id,
            name=result.metadata.name,
            version=result.metadata.version,
            odcs_api_version=result.metadata.odcs_api_version,
            status=result.metadata.status,
            owner=result.metadata.owner,
            domain=result.metadata.domain,
            description=result.metadata.description,
        ),
        schemas=[
            ContractSchemaRulesOut(
                schema_name=s.schema_name,
                physical_name=s.physical_name,
                property_count=s.property_count,
                rules=s.rules,
            )
            for s in result.schemas
        ],
        unassigned_rules=result.unassigned_rules,
        total_rules=result.total_rules,
        warnings=result.warnings,
        validation_errors=result.validation_errors,
    )
