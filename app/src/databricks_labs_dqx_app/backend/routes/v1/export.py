"""Export routes — download registry rules / monitored tables / table spaces as YAML.

Two formats (see :mod:`~databricks_labs_dqx_app.backend.services.export_service`):

* ``dqx`` — a DQX check-list YAML (re-importable into the registry).
* ``odcs`` — an ODCS v3 DataContract (monitored tables + table spaces only;
  the table-less rule registry has no ``physicalName`` to bind to).

Every endpoint is a read; all roles (incl. viewers) may export. The rendered
YAML is returned as an :class:`ExportOut` envelope and the frontend triggers
the browser download.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_export_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import ExportOut
from databricks_labs_dqx_app.backend.services.export_service import ExportError, ExportFormat, ExportResult, ExportService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]

# Both output formats for the table-bound surfaces; the registry is DQX-only.
_FormatQuery = Annotated[ExportFormat, Query(description="Export format: 'dqx' or 'odcs'.")]


def _to_out(result: ExportResult) -> ExportOut:
    return ExportOut(filename=result.filename, content=result.content, format=result.format)


# ------------------------------------------------------------------
# Rule Registry (DQX only)
# ------------------------------------------------------------------


@router.get(
    "/registry-rules",
    response_model=ExportOut,
    operation_id="exportRegistryRules",
    dependencies=[require_role(*_ALL_ROLES)],
)
def export_registry_rules(
    svc: Annotated[ExportService, Depends(get_export_service)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
    dimension: Annotated[str | None, Query(description="Filter by the 'dimension' tag")] = None,
    severity: Annotated[str | None, Query(description="Filter by the 'severity' tag")] = None,
    steward: Annotated[str | None, Query(description="Filter by steward")] = None,
    tag: Annotated[str | None, Query(description="Filter by presence of a free-text tag key")] = None,
    rule_id: Annotated[
        list[str] | None,
        Query(description="Restrict export to this explicit set of rule ids (repeatable)"),
    ] = None,
) -> ExportOut:
    """Export all (filtered) registry rules as a DQX check-list YAML."""
    try:
        return _to_out(
            svc.export_registry_rules(
                status=status,
                dimension=dimension,
                severity=severity,
                steward=steward,
                tag=tag,
                rule_ids=rule_id,
            )
        )
    except Exception as e:
        logger.error(f"Failed to export registry rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export registry rules.")


@router.get(
    "/registry-rules/{rule_id}",
    response_model=ExportOut,
    operation_id="exportRegistryRule",
    dependencies=[require_role(*_ALL_ROLES)],
)
def export_registry_rule(
    rule_id: str,
    svc: Annotated[ExportService, Depends(get_export_service)],
) -> ExportOut:
    """Export a single registry rule as a DQX check-list YAML."""
    try:
        return _to_out(svc.export_registry_rule(rule_id))
    except ExportError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export registry rule.")


# ------------------------------------------------------------------
# Monitored tables (DQX or ODCS)
# ------------------------------------------------------------------


@router.get(
    "/monitored-tables",
    response_model=ExportOut,
    operation_id="exportMonitoredTables",
    dependencies=[require_role(*_ALL_ROLES)],
)
def export_monitored_tables(
    svc: Annotated[ExportService, Depends(get_export_service)],
    format: _FormatQuery = "dqx",
    status: Annotated[str | None, Query(description="Filter by status")] = None,
    steward: Annotated[str | None, Query(description="Filter by steward")] = None,
    catalog: Annotated[str | None, Query(description="Filter by catalog")] = None,
    schema: Annotated[str | None, Query(description="Filter by schema")] = None,
    name: Annotated[str | None, Query(description="Filter by table name")] = None,
    binding_id: Annotated[
        list[str] | None,
        Query(description="Restrict export to these binding ids (selection action bar)"),
    ] = None,
) -> ExportOut:
    """Export all (filtered) monitored tables' checks as DQX or ODCS YAML."""
    try:
        return _to_out(
            svc.export_monitored_tables(
                format,
                status=status,
                steward=steward,
                catalog=catalog,
                schema=schema,
                name=name,
                binding_ids=binding_id,
            )
        )
    except Exception as e:
        logger.error(f"Failed to export monitored tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export monitored tables.")


@router.get(
    "/monitored-tables/{binding_id}",
    response_model=ExportOut,
    operation_id="exportMonitoredTable",
    dependencies=[require_role(*_ALL_ROLES)],
)
def export_monitored_table(
    binding_id: str,
    svc: Annotated[ExportService, Depends(get_export_service)],
    format: _FormatQuery = "dqx",
) -> ExportOut:
    """Export a single monitored table's checks as DQX or ODCS YAML."""
    try:
        return _to_out(svc.export_monitored_table(binding_id, format))
    except ExportError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export monitored table.")


# ------------------------------------------------------------------
# Table spaces / data products (DQX or ODCS)
# ------------------------------------------------------------------


@router.get(
    "/data-products",
    response_model=ExportOut,
    operation_id="exportDataProducts",
    dependencies=[require_role(*_ALL_ROLES)],
)
def export_data_products(
    svc: Annotated[ExportService, Depends(get_export_service)],
    format: _FormatQuery = "dqx",
    product_id: Annotated[
        list[str] | None,
        Query(description="Restrict export to these product ids (selection action bar)"),
    ] = None,
) -> ExportOut:
    """Export every (filtered) table space's member checks as DQX or ODCS YAML."""
    try:
        return _to_out(svc.export_data_products(format, product_ids=product_id))
    except Exception as e:
        logger.error(f"Failed to export table spaces: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export table spaces.")


@router.get(
    "/data-products/{product_id}",
    response_model=ExportOut,
    operation_id="exportDataProduct",
    dependencies=[require_role(*_ALL_ROLES)],
)
def export_data_product(
    product_id: str,
    svc: Annotated[ExportService, Depends(get_export_service)],
    format: _FormatQuery = "dqx",
) -> ExportOut:
    """Export a single table space (all member tables) as DQX or ODCS YAML."""
    try:
        return _to_out(svc.export_data_product(product_id, format))
    except ExportError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export table space {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export table space.")
