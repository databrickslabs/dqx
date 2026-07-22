"""Export registry rules / monitored tables / table spaces to YAML.

Two output formats, both text (the frontend downloads the returned string as a
file):

* ``dqx`` — a list of DQX check dicts, the SAME shape the runner consumes and
  the import flow accepts. A single check dict round-trips through
  ``ui/lib/import-registry-rules`` (``normalizeImportedCheck`` +
  ``parseDqxCheckJson``), so a DQX export can be re-imported into the registry.
  For a table it is the column-substituted, runner-ready check list produced by
  the :class:`~databricks_labs_dqx_app.backend.services.materializer.Materializer`.

* ``odcs`` — an ODCS v3 ``DataContract`` with one ``schema`` entry per table
  (``physicalName`` = the full 3-part UC FQN), each carrying its DQX checks
  under a table-level ``quality[]`` list as
  ``{type: custom, engine: dqx, implementation: <check>}`` — the exact shape
  the Bulk Contract Import reads back. ODCS needs a real table binding, so it
  is offered for monitored tables and table spaces only, never the (table-less)
  rule registry.

Serialization is kept as pure module functions (unit-testable without any DB)
and orchestrated by :class:`ExportService`, which pulls the domain data through
the existing registry / monitored-table / data-product services and the
materializer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

import yaml

from databricks_labs_dqx_app.backend.registry_models import (
    RegistryRule,
    get_rule_name,
    get_rule_severity,
    resolve_criticality,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService

ExportFormat = Literal["dqx", "odcs"]

_ODCS_API_VERSION = "v3.0.2"


class ExportError(Exception):
    """A requested export target does not exist or the format is unsupported."""


@dataclass
class ExportResult:
    """The rendered export payload the route hands back to the client."""

    filename: str
    content: str
    format: str


# ---------------------------------------------------------------------------
# Pure serialization helpers (no DB access — unit-testable in isolation)
# ---------------------------------------------------------------------------


def registry_rule_to_check_dict(rule: RegistryRule, app_settings: AppSettingsService) -> dict[str, Any]:
    """Render a LIVE registry rule template to a DQX check dict.

    The Python mirror of the frontend ``buildDqxCheckJson`` (kept in sync with
    ``materializer.render_check``, minus column substitution): a registry rule
    is table-agnostic, so ``{{slot}}`` placeholders are preserved verbatim.
    """
    definition = rule.definition
    body: dict[str, Any] = definition.body or {}
    parameters = definition.parameters or []

    if rule.mode == "dqx_native":
        arguments: dict[str, Any] = dict(body.get("arguments") or {})
        for param in parameters:
            if param.value is not None:
                arguments[param.name] = param.value
        # A native check that supports ``negate`` carries polarity as the
        # injected boolean, matching render_check so the export is faithful.
        if rule.polarity is not None:
            arguments["negate"] = rule.polarity == "fail"
        check_inner: dict[str, Any] = {"function": str(body.get("function", "")), "arguments": arguments}
    else:
        negate = rule.polarity == "fail"
        arguments = {"negate": negate}
        sql_query = body.get("sql_query")
        if isinstance(sql_query, str):
            function_name = "sql_query"
            arguments["query"] = sql_query
            merge_columns = body.get("merge_columns")
            if isinstance(merge_columns, list) and merge_columns:
                arguments["merge_columns"] = merge_columns
        else:
            function_name = "sql_expression"
            predicate = body.get("predicate")
            arguments["expression"] = predicate if isinstance(predicate, str) else ""
        for param in parameters:
            if param.value is not None:
                arguments[param.name] = param.value
        check_inner = {"function": function_name, "arguments": arguments}

    severity = get_rule_severity(rule.user_metadata)
    check: dict[str, Any] = {
        "criticality": resolve_criticality(severity, app_settings),
        "check": check_inner,
        "user_metadata": dict(rule.user_metadata or {}),
    }
    name = get_rule_name(rule.user_metadata)
    if name:
        check["name"] = name
    if definition.error_message:
        check["message_expr"] = definition.error_message
    return check


def dump_dqx_yaml(checks: list[dict[str, Any]]) -> str:
    """Serialize a list of DQX check dicts to YAML (insertion order preserved)."""
    return yaml.dump(
        checks,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=100,
    )


def _slugify(value: str) -> str:
    """A filesystem/URN-safe slug: lowercase alphanumerics + single hyphens."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "export"


def build_odcs_contract(
    *,
    name: str,
    tables: list[tuple[str, list[dict[str, Any]]]],
    description: str | None = None,
) -> dict[str, Any]:
    """Build an ODCS v3 ``DataContract`` dict from ``(table_fqn, checks)`` pairs.

    Each table becomes one ``schema`` entry with its checks under a table-level
    ``quality[]`` list (``type: custom``, ``engine: dqx``, ``implementation`` =
    the DQX check dict) — the shape the contract-import reader accepts.
    """
    schema: list[dict[str, Any]] = []
    for table_fqn, checks in tables:
        short_name = table_fqn.split(".")[-1] if table_fqn else table_fqn
        entry: dict[str, Any] = {
            "name": short_name,
            "physicalName": table_fqn,
            "physicalType": "table",
        }
        if checks:
            entry["quality"] = [
                {"type": "custom", "engine": "dqx", "implementation": check} for check in checks
            ]
        schema.append(entry)

    contract: dict[str, Any] = {
        "kind": "DataContract",
        "apiVersion": _ODCS_API_VERSION,
        "id": f"urn:datacontract:dqx:{_slugify(name)}",
        "name": name,
        "version": "1.0.0",
        "status": "draft",
    }
    if description:
        contract["description"] = {"purpose": description}
    contract["schema"] = schema
    return contract


def dump_odcs_yaml(contract: dict[str, Any]) -> str:
    """Serialize an ODCS contract dict to YAML (top-level key order preserved)."""
    return yaml.dump(
        contract,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=100,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


class ExportService:
    """Assemble export payloads from the registry / table / product services."""

    def __init__(
        self,
        *,
        registry: RegistryService,
        app_settings: AppSettingsService,
        materializer: Materializer,
        monitored_tables: MonitoredTableService,
        data_products: DataProductService,
    ) -> None:
        self._registry = registry
        self._app_settings = app_settings
        self._materializer = materializer
        self._monitored_tables = monitored_tables
        self._data_products = data_products

    # -- Registry (DQX only) ------------------------------------------------

    def export_registry_rules(
        self,
        *,
        status: str | None = None,
        dimension: str | None = None,
        severity: str | None = None,
        steward: str | None = None,
        tag: str | None = None,
        rule_ids: list[str] | None = None,
    ) -> ExportResult:
        """Export all (filtered) registry rules as a DQX check-list YAML.

        ``rule_ids``, when given, restricts the export to that explicit set —
        used by the overview's selection action bar to export only the ticked
        rows. Combines with the other filters (all are AND-ed).
        """
        rules = self._registry.list_rules(
            status=status,
            dimension=dimension,
            severity=severity,
            steward=steward,
            tag=tag,
            rule_ids=rule_ids,
        )
        checks = [registry_rule_to_check_dict(rule, self._app_settings) for rule in rules]
        return ExportResult(
            filename="registry_rules.dqx.yaml",
            content=dump_dqx_yaml(checks),
            format="dqx",
        )

    def export_registry_rule(self, rule_id: str) -> ExportResult:
        """Export a single registry rule as a DQX check-list YAML."""
        result = self._registry.get_rule_with_version(rule_id)
        if result is None:
            raise ExportError(f"Registry rule not found: {rule_id}")
        rule, _version = result
        check = registry_rule_to_check_dict(rule, self._app_settings)
        name = get_rule_name(rule.user_metadata) or rule.rule_id
        return ExportResult(
            filename=f"rule_{_slugify(name)}.dqx.yaml",
            content=dump_dqx_yaml([check]),
            format="dqx",
        )

    # -- Monitored tables (DQX or ODCS) -------------------------------------

    def _render_binding(self, binding_id: str) -> list[dict[str, Any]]:
        """Render one binding's live applied rules to runner-shaped check dicts."""
        try:
            return self._materializer.render_binding_checks(binding_id)
        except MaterializationError:
            return []

    def export_monitored_table(self, binding_id: str, fmt: ExportFormat) -> ExportResult:
        """Export a single monitored table's checks as DQX or ODCS YAML."""
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise ExportError(f"Monitored table not found: {binding_id}")
        table_fqn = detail.table.table_fqn
        checks = self._render_binding(binding_id)
        short = table_fqn.split(".")[-1] if table_fqn else binding_id
        if fmt == "odcs":
            contract = build_odcs_contract(name=table_fqn, tables=[(table_fqn, checks)])
            return ExportResult(
                filename=f"{_slugify(short)}.odcs.yaml",
                content=dump_odcs_yaml(contract),
                format="odcs",
            )
        return ExportResult(
            filename=f"{_slugify(short)}.dqx.yaml",
            content=dump_dqx_yaml(checks),
            format="dqx",
        )

    def export_monitored_tables(
        self,
        fmt: ExportFormat,
        *,
        status: str | None = None,
        steward: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        name: str | None = None,
        binding_ids: list[str] | None = None,
    ) -> ExportResult:
        """Export all (filtered) monitored tables' checks as DQX or ODCS YAML.

        ``binding_ids``, when given, restricts the export to that explicit set —
        the selection action bar passes exactly the ticked rows, mirroring
        ``export_registry_rules(rule_ids=...)``.
        """
        summaries = self._monitored_tables.list_monitored_tables(
            status=status, steward=steward, catalog=catalog, schema=schema, name=name
        )
        if binding_ids is not None:
            wanted = set(binding_ids)
            summaries = [s for s in summaries if s.table.binding_id in wanted]
        per_table: list[tuple[str, list[dict[str, Any]]]] = [
            (s.table.table_fqn, self._render_binding(s.table.binding_id)) for s in summaries
        ]
        if fmt == "odcs":
            contract = build_odcs_contract(name="Monitored tables", tables=per_table)
            return ExportResult(
                filename="monitored_tables.odcs.yaml",
                content=dump_odcs_yaml(contract),
                format="odcs",
            )
        # DQX: a single flat, runner-shaped check list across every table.
        checks = [check for _fqn, table_checks in per_table for check in table_checks]
        return ExportResult(
            filename="monitored_tables.dqx.yaml",
            content=dump_dqx_yaml(checks),
            format="dqx",
        )

    # -- Table spaces / data products (DQX or ODCS) -------------------------

    def export_data_product(self, product_id: str, fmt: ExportFormat) -> ExportResult:
        """Export one table space (all member tables' checks) as DQX or ODCS YAML."""
        detail = self._data_products.get(product_id)
        if detail is None:
            raise ExportError(f"Table space not found: {product_id}")
        per_table = [(m.table_fqn, self._render_binding(m.binding_id)) for m in detail.members]
        space_name = detail.product.name
        if fmt == "odcs":
            contract = build_odcs_contract(
                name=space_name, tables=per_table, description=detail.product.description
            )
            return ExportResult(
                filename=f"{_slugify(space_name)}.odcs.yaml",
                content=dump_odcs_yaml(contract),
                format="odcs",
            )
        checks = [check for _fqn, table_checks in per_table for check in table_checks]
        return ExportResult(
            filename=f"{_slugify(space_name)}.dqx.yaml",
            content=dump_dqx_yaml(checks),
            format="dqx",
        )

    def export_data_products(
        self, fmt: ExportFormat, *, product_ids: list[str] | None = None
    ) -> ExportResult:
        """Export every (filtered) table space's member checks as DQX or ODCS YAML.

        ``product_ids``, when given, restricts the export to that explicit set —
        the selection action bar passes exactly the ticked rows, mirroring
        ``export_registry_rules(rule_ids=...)``.
        """
        products = self._data_products.list_products()
        if product_ids is not None:
            wanted = set(product_ids)
            products = [d for d in products if d.product.product_id in wanted]
        per_table: list[tuple[str, list[dict[str, Any]]]] = []
        for detail in products:
            for member in detail.members:
                per_table.append((member.table_fqn, self._render_binding(member.binding_id)))
        if fmt == "odcs":
            contract = build_odcs_contract(name="Table spaces", tables=per_table)
            return ExportResult(
                filename="table_spaces.odcs.yaml",
                content=dump_odcs_yaml(contract),
                format="odcs",
            )
        checks = [check for _fqn, table_checks in per_table for check in table_checks]
        return ExportResult(
            filename="table_spaces.dqx.yaml",
            content=dump_dqx_yaml(checks),
            format="dqx",
        )
