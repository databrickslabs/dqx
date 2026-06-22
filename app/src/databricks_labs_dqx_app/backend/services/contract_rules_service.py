"""Generate DQX quality rules from ODCS v3.x data contracts.

Thin wrapper around DQX's :class:`DataContractRulesGenerator`. We do **not**
go through :class:`DQGenerator`, because that class eagerly calls
``SparkSession.builder.getOrCreate()`` in its constructor — fine on a
Databricks cluster, fatal inside a stateless Databricks App container that
has no Spark runtime.

Generated rules are returned untouched from DQX. We additionally bucket
them by ODCS ``schema`` (extracted from ``user_metadata.schema``) so the
UI can let the user assign each ODCS schema to a Unity Catalog table
before saving — most contracts describe the data product abstractly and
don't carry a fully-qualified UC name.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from databricks.sdk import WorkspaceClient

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.services.ai_rules_service import AiRulesService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ContractMetadata:
    """Subset of ODCS top-level fields surfaced in the UI."""

    contract_id: str | None
    name: str | None
    version: str | None
    odcs_api_version: str | None
    status: str | None
    owner: str | None
    domain: str | None
    description: str | None


@dataclass(frozen=True)
class ContractSchemaRules:
    """One ODCS schema and the rules generated for it."""

    schema_name: str
    physical_name: str | None
    property_count: int
    rules: list[dict[str, Any]]


@dataclass(frozen=True)
class ContractGenerationResult:
    metadata: ContractMetadata
    schemas: list[ContractSchemaRules]
    unassigned_rules: list[dict[str, Any]]
    total_rules: int
    warnings: list[str]


@dataclass(frozen=True)
class _TextExpectation:
    """One ODCS ``type: text`` quality expectation to feed to the LLM."""

    schema_name: str
    field: str | None
    description: str
    schema_info: str  # JSON string of the schema's columns for LLM context


class ContractRulesService:
    """Generate DQX rules from a raw ODCS contract YAML/JSON string."""

    def __init__(
        self,
        sp_ws: WorkspaceClient,
        ai_service: "AiRulesService | None" = None,
    ) -> None:
        # Service principal client is sufficient: contract parsing is
        # local, no UC reads happen during predefined/explicit/schema
        # rule generation.
        self._ws = sp_ws
        # Optional AI service for natural-language (``type: text``) quality
        # expectations. DQX's own contract text-rule path requires ``dspy``
        # + a SparkSession (see DataContractRulesGenerator.__init__), neither
        # of which exists in the stateless app container — so we extract the
        # text expectations here and route them through the same
        # ChatDatabricks LLM leg the AI-Assisted Generation page uses.
        self._ai_service = ai_service

    def generate(
        self,
        contract_text: str,
        *,
        generate_predefined_rules: bool = True,
        process_text_rules: bool = False,
        generate_schema_validation: bool = True,
        strict_schema_validation: bool = True,
        default_criticality: str = "error",
    ) -> ContractGenerationResult:
        # Imports are local so a missing ``[datacontract]`` extra surfaces
        # as a clean 500 in this single endpoint instead of breaking app
        # boot.
        try:
            from datacontract.data_contract import DataContract  # type: ignore[import-untyped]
            from databricks.labs.dqx.datacontract.contract_rules_generator import (
                DataContractRulesGenerator,
            )
        except ImportError as exc:
            raise RuntimeError(
                "Data contract support is not installed. Install with " "'databricks-labs-dqx[datacontract]'."
            ) from exc

        if not contract_text.strip():
            raise ValueError("Contract text is empty")

        metadata, schema_index = self._parse_metadata_and_schemas(contract_text)

        contract = DataContract(data_contract_str=contract_text)
        generator = DataContractRulesGenerator(workspace_client=self._ws)

        warnings: list[str] = []

        # Predefined + explicit + schema-validation rules come straight from
        # DQX. ``process_text_rules=False`` here because DQX's text path needs
        # an LLM engine (dspy + Spark) we can't construct in-container; we
        # handle text expectations separately below via the app's LLM leg.
        rules = generator.generate_rules_from_contract(
            contract=contract,
            generate_predefined_rules=generate_predefined_rules,
            process_text_rules=False,
            generate_schema_validation=generate_schema_validation,
            strict_schema_validation=strict_schema_validation,
            default_criticality=default_criticality,
        )

        # Natural-language (``type: text``) expectations: run each through the
        # ChatDatabricks-backed AI service and tag the output as ``text_llm``
        # so it carries the same lineage metadata as DQX-native contract rules.
        if process_text_rules:
            text_rules, text_warnings = self._generate_text_rules(
                contract_text,
                metadata,
                default_criticality=default_criticality,
            )
            rules.extend(text_rules)
            warnings.extend(text_warnings)

        buckets, unassigned = self._bucket_rules_by_schema(rules, schema_index)
        return ContractGenerationResult(
            metadata=metadata,
            schemas=buckets,
            unassigned_rules=unassigned,
            total_rules=len(rules),
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Text / natural-language expectations
    # ------------------------------------------------------------------

    def _generate_text_rules(
        self,
        contract_text: str,
        metadata: ContractMetadata,
        *,
        default_criticality: str,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Generate DQX rules from the contract's ``type: text`` expectations.

        Returns ``(rules, warnings)``. Each generated rule is tagged with
        ``user_metadata`` mirroring DQX's contract lineage fields plus
        ``rule_type: text_llm`` and the original ``text_expectation`` so the
        UI groups and traces them exactly like DQX-native contract rules.
        """
        if self._ai_service is None:
            return [], [
                "Natural-language expectations were skipped: AI-Assisted generation "
                "is not configured on this deployment."
            ]

        expectations = self._extract_text_expectations(contract_text)
        if not expectations:
            return [], []

        rules: list[dict[str, Any]] = []
        warnings: list[str] = []
        for exp in expectations:
            try:
                generated = self._ai_service.generate_from_schema_info(
                    user_input=exp.description,
                    schema_info=exp.schema_info,
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.warning(
                    "Failed to generate text rule for schema '%s' field '%s': %s",
                    exp.schema_name,
                    exp.field,
                    exc,
                    exc_info=True,
                )
                warnings.append(
                    f"Could not generate a rule for the text expectation on " f"'{exp.field or exp.schema_name}': {exc}"
                )
                continue
            for rule in generated:
                if not isinstance(rule, dict):
                    continue
                user_metadata = {
                    "contract_id": metadata.contract_id or "unknown",
                    "contract_version": metadata.version or "unknown",
                    "odcs_version": metadata.odcs_api_version or "unknown",
                    "schema": exp.schema_name,
                    "rule_type": "text_llm",
                    "text_expectation": exp.description,
                    **(rule.get("user_metadata") or {}),
                }
                if exp.field:
                    user_metadata["field"] = exp.field
                rule["user_metadata"] = user_metadata
                rule.setdefault("criticality", default_criticality)
                rules.append(rule)
        if not rules and not warnings:
            warnings.append("No rules were generated from the contract's text expectations.")
        return rules, warnings

    @staticmethod
    def _extract_text_expectations(contract_text: str) -> list[_TextExpectation]:
        """Pull ODCS ``type: text`` quality expectations from the contract.

        Mirrors DQX's ``_process_text_rules_for_schema`` extraction: both
        property-level and schema-level ``quality`` entries with ``type:
        text`` are collected, each paired with a JSON schema_info blob so the
        LLM has column context.
        """
        import yaml

        try:
            data = yaml.safe_load(contract_text) or {}
        except yaml.YAMLError:
            return []
        if not isinstance(data, dict):
            return []

        raw_schemas = data.get("schema") or []
        if not isinstance(raw_schemas, list):
            return []

        expectations: list[_TextExpectation] = []
        for entry in raw_schemas:
            if not isinstance(entry, dict):
                continue
            schema_name = _first_str(entry.get("name")) or "unknown_schema"
            schema_info = ContractRulesService._build_schema_info(entry)

            for q in _text_quality_descriptions(entry.get("quality")):
                expectations.append(
                    _TextExpectation(schema_name=schema_name, field=None, description=q, schema_info=schema_info)
                )

            props = entry.get("properties") or []
            if isinstance(props, list):
                for prop in props:
                    if not isinstance(prop, dict):
                        continue
                    field = _first_str(prop.get("name"))
                    for q in _text_quality_descriptions(prop.get("quality")):
                        expectations.append(
                            _TextExpectation(
                                schema_name=schema_name,
                                field=field,
                                description=q,
                                schema_info=schema_info,
                            )
                        )
        return expectations

    @staticmethod
    def _build_schema_info(schema_entry: dict[str, Any]) -> str:
        """Build a JSON ``{name, columns:[...]}`` blob for LLM schema context."""
        columns: list[dict[str, str]] = []
        props = schema_entry.get("properties") or []
        if isinstance(props, list):
            for prop in props:
                if not isinstance(prop, dict):
                    continue
                name = _first_str(prop.get("name"))
                if not name:
                    continue
                col: dict[str, str] = {"name": name}
                type_value = _first_str(prop.get("logicalType"), prop.get("physicalType"))
                if type_value:
                    col["type"] = type_value
                description = _first_str(prop.get("description"))
                if description:
                    col["description"] = description
                columns.append(col)
        return json.dumps({"name": _first_str(schema_entry.get("name")), "columns": columns})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_metadata_and_schemas(
        contract_text: str,
    ) -> tuple[ContractMetadata, dict[str, dict[str, Any]]]:
        """Pull display metadata + per-schema property counts directly from YAML.

        We intentionally do this on the raw dict rather than the parsed
        ``OpenDataContractStandard`` model so we don't fail rendering when
        the contract has minor unknown fields the model doesn't like — the
        downstream generator will fail with a clear error if the contract
        is truly invalid.
        """
        import yaml  # local import to keep module import cheap

        try:
            data = yaml.safe_load(contract_text) or {}
        except yaml.YAMLError as exc:
            raise ValueError(f"Contract YAML is invalid: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("Contract must be a YAML mapping at the top level")

        info = data.get("info") or {}
        owner_value = data.get("owner") or info.get("owner")
        metadata = ContractMetadata(
            contract_id=_first_str(data.get("id"), info.get("id")),
            name=_first_str(data.get("name"), info.get("title"), info.get("name")),
            version=_first_str(data.get("version"), info.get("version")),
            odcs_api_version=_first_str(data.get("apiVersion")),
            status=_first_str(data.get("status"), info.get("status")),
            owner=_first_str(owner_value),
            domain=_first_str(data.get("domain"), info.get("domain")),
            description=_first_str(data.get("description"), info.get("description")),
        )

        raw_schemas = data.get("schema") or []
        if not isinstance(raw_schemas, list):
            raw_schemas = []
        schema_index: dict[str, dict[str, Any]] = {}
        for entry in raw_schemas:
            if not isinstance(entry, dict):
                continue
            name = _first_str(entry.get("name"))
            if not name:
                continue
            props = entry.get("properties") or []
            schema_index[name] = {
                "physical_name": _first_str(entry.get("physicalName")),
                "property_count": len(props) if isinstance(props, list) else 0,
            }
        return metadata, schema_index

    @staticmethod
    def _bucket_rules_by_schema(
        rules: list[dict[str, Any]],
        schema_index: dict[str, dict[str, Any]],
    ) -> tuple[list[ContractSchemaRules], list[dict[str, Any]]]:
        buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in schema_index}
        unassigned: list[dict[str, Any]] = []
        for rule in rules:
            schema_name = _extract_schema_name(rule)
            if schema_name and schema_name in buckets:
                buckets[schema_name].append(rule)
            elif schema_name:
                # Schema appeared in metadata of a rule but not in the
                # contract's ``schema`` list — keep it visible rather than
                # silently dropping.
                buckets.setdefault(schema_name, []).append(rule)
            else:
                unassigned.append(rule)
        result: list[ContractSchemaRules] = []
        for name, info in schema_index.items():
            result.append(
                ContractSchemaRules(
                    schema_name=name,
                    physical_name=info.get("physical_name"),
                    property_count=int(info.get("property_count") or 0),
                    rules=buckets.pop(name, []),
                )
            )
        # Tail-end: any schema that only appeared in rule metadata.
        for name, rules_for_schema in buckets.items():
            result.append(
                ContractSchemaRules(
                    schema_name=name,
                    physical_name=None,
                    property_count=0,
                    rules=rules_for_schema,
                )
            )
        return result, unassigned


def _first_str(*values: Any) -> str | None:
    for v in values:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _text_quality_descriptions(quality: Any) -> list[str]:
    """Return the ``description`` of every ``type: text`` entry in a quality list."""
    if not isinstance(quality, list):
        return []
    out: list[str] = []
    for q in quality:
        if not isinstance(q, dict):
            continue
        if q.get("type") != "text":
            continue
        desc = _first_str(q.get("description"))
        if desc:
            out.append(desc)
    return out


def _extract_schema_name(rule: dict[str, Any]) -> str | None:
    meta = rule.get("user_metadata")
    if isinstance(meta, dict):
        schema = meta.get("schema")
        if isinstance(schema, str) and schema.strip():
            return schema.strip()
    return None
