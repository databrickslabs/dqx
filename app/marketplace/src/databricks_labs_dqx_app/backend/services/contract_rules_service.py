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

import logging
from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient

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
    validation_errors: list[str]


class ContractRulesService:
    """Generate DQX rules from a raw ODCS contract YAML/JSON string."""

    def __init__(self, sp_ws: WorkspaceClient) -> None:
        # Service principal client is sufficient: contract parsing is
        # local, no UC reads happen during predefined/explicit/schema
        # rule generation.
        self._ws = sp_ws

    def generate(
        self,
        contract_text: str,
        *,
        generate_predefined_rules: bool = True,
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

        # Contract import is deterministic: rules are derived only from
        # machine-checkable ODCS fields. ``process_text_rules=False`` because
        # ``type: text`` expectations are free prose with no executable
        # semantics — converting them requires an LLM, which this import path
        # deliberately does not do.
        rules = generator.generate_rules_from_contract(
            contract=contract,
            generate_predefined_rules=generate_predefined_rules,
            process_text_rules=False,
            generate_schema_validation=generate_schema_validation,
            strict_schema_validation=strict_schema_validation,
            default_criticality=default_criticality,
        )

        # Report skipped ``type: text`` expectations rather than dropping them
        # silently: the contract author declared an intent we can't honour, so
        # the owner needs to know it didn't become a rule.
        skipped_text = _count_text_expectations(contract_text)
        if skipped_text:
            warnings.append(
                f"{skipped_text} natural-language quality expectation(s) (ODCS 'type: text') "
                "were skipped. Rules are derived only from machine-checkable contract fields; "
                "express these as 'type: sql' or 'type: library' to have them imported."
            )

        # Gate every generated rule through the same DQEngine.validate_checks
        # used by the AI-assisted ``/generate`` endpoint, so malformed or
        # unresolvable rules are surfaced here instead of only failing later
        # at execution time. Non-blocking: errors are returned for the UI to
        # flag, mirroring the AI page's behaviour.
        validation_errors = self._validate_rules(rules)

        buckets, unassigned = self._bucket_rules_by_schema(rules, schema_index)
        return ContractGenerationResult(
            metadata=metadata,
            schemas=buckets,
            unassigned_rules=unassigned,
            total_rules=len(rules),
            warnings=warnings,
            validation_errors=validation_errors,
        )

    @staticmethod
    def _validate_rules(rules: list[dict[str, Any]]) -> list[str]:
        """Run ``DQEngine.validate_checks`` over the generated rules.

        Returns a flat list of validation error strings (empty when all
        rules are valid). Validation failures are reported, not raised, so a
        single bad rule doesn't sink an otherwise-usable contract import.
        """
        if not rules:
            return []
        try:
            from databricks.labs.dqx.engine import DQEngine
        except ImportError:  # pragma: no cover - core dep, defensive only
            logger.warning("DQEngine is unavailable; skipping contract rule validation.")
            return []
        status = DQEngine.validate_checks(rules)
        return list(status.errors) if status.has_errors else []

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


def _count_text_expectations(contract_text: str) -> int:
    """Count ODCS ``type: text`` quality entries, schema- and property-level.

    Used only to warn that these expectations produced no rules. Parsing is
    best-effort: a contract that fails to re-parse here has already been
    parsed successfully upstream, so silence is preferable to raising.
    """
    import yaml

    try:
        data = yaml.safe_load(contract_text) or {}
    except yaml.YAMLError:  # pragma: no cover - upstream parse already succeeded
        return 0
    if not isinstance(data, dict):
        return 0
    raw_schemas = data.get("schema") or []
    if not isinstance(raw_schemas, list):
        return 0

    count = 0
    for entry in raw_schemas:
        if not isinstance(entry, dict):
            continue
        count += _count_text_quality(entry.get("quality"))
        props = entry.get("properties") or []
        if isinstance(props, list):
            for prop in props:
                if isinstance(prop, dict):
                    count += _count_text_quality(prop.get("quality"))
    return count


def _count_text_quality(quality: Any) -> int:
    """Count ``type: text`` entries in a single ODCS ``quality`` list."""
    if not isinstance(quality, list):
        return 0
    return sum(1 for q in quality if isinstance(q, dict) and q.get("type") == "text")


def _extract_schema_name(rule: dict[str, Any]) -> str | None:
    meta = rule.get("user_metadata")
    if isinstance(meta, dict):
        schema = meta.get("schema")
        if isinstance(schema, str) and schema.strip():
            return schema.strip()
    return None
