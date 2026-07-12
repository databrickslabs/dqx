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
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

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
    validation_errors: list[str]


@dataclass(frozen=True)
class _TextExpectation:
    """One ODCS ``type: text`` quality expectation to feed to the LLM."""

    schema_name: str
    field: str | None
    description: str
    schema_info: str  # JSON string of the schema's columns for LLM context


class ContractRulesService:
    """Generate DQX rules from a raw ODCS contract YAML/JSON string."""

    # Upper bound on how many ``type: text`` expectations we route to the
    # LLM in a single contract import. Each expectation costs one
    # ChatDatabricks call, so without a cap an author-supplied contract with
    # thousands of text quality entries amplifies into thousands of LLM
    # invocations (cost/latency DoS — OWASP LLM04, see AGENTS.md). Beyond
    # this limit we process the first N and warn the caller.
    _MAX_TEXT_EXPECTATIONS: ClassVar[int] = 100

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

        # Gate every generated rule (DQX-native predefined/schema rules *and*
        # LLM-produced text rules) through the same DQEngine.validate_checks
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

        # Bound LLM fan-out before issuing any calls: one ChatDatabricks
        # invocation per expectation, so cap the count to keep cost/latency
        # bounded for adversarially large contracts (OWASP LLM04).
        if len(expectations) > self._MAX_TEXT_EXPECTATIONS:
            warnings.append(
                f"Contract has {len(expectations)} natural-language expectations; only the "
                f"first {self._MAX_TEXT_EXPECTATIONS} were processed to bound LLM cost. "
                "Reduce the number of text quality expectations or split the contract to "
                "process the rest."
            )
            expectations = expectations[: self._MAX_TEXT_EXPECTATIONS]

        for exp in expectations:
            try:
                generated = self._ai_service.generate_from_schema_info(
                    user_input=exp.description,
                    schema_info=exp.schema_info,
                )
            except Exception:  # pragma: no cover - defensive guard
                # Log full detail server-side (with newline-scrubbed,
                # untrusted contract values) but never relay the raw LLM/SDK
                # exception text back to the caller — it can echo prompt
                # content or internal structure (AGENTS.md LLM06 / CWE-209).
                logger.warning(
                    "Failed to generate text rule for schema '%s' field '%s'.",
                    _scrub_for_log(exp.schema_name),
                    _scrub_for_log(exp.field),
                    exc_info=True,
                )
                warnings.append(
                    "Could not generate a rule for the text expectation on "
                    f"'{exp.field or exp.schema_name}'. See server logs for details."
                )
                continue
            for rule in generated:
                if not isinstance(rule, dict):
                    continue
                # LLM output is untrusted (AGENTS.md): drop any rule whose
                # check function does not resolve through CHECK_FUNC_REGISTRY
                # or whose generated SQL fails is_sql_query_safe(), before it
                # can flow to the UI / be saved / executed.
                if not self._is_llm_rule_safe(rule):
                    logger.warning(
                        "Discarded unsafe/unresolved LLM-generated rule for schema '%s' field '%s'.",
                        _scrub_for_log(exp.schema_name),
                        _scrub_for_log(exp.field),
                    )
                    warnings.append(
                        "An AI-generated rule for "
                        f"'{exp.field or exp.schema_name}' was discarded because it "
                        "referenced an unknown check function or unsafe SQL."
                    )
                    continue
                # Spread the LLM-supplied metadata FIRST so the trusted
                # contract-lineage keys below always win. Letting LLM output
                # override ``schema``/``rule_type``/``contract_id`` would
                # corrupt lineage and, worse, mis-route the rule in
                # ``_bucket_rules_by_schema`` to a wrong/nonexistent schema
                # bucket — which on save can target an unintended table.
                user_metadata = {
                    **(rule.get("user_metadata") or {}),
                    "contract_id": metadata.contract_id or "unknown",
                    "contract_version": metadata.version or "unknown",
                    "odcs_version": metadata.odcs_api_version or "unknown",
                    "schema": exp.schema_name,
                    "rule_type": "text_llm",
                    "text_expectation": exp.description,
                }
                if exp.field:
                    user_metadata["field"] = exp.field
                rule["user_metadata"] = user_metadata
                rule.setdefault("criticality", default_criticality)
                rules.append(rule)
        if not rules and not warnings:
            warnings.append("No rules were generated from the contract's text expectations.")
        return rules, warnings

    @classmethod
    def _is_llm_rule_safe(cls, rule: dict[str, Any]) -> bool:
        """Validate an LLM-produced rule before it reaches the UI.

        Per AGENTS.md, LLM-generated output is untrusted: the check function
        name must resolve through ``CHECK_FUNC_REGISTRY`` and any generated
        SQL fragment must pass ``is_sql_query_safe()``. Returns ``False`` for
        hallucinated function names or unsafe SQL so the caller drops the rule.

        We do **not** gate on a fixed argument-name allowlist. A
        prompt-injected ``type: text`` expectation can steer the LLM to emit
        a rule that resolves to a real check function yet stashes a
        destructive statement in *any* argument (``column``, a nested value,
        or an argument a future check adds), which a name-based allowlist
        would wave through to save/execute. The app layer can't know which
        arguments a given check ultimately interpolates into Spark SQL, so we
        conservatively treat **every** string the rule carries — the
        top-level ``filter`` plus all (possibly nested) argument values — as
        a potential SQL fragment and require each to pass
        ``is_sql_query_safe()``. Rejected rules are dropped non-fatally with
        a warning, so over-rejecting a benign-but-keyword-bearing literal is
        an acceptable trade-off for closing the injection vector.
        """
        # Local imports keep module import cheap and ensure the check-function
        # registry is populated (importing ``check_funcs`` runs the
        # ``@register_rule`` decorators).
        from databricks.labs.dqx import check_funcs  # noqa: F401  pylint: disable=unused-import
        from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
        from databricks.labs.dqx.utils import is_sql_query_safe

        check = rule.get("check")
        if not isinstance(check, dict):
            return False
        function = check.get("function")
        if not isinstance(function, str) or function not in CHECK_FUNC_REGISTRY:
            return False

        sql_fragments: list[str] = []
        _collect_string_fragments(rule.get("filter"), sql_fragments)
        _collect_string_fragments(check.get("arguments"), sql_fragments)
        return all(is_sql_query_safe(sql) for sql in sql_fragments if sql.strip())

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


def _collect_string_fragments(value: Any, out: list[str]) -> None:
    """Recursively collect every string scalar reachable within *value*.

    Used by :meth:`ContractRulesService._is_llm_rule_safe` to gather all
    strings an LLM-produced rule carries (the rule ``filter`` and the
    arbitrarily nested ``arguments`` structure) so each can be screened by
    ``is_sql_query_safe()``. Walks dicts (values only — keys are rule schema,
    not LLM SQL), lists, and tuples; ignores non-string scalars.
    """
    if isinstance(value, str):
        out.append(value)
    elif isinstance(value, dict):
        for v in value.values():
            _collect_string_fragments(v, out)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _collect_string_fragments(v, out)


def _scrub_for_log(value: str | None) -> str:
    """Strip newlines/control chars from untrusted strings before logging.

    Contract-supplied identifiers (schema/field names) are user-controlled;
    embedding them verbatim in log messages risks log forging/injection
    (CWE-117). Collapse control characters and bound the length.
    """
    if not value:
        return ""
    return re.sub(r"[\x00-\x1f\x7f]+", " ", value)[:200]


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
