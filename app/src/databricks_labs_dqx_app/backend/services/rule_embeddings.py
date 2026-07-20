"""Rule embeddings — Rules Registry Phase 4B.

Builds a normalized text representation of a published registry rule
(:func:`build_rule_embed_text`) and, when an embedding serving endpoint is
configured, embeds + stores it in the ``dq_rule_embeddings`` corpus table
(:class:`RuleEmbeddingsService`).

**Deploy-safe by construction**: every public method degrades gracefully
when ``embedding_endpoint_name`` (see ``AppSettingsService``) is unset — the
default on every fresh deploy. :meth:`RuleEmbeddingsService.embed_and_store`
never raises; it is safe to call unconditionally from the registry-rule
approve route on every publish. No Vector Search or embedding infrastructure
is required for the app to build, deploy, or serve any other feature.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.registry_models import (
    RegistryRule,
    get_rule_description,
    get_rule_dimension,
    get_rule_name,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql

logger = logging.getLogger(__name__)

# Reserved ``user_metadata`` tag keys already surfaced explicitly by
# :func:`build_rule_embed_text` — excluded from the generic "tags: ..." pass
# so they aren't duplicated in the embedded text.
_RESERVED_TAG_KEYS = {"name", "description", "dimension", "severity"}

# Predicate/body text is truncated before embedding: it may be a full SQL
# query or low-code AST, and this text can end up replicated into a shared
# Vector Search index — keep it bounded (OWASP LLM04-style budget, applied
# here defensively even though there is no LLM call in this module).
_MAX_PREDICATE_CHARS = 500


def build_rule_embed_text(rule: RegistryRule) -> str:
    """Build the normalized text blob embedded for *rule*.

    Combines name, description, dimension tag, slot family/cardinality, free-
    text tags, check function name, and a truncated predicate/body summary so
    semantically similar rules land close together in embedding space (e.g.
    "email format" and "valid email regex").

    Slot family and cardinality use the same vocabulary as the query-side
    ``family_for_type`` token (numeric|text|temporal|boolean|array|any), so
    adding them creates a direct match channel between document and query.

    Severity is intentionally omitted — nearly every rule has one, so it
    dilutes cosine similarity without improving ranking.

    Args:
        rule: The registry rule to summarize. Any mode (``dqx_native`` /
            ``lowcode`` / ``sql``) is supported — the predicate extraction
            in :func:`_extract_predicate` handles each shape.

    Returns:
        A newline-joined text blob, never containing raw column data.
    """
    parts: list[str] = []
    name = get_rule_name(rule.user_metadata)
    if name:
        parts.append(name)
    description = get_rule_description(rule.user_metadata)
    if description:
        parts.append(description)
    dimension = get_rule_dimension(rule.user_metadata)
    if dimension:
        parts.append(f"dimension: {dimension}")
    if rule.definition.slots:
        slot_summaries = ", ".join(
            f"{s.name} ({s.family}, {s.cardinality})" for s in rule.definition.slots
        )
        parts.append(f"input columns: {slot_summaries}")
    body = rule.definition.body
    check_func = body.get("function")
    if isinstance(check_func, str) and check_func.strip():
        parts.append(f"check: {check_func.strip()}")
    tags = [
        f"{key}: {value}"
        for key, value in rule.user_metadata.items()
        if key not in _RESERVED_TAG_KEYS and isinstance(value, str) and value
    ]
    if tags:
        parts.append("tags: " + ", ".join(tags))
    predicate = _extract_predicate(body)
    if predicate:
        parts.append(f"predicate: {predicate[:_MAX_PREDICATE_CHARS]}")
    return "\n".join(parts).strip()


def _extract_predicate(body: dict[str, Any]) -> str | None:
    """Extract a short textual summary of a rule definition's mode-specific body."""
    for key in ("predicate", "sql_query", "function"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    arguments = body.get("arguments")
    if isinstance(arguments, dict) and arguments:
        try:
            return json.dumps(arguments, sort_keys=True)
        except (TypeError, ValueError):
            return None
    return None


class RuleEmbeddingsService:
    """Builds + stores embeddings for published registry rules.

    Owns the ``dq_rule_embeddings`` corpus table (rule_id, rule_version,
    embed_text, embedding, model, updated_at) — the source-of-truth text
    corpus a Databricks Vector Search index syncs from (see
    ``services.rule_retriever.VectorSearchRetriever``).
    """

    def __init__(self, sql: OltpExecutorProtocol, sp_ws: WorkspaceClient, app_settings: AppSettingsService) -> None:
        self._sql = sql
        self._sp_ws = sp_ws
        self._app_settings = app_settings
        self._table = sql.fqn("dq_rule_embeddings")

    def is_configured(self) -> bool:
        """Return whether an embedding serving endpoint is configured."""
        return bool(self._app_settings.get_embedding_endpoint_name())

    def embed_text(self, text: str) -> list[float] | None:
        """Call the configured embedding endpoint for *text*.

        Returns ``None`` (never raises) when unconfigured. Propagates SDK
        errors from an actual call failure — callers that want a
        best-effort no-op should use :meth:`embed_and_store` instead, which
        catches and logs them.
        """
        endpoint = self._app_settings.get_embedding_endpoint_name()
        if not endpoint:
            return None
        response = self._sp_ws.serving_endpoints.query(name=endpoint, input=[text])
        data = getattr(response, "data", None) or []
        for item in data:
            embedding = getattr(item, "embedding", None)
            if embedding:
                return list(embedding)
        return None

    def embed_and_store(self, rule: RegistryRule) -> bool:
        """Embed *rule* and upsert it into ``dq_rule_embeddings``.

        Best-effort and never raises: returns ``False`` when unconfigured
        or on any failure (network error, malformed endpoint response,
        etc.) so a Vector Search / embedding hiccup never fails a rule
        publish. Safe to call unconditionally from the approve route.

        When ``vs_endpoint_name``/``vs_index_name`` are also configured,
        also best-effort upserts the same row into the Direct Access Vector
        Search index (see ``services.vector_store``) — the OLTP corpus
        table isn't a UC Delta source a Delta-Sync index could read from,
        so the app keeps the index in sync itself. A failure on that leg
        (e.g. index not yet ONLINE) never fails the OLTP write, which is
        the source of truth.

        Args:
            rule: The just-published registry rule.

        Returns:
            ``True`` iff the OLTP corpus row was written.
        """
        if not self.is_configured():
            logger.debug("Embedding endpoint not configured; skipping embed for rule %s", rule.rule_id)
            return False
        try:
            text = build_rule_embed_text(rule)
            embedding = self.embed_text(text)
            if embedding is None:
                logger.warning("Embedding endpoint returned no vector for rule %s", rule.rule_id)
                return False
            self._upsert(rule.rule_id, rule.version, text, embedding)
        except Exception:
            logger.warning("Failed to embed rule %s (non-fatal)", rule.rule_id, exc_info=True)
            return False

        self._upsert_vector_search_index(rule.rule_id, text, embedding)
        return True

    def iter_embeddings(self) -> list[tuple[str, list[float]]]:
        """Load every stored ``(rule_id, vector)`` row from the OLTP corpus.

        This is the source the in-app cosine retriever
        (:class:`~databricks_labs_dqx_app.backend.services.rule_retriever.CosineRuleRetriever`)
        scans at query time — the same ``dq_rule_embeddings`` corpus written
        best-effort on every publish (see :meth:`embed_and_store`) and by
        :meth:`backfill`.

        Best-effort by construction: a read failure or a malformed stored
        vector never raises — it is logged and skipped so retrieval degrades
        to "no candidates" rather than a 500.

        Returns:
            A list of ``(rule_id, embedding)`` tuples. Rows with an empty,
            non-list, or unparseable embedding are omitted.
        """
        try:
            rows = self._sql.query_dicts(f"SELECT rule_id, embedding FROM {self._table}")  # noqa: S608
        except Exception:
            logger.warning("Failed to read rule embeddings corpus %s (non-fatal)", self._table, exc_info=True)
            return []
        out: list[tuple[str, list[float]]] = []
        for row in rows:
            rule_id = row.get("rule_id")
            raw = row.get("embedding")
            if not rule_id or not raw:
                continue
            try:
                vector = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(vector, list) or not vector:
                continue
            try:
                out.append((str(rule_id), [float(x) for x in vector]))
            except (TypeError, ValueError):
                continue
        return out

    def backfill(self, rules: list[RegistryRule]) -> int:
        """Embed every rule in *rules* (e.g. all currently-published rules).

        Args:
            rules: Rules to (re-)embed, typically ``RegistryService.list_rules(status="approved")``.

        Returns:
            The count of rules successfully stored.
        """
        return sum(1 for rule in rules if self.embed_and_store(rule))

    def _upsert(self, rule_id: str, rule_version: int, text: str, embedding: list[float]) -> None:
        model = self._app_settings.get_embedding_endpoint_name()
        self._sql.upsert(
            self._table,
            key_cols={"rule_id": rule_id},
            value_cols={
                "rule_version": rule_version,
                "embed_text": text,
                "embedding": json.dumps(embedding),
                "model": model,
                "updated_at": RawSql("current_timestamp()"),
            },
        )

    def _upsert_vector_search_index(self, rule_id: str, text: str, embedding: list[float]) -> None:
        """Best-effort mirror of the OLTP row into the Direct Access VS index.

        No-op when ``vs_endpoint_name``/``vs_index_name`` are unset (the
        suggester reports ``available=False`` in that case anyway). Any SDK
        failure — e.g. the index hasn't finished provisioning yet — is
        logged and swallowed; the OLTP corpus row (the source of truth) is
        already written by the time this runs.
        """
        index_name = self._app_settings.get_vs_index_name()
        if not self._app_settings.get_vs_endpoint_name() or not index_name:
            return
        try:
            self._sp_ws.vector_search_indexes.upsert_data_vector_index(
                index_name=index_name,
                inputs_json=json.dumps([{"rule_id": rule_id, "embed_text": text, "embedding": embedding}]),
            )
        except Exception:
            logger.warning("Failed to upsert rule %s into Vector Search index %s (non-fatal)", rule_id, index_name)
