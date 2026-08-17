"""Rule embeddings — Rules Registry Phase 4B.

Builds a normalized text representation of a published registry rule
(:func:`build_rule_embed_text`) and, when an embedding serving endpoint is
configured, embeds + stores it in the ``dq_rule_embeddings`` corpus table
(:class:`RuleEmbeddingsService`).

**Deploy-safe by construction**: every public method degrades gracefully
when ``embedding_endpoint_name`` (see ``AppSettingsService``) is unset — the
default on every fresh deploy. :meth:`RuleEmbeddingsService.embed_and_store`
never raises; it is safe to call unconditionally from the registry-rule
approve route on every publish. No embedding infrastructure is required for
the app to build, deploy, or serve any other feature.
"""

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
# query or low-code AST — keep it bounded (OWASP LLM04-style budget, applied
# here defensively even though there is no LLM call in this module).
_MAX_PREDICATE_CHARS = 500

# Foundation-model / serving-endpoint ``input`` arrays have per-request payload
# and token limits. Chunk so a wide-table retrieval batch never blows the
# endpoint while still collapsing N round trips into a few.
EMBED_BATCH_SIZE = 32


def build_rule_embed_text(rule: RegistryRule) -> str:
    """Build the normalized text blob embedded for *rule*.

    Combines name, description, dimension tag, slot family/cardinality, free-
    text tags, check function name, and a truncated predicate/body summary so
    semantically similar rules land close together in embedding space (e.g.
    "email format" and "valid email regex").

    Slot family and cardinality use the same vocabulary as the query-side
    ``family_for_type`` token (numeric|text|temporal|boolean|any), so
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
        slot_summaries = ", ".join(f"{s.name} ({s.family}, {s.cardinality})" for s in rule.definition.slots)
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
    corpus scanned by
    :class:`~databricks_labs_dqx_app.backend.services.rule_retriever.CosineRuleRetriever`.

    Auth is split on purpose:

    * **Writes / backfill** (:meth:`embed_and_store`) always use the app
      service principal — publish and startup backfill often have no end-user
      token in scope.
    * **Query-time embeds** (:meth:`embed_texts` / :meth:`embed_text`) use the
      caller's OBO client when one was injected, matching :class:`AIGateway`
      (user-facing, request-scoped). Falls back to the SP only when no user
      client is available (tests, startup-only construction).
    """

    def __init__(
        self,
        sql: OltpExecutorProtocol,
        sp_ws: WorkspaceClient,
        app_settings: AppSettingsService,
        user_ws: WorkspaceClient | None = None,
    ) -> None:
        self._sql = sql
        self._sp_ws = sp_ws
        self._user_ws = user_ws
        self._app_settings = app_settings
        self._table = sql.fqn("dq_rule_embeddings")
        # Per-request Depends factory → this cache lives only for one request.
        # Cleared on write so a same-request publish+retrieve never sees a
        # stale corpus (see :meth:`_upsert`).
        self._corpus_cache: list[tuple[str, list[float]]] | None = None

    def is_configured(self) -> bool:
        """Return whether an embedding serving endpoint is configured."""
        return bool(self._app_settings.get_embedding_endpoint_name())

    def embed_text(self, text: str) -> list[float] | None:
        """Call the configured embedding endpoint for *text* (query-time / OBO).

        Returns ``None`` (never raises) when unconfigured. Propagates SDK
        errors from an actual call failure — callers that want a
        best-effort no-op should use :meth:`embed_and_store` instead, which
        catches and logs them.
        """
        return self.embed_texts([text])[0]

    def embed_texts(self, texts: list[str], *, batch_size: int = EMBED_BATCH_SIZE) -> list[list[float] | None]:
        """Embed many texts in chunked serving-endpoint calls (query-time).

        Uses the caller's OBO ``WorkspaceClient`` when one was injected so
        retrieval sits on the same identity / endpoint ACL as the AIGateway
        judge. Falls back to the service principal only when no user client
        is available.

        The Databricks serving-endpoints ``input`` field accepts an array and
        returns one element per input (with an ``index``). Batching collapses
        per-column retrieval round trips; *batch_size* keeps each call under
        FMAPI payload/token limits so a very wide table doesn't fail the
        whole request.

        Args:
            texts: Query (or document) texts to embed, in order.
            batch_size: Max texts per ``serving_endpoints.query`` call.

        Returns:
            A list aligned with *texts*. Each entry is the embedding vector,
            or ``None`` when the endpoint is unconfigured or that element was
            missing from the response. Propagates SDK errors from a failed
            call (same contract as :meth:`embed_text`).
        """
        return self._embed_texts_with(self._user_ws or self._sp_ws, texts, batch_size=batch_size)

    def _embed_texts_with(
        self,
        ws: WorkspaceClient,
        texts: list[str],
        *,
        batch_size: int = EMBED_BATCH_SIZE,
    ) -> list[list[float] | None]:
        if not texts:
            return []
        endpoint = self._app_settings.get_embedding_endpoint_name()
        if not endpoint:
            return [None] * len(texts)

        out: list[list[float] | None] = [None] * len(texts)
        chunk = max(1, batch_size)
        for start in range(0, len(texts), chunk):
            batch = texts[start : start + chunk]
            response = ws.serving_endpoints.query(name=endpoint, input=batch)
            data = getattr(response, "data", None) or []
            for position, item in enumerate(data):
                embedding = getattr(item, "embedding", None)
                if not embedding:
                    continue
                # Prefer the endpoint's ``index`` when present; fall back to
                # response order so older/mock shapes still work.
                idx = getattr(item, "index", None)
                offset = idx if isinstance(idx, int) else position
                abs_idx = start + offset
                if 0 <= abs_idx < len(out):
                    out[abs_idx] = list(embedding)
        return out

    def embed_and_store(self, rule: RegistryRule) -> bool:
        """Embed *rule* and upsert it into ``dq_rule_embeddings``.

        Always embeds with the **service principal** (startup backfill and
        publish have no end-user serving identity to rely on). Best-effort
        and never raises: returns ``False`` when unconfigured or on any
        failure so an embedding hiccup never fails a rule publish. Safe to
        call unconditionally from the approve route.

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
            embedding = self._embed_texts_with(self._sp_ws, [text])[0]
            if embedding is None:
                logger.warning("Embedding endpoint returned no vector for rule %s", rule.rule_id)
                return False
            self._upsert(rule.rule_id, rule.version, text, embedding)
            return True
        except Exception:
            logger.warning("Failed to embed rule %s (non-fatal)", rule.rule_id, exc_info=True)
            return False

    def iter_embeddings(self) -> list[tuple[str, list[float]]]:
        """Load every stored ``(rule_id, vector)`` row from the OLTP corpus.

        This is the source the in-app cosine retriever
        (:class:`~databricks_labs_dqx_app.backend.services.rule_retriever.CosineRuleRetriever`)
        scans at query time — the same ``dq_rule_embeddings`` corpus written
        best-effort on every publish (see :meth:`embed_and_store`) and by
        :meth:`backfill`.

        Memoised on the service instance (a per-request Depends factory), so
        per-column retrieval in one request reuses a single SELECT + JSON
        parse. Writes clear the cache via :meth:`_upsert`.

        Best-effort by construction: a read failure or a malformed stored
        vector never raises — it is logged and skipped so retrieval degrades
        to "no candidates" rather than a 500.

        Returns:
            A list of ``(rule_id, embedding)`` tuples. Rows with an empty,
            non-list, or unparseable embedding are omitted.
        """
        if self._corpus_cache is None:
            self._corpus_cache = self._load_embeddings()
        return self._corpus_cache

    def _load_embeddings(self) -> list[tuple[str, list[float]]]:
        try:
            rows = self._sql.select_dicts(self._table, ["rule_id", "embedding"])
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
        # Same-request publish then retrieve must not score against a stale
        # snapshot taken before this write.
        self._corpus_cache = None
