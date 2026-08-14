"""Shared harness for the AI rule-suggester evaluation (golden set).

Why this exists
---------------
``tests/test_rule_suggester.py`` proves the suggester's *plumbing*: every
degrade path, the post-process validation gates, the retrieval fan-out shape.
It cannot tell us whether the suggestions are any **good**, because both the
retriever and the judge are replaced by test-local fakes. So a retrieval
refactor (batched embeddings, a corpus cache, a new ``MAX_RETRIEVAL_COLUMNS``
cap) can land fully green while suggestion quality silently moves.

This module supplies the missing half: a labelled fixture set plus a scorer,
so "did that change help or hurt" has a number attached.

The two tiers
-------------
**Tier 1 — deterministic replay** (``test_rule_suggester_eval.py``, runs in
``make app-test``, seconds, no workspace, no tokens). The pipeline under test is
**real**: a real :class:`RuleSuggester`, a real :class:`CosineRuleRetriever`, a
real :class:`RuleEmbeddingsService`, a real :class:`AIGateway`. The only faked
surface is the outbound HTTP call, ``WorkspaceClient.serving_endpoints.query``,
which is replayed from ``fixtures/suggester_eval/recorded/``.

Replaying at the HTTP boundary rather than at the ``RuleRetriever`` /
``AIGateway`` seam is deliberate. Stubbing those seams is exactly what the
existing suite already does, and a stub at the seam can pass while the real
path is inert — we have shipped that bug before (the MCP telemetry wrapper was
green in every test and sent nothing in production, because every test called
the inner function directly).

**Tier 2 — live measurement** (``tests/eval_live/``, on demand). Same fixtures,
same :func:`score`, real embedding and judge endpoints. This is the tier that
answers "is it any good" and "what does the 64-column retrieval cap cost".

What Tier 1 does and does not hold fixed
----------------------------------------
Tier 1 measures **retrieval + post-process**. The judge is held fixed as a
recorded oracle: ``recorded/judge.json`` stores one verdict per table, produced
against the *whole* pinned corpus, and :class:`ReplayEndpoints` returns that
verdict filtered to the rules retrieval actually offered on this run. So:

* a retrieval change that stops surfacing a correct rule shows up as a recall
  drop — which is the point;
* a change to the judge's system prompt does **not** show up in Tier 1 at all.
  Prompt changes have to be measured live, in Tier 2.

Stating that boundary plainly matters more than pretending Tier 1 covers
everything. The real ``AIGateway.query``, ``parse_json_object`` and
``_post_process`` all execute on every Tier 1 run; only the model's answer is
canned.

Numbers from Tier 1 are for **comparing runs against each other**, never a
quality claim in absolute terms — the fixture tables are synthetic and cleaner
than real customer data.
"""

import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.mixins.open_ai_client import ServingEndpointsExt

from databricks_labs_dqx_app.backend.registry_models import (
    ColumnMappingGroup,
    MonitoredTable,
    RegistryRule,
    compute_mapping_hash,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, TableColumn
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService, build_rule_embed_text
from databricks_labs_dqx_app.backend.services.rule_retriever import CosineRuleRetriever
from databricks_labs_dqx_app.backend.services.rule_suggester import RuleSuggester, RuleSuggestion
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

FIXTURES = Path(__file__).parent / "fixtures" / "suggester_eval"
RECORDED = FIXTURES / "recorded"

# The suggestions dialog surfaces this many rows without the reviewer
# scrolling, so precision@k at this k is the number a user actually feels.
DEFAULT_PRECISION_AT_K = 10

EMBED_ENDPOINT = "eval-embedding-endpoint"
JUDGE_ENDPOINT = "eval-judge-endpoint"

EVAL_USER = "eval@example.com"


class MissingRecordingError(AssertionError):
    """Raised when the replay cache has no entry for an outbound call.

    Deliberately loud rather than degrading to a default. A miss means the
    fixtures have drifted from the code — a rule's embed text changed, or a new
    table was added without re-recording — and a silently-substituted answer
    would turn the whole eval into theatre. The message carries what is needed
    to re-record.
    """


# ---------------------------------------------------------------------------
# Fixture data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EvalTable:
    """One synthetic table in the golden set.

    Synthetic by design: hand-labelling real tables is the reason this work
    never gets done. These are authored so exactly one answer is defensible per
    column (an ``email`` column commented "customer contact email" wants the
    email-format rule), which means the answer key can be written before the
    table.

    No rows are needed anywhere — ``RuleSuggester._resolve_columns`` reads only
    schema metadata, so nothing has to be materialised in Unity Catalog.
    """

    binding_id: str
    table_fqn: str
    columns: tuple[TableColumn, ...]
    note: str = ""


@dataclass(frozen=True)
class Label:
    """One expected ``(rule_id, slot→column)`` answer, or one forbidden answer.

    A ``mapping`` of ``None`` on a *forbidden* label means "this rule must not
    be suggested for this table under any mapping".
    """

    rule_id: str
    mapping: ColumnMappingGroup | None = None

    def keys(self) -> tuple[str, str | None]:
        """Identity used for set comparison against predictions."""
        if self.mapping is None:
            return self.rule_id, None
        return self.rule_id, compute_mapping_hash([self.mapping])


@dataclass(frozen=True)
class TableLabels:
    """The answer key for one table: what should be suggested, and what must not.

    ``must_not_suggest`` carries the failure that matters most and that
    ``_post_process`` structurally cannot catch: a rule bound to a
    plausible-but-wrong column (an email-format rule on ``secondary_contact``
    rather than ``email``). Such a mapping is completely valid — real column,
    right family, all slots filled — so only a labelled expectation catches it.
    """

    binding_id: str
    expected: tuple[Label, ...] = ()
    must_not_suggest: tuple[Label, ...] = ()


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_corpus() -> list[RegistryRule]:
    """Load the pinned registry-rule corpus the labels were written against.

    Frozen on purpose: the labels name ``rule_id``s, so editing a rule's slots
    or metadata silently invalidates the answer key. Add rules; do not rewrite
    them in place without re-checking every label that references them.
    """
    return [RegistryRule.model_validate(row) for row in _read_json(FIXTURES / "rules.json")]


def load_tables() -> list[EvalTable]:
    """Load the synthetic table schemas."""
    tables: list[EvalTable] = []
    for row in _read_json(FIXTURES / "tables.json"):
        columns = tuple(
            TableColumn(
                name=column["name"],
                type_name=column["type"],
                comment=column.get("comment"),
                nullable=column.get("nullable", True),
                position=position,
            )
            for position, column in enumerate(row["columns"])
        )
        tables.append(
            EvalTable(
                binding_id=row["binding_id"],
                table_fqn=row["table_fqn"],
                columns=columns,
                note=row.get("note", ""),
            )
        )
    return tables


def _labels_from_rows(rows: Sequence[dict[str, Any]]) -> tuple[Label, ...]:
    return tuple(Label(rule_id=row["rule_id"], mapping=row.get("mapping")) for row in rows)


def load_labels() -> dict[str, TableLabels]:
    """Load the answer key, keyed by ``binding_id``."""
    out: dict[str, TableLabels] = {}
    for binding_id, row in _read_json(FIXTURES / "labels.json").items():
        out[binding_id] = TableLabels(
            binding_id=binding_id,
            expected=_labels_from_rows(row.get("expected", [])),
            must_not_suggest=_labels_from_rows(row.get("must_not_suggest", [])),
        )
    return out


def text_digest(text: str) -> str:
    """Stable short key for a recorded embedding request."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


@dataclass
class Recordings:
    """Replay cache captured at the ``serving_endpoints.query`` boundary.

    ``embeddings`` maps :func:`text_digest` of the request text to its vector,
    and covers both sides of retrieval — the corpus documents and the
    per-column query texts — because both go through the same endpoint call.

    ``judge`` maps ``table_fqn`` to the oracle verdict for that table over the
    **whole** corpus. See the module docstring for why it is filtered rather
    than keyed on the exact prompt.

    ``embedder`` is set only by ``suggester_eval_recorder.py``. With it, a cache
    miss is *filled* instead of raised, which is how the cache gets built: the
    recorder drives the real pipeline and captures whatever texts flow past the
    boundary. That is deliberate — it means no part of this harness has to
    duplicate the suggester's query-text format, so the two cannot drift.
    """

    embeddings: dict[str, list[float]]
    judge: dict[str, list[dict[str, Any]]]
    embedder: Callable[[str], list[float]] | None = None

    def vector_for(self, text: str) -> list[float]:
        digest = text_digest(text)
        vector = self.embeddings.get(digest)
        if vector is None and self.embedder is not None:
            vector = self.embedder(text)
            self.embeddings[digest] = vector
        if vector is None:
            raise MissingRecordingError(
                f"No recorded embedding for digest {digest}. The text below is not in "
                f"recorded/embeddings.json, which means a rule's embed text or a column "
                f"query text changed. Re-record with "
                f"`uv run --group test python -m tests.suggester_eval_recorder` from app/.\n---\n{text}\n---"
            )
        return vector

    def verdict_for(self, table_fqn: str) -> list[dict[str, Any]]:
        verdict = self.judge.get(table_fqn)
        if verdict is None:
            raise MissingRecordingError(
                f"No recorded judge verdict for table {table_fqn}. Re-record with "
                f"`uv run --group test python -m tests.suggester_eval_recorder` from app/."
            )
        return verdict


def load_recordings() -> Recordings:
    """Load the replay cache."""
    return Recordings(
        embeddings=_read_json(RECORDED / "embeddings.json"),
        judge=_read_json(RECORDED / "judge.json"),
    )


# ---------------------------------------------------------------------------
# Replay at the serving-endpoint boundary
# ---------------------------------------------------------------------------


@dataclass
class _EmbeddingItem:
    embedding: list[float]
    index: int


@dataclass
class _EmbeddingResponse:
    data: list[_EmbeddingItem]


@dataclass
class _ChatMessage:
    content: str


@dataclass
class _ChatChoice:
    message: _ChatMessage
    finish_reason: str = "stop"


@dataclass
class _ChatResponse:
    choices: list[_ChatChoice]


@dataclass
class JudgeCall:
    """One observed outbound judge call.

    Recorded so a test can measure **retrieval** recall — which candidate rules
    reached the judge — without reaching into ``RuleSuggester``'s private
    methods. The prompt is the observable boundary; the candidate list is in it.
    """

    table_fqn: str
    offered_rule_ids: tuple[str, ...]
    column_names: tuple[str, ...]


class ReplayEndpoints:
    """Replays ``serving_endpoints.query`` for both the embedding and judge calls.

    One class handles both because both features call the same SDK method; the
    keyword arguments distinguish them (``input=`` for embeddings, ``messages=``
    for chat completion).

    Attach it as the ``side_effect`` of an autospecced client rather than
    duck-typing a whole ``WorkspaceClient`` — see :func:`replay_client`.
    """

    def __init__(self, recordings: Recordings) -> None:
        self._recordings = recordings
        self.judge_calls: list[JudgeCall] = []
        self.embed_call_count = 0
        self.embedded_text_count = 0

    def query(self, **kwargs: Any) -> Any:
        """Serve whichever of the two call shapes the caller used.

        Takes ``**kwargs`` rather than restating the endpoint's parameters:
        the autospecced mock this hangs off has already bound the call against
        the real SDK signature by the time it gets here, so restating them
        would only add a second, driftable copy (and shadow the ``input``
        builtin for no benefit).
        """
        texts = kwargs.get("input")
        if texts is not None:
            return self._embed(list(texts))
        messages = kwargs.get("messages")
        if messages is not None:
            return self._judge(messages)
        raise MissingRecordingError("serving_endpoints.query called with neither `input` nor `messages`")

    def _embed(self, texts: list[str]) -> _EmbeddingResponse:
        self.embed_call_count += 1
        self.embedded_text_count += len(texts)
        return _EmbeddingResponse(
            data=[
                _EmbeddingItem(embedding=self._recordings.vector_for(text), index=position)
                for position, text in enumerate(texts)
            ]
        )

    def _judge(self, messages: Sequence[Any]) -> _ChatResponse:
        prompt = self._user_prompt(messages)
        table_fqn = str(prompt.get("table", ""))
        offered = tuple(
            str(candidate.get("rule_id", ""))
            for candidate in prompt.get("candidate_rules", [])
            if isinstance(candidate, dict)
        )
        columns = tuple(str(column.get("name", "")) for column in prompt.get("columns", []) if isinstance(column, dict))
        self.judge_calls.append(JudgeCall(table_fqn=table_fqn, offered_rule_ids=offered, column_names=columns))

        # The oracle was recorded over the whole corpus; a real judge only ever
        # sees what retrieval offered, so restrict its verdict accordingly.
        offered_set = set(offered)
        suggestions = [item for item in self._recordings.verdict_for(table_fqn) if item.get("rule_id") in offered_set]
        content = json.dumps({"suggestions": suggestions})
        return _ChatResponse(choices=[_ChatChoice(message=_ChatMessage(content=content))])

    @staticmethod
    def _user_prompt(messages: Sequence[Any]) -> dict[str, Any]:
        """Pull the JSON payload out of the last user message.

        ``AIGateway`` converts the caller's dicts into SDK ``ChatMessage``
        objects before the boundary, so read ``role``/``content`` off the
        object rather than assuming a dict.
        """
        for message in reversed(list(messages)):
            role = str(getattr(message, "role", "") or "")
            if not role.lower().endswith("user"):
                continue
            content = getattr(message, "content", "") or ""
            try:
                parsed = json.loads(content)
            except (TypeError, ValueError) as e:
                raise MissingRecordingError(f"Judge user prompt was not JSON: {content[:200]}") from e
            if isinstance(parsed, dict):
                return parsed
        raise MissingRecordingError("Judge call carried no user message")


def replay_client(recordings: Recordings) -> tuple[Any, ReplayEndpoints]:
    """Build an autospecced ``WorkspaceClient`` whose only live surface is replay.

    The ``serving_endpoints`` child is autospecced *explicitly* against
    :class:`ServingEndpointsExt`. That matters: ``serving_endpoints`` is a
    property on :class:`WorkspaceClient`, and ``create_autospec`` does not
    follow a property's return type, so the default child mock accepts any
    keyword at all. Speccing it directly means every outbound call is bound
    against the real SDK signature — a kwarg the endpoint would reject fails
    here instead of in production, which is precisely the assurance a
    hand-rolled duck-typed double cannot give.
    """
    endpoints = ReplayEndpoints(recordings)
    serving_endpoints = create_autospec(ServingEndpointsExt, instance=True)
    serving_endpoints.query.side_effect = endpoints.query
    client = create_autospec(WorkspaceClient, instance=True)
    client.serving_endpoints = serving_endpoints
    return client, endpoints


# ---------------------------------------------------------------------------
# Real pipeline, faked data sources
# ---------------------------------------------------------------------------


def _app_settings(*, embed_endpoint: str = EMBED_ENDPOINT, judge_endpoint: str = JUDGE_ENDPOINT) -> Any:
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_embedding_endpoint_name.return_value = embed_endpoint
    settings.get_ai_enabled.return_value = True
    settings.get_ai_endpoint_name.return_value = judge_endpoint
    # 0 means unlimited in AIGateway. The limiter is per-instance and
    # in-memory; a 20-table sweep would otherwise trip the production default
    # of 30/hour partway through and score the remaining tables as failures.
    settings.get_ai_rate_limit_per_user_per_hour.return_value = 0
    return settings


def corpus_executor(corpus: Sequence[RegistryRule], vector_for: Callable[[str], list[float]]) -> Any:
    """Fake the OLTP executor so ``dq_rule_embeddings`` reads come from *vector_for*.

    Each rule's stored vector is derived by running it through the **real**
    :func:`build_rule_embed_text`. That is deliberate on the replay path: if
    someone changes how a rule becomes embed text, the digest stops matching and
    :class:`MissingRecordingError` says so, instead of the eval quietly scoring
    against a stale corpus.

    Args:
        corpus: The pinned registry rules to stock the corpus table with.
        vector_for: Maps embed text to a vector. Replay passes
            :meth:`Recordings.vector_for`; the live tier passes a closure over
            the real embedding endpoint.
    """
    rows = [
        {"rule_id": rule.rule_id, "embedding": json.dumps(vector_for(build_rule_embed_text(rule)))} for rule in corpus
    ]
    executor = create_autospec(SqlExecutor, instance=True)
    executor.fqn.return_value = "eval.dqx_studio.dq_rule_embeddings"
    executor.query_dicts.return_value = rows
    return executor


def assemble_suggester(
    table: EvalTable,
    corpus: Sequence[RegistryRule],
    client: Any,
    vector_for: Callable[[str], list[float]],
    *,
    embed_endpoint: str = EMBED_ENDPOINT,
    judge_endpoint: str = JUDGE_ENDPOINT,
    applied: Sequence[Any] = (),
) -> RuleSuggester:
    """Assemble the real suggester pipeline for one table around *client*.

    Real: :class:`RuleSuggester`, :class:`CosineRuleRetriever`,
    :class:`RuleEmbeddingsService`, :class:`AIGateway`, and every
    post-processing gate.

    Faked: the data sources only — the monitored-table, registry, apply-rules
    and discovery services are ``create_autospec`` doubles because they are
    *inputs* to the thing being measured, not part of it. This mirrors the
    fixture style already used in ``test_rule_suggester.py``.

    Both tiers come through here, differing only in the ``WorkspaceClient`` they
    are handed. Sharing the assembly is the point: if the live tier built its
    own pipeline, the two tiers could drift and stop being comparable.

    One suggester is built per table, which also matches production: these
    services come from per-request ``Depends`` factories, so the embeddings
    corpus cache lives for exactly one ``suggest`` call.
    """
    settings = _app_settings(embed_endpoint=embed_endpoint, judge_endpoint=judge_endpoint)

    embeddings = RuleEmbeddingsService(
        sql=corpus_executor(corpus, vector_for),
        sp_ws=client,
        app_settings=settings,
        user_ws=client,
    )
    retriever = CosineRuleRetriever(embeddings)
    gateway = AIGateway(user_ws=client, app_settings=settings)

    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    monitored_tables.get.return_value = MonitoredTableDetail(
        table=MonitoredTable(binding_id=table.binding_id, table_fqn=table.table_fqn, status="approved"),
        applied_rules=[],
    )
    monitored_tables.get_latest_profile.return_value = None

    rules_by_id = {rule.rule_id: rule for rule in corpus}
    registry = create_autospec(RegistryService, instance=True)
    registry.get_rule.side_effect = rules_by_id.get

    apply_rules = create_autospec(ApplyRulesService, instance=True)
    apply_rules.list_applied.return_value = list(applied)

    discovery = create_autospec(DiscoveryService, instance=True)
    discovery.get_table_columns_async.return_value = list(table.columns)

    return RuleSuggester(
        monitored_tables=monitored_tables,
        registry=registry,
        apply_rules=apply_rules,
        retriever=retriever,
        ai_gateway=gateway,
        discovery=discovery,
    )


def build_suggester(
    table: EvalTable,
    corpus: Sequence[RegistryRule],
    recordings: Recordings,
    *,
    applied: Sequence[Any] = (),
) -> tuple[RuleSuggester, ReplayEndpoints]:
    """Tier 1 assembly: the shared pipeline wired to the replaying client."""
    client, endpoints = replay_client(recordings)
    suggester = assemble_suggester(table, corpus, client, recordings.vector_for, applied=applied)
    return suggester, endpoints


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


@dataclass
class Metrics:
    """Aggregate suggestion-quality counts.

    Counts rather than per-table ratios, so tables can be summed and the
    ratios computed once at the end. Averaging per-table precision is what
    makes a table whose correct answer is "nothing" either divide by zero or
    dominate the mean.
    """

    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    predicted: int = 0
    expected: int = 0
    hits_at_k: int = 0
    ranked_at_k: int = 0
    violations: tuple[str, ...] = ()
    retrieved_expected: int = 0
    retrievable_expected: int = 0
    # Whether retrieval coverage was observable at all. It depends on reading the
    # outgoing judge prompt, which only the replay client can do, so the live
    # tier cannot measure it. Without this flag an unmeasured run reports
    # retrieval_recall as 0.000, which reads as a total retrieval failure.
    retrieval_observed: bool = False
    # Missed answers, split by *where* they were lost. A rule the judge never
    # saw is a retrieval problem; one it saw and passed over is a judge or
    # prompt problem. Merging the two into a single recall figure hides which
    # half to go and fix, so both are carried through the aggregate.
    missed_unretrieved: tuple[str, ...] = ()
    missed_after_retrieval: tuple[str, ...] = ()
    # Misses from a run where retrieval was not observable (the live tier), so
    # they cannot honestly be blamed on either stage. Kept separate rather than
    # lumped into "unretrieved", which would invent an attribution.
    missed_unattributed: tuple[str, ...] = ()

    def __add__(self, other: "Metrics") -> "Metrics":
        return Metrics(
            true_positives=self.true_positives + other.true_positives,
            false_positives=self.false_positives + other.false_positives,
            false_negatives=self.false_negatives + other.false_negatives,
            predicted=self.predicted + other.predicted,
            expected=self.expected + other.expected,
            hits_at_k=self.hits_at_k + other.hits_at_k,
            ranked_at_k=self.ranked_at_k + other.ranked_at_k,
            violations=self.violations + other.violations,
            retrieved_expected=self.retrieved_expected + other.retrieved_expected,
            retrievable_expected=self.retrievable_expected + other.retrievable_expected,
            retrieval_observed=self.retrieval_observed or other.retrieval_observed,
            missed_unretrieved=self.missed_unretrieved + other.missed_unretrieved,
            missed_after_retrieval=self.missed_after_retrieval + other.missed_after_retrieval,
            missed_unattributed=self.missed_unattributed + other.missed_unattributed,
        )

    @staticmethod
    def _ratio(numerator: int, denominator: int) -> float:
        # An empty denominator means "nothing was asked of this metric", which
        # is a pass, not a zero — the fixture set deliberately includes a table
        # whose correct answer is no suggestions at all.
        return 1.0 if denominator == 0 else numerator / denominator

    @property
    def precision(self) -> float:
        return self._ratio(self.true_positives, self.predicted)

    @property
    def recall(self) -> float:
        return self._ratio(self.true_positives, self.expected)

    @property
    def f1(self) -> float:
        precision, recall = self.precision, self.recall
        return 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)

    @property
    def precision_at_k(self) -> float:
        return self._ratio(self.hits_at_k, self.ranked_at_k)

    @property
    def retrieval_recall(self) -> float | None:
        """Share of expected rules that retrieval put in front of the judge.

        Separates the two failure modes: a rule the judge never saw is a
        retrieval miss (and the number the ``MAX_RETRIEVAL_COLUMNS`` cap moves);
        a rule it saw and passed over is a judge miss.

        ``None`` when retrieval was not observed — measuring it means reading the
        outgoing judge prompt, which only the replay client can do. Returning
        ``None`` rather than ``0.0`` keeps "not measured" from being read as
        "retrieved nothing".
        """
        if not self.retrieval_observed:
            return None
        return self._ratio(self.retrieved_expected, self.retrievable_expected)

    def summary(self) -> str:
        coverage = "n/a" if self.retrieval_recall is None else f"{self.retrieval_recall:.3f}"
        return (
            f"precision={self.precision:.3f} recall={self.recall:.3f} f1={self.f1:.3f} "
            f"p@k={self.precision_at_k:.3f} retrieval_recall={coverage} "
            f"tp={self.true_positives} fp={self.false_positives} fn={self.false_negatives} "
            f"(unretrieved={len(self.missed_unretrieved)} post-retrieval={len(self.missed_after_retrieval)}) "
            f"violations={len(self.violations)}"
        )


def prediction_keys(suggestions: Sequence[RuleSuggestion]) -> list[tuple[str, str]]:
    """Identity of each suggestion, in the order it was returned.

    Reuses ``compute_mapping_hash`` — the same order-insensitive identity the
    app already uses for its ``(rule_id, mapping_hash)`` exclusion keys — so a
    label and a prediction compare exactly, and a mapping listed in a different
    order is not counted as a different answer.
    """
    return [(s.rule_id, compute_mapping_hash([s.column_mapping])) for s in suggestions]


def score(
    suggestions: Sequence[RuleSuggestion],
    labels: TableLabels,
    judge_calls: Sequence[JudgeCall] = (),
    *,
    k: int = DEFAULT_PRECISION_AT_K,
) -> Metrics:
    """Score one table's suggestions against its answer key."""
    predicted = prediction_keys(suggestions)
    predicted_set = set(predicted)
    expected_set = {label.keys() for label in labels.expected}

    forbidden_exact = {label.keys() for label in labels.must_not_suggest if label.mapping is not None}
    forbidden_any_rule = {label.rule_id for label in labels.must_not_suggest if label.mapping is None}
    violations = tuple(
        f"{labels.binding_id}:{rule_id}:{mapping_hash[:8]}"
        for rule_id, mapping_hash in predicted
        if (rule_id, mapping_hash) in forbidden_exact or rule_id in forbidden_any_rule
    )

    top_k = predicted[:k]
    offered: set[str] = set()
    for call in judge_calls:
        offered.update(call.offered_rule_ids)
    expected_rule_ids = {label.rule_id for label in labels.expected}

    # Attribute every miss to the stage that lost it, so a recall drop points at
    # a stage rather than just at a number.
    unretrieved: list[str] = []
    after_retrieval: list[str] = []
    unattributed: list[str] = []
    for label in labels.expected:
        if label.keys() in predicted_set:
            continue
        described = f"{labels.binding_id}:{label.rule_id}->{label.mapping}"
        if not judge_calls:
            unattributed.append(described)
        elif label.rule_id in offered:
            after_retrieval.append(described)
        else:
            unretrieved.append(described)

    return Metrics(
        true_positives=len(predicted_set & expected_set),
        false_positives=len(predicted_set - expected_set),
        false_negatives=len(expected_set - predicted_set),
        predicted=len(predicted_set),
        expected=len(expected_set),
        hits_at_k=sum(1 for key in top_k if key in expected_set),
        ranked_at_k=len(top_k),
        violations=violations,
        retrieved_expected=len(expected_rule_ids & offered),
        retrievable_expected=len(expected_rule_ids),
        retrieval_observed=bool(judge_calls),
        missed_unretrieved=tuple(unretrieved),
        missed_after_retrieval=tuple(after_retrieval),
        missed_unattributed=tuple(unattributed),
    )


@dataclass
class TableResult:
    """One table's eval outcome, kept together for readable failure output."""

    table: EvalTable
    suggestions: list[RuleSuggestion] = field(default_factory=list)
    metrics: Metrics = field(default_factory=Metrics)
    reason: str = ""

    def summary(self) -> str:
        return f"{self.table.table_fqn}: {self.metrics.summary()}"


async def run_table(
    table: EvalTable,
    labels: TableLabels,
    corpus: Sequence[RegistryRule],
    recordings: Recordings,
    *,
    k: int = DEFAULT_PRECISION_AT_K,
) -> TableResult:
    """Run the real suggester over one fixture table and score the result."""
    suggester, endpoints = build_suggester(table, corpus, recordings)
    result = await suggester.suggest(table.binding_id, EVAL_USER)
    if not result.available:
        raise AssertionError(
            f"Suggester degraded to unavailable for {table.table_fqn}: {result.reason}. "
            f"The eval fixtures configure every dependency, so this is a real failure, "
            f"not a missing-infrastructure skip."
        )
    return TableResult(
        table=table,
        suggestions=list(result.suggestions),
        metrics=score(result.suggestions, labels, endpoints.judge_calls, k=k),
        reason=result.reason,
    )


async def run_all(
    *,
    k: int = DEFAULT_PRECISION_AT_K,
) -> tuple[list[TableResult], Metrics]:
    """Run the whole golden set and return per-table results plus the total."""
    corpus = load_corpus()
    tables = load_tables()
    labels = load_labels()
    recordings = load_recordings()

    results: list[TableResult] = []
    total = Metrics()
    for table in tables:
        table_labels = labels.get(table.binding_id)
        if table_labels is None:
            raise AssertionError(f"No labels for fixture table {table.binding_id}; labels.json is incomplete")
        result = await run_table(table, table_labels, corpus, recordings, k=k)
        results.append(result)
        total = total + result.metrics
    return results, total
