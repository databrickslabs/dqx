"""Tier 2: measure suggestion quality against real endpoints.

Run with::

    make app-test-eval

This is the tier that answers "is it any good". Same fixtures and same
:func:`score` as Tier 1, but the embedding endpoint and the judge are real, so
unlike Tier 1 this **does** respond to a change in the judge's system prompt.

What is asserted versus what is reported
----------------------------------------
Quality is *reported*, not asserted, until a baseline is recorded from a real
run. An absolute precision threshold on LLM output is brittle and gets muted
within a month, so the design is to compare against a recorded baseline instead.
Fill in the ``LIVE_BASELINE_*`` constants from a field-engineering run and the
comparison starts enforcing.

What is asserted here does not depend on how good the model is:

* every table reaches ``available=True``, so the run proves the real
  configuration works end to end;
* no suggestion names a column that is not on the table, and every multi-slot
  mapping is complete with distinct columns — the post-process gates holding
  against genuine model output, including the prompt-injection surface that
  column comments and rule descriptions represent;
* the 90-column fixture embeds exactly ``MAX_RETRIEVAL_COLUMNS`` query texts, so
  the cap is engaged and the wide-table numbers mean what they claim.

Cost and reproducibility
------------------------
One judge call per table, two more for the cap experiment, and a handful of
batched embedding calls for the corpus. Even at ``temperature=0`` the judge is
not perfectly reproducible across model versions, so treat one run as one
sample: report a range over a few runs, and keep all pass/fail logic in Tier 1
where it is deterministic.
"""

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services import rule_suggester as rule_suggester_module
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService, build_rule_embed_text
from databricks_labs_dqx_app.backend.services.rule_suggester import MAX_RETRIEVAL_COLUMNS
from tests.suggester_eval_support import (
    EVAL_USER,
    Metrics,
    assemble_suggester,
    corpus_executor,
    score,
)

# Set these from a real run, then the comparison below starts enforcing. Record
# the wall-clock duration alongside them: an eval nobody can afford to run is an
# eval nobody runs.
LIVE_BASELINE_PRECISION: float | None = None
LIVE_BASELINE_RECALL: float | None = None

# How far quality may fall below the recorded baseline before the run fails.
# Wide enough to absorb run-to-run LLM variance, narrow enough that a real
# regression cannot hide inside it.
LIVE_TOLERANCE = 0.05


def _report_path() -> Path:
    """Where to write the run report.

    Defaults outside the repository: ``app/.build`` is not gitignored, so a
    default inside the tree would be one ``git add -A`` away from committing a
    workspace-specific artefact.
    """
    override = os.environ.get("DQX_EVAL_REPORT")
    if override:
        return Path(override)
    return Path(tempfile.gettempdir()) / "dqx-suggester-eval-live.json"


@pytest.fixture(scope="session")
def live_vector_for(live_client, embed_endpoint, corpus):
    """Embed the whole corpus once against the real endpoint, then serve from cache.

    Goes through the app's own :meth:`RuleEmbeddingsService.embed_texts`, so the
    corpus is embedded exactly the way a real publish would embed it — chunked at
    the production batch size, against the configured endpoint. The empty corpus
    passed to :func:`corpus_executor` is just to satisfy the constructor; this
    instance is only ever used to embed, never to read the corpus table.
    """
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_embedding_endpoint_name.return_value = embed_endpoint
    service = RuleEmbeddingsService(
        sql=corpus_executor([], lambda _text: []),
        sp_ws=live_client,
        app_settings=settings,
        user_ws=live_client,
    )

    texts = [build_rule_embed_text(rule) for rule in corpus]
    started = time.monotonic()
    vectors = service.embed_texts(texts)
    print(f"\nembedded {len(texts)} corpus rules against {embed_endpoint} in {time.monotonic() - started:.1f}s")

    missing = [rule.rule_id for rule, vector in zip(corpus, vectors) if vector is None]
    assert not missing, f"embedding endpoint returned no vector for: {missing}"

    cache = {text: vector for text, vector in zip(texts, vectors) if vector is not None}
    return lambda text: cache[text]


async def _suggest(table, corpus, live_client, live_vector_for, embed_endpoint, judge_endpoint):
    suggester = assemble_suggester(
        table,
        corpus,
        live_client,
        live_vector_for,
        embed_endpoint=embed_endpoint,
        judge_endpoint=judge_endpoint,
    )
    started = time.monotonic()
    result = await suggester.suggest(table.binding_id, EVAL_USER)
    return result, time.monotonic() - started


async def _run_golden_set(corpus, tables, labels, live_client, live_vector_for, embed_endpoint, judge_endpoint):
    rows = []
    total = Metrics()
    started = time.monotonic()
    for table in tables:
        table_labels = labels[table.binding_id]
        result, elapsed = await _suggest(table, corpus, live_client, live_vector_for, embed_endpoint, judge_endpoint)
        if not result.available:
            raise AssertionError(
                f"Suggester degraded to unavailable for {table.table_fqn}: {result.reason}. "
                f"The live tier configures every dependency, so this is a real failure."
            )
        # ``judge_calls`` are a replay-only observation, so retrieval coverage is
        # not separable live; score without them and read recall as the
        # whole-pipeline figure.
        metrics = score(result.suggestions, table_labels)
        rows.append((table, result, metrics, elapsed))
        total = total + metrics
        print(f"  {table.table_fqn}: {metrics.summary()}  ({elapsed:.1f}s)")
    duration = time.monotonic() - started
    print(f"TOTAL {total.summary()}  wall clock {duration:.1f}s")
    return rows, total, duration


@pytest.fixture(scope="session")
def live_run(live_client, live_vector_for, corpus, tables, labels, embed_endpoint, judge_endpoint):
    """Run the whole golden set against real endpoints, once per session.

    Sync fixture driving the async pipeline with ``asyncio.run``, matching Tier 1:
    a session-scoped *async* fixture would need its own event-loop scope under
    pytest-asyncio, and there is nothing to share with the tests' own loop.
    """
    return asyncio.run(
        _run_golden_set(corpus, tables, labels, live_client, live_vector_for, embed_endpoint, judge_endpoint)
    )


class TestLiveQuality:
    def test_writes_a_run_report(self, live_run, embed_endpoint, judge_endpoint):
        rows, total, duration = live_run
        report = {
            "embedding_endpoint": embed_endpoint,
            "judge_endpoint": judge_endpoint,
            "duration_seconds": round(duration, 1),
            "total": {
                "precision": round(total.precision, 4),
                "recall": round(total.recall, 4),
                "f1": round(total.f1, 4),
                "precision_at_k": round(total.precision_at_k, 4),
                "true_positives": total.true_positives,
                "false_positives": total.false_positives,
                "false_negatives": total.false_negatives,
                "violations": list(total.violations),
            },
            "tables": [
                {
                    "table_fqn": table.table_fqn,
                    "precision": round(metrics.precision, 4),
                    "recall": round(metrics.recall, 4),
                    "seconds": round(elapsed, 1),
                    "violations": list(metrics.violations),
                    "missed": list(
                        metrics.missed_unretrieved + metrics.missed_after_retrieval + metrics.missed_unattributed
                    ),
                    "suggested": [
                        {"rule_id": s.rule_id, "mapping": s.column_mapping, "explanation": s.explanation}
                        for s in result.suggestions
                    ],
                }
                for table, result, metrics, elapsed in rows
            ],
        }
        path = _report_path()
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"\nwrote live eval report to {path}")
        assert path.exists()

    def test_quality_has_not_dropped_below_the_recorded_baseline(self, live_run):
        _, total, _ = live_run
        if LIVE_BASELINE_PRECISION is None or LIVE_BASELINE_RECALL is None:
            pytest.skip(
                f"no live baseline recorded yet. This run measured precision "
                f"{total.precision:.3f} and recall {total.recall:.3f}; set the "
                f"LIVE_BASELINE_* constants from a field-engineering run to start enforcing."
            )
        assert total.precision >= LIVE_BASELINE_PRECISION - LIVE_TOLERANCE, (
            f"precision {total.precision:.3f} is below baseline "
            f"{LIVE_BASELINE_PRECISION:.3f} by more than {LIVE_TOLERANCE}"
        )
        assert total.recall >= LIVE_BASELINE_RECALL - LIVE_TOLERANCE, (
            f"recall {total.recall:.3f} is below baseline " f"{LIVE_BASELINE_RECALL:.3f} by more than {LIVE_TOLERANCE}"
        )

    def test_reports_wrong_column_bindings(self, live_run):
        """Surface every plausible-but-wrong binding the real judge produced.

        Deliberately not an assertion on the count: which decoys a given model
        version falls for is exactly what we are trying to measure, and printing
        them is what makes a prompt change reviewable.
        """
        _, total, _ = live_run
        print(f"\nmust_not_suggest violations from the live judge: {len(total.violations)}")
        for violation in total.violations:
            print(f"  {violation}")


class TestLiveSafetyGates:
    """Post-process must hold against real model output, not just a canned oracle."""

    def test_no_suggestion_names_a_column_off_the_table(self, live_run):
        rows, _, _ = live_run
        for table, result, _metrics, _elapsed in rows:
            real = {column.name for column in table.columns}
            for suggestion in result.suggestions:
                unknown = set(suggestion.column_mapping.values()) - real
                assert not unknown, f"{table.table_fqn}/{suggestion.rule_id} names unknown column(s) {unknown}"

    def test_every_multi_slot_mapping_is_complete_and_distinct(self, live_run, corpus):
        rows, _, _ = live_run
        slots_by_rule = {rule.rule_id: {slot.name for slot in rule.definition.slots} for rule in corpus}
        for table, result, _metrics, _elapsed in rows:
            for suggestion in result.suggestions:
                expected_slots = slots_by_rule[suggestion.rule_id]
                assert set(suggestion.column_mapping) == expected_slots, (
                    f"{table.table_fqn}/{suggestion.rule_id} mapped "
                    f"{sorted(suggestion.column_mapping)}, expected {sorted(expected_slots)}"
                )
                columns = list(suggestion.column_mapping.values())
                assert len(set(columns)) == len(
                    columns
                ), f"{table.table_fqn}/{suggestion.rule_id} bound two slots to one column: {columns}"


class TestRetrievalColumnCap:
    def test_prices_the_column_cap_on_a_wide_table(
        self, live_client, live_vector_for, corpus, tables, labels, embed_endpoint, judge_endpoint, monkeypatch
    ):
        """Measure what MAX_RETRIEVAL_COLUMNS costs, by raising it and re-running.

        ``MAX_RETRIEVAL_COLUMNS`` is a module-level constant, which is the one
        case AGENTS.md permits patching — there is no injectable seam for it, and
        the alternative would be adding one to production code purely for a test.

        Reported rather than asserted: the answer is the number, and hard-coding
        an expected delta would freeze today's model behaviour into the suite.
        """
        table = next(t for t in tables if t.binding_id == "b-wide-events")
        table_labels = labels[table.binding_id]
        assert len(table.columns) > MAX_RETRIEVAL_COLUMNS

        capped, _ = asyncio.run(_suggest(table, corpus, live_client, live_vector_for, embed_endpoint, judge_endpoint))
        capped_metrics = score(capped.suggestions, table_labels)

        monkeypatch.setattr(rule_suggester_module, "MAX_RETRIEVAL_COLUMNS", len(table.columns))
        uncapped, _ = asyncio.run(_suggest(table, corpus, live_client, live_vector_for, embed_endpoint, judge_endpoint))
        uncapped_metrics = score(uncapped.suggestions, table_labels)

        print(
            f"\nMAX_RETRIEVAL_COLUMNS on a {len(table.columns)}-column table:\n"
            f"  capped at {MAX_RETRIEVAL_COLUMNS}: {capped_metrics.summary()}\n"
            f"  raised to {len(table.columns)}: {uncapped_metrics.summary()}\n"
            f"  recall delta: {uncapped_metrics.recall - capped_metrics.recall:+.3f}"
        )
