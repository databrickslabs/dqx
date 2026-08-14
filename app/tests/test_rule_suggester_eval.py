"""Tier 1 evaluation of the AI rule suggester — deterministic, offline, in CI.

Runs the **real** suggester pipeline (retriever, embeddings service, AI gateway,
post-process) over a labelled golden set, replaying only the outbound
``serving_endpoints.query`` call. See ``suggester_eval_support`` for the design
and, importantly, for what this tier does and does not hold fixed: it measures
**retrieval + post-process** against a recorded judge oracle, so a change to the
judge's prompt is invisible here and has to be measured live in
``tests/eval_live/``.

The point of these tests is not to prove the suggester is good. It is to make a
change in suggestion quality *visible*, so the next retrieval refactor lands
with a number attached rather than an assumption.
"""

import asyncio
from collections.abc import Sequence

import pytest

from databricks_labs_dqx_app.backend.registry_models import ColumnMappingGroup, compute_mapping_hash
from databricks_labs_dqx_app.backend.services.rule_suggester import MAX_RETRIEVAL_COLUMNS, RuleSuggestion
from tests.suggester_eval_support import (
    Label,
    Metrics,
    Recordings,
    TableLabels,
    build_suggester,
    load_corpus,
    load_labels,
    load_recordings,
    load_tables,
    prediction_keys,
    run_all,
    run_table,
    score,
)

# ---------------------------------------------------------------------------
# Committed baseline
# ---------------------------------------------------------------------------
# Tier 1 is fully deterministic, so the baseline is an exact expectation rather
# than a threshold: any change to retrieval, to the post-process gates, or to
# the fixtures moves one of these numbers and this test says which.
#
# Updating it is legitimate — but it is a decision, not a chore. Re-run the
# suite, read the new number, and only commit it once you can say why the
# change is an improvement. Silently re-baselining is how an eval harness stops
# meaning anything.
#
# The absolute values are NOT a quality claim. The fixture tables are synthetic
# and far cleaner than real customer data, and the oracle's mistakes are ones we
# planted. Compare runs to each other, never to a marketing number.
BASELINE_TRUE_POSITIVES = 41
BASELINE_FALSE_POSITIVES = 5
BASELINE_FALSE_NEGATIVES = 7
BASELINE_PREDICTED = 46
BASELINE_EXPECTED = 48

# Retrieval does NOT surface every correct rule, so this is a measured baseline
# rather than an invariant. All seven current misses are rules the judge never
# saw, and six of the seven are the universal not-null rule: it carries no
# column-specific wording to match on, so against an 82-rule corpus with top-20
# per-column retrieval it loses to eighty more specific rules on nearly every
# table. The judge's prompt instructs it at length to apply not-null broadly,
# which it cannot do for a rule that was never a candidate.
#
# Counted over distinct rule *ids*, not label rows: retrieval returns rule ids,
# so a rule correctly wanted on two columns of the same table is one retrieval
# opportunity, not two.
BASELINE_RETRIEVED_EXPECTED = 40
BASELINE_RETRIEVABLE_EXPECTED = 45

# The oracle plants three plausible-but-wrong column bindings that every
# structural gate in ``_post_process`` accepts, because each one is a real
# column of the right family with all slots filled. Only a labelled
# expectation catches them, which is the whole argument for this fixture.
EXPECTED_VIOLATIONS = {
    "b-customers:rr-email-format",  # bound to secondary_contact, not email
    "b-orders:rr-unique",  # bound to a foreign key that legitimately repeats
    "b-invoices:rr-not-in-future",  # bound to due_at, which is meant to be future-dated
    "b-shipments:rr-start-before-end",  # the two temporal slots bound the wrong way round
    "b-employees:rr-email-format",  # bound to a free-text manager note
}

# Two further violations are planted in the oracle but are NOT currently
# exercised, because retrieval never offers the rule for that table so the entry
# is filtered out before post-process ever sees it:
#
#   b-raw-landing  rr-not-null on a column documented as legitimately null
#   b-sensors      rr-positive-amount (a monetary rule) on a humidity reading
#
# Both are left in place rather than re-tuned. Bending the answer key so the
# stand-in embedder happens to surface them would be fitting the fixture to the
# artefact; when Tier 2 supplies real vectors these should start firing, and if
# they do, this set is what needs updating.


@pytest.fixture(scope="module")
def evaluated():
    """Run the golden set once; every test in this module reads the same result.

    Sync fixture driving the async pipeline with ``asyncio.run`` on purpose: a
    module-scoped *async* fixture would need its own event-loop scope under
    pytest-asyncio, and the eval is a single self-contained run with nothing to
    share with the loop the tests themselves use.
    """
    return asyncio.run(run_all())


class TestGoldenSetBaseline:
    def test_matches_committed_baseline(self, evaluated):
        results, total = evaluated
        detail = "\n".join(f"  {result.summary()}" for result in results)
        assert (
            total.true_positives,
            total.false_positives,
            total.false_negatives,
            total.predicted,
            total.expected,
        ) == (
            BASELINE_TRUE_POSITIVES,
            BASELINE_FALSE_POSITIVES,
            BASELINE_FALSE_NEGATIVES,
            BASELINE_PREDICTED,
            BASELINE_EXPECTED,
        ), (
            f"Suggestion quality moved against the committed baseline.\n"
            f"  now:      {total.summary()}\n"
            f"  baseline: tp={BASELINE_TRUE_POSITIVES} fp={BASELINE_FALSE_POSITIVES} "
            f"fn={BASELINE_FALSE_NEGATIVES} predicted={BASELINE_PREDICTED}\n"
            f"per table:\n{detail}\n"
            f"If the change is an improvement, update the BASELINE_* constants and say why "
            f"in the commit message."
        )

    def test_every_table_produced_a_result(self, evaluated):
        results, _ = evaluated
        assert len(results) == len(load_tables())

    def test_reports_readable_aggregate_metrics(self, evaluated):
        _, total = evaluated
        # Not a threshold assertion — this documents the shape of what the
        # harness computes, so a scorer bug that zeroes or saturates a metric is
        # still caught while the baseline above is being deliberately re-based.
        assert 0.0 < total.precision < 1.0
        assert 0.0 < total.recall < 1.0
        assert 0.0 < total.f1 < 1.0
        assert 0.0 < total.precision_at_k <= 1.0


class TestRetrievalCoverage:
    def test_every_expected_rule_reaches_the_judge(self, evaluated):
        """Separate a retrieval miss from a judge miss.

        A rule the judge never saw cannot be suggested no matter how good the
        judge is, so this is the number ``MAX_RETRIEVAL_COLUMNS`` moves. Keeping
        it distinct from recall is what makes the two failure modes
        distinguishable at a glance.
        """
        _, total = evaluated
        assert (total.retrieved_expected, total.retrievable_expected) == (
            BASELINE_RETRIEVED_EXPECTED,
            BASELINE_RETRIEVABLE_EXPECTED,
        ), (
            f"Retrieval coverage moved: now {total.retrieved_expected}/"
            f"{total.retrievable_expected}, baseline {BASELINE_RETRIEVED_EXPECTED}/"
            f"{BASELINE_RETRIEVABLE_EXPECTED}.\nRules the judge never saw:\n"
            + "\n".join(f"  {line}" for line in total.missed_unretrieved)
        )

    def test_every_miss_is_attributed_to_a_stage(self, evaluated):
        """A recall miss must be pinned on retrieval or on the judge, never both.

        Without the split, a recall drop says only that quality fell. With it,
        the failure output names which half of the pipeline to go and look at.
        """
        _, total = evaluated
        attributed = len(total.missed_unretrieved) + len(total.missed_after_retrieval)
        assert attributed == total.false_negatives
        # Tier 1 always observes retrieval, so nothing should land in the
        # can't-attribute bucket that exists for the live tier.
        assert total.missed_unattributed == ()

    def test_the_wide_table_exercises_the_retrieval_column_cap(self):
        """Confirm the 64-column cap is actually engaged, and that we can see it.

        Independent of how good the embeddings are: a 90-column table must
        produce exactly ``MAX_RETRIEVAL_COLUMNS`` query texts. If the cap is
        raised, removed, or applied to the wrong list, this count moves and the
        wide-table numbers stop meaning what the fixture note claims.
        """
        table = next(t for t in load_tables() if t.binding_id == "b-wide-events")
        assert len(table.columns) > MAX_RETRIEVAL_COLUMNS

        suggester, endpoints = build_suggester(table, load_corpus(), load_recordings())
        asyncio.run(suggester.suggest(table.binding_id, "eval@example.com"))

        assert endpoints.embedded_text_count == MAX_RETRIEVAL_COLUMNS, (
            f"A {len(table.columns)}-column table produced "
            f"{endpoints.embedded_text_count} query texts; the cap is {MAX_RETRIEVAL_COLUMNS}."
        )

    def test_per_column_retrieval_costs_one_embedding_round_trip(self):
        """Every column's query text must go out in a single batched call.

        Retrieval builds one query per column, so before batching a seven-column
        table cost seven embedding round trips. Asserting the HTTP call count at
        the boundary is what keeps that from regressing quietly — the call count
        is invisible from inside the retriever, and a per-column round trip is a
        latency regression rather than a wrong answer, so no correctness test
        would notice.
        """
        table = next(t for t in load_tables() if t.binding_id == "b-orders")
        suggester, endpoints = build_suggester(table, load_corpus(), load_recordings())

        asyncio.run(suggester.suggest(table.binding_id, "eval@example.com"))

        assert endpoints.embedded_text_count == len(table.columns)
        assert endpoints.embed_call_count == 1, (
            f"{endpoints.embedded_text_count} column query texts went out in "
            f"{endpoints.embed_call_count} calls; expected one batch."
        )


class TestWrongColumnBindings:
    def test_planted_violations_are_all_detected(self, evaluated):
        """The harness must catch a rule bound to a plausible-but-wrong column.

        This is the failure ``_post_process`` structurally cannot see, and the
        reason a golden set is needed rather than more unit tests.
        """
        _, total = evaluated
        detected = {":".join(violation.split(":")[:2]) for violation in total.violations}
        assert detected == EXPECTED_VIOLATIONS, (
            f"must_not_suggest detection changed.\n  detected: {sorted(detected)}\n"
            f"  expected: {sorted(EXPECTED_VIOLATIONS)}"
        )


class TestPostProcessGates:
    """The oracle deliberately emits malformed entries; none may survive.

    Asserted through the public result rather than by inspecting
    ``_post_process``, so these stay behavioural.
    """

    def test_hallucinated_column_is_dropped(self, evaluated):
        results, _ = evaluated
        customers = next(r for r in results if r.table.binding_id == "b-customers")
        mapped_columns = {column for s in customers.suggestions for column in s.column_mapping.values()}
        real_columns = {column.name for column in customers.table.columns}
        assert (
            mapped_columns <= real_columns
        ), f"Suggested a column that is not on the table: {mapped_columns - real_columns}"

    def test_incomplete_multi_slot_mapping_is_dropped(self, evaluated):
        results, _ = evaluated
        customers = next(r for r in results if r.table.binding_id == "b-customers")
        # The oracle offers rr-start-before-end with only start_ts filled.
        assert all(s.rule_id != "rr-start-before-end" for s in customers.suggestions)

    def test_same_column_multi_slot_mapping_is_dropped(self, evaluated):
        results, _ = evaluated
        orders = next(r for r in results if r.table.binding_id == "b-orders")
        multi_slot = [s for s in orders.suggestions if s.rule_id == "rr-start-before-end"]
        assert len(multi_slot) == 1
        assert multi_slot[0].column_mapping == {"start_ts": "ordered_at", "end_ts": "shipped_at"}

    def test_unknown_rule_id_is_dropped(self, evaluated):
        results, _ = evaluated
        orders = next(r for r in results if r.table.binding_id == "b-orders")
        assert all(s.rule_id != "rr-does-not-exist" for s in orders.suggestions)

    def test_duplicate_mapping_is_collapsed(self, evaluated):
        results, _ = evaluated
        customers = next(r for r in results if r.table.binding_id == "b-customers")
        keys = prediction_keys(customers.suggestions)
        assert len(keys) == len(set(keys))


class TestScorer:
    """The scorer itself needs teeth, or the metrics above prove nothing."""

    def test_mapping_order_does_not_change_identity(self):
        """A multi-slot mapping written in a different key order is the same answer.

        Guaranteed by reusing ``compute_mapping_hash`` — the identity the app
        already uses for its own dedup keys — rather than inventing one here.
        """
        forward = {"start_ts": "ordered_at", "end_ts": "shipped_at"}
        reverse = {"end_ts": "shipped_at", "start_ts": "ordered_at"}
        assert compute_mapping_hash([forward]) == compute_mapping_hash([reverse])

    def test_a_dropped_correct_answer_shows_up_as_recall_loss(self):
        labels = TableLabels(
            binding_id="b-test",
            expected=(
                Label("rr-not-null", {"column": "id"}),
                Label("rr-unique", {"column": "id"}),
            ),
        )
        both = _suggestions([("rr-not-null", {"column": "id"}), ("rr-unique", {"column": "id"})])
        one = _suggestions([("rr-not-null", {"column": "id"})])

        assert score(both, labels).recall == 1.0
        assert score(one, labels).recall == 0.5

    def test_a_wrong_column_shows_up_as_precision_loss_and_a_violation(self):
        labels = TableLabels(
            binding_id="b-test",
            expected=(Label("rr-email-format", {"column": "email"}),),
            must_not_suggest=(Label("rr-email-format", {"column": "secondary_contact"}),),
        )
        shifted = _suggestions([("rr-email-format", {"column": "secondary_contact"})])

        metrics = score(shifted, labels)
        assert metrics.precision == 0.0
        assert metrics.recall == 0.0
        assert len(metrics.violations) == 1

    def test_a_table_with_no_expected_answer_scores_clean_when_nothing_is_returned(self):
        labels = TableLabels(binding_id="b-test")
        metrics = score([], labels)
        # An empty denominator is "nothing was asked", not a zero — otherwise
        # the deliberately-empty fixture table would drag every aggregate down.
        assert metrics.precision == 1.0
        assert metrics.recall == 1.0

    def test_totals_are_summable_across_tables(self):
        left = Metrics(true_positives=2, false_positives=1, predicted=3, expected=2)
        right = Metrics(true_positives=1, false_negatives=1, predicted=1, expected=2)
        total = left + right
        assert (total.true_positives, total.false_positives, total.false_negatives) == (3, 1, 1)
        assert total.precision == 0.75
        assert total.recall == 0.75


class TestHarnessSensitivity:
    """Prove the harness fails when quality actually regresses.

    An eval that has never been shown to go red is not evidence — it is a
    second copy of the happy path. Each test here injects a *different* kind of
    regression and asserts the corresponding metric moves, so a future change
    that neuters the harness (a scorer that always returns 1.0, a label file
    that stops being read) is caught by the harness's own tests.

    Every injection is done by varying the harness's **input data**, never by
    patching the suggester's internals — a break that only exists behind a
    monkeypatched private method proves nothing about the real path.
    """

    def _run(self, binding_id: str, recordings: Recordings, corpus=None):
        table = next(t for t in load_tables() if t.binding_id == binding_id)
        labels = load_labels()[binding_id]
        corpus = load_corpus() if corpus is None else corpus
        return asyncio.run(run_table(table, labels, corpus, recordings))

    def test_rotating_column_attribution_collapses_precision(self):
        """The regression the current code is most exposed to.

        ``RetrievedRule`` carries no column, so the column that earned a match
        is discarded before the judge is asked — nothing downstream can tell a
        right binding from a plausible wrong one. Rotating the oracle's columns
        simulates exactly that going wrong, and precision has to notice.
        """
        recordings = load_recordings()
        verdict = recordings.judge["eval.golden.orders"]
        columns = [entry["mapping"] for entry in verdict]
        rotated = columns[1:] + columns[:1]
        recordings.judge["eval.golden.orders"] = [
            {**entry, "mapping": mapping} for entry, mapping in zip(verdict, rotated)
        ]

        baseline = self._run("b-orders", load_recordings())
        broken = self._run("b-orders", recordings)

        assert broken.metrics.precision < baseline.metrics.precision, (
            f"Rotating every column binding did not move precision "
            f"(baseline {baseline.metrics.summary()} / broken {broken.metrics.summary()}). "
            f"The harness cannot detect a mis-attribution regression."
        )

    def test_dropping_a_correct_answer_lowers_recall(self):
        """Remove a named correct answer, not an arbitrary index.

        Dropping ``judge[table][0]`` looked equivalent and was not: the first
        oracle entry is the not-null rule, which retrieval no longer surfaces, so
        removing it changed nothing and the test passed while proving nothing.
        Naming the rule — and asserting the baseline really did suggest it —
        keeps the injection honest as the fixtures grow.
        """
        dropped = "rr-email-format"
        recordings = load_recordings()
        recordings.judge["eval.golden.customers"] = [
            entry for entry in recordings.judge["eval.golden.customers"] if entry["rule_id"] != dropped
        ]

        baseline = self._run("b-customers", load_recordings())
        broken = self._run("b-customers", recordings)

        assert any(s.rule_id == dropped for s in baseline.suggestions), (
            f"{dropped} is not in the baseline suggestions, so removing it cannot "
            f"demonstrate anything. Pick a rule the baseline actually returns."
        )
        assert broken.metrics.recall < baseline.metrics.recall

    def test_a_labelled_rule_missing_from_the_corpus_lowers_retrieval_recall(self):
        """Retrieval recall is the metric that isolates "the judge never saw it"."""
        corpus = [rule for rule in load_corpus() if rule.rule_id != "rr-currency-code"]

        baseline = self._run("b-orders", load_recordings())
        broken = self._run("b-orders", load_recordings(), corpus=corpus)

        assert broken.metrics.retrieval_recall < baseline.metrics.retrieval_recall
        assert broken.metrics.recall < baseline.metrics.recall

    def test_a_drifted_rule_definition_fails_loudly_rather_than_scoring_wrong(self):
        """Corpus drift must raise, not quietly score against a stale embedding.

        The embed text is derived by the real ``build_rule_embed_text``, so
        editing a rule's metadata changes its digest. Degrading to a default
        vector there would let the eval keep reporting a number that no longer
        describes the code.
        """
        corpus = load_corpus()
        corpus[0].user_metadata["description"] = "An edited description that was never embedded."

        with pytest.raises(AssertionError, match="No recorded embedding"):
            self._run("b-customers", load_recordings(), corpus=corpus)


class TestLabelsAndFixtures:
    def test_every_table_has_labels(self):
        labels = load_labels()
        missing = [table.binding_id for table in load_tables() if table.binding_id not in labels]
        assert not missing, f"Fixture tables without an answer key: {missing}"

    def test_every_labelled_mapping_names_real_columns(self):
        """A typo in the answer key would silently depress recall forever."""
        labels = load_labels()
        for table in load_tables():
            table_labels = labels[table.binding_id]
            real = {column.name for column in table.columns}
            for label in table_labels.expected + table_labels.must_not_suggest:
                if label.mapping is None:
                    continue
                unknown = set(label.mapping.values()) - real
                assert not unknown, f"{table.binding_id}/{label.rule_id} maps unknown column(s) {unknown}"


def _suggestions(pairs: Sequence[tuple[str, ColumnMappingGroup]]) -> list[RuleSuggestion]:
    """Build minimal suggestions for the scorer unit tests."""
    return [
        RuleSuggestion(
            rule_id=rule_id,
            rule_name=rule_id,
            dimension=None,
            severity=None,
            column_mapping=mapping,
        )
        for rule_id, mapping in pairs
    ]
