"""Unit tests for the public ai_query-based group explainer interface.

Spark is never started. These tests exercise the public prompt, endpoint probe, and
ExplanationContext surfaces.
"""

import json
from pathlib import Path
from unittest.mock import create_autospec

from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql import SparkSession

import pytest

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import ExplanationContext
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig, ScoringOutputColumns
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.errors import InvalidParameterError


class _FakeSpark:
    """Minimal Spark stand-in for ``probe_endpoint_reachable``: records SQL and either returns a
    collectable result or raises, mirroring an endpoint that is reachable / unreachable."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.queries: list[str] = []

    def sql(self, query: str) -> "_FakeSpark":
        self.queries.append(query)
        if self.fail:
            raise AnalysisException("private endpoint detail", errorClass="PERMISSION_DENIED")
        return self

    def collect(self) -> list:
        return []


def _prompt_input_descriptions(header: str) -> dict[str, str]:
    """Extract the rendered public input-field section from the production prompt."""
    input_section = header.partition("\nInputs:\n")[2].partition("\n\nRespond with ONLY a JSON object.")[0]
    return dict(line.removeprefix("- ").split(": ", maxsplit=1) for line in input_section.splitlines())


def _prompt_exemplar_field_values(field_name: str) -> list[str]:
    """Read a named field from the public prompt's few-shot examples."""
    prefix = f"{field_name}: "
    return [
        line.removeprefix(prefix)
        for line in llm_explainer.AI_QUERY_PROMPT_HEADER.splitlines()
        if line.startswith(prefix)
    ]


def test_ai_query_prompt_header_includes_instructions_and_field_descriptions():
    """The public prompt retains its instructions and the independently named fields."""
    header = llm_explainer.AI_QUERY_PROMPT_HEADER
    assert "data quality analyst" in header  # instructions line
    expected_input_fields = {
        "attribution_basis",
        "evidence_disclosure",
        "feature_contributions",
        "basis_contributions",
        "group_size",
        "severity_range",
        "confidence",
        "baseline_grouping",
        "temporal_baseline",
        "threshold",
        "drift_summary",
    }
    assert set(_prompt_input_descriptions(header)) == expected_input_fields
    for output_name in ("narrative", "business_impact", "action"):
        assert f"- {output_name}:" in header
    assert "Respond with ONLY a JSON object" in header


_PROMPT_SNAPSHOT = Path(__file__).resolve().parents[1] / "resources" / "ai_query_prompt_header.txt"


def test_ai_query_prompt_header_matches_snapshot():
    """Change-control gate for the LLM prompt.

    The public prompt is the contract the model sees. Pinning it to a committed snapshot forces a
    prompt change to be deliberate and surfaces it in review as a diff to the golden file (no LLM,
    no workspace, runs in ms).

    If the change is intentional, regenerate the snapshot and review the diff:

        python -c "from databricks.labs.dqx.anomaly.anomaly_llm_explainer import \\
        AI_QUERY_PROMPT_HEADER; \\
        open('tests/resources/ai_query_prompt_header.txt','w').write(AI_QUERY_PROMPT_HEADER)"
    """
    expected = _PROMPT_SNAPSHOT.read_text(encoding="utf-8")
    assert llm_explainer.AI_QUERY_PROMPT_HEADER == expected, (
        "AI-explanation prompt changed vs the committed snapshot "
        f"({_PROMPT_SNAPSHOT.name}). If this is intentional, regenerate it (see the docstring) "
        "and review the diff; otherwise revert the prompt edit."
    )


def test_prompt_drops_model_name_and_includes_examples():
    """model_name was removed from the prompt (review feedback) and few-shot exemplars added."""
    header = llm_explainer.AI_QUERY_PROMPT_HEADER
    assert "model_name" not in _prompt_input_descriptions(header)
    assert "model_name" not in header
    # Two few-shot exemplars pin style + JSON shape.
    assert header.count("Example (") == 2
    assert "Response:" in header


@pytest.mark.parametrize(
    "model_name, expected_match",
    [
        # Wrong provider prefix — caught by the provider check before the regex runs.
        pytest.param("openai/gpt-4", "require a Databricks serving endpoint", id="wrong_provider"),
        # SQL-injection shapes: quote, semicolon, comment marker. The endpoint string is
        # f-string-interpolated into the *ai_query* SQL call, so anything the regex doesn't
        # whitelist must be rejected here rather than reaching the SQL string.
        pytest.param(
            "my-endpoint'; DROP TABLE x--", "not a valid Databricks Model Serving name", id="sql_injection_quote"
        ),
        pytest.param('ep"injected', "not a valid Databricks Model Serving name", id="double_quote"),
        pytest.param("ep with spaces", "not a valid Databricks Model Serving name", id="contains_space"),
        # Databricks-prefixed but the bare part is invalid — the prefix is stripped first,
        # then the regex must still reject what's left.
        pytest.param(
            "databricks/bad name", "not a valid Databricks Model Serving name", id="databricks_prefix_invalid_suffix"
        ),
        # Databricks endpoint names must start with a letter.
        pytest.param("1starts-with-digit", "not a valid Databricks Model Serving name", id="leading_digit"),
        pytest.param("-leading-hyphen", "not a valid Databricks Model Serving name", id="leading_hyphen"),
        # 64 chars (cap is 63) — boundary case for the length rule.
        pytest.param("a" * 64, "not a valid Databricks Model Serving name", id="length_64_over_cap"),
    ],
)
def test_probe_endpoint_reachable_rejects_invalid_endpoint_before_query(model_name, expected_match):
    spark = _FakeSpark()
    with pytest.raises(InvalidParameterError, match=expected_match):
        llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name=model_name))
    assert not spark.queries


@pytest.mark.parametrize(
    "model_name, expected",
    [
        pytest.param("my-endpoint", "my-endpoint", id="bare_simple"),
        pytest.param("ep_with_underscores", "ep_with_underscores", id="underscores"),
        pytest.param("ep-with-hyphens-123", "ep-with-hyphens-123", id="hyphens_and_digits"),
        pytest.param("a" * 63, "a" * 63, id="length_63_at_cap"),
    ],
)
def test_probe_endpoint_reachable_emits_valid_endpoint(model_name, expected):
    spark = _FakeSpark()

    assert llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name=model_name)) is True
    assert len(spark.queries) == 1
    assert f"ai_query('{expected}'," in spark.queries[0]


def test_probe_endpoint_reachable_rejects_empty_model_name_before_query():
    spark = _FakeSpark()
    with pytest.raises(InvalidParameterError, match="model_name is required"):
        llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name=""))
    assert not spark.queries


def test_explanation_context_pattern_col_defaults_to_fixed_name():
    ctx = ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.m",
    )
    assert ctx.pattern_col == "__dqx_pattern"


def test_explanation_context_threads_pattern_col_from_scoring_config():
    """Production scoring threads a UUID-suffixed pattern column so it can't collide with a
    user column; from_scoring_config must carry it through."""
    config = ScoringConfig(
        columns=["amount"],
        model_name="catalog.schema.m",
        registry_table="catalog.schema.reg",
        threshold=95.0,
        merge_columns=["__dqx_row_id_x"],
        output_columns=ScoringOutputColumns(
            pattern="__dq_anomaly_pattern_abc123", basis_contributions="__dq_basis_abc123"
        ),
    )
    ctx = ExplanationContext.from_scoring_config(config)
    assert ctx.pattern_col == "__dq_anomaly_pattern_abc123"
    assert ctx.basis_contributions_col == "__dq_basis_abc123"


def test_probe_endpoint_reachable_true_resolves_endpoint_and_probes_once():
    """A reachable endpoint returns True after exactly one ai_query probe against the resolved
    (provider-prefix-stripped) endpoint name."""
    spark = _FakeSpark()
    assert llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name="databricks/my-endpoint")) is True
    assert len(spark.queries) == 1
    assert "ai_query('my-endpoint'" in spark.queries[0]


def test_probe_endpoint_reachable_false_on_probe_failure():
    """An unreachable endpoint degrades to False (the caller then skips explanations) rather than
    raising, so a missing/un-entitled endpoint doesn't break scoring."""
    spark = _FakeSpark(fail=True)
    assert llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name="my-endpoint")) is False


def test_probe_endpoint_reachable_defaults_to_config_default_model():
    """None config falls back to LLMModelConfig() — the default Databricks endpoint, not an error."""
    spark = _FakeSpark()
    assert llm_explainer.probe_endpoint_reachable(spark, None) is True
    assert len(spark.queries) == 1


def test_probe_endpoint_reachable_rejects_non_databricks_provider():
    """Endpoint resolution still guards the provider — a non-Databricks model raises before any
    probe, so the misconfiguration surfaces instead of silently degrading."""
    spark = _FakeSpark()
    with pytest.raises(InvalidParameterError, match="require a Databricks serving endpoint"):
        llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name="openai/gpt-4"))
    assert not spark.queries


def test_attribution_semantics_distinguishes_joint_and_value_oriented_score_attribution():
    """Detector context describes score attribution, never a feature's deviation or correctness."""
    correlation = llm_explainer.attribution_semantics("Mahalanobis")
    assert "joint model-score attribution conditioned on the other metrics" in correlation
    assert "does not measure this metric's residual or departure size" in correlation
    assert "cannot determine" in correlation
    assert "do not assert a relationship broke or that any metric was abnormal" in correlation

    values = llm_explainer.attribution_semantics("IsolationForest")
    assert "value-oriented model-score attribution" in values
    assert "does not measure how far its value is from training values" in values
    assert "not which comparison received attribution" in values
    assert correlation != values

    for semantics in (correlation, values):
        assert "how far each metric's own value sits" not in semantics
        assert "was unusual for the rows it was compared against" not in semantics


def test_attribution_semantics_falls_back_to_value_oriented_score_attribution():
    """Unknown or absent algorithms get the conservative value-oriented attribution reading."""
    fallback = llm_explainer.attribution_semantics("IsolationForest")
    for algorithm in (None, "", "SomeFutureAlgorithm"):
        assert llm_explainer.attribution_semantics(algorithm) == fallback


def test_attribution_semantics_matches_ensemble_algorithm_strings():
    """Ensemble models persist as 'IsolationForest_Ensemble_3', so matching is by prefix.

    A registry value that failed to match would silently fall back, which is safe but would lose the
    distinction for every ensemble model -- i.e. the default configuration.
    """
    assert llm_explainer.attribution_semantics("IsolationForest_Ensemble_3") == llm_explainer.attribution_semantics(
        "IsolationForest"
    )


def test_explanation_context_defaults_algorithm_to_none():
    """The field is additive: a caller building the context directly keeps working, and gets the
    conservative value-based reading."""
    ctx = llm_explainer.ExplanationContext(
        severity_col="s",
        contributions_col="c",
        score_std_col="std",
        ai_explanation_col="ai",
        threshold=95.0,
        model_name="cat.sch.model",
    )
    assert ctx.algorithm is None
    assert llm_explainer.attribution_semantics(ctx.algorithm) == llm_explainer.attribution_semantics("IsolationForest")


# ── the prompt must not teach the model to invent direction ──────────────────────────────────────────

# Words that assert which way a metric moved. The prompt inputs carry no raw metric values or signed changes,
# so none supports directional language.
_DIRECTIONAL_WORDS = (
    "far above",
    "far below",
    "elevated",
    "inflated",
    "dropped",
    "spiked",
    "surged",
    "plummeted",
    "too high",
    "too low",
)


def _exemplar_responses() -> list[str]:
    """The JSON responses from the few-shot exemplars, which is the part a model imitates."""
    return _prompt_exemplar_field_values("Response")


def test_the_few_shot_responses_assert_no_direction():
    """A few-shot example is an instruction, so an unfounded exemplar teaches unfounded output.

    The previous pair said "sits far above the norm" and "Inflated amount fields overstate revenue"
    from inputs with no sign in them at all. Because a smaller serving model copies the shape of these
    responses, that made confident, business-language, half-of-the-time-backwards claims the house style.
    """
    responses = _exemplar_responses()
    assert len(responses) == 2, "expected both exemplars to still carry a response to check"

    for response in responses:
        lowered = response.lower()
        offenders = [word for word in _DIRECTIONAL_WORDS if word in lowered]
        assert not offenders, f"exemplar asserts direction its inputs cannot support: {offenders}"


def test_the_instructions_name_the_absence_of_direction_and_reconcile_it_with_being_direct():
    """Two rules could otherwise be read as licensing invention: "be direct, avoid hedging" and the
    detector-family reading. Being direct must mean stating what the input holds, not filling the gap."""
    header = llm_explainer.AI_QUERY_PROMPT_HEADER

    assert "NO DIRECTION" in header
    assert "does not license asserting a direction" in header


def test_every_exemplar_shows_the_attribution_basis_it_is_reading():
    """The field that decides how contributions may be described has to appear in the demonstrations.

    Both readings are shown, because an exemplar set that only ever displays one teaches the model to
    treat that one as the default and ignore the field.
    """
    bases = _prompt_exemplar_field_values("attribution_basis")

    assert len(bases) == 2, "each exemplar must state the basis it is reading"
    assert len(set(bases)) == 2, "the exemplars must demonstrate both readings, not one twice"


def test_the_correlation_aware_reading_does_not_assert_a_broken_relationship():
    """Joint score attribution cannot identify a relationship failure or a metric abnormality."""
    semantics = llm_explainer.attribution_semantics("Mahalanobis")

    assert "cannot determine" in semantics
    assert "do not assert a relationship broke or that any metric was abnormal" in semantics


def test_ensemble_agreement_is_not_presented_as_confidence_in_the_finding():
    """*confidence* is seed agreement on one training set. It says nothing about whether the flag is
    right or whether the data has drifted since, and the prompt has to say so or the narrative will
    imply otherwise."""
    description = _prompt_input_descriptions(llm_explainer.AI_QUERY_PROMPT_HEADER)["confidence"]

    assert "random seed" in description
    assert "NOT how reliable the flag is" in description


# ── how the row was judged is a per-run fact the contributions cannot carry ───────────────────────────


def test_the_temporal_baseline_reaches_the_prompt_as_its_own_field():
    """The gap this closes: grouping was told to the model and temporal conditioning was not.

    Temporal conditioning used to leak through by accident, because contributions were keyed by
    engineered feature and one of those keys rendered as "X vs its expected level at that time". Keying by
    source column closed that channel -- correctly, since one column should report once however many ways
    it was compared -- and left the model with no way to know the comparison was against time at all.
    """
    assert "temporal_baseline" in _prompt_input_descriptions(llm_explainer.AI_QUERY_PROMPT_HEADER)


def test_the_temporal_field_describes_an_available_comparison_not_one_that_happened():
    """The field widens what a contribution is consistent with; it does not explain it.

    An earlier version of this test pinned the opposite, and was wrong. Setting a time column does not mean
    every metric was judged against time: where no expectation could be fitted for a metric, feature
    engineering emits a constant zero for its time-relative feature, so that comparison contributes nothing
    for it. Nor is the contribution attributable to one comparison even when several ran, because the share
    is the total across them.

    What the field legitimately buys is the *absence* of a wrong conclusion: with it set, a metric can be
    ordinary for the table and still have departed from a narrower comparison, so "unusually high" stops
    being the obvious reading of a large share.
    """
    description = _prompt_input_descriptions(llm_explainer.AI_QUERY_PROMPT_HEADER)["temporal_baseline"]

    assert "MAY have been compared" in description
    assert "not every metric is" in description
    assert "say the comparison was available" in description
    # The overclaim this replaced: asserting the departure rather than the availability.
    assert "departed from the level expected" not in description


def test_both_comparison_states_are_demonstrated_in_the_exemplars():
    """A field the header tells the model to follow has to appear in the demonstrations, in both states.

    Showing only one state teaches the model to treat it as the default and stop reading the field, which
    is how the same mistake would come back.
    """
    values = _prompt_exemplar_field_values("temporal_baseline")

    assert len(values) == 2, "each exemplar must state whether a temporal baseline applied"
    assert sorted(values) == ["event_ts", "none"]


def test_no_exemplar_attributes_the_departure_to_a_particular_comparison():
    """An exemplar is an instruction, so it must not model a claim the inputs cannot support.

    The previous version of this test required the temporal exemplar to say the metric "departs from the
    level expected of it at that point in time" -- which is precisely the attribution that is unavailable.
    Both exemplars now name the comparisons in use and stop there.
    """
    responses = _exemplar_responses()
    assert len(responses) == 2

    for response in responses:
        lowered = response.lower()
        for phrase in ("departs from the level expected", "no longer tracks its expected level"):
            assert phrase not in lowered, f"exemplar attributes the departure to one comparison: {phrase}"
        for word in ("far above", "far below", "elevated", "inflated", "spiked"):
            assert word not in lowered


def test_the_exemplars_state_business_impact_conditionally():
    """Whether a contribution means real damage depends on facts the model does not have.

    An unusual amount need not distort revenue and a latency change need not breach an SLA -- the row may
    be legitimate. Both exemplars previously asserted the consequence outright, which is what a smaller
    model copies into every explanation it writes.
    """
    for response in _exemplar_responses():
        explanation = json.loads(response)
        impact = explanation["business_impact"]
        assert impact.lower().startswith("if source "), f"impact should be conditional, got {impact!r}"
        assert "incorrect" in impact
        assert "review" in impact
        assert "would" not in impact.lower()

    actions = [json.loads(response)["action"].lower() for response in _exemplar_responses()]
    assert any("source records" in action for action in actions)
    assert all("source orders" not in action for action in actions)


def test_rendered_prompt_separates_attribution_from_deviation_and_dq_diagnosis():
    """The production prompt must retain the live-review evidence boundary.

    This deliberately renders the full prompt rather than inspecting the source tables: the header is the
    exact text that reaches ai_query. The prior prompt passed its percentages through examples as comparable
    departures, despite contribution shares being normalized allocations of model evidence.
    """
    header = llm_explainer.AI_QUERY_PROMPT_HEADER

    assert "normalized shares of the model's attribution evidence, not observations" in header
    assert "Similar shares mean only similar shares of model evidence" in header
    assert "never similarly sized departures" in header
    assert "displayed feature list can omit lower-ranked metrics" in header
    assert "entries need not total 100" in header
    assert "or proof that a value is wrong" in header
    assert "Treat the explanation as DQ triage" in header
    assert "NO DIRECTION OR DEVIATION MAGNITUDE" in header
    assert "far above its norm and one equally far below" not in header
    assert "Say it departed from its expected pattern" not in header
    assert "Which comparison made each column depart" not in header


def test_rendered_prompt_makes_downstream_impact_a_conditional_review_hypothesis():
    """Impact belongs to analyst follow-up; model attribution cannot establish observed business harm."""
    header = llm_explainer.AI_QUERY_PROMPT_HEADER

    assert "conditional downstream-review hypothesis, not observed impact" in header
    assert "condition it on source values being incorrect and used downstream" in header
    assert "Do not claim harm occurred or will occur" in header
    assert "When no domain evidence is provided, use generic downstream review" in header
    assert "do not invent a system or use" in header


@pytest.mark.parametrize("algorithm", ["Mahalanobis", "IsolationForest"])
def test_production_prompt_and_dynamic_attribution_basis_do_not_claim_deviation_size(algorithm):
    """The public static header and dynamic detector text must agree on the same evidence boundary."""
    production_prompt = llm_explainer.AI_QUERY_PROMPT_HEADER + llm_explainer.attribution_semantics(algorithm)

    assert "normalized shares of the model's attribution evidence, not observations" in production_prompt
    assert "does not measure" in production_prompt
    assert "far above its norm and one equally far below" not in production_prompt
    assert "was unusual for the rows it was compared against" not in production_prompt


def test_the_instructions_present_the_comparisons_as_context_not_cause():
    """Naming a responsible comparison needs evidence, and the prompt must tie the two together.

    The original concern stands: "avoid hedging" plus a large share reads as licence to pick a comparison
    and blame it. What changed is that *basis_contributions* now supplies that evidence when the detector
    can measure it, so the prohibition has to be conditional rather than absolute -- otherwise the model is
    told to ignore the field that answers the question. Both halves are asserted here, because dropping
    either one is a silent regression: without the first the model invents an attribution, and without the
    second it refuses to use a real one.
    """
    header = llm_explainer.AI_QUERY_PROMPT_HEADER

    # With evidence: use the comparison that received model attribution.
    assert "basis_contributions says WHICH modeled comparison received attribution" in header
    # Without it: do not invent a comparison.
    assert "not measured" in header
    assert "do not assign the model evidence to one of them" in header
    # And a share on its own still never establishes which comparison objected.
    assert "each feature_contributions share is the total across those" in header
    assert "do not call it unusual outright" in header


def test_spread_contributions_do_not_license_a_broken_relationship_claim():
    """Spread means several metrics contributed. It does not identify a mechanism.

    A review probe settled this: two metrics with training correlation 6e-18, both marginally extreme at
    once, produce contributions of 50% and 50%. Nothing about that shape distinguishes metrics that stopped
    agreeing with each other from unrelated metrics that happened to be unusual together, so the earlier
    instruction to describe spread as "the metrics no longer agreeing" was asserting a mechanism from
    evidence that does not carry one.
    """
    correlation = llm_explainer.attribution_semantics("Mahalanobis")

    assert "input cannot determine whether" in correlation
    assert "Name the metrics the model attributed evidence to" in correlation
    assert "do not assert a relationship broke" in correlation.lower()
    # The instruction this replaced.
    assert "describe it as the metrics no longer agreeing with each other" not in correlation


@pytest.mark.parametrize("status", [403, 404, 429, 503])
def test_endpoint_availability_failure_logs_no_response_payload(status, caplog):
    spark = create_autospec(SparkSession)
    spark.sql.side_effect = AnalysisException(
        f"request failed with code {status}, SYNTHETIC_SECRET_MARKER\n/Workspace/private/path",
        errorClass="REMOTE_FUNCTION_HTTP_FAILED_ERROR",
    )
    assert llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name="my-endpoint")) is False
    assert "SYNTHETIC_SECRET_MARKER" not in caplog.text
    assert "/Workspace/private/path" not in caplog.text
    assert str(status) in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("unexpected application failure"),
        AnalysisException("invalid expression", errorClass="UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        AnalysisException("request failed with code 500", errorClass="REMOTE_FUNCTION_HTTP_FAILED_ERROR"),
        AnalysisException("invalid response schema", errorClass="REMOTE_FUNCTION_HTTP_FAILED_ERROR"),
    ],
)
def test_endpoint_probe_propagates_unexpected_failures(error):
    spark = create_autospec(SparkSession)
    spark.sql.side_effect = error
    with pytest.raises(type(error), match="unexpected application|invalid expression|code 500|invalid response"):
        llm_explainer.probe_endpoint_reachable(spark, LLMModelConfig(model_name="my-endpoint"))


def test_grouping_context_does_not_claim_the_group_comparison_caused_the_flag():
    header = llm_explainer.AI_QUERY_PROMPT_HEADER
    assert "group comparison was available" in header
    assert "rather than that it was unusual outright" not in header
