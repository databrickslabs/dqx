"""Unit tests for the ai_query-based group explainer in anomaly_llm_explainer.

Spark is never started — these exercise the pure helpers: prompt rendering, endpoint
resolution, baseline-grouping and human-label rendering, SQL-literal escaping, the
structured-output schema, and the pattern-column threading through ExplanationContext.
"""

from pathlib import Path

import pytest

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import ExplanationContext
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
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
            raise RuntimeError("endpoint not entitled")
        return self

    def collect(self) -> list:
        return []


def test_ai_query_prompt_header_includes_instructions_and_field_descriptions():
    """The header is rendered from the shared prompt tables; assert representative tokens from
    each section so accidental drift is caught without coupling to exact wording."""
    header = llm_explainer._render_ai_query_prompt_header()
    assert "data quality analyst" in header  # instructions line
    for input_name, _ in llm_explainer._PROMPT_INPUT_FIELDS:
        assert f"- {input_name}:" in header
    for output_name, _ in llm_explainer._PROMPT_OUTPUT_FIELDS:
        assert f"- {output_name}:" in header
    assert "Respond with ONLY a JSON object" in header


_PROMPT_SNAPSHOT = Path(__file__).resolve().parents[1] / "resources" / "ai_query_prompt_header.txt"


def test_ai_query_prompt_header_matches_snapshot():
    """Change-control gate for the LLM prompt.

    The rendered prompt is the contract the model sees — any edit to _PROMPT_INSTRUCTIONS,
    _PROMPT_INPUT_FIELDS, _PROMPT_OUTPUT_FIELDS, or _PROMPT_EXAMPLES silently changes every
    explanation. Pinning it to a committed snapshot forces a prompt change to be deliberate and
    surfaces it in review as a diff to the golden file (no LLM, no workspace, runs in ms).

    If the change is intentional, regenerate the snapshot and review the diff:

        python -c "from databricks.labs.dqx.anomaly.anomaly_llm_explainer import \\
        _render_ai_query_prompt_header as r, _AI_QUERY_PROMPT_HEADER as _; \\
        open('tests/resources/ai_query_prompt_header.txt','w').write(r())"
    """
    expected = _PROMPT_SNAPSHOT.read_text(encoding="utf-8")
    assert llm_explainer._render_ai_query_prompt_header() == expected, (
        "AI-explanation prompt changed vs the committed snapshot "
        f"({_PROMPT_SNAPSHOT.name}). If this is intentional, regenerate it (see the docstring) "
        "and review the diff; otherwise revert the prompt edit."
    )


def test_prompt_drops_model_name_and_includes_examples():
    """model_name was removed from the prompt (review feedback) and few-shot exemplars added."""
    input_names = {name for name, _ in llm_explainer._PROMPT_INPUT_FIELDS}
    assert "model_name" not in input_names
    header = llm_explainer._render_ai_query_prompt_header()
    assert "model_name" not in header
    # Two few-shot exemplars pin style + JSON shape.
    assert header.count("Example (") == 2
    assert "Response:" in header


def test_resolve_ai_query_endpoint_strips_databricks_prefix():
    assert llm_explainer._resolve_ai_query_endpoint("databricks/databricks-claude-sonnet-4-5") == (
        "databricks-claude-sonnet-4-5"
    )


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
        pytest.param("ep\"injected", "not a valid Databricks Model Serving name", id="double_quote"),
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
def test_resolve_ai_query_endpoint_rejects_invalid_endpoint(model_name, expected_match):
    with pytest.raises(InvalidParameterError, match=expected_match):
        llm_explainer._resolve_ai_query_endpoint(model_name)


@pytest.mark.parametrize(
    "model_name, expected",
    [
        pytest.param("my-endpoint", "my-endpoint", id="bare_simple"),
        pytest.param("ep_with_underscores", "ep_with_underscores", id="underscores"),
        pytest.param("ep-with-hyphens-123", "ep-with-hyphens-123", id="hyphens_and_digits"),
        pytest.param("a" * 63, "a" * 63, id="length_63_at_cap"),
    ],
)
def test_resolve_ai_query_endpoint_accepts_valid_names(model_name, expected):
    assert llm_explainer._resolve_ai_query_endpoint(model_name) == expected


def test_resolve_ai_query_endpoint_rejects_empty_model_name():
    with pytest.raises(InvalidParameterError, match="model_name is required"):
        llm_explainer._resolve_ai_query_endpoint("")


def test_ai_query_response_format_is_strict_json_schema_built_from_output_fields():
    """Response format pins the LLM to the output fields with strict mode, and is built from
    *_PROMPT_OUTPUT_FIELDS* so the schema and the prompt rules cannot drift.

    Strict mode + ``additionalProperties:false`` blocks the model from smuggling extra fields.
    Length-capping happens post-parse via *_sanitize* (Databricks ai_query rejects ``maxLength``
    on string types in responseFormat).
    """
    schema = llm_explainer._AI_QUERY_RESPONSE_FORMAT
    assert '"strict":true' in schema
    assert '"additionalProperties":false' in schema
    for field, _ in llm_explainer._PROMPT_OUTPUT_FIELDS:
        assert f'"{field}"' in schema
    # Derived, not hand-rolled: rebuilding from the fields reproduces the constant exactly.
    assert llm_explainer._build_ai_query_response_format() == schema


def test_baseline_grouping_str_reports_the_grouping_columns():
    """The prompt's baseline_grouping field is the baseline_by columns, a per-run constant."""
    metadata = SparkFeatureMetadata(
        column_infos=[{"name": "amount", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["amount"],
        baseline_by=["region", "product"],
    )
    assert llm_explainer._baseline_grouping_str(metadata) == "region, product"


def test_baseline_grouping_str_is_none_when_ungrouped_or_metadataless():
    metadata = SparkFeatureMetadata(
        column_infos=[{"name": "amount", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["amount"],
    )
    assert llm_explainer._baseline_grouping_str(metadata) == "none"
    assert llm_explainer._baseline_grouping_str(None) == "none"


def test_human_labels_map_omits_identity_and_labels_derived_features():
    """Only features whose label differs from the raw name are in the map; identities are dropped
    so the SQL lookup stays small and unmapped keys fall back to the raw name."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric"},
            {"name": "country", "category": "categorical"},
        ],
        categorical_frequency_maps={"country": {"US": 0.7}},
        onehot_categories={"country": ["US"]},
        engineered_feature_names=["amount", "amount_rel_baseline", "country_US", "country_freq"],
        # Recorded because suffix resolution is gated on it: without a basis, ``amount_rel_baseline``
        # would be a column in its own right rather than a derived feature, and would correctly label
        # as itself. A grouping column is not a feature, so it is absent from column_infos.
        baseline_by=["region"],
    )
    labels = llm_explainer._human_labels(metadata)

    assert "amount" not in labels  # identity, omitted
    assert labels["amount_rel_baseline"] == "amount vs its group baseline"
    assert labels["country_US"] == "country = US"
    assert labels["country_freq"] == "country frequency"


def test_human_labels_map_is_empty_without_metadata():
    assert not llm_explainer._human_labels(None)


def test_sql_string_literal_escapes_quote_and_backslash():
    """Both single quote and backslash must be escaped — Spark SQL treats backslash as an
    escape char inside string literals, so quote-doubling alone is insufficient."""
    assert llm_explainer._sql_string_literal("o'brien") == "o''brien"
    assert llm_explainer._sql_string_literal("a\\b") == "a\\\\b"
    assert llm_explainer._sql_string_literal("x'\\y") == "x''\\\\y"


def test_explanation_context_pattern_col_defaults_to_fixed_name():
    ctx = ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.m",
    )
    assert ctx.pattern_col == llm_explainer._DEFAULT_PATTERN_COL


def test_explanation_context_threads_pattern_col_from_scoring_config():
    """Production scoring threads a UUID-suffixed pattern column so it can't collide with a
    user column; from_scoring_config must carry it through."""
    config = ScoringConfig(
        columns=["amount"],
        model_name="catalog.schema.m",
        registry_table="catalog.schema.reg",
        threshold=95.0,
        merge_columns=["__dqx_row_id_x"],
        output_columns=ScoringOutputColumns(pattern="__dq_anomaly_pattern_abc123"),
    )
    ctx = ExplanationContext.from_scoring_config(config)
    assert ctx.pattern_col == "__dq_anomaly_pattern_abc123"


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


def test_attribution_semantics_distinguishes_correlation_from_value_anomalies():
    """The prompt must say what a contribution *measures*, because the two detectors differ.

    Measured on a live workspace before this existed: with the correlation-aware detector the LLM wrote
    "Abnormal coolant flow and bearing temperature" and advised "Inspect coolant system and bearing
    sensors" for rows whose every reading sat *inside* its healthy range. The values were normal; only the
    relationship between them had broken. Given per-feature importances and nothing else, the model cannot
    tell the two situations apart -- they look identical in shape -- so it defaults to the value reading
    and asserts something the data does not support.

    Asserted on the protections rather than on any single word, so a rewording that keeps the guarantees
    passes and one that drops them fails. The correlation-aware text no longer *mandates* the relationship
    reading either -- a high contribution is genuinely consistent with a large individual move, so naming
    one reading as the truth was its own overclaim -- but it must still refuse the value reading as a
    default, which is what produced "Abnormal coolant flow".
    """
    correlation = llm_explainer.attribution_semantics("Mahalanobis")
    # The contribution is joint, not univariate: the reason the observed claim was wrong.
    assert "once every other metric is accounted for" in correlation
    assert "inside its normal range" in correlation
    # The instruction that prevents the specific false claim observed.
    assert "does not distinguish" in correlation
    assert "do not assert either" in correlation

    values = llm_explainer.attribution_semantics("IsolationForest")
    assert "metric's own value" in values
    assert correlation != values

    value_based = llm_explainer.attribution_semantics("IsolationForest")
    assert "was unusual for the rows it was compared against" in value_based
    assert "relationship" not in value_based
    # A key covers a column with every comparison made of it, so the reading must not promise which
    # comparison objected -- a share of 60% on a metric compared three ways says the metric was involved.
    assert "not which comparison objected" in value_based

    assert correlation != value_based


def test_attribution_semantics_falls_back_to_the_value_reading():
    """Unknown or absent algorithms get the value-based reading.

    That is both the historical behaviour and the more conservative claim: describing an extreme value
    where a relationship broke understates the finding, whereas the reverse invents a relationship claim.
    """
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

# Words that assert which way a metric moved. The inputs the prompt is built from carry contribution
# magnitudes, severity percentiles and unsigned drift scores -- nothing that distinguishes a metric far
# above its norm from one equally far below. A row at (8, 0.5) and its mirror at (-8, -0.5) score
# identically and produce identical contribution maps, so any of these words is right half the time.
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
    return [
        line.partition("Response: ")[2]
        for line in llm_explainer._PROMPT_EXAMPLES.splitlines()
        if line.startswith("Response: ")
    ]


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
    header = llm_explainer._render_ai_query_prompt_header()

    assert "NO DIRECTION" in header
    assert "does not license asserting a direction" in header


def test_every_exemplar_shows_the_attribution_basis_it_is_reading():
    """The field that decides how contributions may be described has to appear in the demonstrations.

    Both readings are shown, because an exemplar set that only ever displays one teaches the model to
    treat that one as the default and ignore the field.
    """
    bases = [
        line.partition("attribution_basis: ")[2]
        for line in llm_explainer._PROMPT_EXAMPLES.splitlines()
        if line.startswith("attribution_basis: ")
    ]

    assert len(bases) == 2, "each exemplar must state the basis it is reading"
    assert bases[0] != bases[1], "the exemplars must demonstrate both readings, not one twice"


def test_the_correlation_aware_reading_does_not_assert_a_broken_relationship():
    """A high contribution there is consistent with a large individual move *or* with a metric that
    stopped tracking the others while staying in its normal range. The input cannot tell them apart, so
    instructing the model to describe a broken relationship states more than is known."""
    semantics = llm_explainer.attribution_semantics("Mahalanobis")

    assert "does not distinguish" in semantics
    assert "do not assert either" in semantics


def test_ensemble_agreement_is_not_presented_as_confidence_in_the_finding():
    """*confidence* is seed agreement on one training set. It says nothing about whether the flag is
    right or whether the data has drifted since, and the prompt has to say so or the narrative will
    imply otherwise."""
    description = dict(llm_explainer._PROMPT_INPUT_FIELDS)["confidence"]

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
    input_names = [name for name, _ in llm_explainer._PROMPT_INPUT_FIELDS]

    assert "temporal_baseline" in input_names
    # Sibling of the grouping field, and adjacent to it, because they answer the same question.
    assert input_names.index("temporal_baseline") == input_names.index("baseline_grouping") + 1


@pytest.mark.parametrize("baseline_over_time, expected", [("event_ts", "event_ts"), ("", "none")])
def test_the_temporal_baseline_string_reports_the_time_column_or_none(baseline_over_time, expected):
    metadata = SparkFeatureMetadata(
        column_infos=[{"name": "revenue", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["revenue"],
        baseline_over_time=baseline_over_time,
    )

    assert llm_explainer._temporal_baseline_str(metadata) == expected


def test_the_temporal_baseline_string_is_none_without_metadata():
    """A caller who built the context directly gets the conservative answer rather than a crash."""
    assert llm_explainer._temporal_baseline_str(None) == "none"


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
    description = dict(llm_explainer._PROMPT_INPUT_FIELDS)["temporal_baseline"]

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
    values = [
        line.partition("temporal_baseline: ")[2]
        for line in llm_explainer._PROMPT_EXAMPLES.splitlines()
        if line.startswith("temporal_baseline: ")
    ]

    assert len(values) == 2, "each exemplar must state whether a temporal baseline applied"
    assert sorted(values) == ["event_ts", "none"]


def test_no_exemplar_attributes_the_departure_to_a_particular_comparison():
    """An exemplar is an instruction, so it must not model a claim the inputs cannot support.

    The previous version of this test required the temporal exemplar to say the metric "departs from the
    level expected of it at that point in time" -- which is precisely the attribution that is unavailable.
    Both exemplars now name the comparisons in use and stop there.
    """
    responses = [line for line in llm_explainer._PROMPT_EXAMPLES.splitlines() if line.startswith("Response: ")]
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
    responses = [line for line in llm_explainer._PROMPT_EXAMPLES.splitlines() if line.startswith("Response: ")]

    for response in responses:
        impact = response.partition('"business_impact":"')[2].partition('","')[0]
        assert impact.lower().startswith("if "), f"impact should be conditional, got {impact!r}"


def test_the_instructions_present_the_comparisons_as_context_not_cause():
    """Otherwise "avoid hedging" plus a large share reads as licence to name a responsible comparison."""
    header = llm_explainer._render_ai_query_prompt_header()

    assert "which comparisons were AVAILABLE to the model, not which one objected" in header
    assert "do not assign the departure to one of them" in header
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

    assert "each of them contributed, and nothing more" in correlation
    assert "do not claim a relationship between them broke" in correlation.lower()
    # The instruction this replaced.
    assert "describe it as the metrics no longer agreeing with each other" not in correlation
