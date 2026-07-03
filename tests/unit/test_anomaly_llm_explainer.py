"""Unit tests for the ai_query-based group explainer in anomaly_llm_explainer.

Spark is never started — these exercise the pure helpers: prompt rendering, endpoint
resolution, segment redaction, SQL-literal escaping, the structured-output schema, and the
pattern-column threading through ExplanationContext.
"""

from pathlib import Path

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


def test_format_segment_empty_returns_empty_string():
    assert llm_explainer._format_segment(None, frozenset()) == ""
    assert llm_explainer._format_segment({}, frozenset()) == ""


def test_format_segment_formats_key_value_pairs():
    out = llm_explainer._format_segment({"region": "US", "product": "electronics"}, frozenset())
    assert out == "region=US, product=electronics"


def test_format_segment_redacts_listed_keys():
    """A segment key in redact_columns must never leak its value into the prompt."""
    out = llm_explainer._format_segment({"region": "US", "customer_id": "C-42"}, frozenset({"customer_id"}))
    assert out == "region=US, customer_id=<redacted>"
    assert "C-42" not in out


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
