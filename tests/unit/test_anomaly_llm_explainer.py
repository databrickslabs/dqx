"""Unit tests for the parallel LLM-call orchestration in anomaly_llm_explainer.

Exercises *_call_llm_for_groups* with a fake predictor — verifies that:
- All retained groups produce a result tuple, regardless of execution order.
- A predictor that raises for one group does not abort the run; that group emits
  a null-explanation tuple and the others succeed (per-group failure isolation).

Spark is never started — these helpers operate on plain Python dicts.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import DSPY_AVAILABLE, ExplanationContext
from databricks.labs.dqx.errors import InvalidParameterError

pytestmark = pytest.mark.skipif(not DSPY_AVAILABLE, reason="dspy not installed")


def _make_group(pattern: str, group_size: int = 10) -> dict:
    return {
        llm_explainer._PATTERN_COL: pattern,
        "group_size": group_size,
        "group_avg_severity": 97.5,
        "severity_min": 95.0,
        "severity_max": 99.5,
        "mean_std": 0.04,
        "mean_contributions": {"amount": 0.8, "quantity": 0.2},
    }


def _make_ctx() -> ExplanationContext:
    return ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.m",
    )


_CANNED = SimpleNamespace(
    narrative="Group flagged due to amount.",
    business_impact="May inflate revenue.",
    action="Investigate transaction source.",
)


def test_call_llm_for_groups_returns_one_tuple_per_group():
    groups = [_make_group(f"pat{i}") for i in range(8)]

    def predictor(**_):
        return _CANNED

    rows = llm_explainer._call_llm_for_groups(
        groups,
        _make_ctx(),
        segment_str="",
        is_ensemble=True,
        drift_summary="none",
        predictor=predictor,
        language_model=object(),
    )

    assert len(rows) == len(groups)
    patterns = {r[0] for r in rows}
    assert patterns == {f"pat{i}" for i in range(8)}
    # Each row carries the canned narrative/impact/action.
    for pattern, narrative, impact, action, size, _avg_sev in rows:
        assert narrative == _CANNED.narrative
        assert impact == _CANNED.business_impact
        assert action == _CANNED.action
        assert size == 10
        assert pattern.startswith("pat")


def test_call_llm_for_groups_isolates_per_group_failures():
    """A predictor that raises for one pattern must not abort the run; that group's row is
    emitted with null narrative/impact/action so downstream join still produces a row, and
    the other groups complete normally."""
    groups = [_make_group(f"pat{i}") for i in range(5)]
    failing_pattern = "pat2"

    def predictor(**kwargs):
        # The pattern is encoded into severity_range only indirectly; key off the unique
        # group_size by patching one group to a distinct size and matching on it instead.
        # Simpler: raise based on a counter — but order isn't guaranteed across threads.
        # We mark the "failing" group via group_size below and check kwargs.group_size.
        if kwargs.get("group_size") == "999 rows":
            raise RuntimeError("boom")
        return _CANNED

    # Make the failing group identifiable inside the predictor by giving it a distinct size.
    for group in groups:
        if group[llm_explainer._PATTERN_COL] == failing_pattern:
            group["group_size"] = 999

    rows = llm_explainer._call_llm_for_groups(
        groups,
        _make_ctx(),
        segment_str="",
        is_ensemble=True,
        drift_summary="none",
        predictor=predictor,
        language_model=object(),
    )

    by_pattern = {r[0]: r for r in rows}
    assert set(by_pattern.keys()) == {f"pat{i}" for i in range(5)}

    failed = by_pattern[failing_pattern]
    # Pattern + size + avg_severity preserved; narrative/impact/action are None.
    assert failed[1] is None
    assert failed[2] is None
    assert failed[3] is None
    assert failed[4] == 999

    for pattern, row in by_pattern.items():
        if pattern == failing_pattern:
            continue
        assert row[1] == _CANNED.narrative
        assert row[2] == _CANNED.business_impact
        assert row[3] == _CANNED.action
        assert row[4] == 10


def test_sanitize_llm_field_passes_through_clean_text():
    assert llm_explainer._sanitize_llm_field("plain narrative") == "plain narrative"


def test_sanitize_llm_field_returns_none_for_none():
    assert llm_explainer._sanitize_llm_field(None) is None


def test_sanitize_llm_field_strips_control_chars():
    raw = "line1\nline2\rline3\ttab\x00nul\x1bescape\x7fdel"
    sanitized = llm_explainer._sanitize_llm_field(raw)
    assert sanitized == "line1 line2 line3 tab nul escape del"


def test_sanitize_llm_field_caps_length():
    sanitized = llm_explainer._sanitize_llm_field("a" * 10_000)
    assert sanitized is not None
    assert len(sanitized) == llm_explainer._LLM_FIELD_MAX_LEN


def test_sanitize_llm_field_coerces_non_str():
    # DSPy may return non-str on malformed completions; we still want a safe string out.
    assert llm_explainer._sanitize_llm_field(123) == "123"  # type: ignore[arg-type]


def test_call_llm_for_groups_sanitizes_llm_output():
    groups = [_make_group("pat0")]
    dirty = SimpleNamespace(
        narrative="bad\nnarrative\x00" + "x" * 1000,
        business_impact="impact\r\nforged",
        action="ok action",
    )

    def predictor(**_):
        return dirty

    rows = llm_explainer._call_llm_for_groups(
        groups,
        _make_ctx(),
        segment_str="",
        is_ensemble=True,
        drift_summary="none",
        predictor=predictor,
        language_model=object(),
    )

    _pattern, narrative, impact, action, _size, _sev = rows[0]
    assert "\n" not in narrative and "\x00" not in narrative
    assert len(narrative) == llm_explainer._LLM_FIELD_MAX_LEN
    assert impact == "impact  forged"
    assert action == "ok action"


def test_ai_query_prompt_header_includes_instructions_and_field_descriptions():
    """The ai_query header is rendered from the same shared dicts the DSPy signature reads.

    Asserting on a few representative tokens from each section catches accidental drift between
    the two executor paths without coupling the test to exact wording.
    """
    header = llm_explainer._render_ai_query_prompt_header()
    assert "data quality analyst" in header  # instructions line
    for input_name, _ in llm_explainer._PROMPT_INPUT_FIELDS:
        assert f"- {input_name}:" in header
    for output_name, _ in llm_explainer._PROMPT_OUTPUT_FIELDS:
        assert f"- {output_name}:" in header
    assert "Respond with ONLY a JSON object" in header


def test_resolve_ai_query_endpoint_strips_databricks_prefix():
    assert llm_explainer._resolve_ai_query_endpoint("databricks/databricks-claude-sonnet-4-5") == (
        "databricks-claude-sonnet-4-5"
    )


@pytest.mark.parametrize(
    "model_name, expected_match",
    [
        # Wrong provider prefix — caught by the provider check before the regex runs.
        pytest.param("openai/gpt-4", "executor='ai_query' requires a Databricks", id="wrong_provider"),
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


def test_ai_query_response_format_is_strict_json_schema():
    """Response format pins the LLM to {narrative, business_impact, action} with strict mode.

    Strict mode + ``additionalProperties:false`` blocks the model from smuggling extra fields.
    Length-capping happens post-parse via *_sanitize* (Databricks ai_query rejects ``maxLength``
    on string types in responseFormat — see comment on *_AI_QUERY_RESPONSE_FORMAT*).
    """
    schema = llm_explainer._AI_QUERY_RESPONSE_FORMAT
    assert '"strict":true' in schema
    assert '"additionalProperties":false' in schema
    for field in ("narrative", "business_impact", "action"):
        assert f'"{field}"' in schema


def test_call_llm_for_groups_empty_input_returns_empty_list():
    rows = llm_explainer._call_llm_for_groups(
        [],
        _make_ctx(),
        segment_str="",
        is_ensemble=True,
        drift_summary="none",
        predictor=lambda **_: _CANNED,
        language_model=object(),
    )
    assert not rows
