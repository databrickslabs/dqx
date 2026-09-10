"""When evidence is withheld, the explanation must say so (no Spark, no workspace).

Redaction keeps a column out of the prompt, and it did that correctly -- a black-box audit found no name
leaking in any of ten tested groups. What it also did was drop that column's share from the map the
narrative is built from, after which the remaining shares were renormalised to sum to 100 with nothing
recording that anything had gone. A column holding about 7% of one group's evidence was consequently
described as accounting for "all of what the model measured (100%)". All ten groups failed evidence-scope
clarity while passing redaction, which is why those two properties are asserted separately here.

The band boundaries and the wording are the product contract, so they are pinned directly. The Spark
plumbing that carries them is exercised by the integration suite.
"""

import re

import pytest

from databricks.labs.dqx.anomaly.anomaly_info_schema import ai_explanation_struct_schema
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import (
    AI_QUERY_PROMPT_HEADER,
    DISCLOSURE_COMPLETE,
    DISCLOSURE_FALLBACK_TEMPLATE,
    DISCLOSURE_LIMITED,
    DISCLOSURE_NARRATIVE_CLAUSE,
    DISCLOSURE_PARTIAL,
    DISCLOSURE_PROMPT_TEXT,
    TOTALISING_CLAIM_PATTERN,
)


def _state(withheld: float, total: float) -> str:
    """The band, mirroring *_disclosure_state_expr*'s arithmetic without a Spark session.

    Duplicated deliberately: the boundaries are the contract and are worth pinning in a fast test. The
    integration suite is what checks the Spark expression agrees.
    """
    if total is None or total <= 0.0:
        return DISCLOSURE_COMPLETE
    if withheld <= 0.0:
        return DISCLOSURE_COMPLETE
    if withheld / total > 0.5:
        return DISCLOSURE_LIMITED
    return DISCLOSURE_PARTIAL


@pytest.mark.parametrize(
    "withheld, total, expected",
    [
        pytest.param(0.0, 100.0, DISCLOSURE_COMPLETE, id="nothing_redacted"),
        pytest.param(7.0, 100.0, DISCLOSURE_PARTIAL, id="minor_contributor_redacted"),
        pytest.param(50.0, 100.0, DISCLOSURE_PARTIAL, id="exactly_half_is_not_yet_limited"),
        pytest.param(93.0, 100.0, DISCLOSURE_LIMITED, id="the_measured_case_dominant_redacted"),
        pytest.param(100.0, 100.0, DISCLOSURE_LIMITED, id="every_contributor_redacted"),
        pytest.param(0.0, 0.0, DISCLOSURE_COMPLETE, id="no_evidence_at_all_has_nothing_to_qualify"),
    ],
)
def test_the_disclosure_band_reflects_how_much_was_withheld(withheld: float, total: float, expected: str):
    """The bands, including the case the defect was measured on: 93% withheld reads as limited."""
    assert _state(withheld, total) == expected


def test_a_dominant_redacted_contributor_is_not_reported_as_partial():
    """The distinction that changes what a narrative may claim.

    Under the old behaviour this group's visible 7% was rendered as 100%. Calling it merely *partial* would
    let a narrative keep treating the remainder as the explanation; *limited* is what tells it not to.
    """
    assert _state(93.0, 100.0) == DISCLOSURE_LIMITED
    assert _state(93.0, 100.0) != DISCLOSURE_PARTIAL


def test_the_band_cannot_be_read_back_as_a_proportion():
    """Coarse on purpose: the state must not disclose by proportion what the name discloses by identity.

    Every share above the boundary maps to one string and every share below it to another, so an observer
    learns which side of one boundary a group sits on and nothing finer.
    """
    limited = {_state(share, 100.0) for share in (50.01, 60.0, 75.0, 93.0, 99.9, 100.0)}
    partial = {_state(share, 100.0) for share in (0.1, 5.0, 7.0, 25.0, 49.9, 50.0)}

    assert limited == {DISCLOSURE_LIMITED}
    assert partial == {DISCLOSURE_PARTIAL}


# ── what the model is told, and what it cannot omit ──────────────────────────────────────────────────


def test_every_band_has_prompt_wording_and_only_the_withheld_ones_carry_a_clause():
    """A complete group must not be qualified: saying evidence was withheld when none was is its own defect."""
    assert set(DISCLOSURE_PROMPT_TEXT) == {
        DISCLOSURE_COMPLETE,
        DISCLOSURE_PARTIAL,
        DISCLOSURE_LIMITED,
    }
    assert set(DISCLOSURE_NARRATIVE_CLAUSE) == {DISCLOSURE_PARTIAL, DISCLOSURE_LIMITED}


@pytest.mark.parametrize("band", [DISCLOSURE_PARTIAL, DISCLOSURE_LIMITED])
def test_the_prompt_forbids_treating_the_remainder_as_the_whole_decision(band: str):
    """The specific false claim that was measured, forbidden in the field the model reads first."""
    text = DISCLOSURE_PROMPT_TEXT[band]

    assert "cannot be disclosed" in text
    assert "only what is shown" in text or "minority of what the model used" in text


def test_the_limited_band_forbids_redirecting_the_reader_to_a_weak_contributor():
    """The other half of the failure: a confident action built on a share that is nearly all that is left."""
    text = DISCLOSURE_PROMPT_TEXT[DISCLOSURE_LIMITED]

    assert "do not direct the reader to investigate a metric on the strength of a share this small" in text
    assert "Never guess what the withheld evidence was" in text


@pytest.mark.parametrize("band", [DISCLOSURE_PARTIAL, DISCLOSURE_LIMITED])
def test_the_appended_clause_states_the_limitation_without_naming_anything(band: str):
    """Deterministic, so a model that ignores the instruction cannot produce an unqualified narrative.

    It must also disclose nothing beyond the fact of withholding -- no name, no value, no share.
    """
    clause = DISCLOSURE_NARRATIVE_CLAUSE[band]

    assert "could not be disclosed" in clause
    for leak in ("%", "latency", "column", "field named"):
        assert leak not in clause


def test_no_band_names_a_share_or_a_column():
    """Applies to every string this feature can publish, prompt wording included."""
    for text in (*DISCLOSURE_PROMPT_TEXT.values(), *DISCLOSURE_NARRATIVE_CLAUSE.values()):
        assert "%" not in text


def test_both_exemplars_show_a_disclosure_state_and_they_differ():
    """A field the header tells the model to read first has to be demonstrated, and in both states.

    One state teaches the model to treat it as the default and stop reading the field -- the lesson this
    module's own comments already record for the conditioning fields.
    """
    values = [
        line.partition("evidence_disclosure: ")[2]
        for line in AI_QUERY_PROMPT_HEADER.splitlines()
        if line.startswith("evidence_disclosure: ")
    ]

    assert len(values) == 2
    assert values[0] != values[1]


def test_the_disclosure_field_is_read_before_the_contributions_it_qualifies():
    """Order matters in a prompt: the qualification has to arrive before the numbers it applies to."""
    assert AI_QUERY_PROMPT_HEADER.index("- evidence_disclosure:") < AI_QUERY_PROMPT_HEADER.index(
        "- feature_contributions:"
    )


def test_the_contributions_field_says_the_shares_may_cover_only_what_is_disclosed():
    """Because the shares themselves are renormalised, the field describing them has to say so."""
    description = AI_QUERY_PROMPT_HEADER.partition("- feature_contributions:")[2].partition("\n")[0]

    assert "normalised across the entries listed here only" in description
    assert "not of the whole decision" in description


# ── a model that claims completeness cannot be fixed by appending a caveat ────────────────────────────


@pytest.mark.parametrize(
    "narrative",
    [
        "event_ts accounts for all of what the model measured (100%).",
        "These two metrics account for all of the decision.",
        "latency_ms fully explains the flag across 88 rows.",
        "amount is 100% of the evidence here.",
        "This is the whole picture for these rows.",
    ],
)
def test_a_totalising_claim_is_detected(narrative: str):
    """The observed failure, and the shapes near it.

    The first entry is close to the sentence the evaluation actually recorded. Appending "some evidence was
    withheld" to any of these produces a narrative that contradicts itself, and a reader believes the first
    half -- so the claim has to be replaced rather than qualified.
    """
    assert re.search(TOTALISING_CLAIM_PATTERN, narrative)


@pytest.mark.parametrize(
    "narrative",
    [
        "Across 312 rows, amount carries most of the evidence shown (61%), with quantity next (22%).",
        "Of what can be shown across these 88 rows, latency_ms carries most (74%) and retries a small part (12%).",
        "Of what can be shown, amount (61%) and quantity (22%) contributed.",
    ],
)
def test_ordinary_prose_is_not_mistaken_for_a_totalising_claim(narrative: str):
    """The guard must not fire on the house style, or it would replace good narratives with the fallback.

    The second entry is an exemplar response verbatim: if the guard flagged our own demonstration, every
    partial group would get the fallback and the model's work would be discarded.
    """
    assert not re.search(TOTALISING_CLAIM_PATTERN, narrative)


def test_the_fallback_carries_the_groups_facts_and_invents_nothing():
    """A fallback has to be useful, not a refusal -- and must not smuggle in a direction or a cause."""
    rendered = DISCLOSURE_FALLBACK_TEMPLATE % ("88", "latency_ms (74%), retries (12%)")

    assert "88 rows" in rendered
    assert "latency_ms" in rendered
    for invented in ("far above", "elevated", "because", "caused"):
        assert invented not in rendered.lower()


@pytest.mark.parametrize("band", [DISCLOSURE_PARTIAL, DISCLOSURE_LIMITED])
def test_the_fallback_states_the_limit_exactly_once_once_the_clause_is_appended(band: str):
    """The fallback fires only when evidence was withheld, and the clause is appended on that same
    condition -- so if the fallback asserted the limit too, every replaced narrative would say it twice,
    the second time more precisely than the first. The clause is the one place that says it.
    """
    rendered = DISCLOSURE_FALLBACK_TEMPLATE % ("88", "latency_ms (74%)") + DISCLOSURE_NARRATIVE_CLAUSE[band]

    assert rendered.lower().count("could not be disclosed") == 1
    assert "cannot fully disclose" not in rendered.lower()


def test_the_scope_is_published_as_a_field_not_only_as_prose():
    """A consumer should be able to filter limited-evidence explanations without parsing a sentence.

    Position is load-bearing: the struct is cast positionally, so evidence_scope must be last in the schema
    and last in the construction. This pins the schema half.
    """
    names = [field.name for field in ai_explanation_struct_schema.fields]

    assert names[-1] == "evidence_scope"
    assert (
        dict(zip(names, (f.dataType.simpleString() for f in ai_explanation_struct_schema.fields)))["evidence_scope"]
        == "string"
    )


def test_no_exemplar_response_would_be_replaced_by_our_own_guard():
    """A few-shot example outweighs an instruction, so an exemplar that trips the guard is worse than none.

    This is the defect the limited-disclosure exemplar had when first written: it declared that most of the
    evidence was withheld and then described the rows as dominated by the one metric it could see, which is
    the redirect the limited band explicitly forbids. Extracted from the rendered header rather than the
    source table so it tracks what the endpoint is actually sent.
    """
    narratives = [
        line.partition('"narrative":"')[2].partition('","')[0]
        for line in AI_QUERY_PROMPT_HEADER.splitlines()
        if line.startswith("Response: {")
    ]

    assert len(narratives) == 2, "both exemplars should carry a narrative"
    for narrative in narratives:
        assert not re.search(TOTALISING_CLAIM_PATTERN, narrative), narrative
