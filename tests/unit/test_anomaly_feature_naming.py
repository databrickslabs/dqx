"""Unit tests for the engineered-feature reverse map (no Spark)."""

import pytest

from databricks.labs.dqx.anomaly.feature_naming import (
    engineered_from,
    human_label,
    source_blocks,
    source_column,
)
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata


@pytest.fixture
def metadata() -> SparkFeatureMetadata:
    """Covers every naming convention across distinct source columns.

    - amount: numeric identity + baseline-relative
    - country: one-hot (US, DE) + a null indicator
    - region: one-hot with an underscore in the value
    - channel: frequency encoding
    - is_active: boolean
    - signup: the seven datetime cyclicals
    """
    return SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric"},
            {"name": "country", "category": "categorical"},
            {"name": "region", "category": "categorical"},
            {"name": "channel", "category": "categorical"},
            {"name": "is_active", "category": "boolean"},
            {"name": "signup", "category": "datetime"},
        ],
        categorical_frequency_maps={"channel": {"web": 0.6, "app": 0.4}},
        onehot_categories={"country": ["US", "DE"], "region": ["north_america"]},
        engineered_feature_names=[
            "amount",
            "country_US",
            "country_DE",
            "country_is_null",
            "region_north_america",
            "channel_freq",
            "is_active_bool",
            "signup_hour_sin",
            "signup_hour_cos",
            "signup_dow_sin",
            "signup_dow_cos",
            "signup_month_sin",
            "signup_month_cos",
            "signup_is_weekend",
            "amount_rel_baseline",
            "amount_rel_time",
        ],
        baseline_by=["country"],
        baseline_over_time="signup",
    )


@pytest.mark.parametrize(
    "engineered_name, expected_source, expected_label",
    [
        ("amount", "amount", "amount"),
        ("amount_rel_baseline", "amount", "amount vs its group baseline"),
        # Deliberately not "unusual amount": a large time-relative contribution is compatible with the
        # metric's own value sitting well inside its normal range, and the label is what an LLM reads.
        ("amount_rel_time", "amount", "amount vs its expected level at that time"),
        ("country_US", "country", "country = US"),
        ("country_DE", "country", "country = DE"),
        ("country_is_null", "country", "country is null"),
        ("region_north_america", "region", "region = north_america"),
        ("channel_freq", "channel", "channel frequency"),
        ("is_active_bool", "is_active", "is_active"),
        ("signup_hour_sin", "signup", "signup hour"),
        ("signup_hour_cos", "signup", "signup hour"),
        ("signup_dow_sin", "signup", "signup day of week"),
        ("signup_dow_cos", "signup", "signup day of week"),
        ("signup_month_sin", "signup", "signup month"),
        ("signup_month_cos", "signup", "signup month"),
        ("signup_is_weekend", "signup", "signup is a weekend"),
    ],
)
def test_every_convention_resolves(
    metadata: SparkFeatureMetadata, engineered_name: str, expected_source: str, expected_label: str
):
    assert source_column(engineered_name, metadata) == expected_source
    assert human_label(engineered_name, metadata) == expected_label


def test_engineered_from_covers_all_derived_features(metadata: SparkFeatureMetadata):
    assert engineered_from("country", metadata) == frozenset({"country_US", "country_DE", "country_is_null"})
    # The time-relative feature has to be in here too, or redacting *amount* would leave a feature
    # derived from it in the contributions map, which is the leak this function exists to prevent.
    assert engineered_from("amount", metadata) == frozenset({"amount", "amount_rel_baseline", "amount_rel_time"})
    assert engineered_from("channel", metadata) == frozenset({"channel_freq"})
    assert engineered_from("signup", metadata) == frozenset(
        {
            "signup_hour_sin",
            "signup_hour_cos",
            "signup_dow_sin",
            "signup_dow_cos",
            "signup_month_sin",
            "signup_month_cos",
            "signup_is_weekend",
        }
    )


def test_engineered_from_is_empty_for_unknown_source(metadata: SparkFeatureMetadata):
    assert engineered_from("not_a_column", metadata) == frozenset()


def test_onehot_value_that_collides_with_a_suffix():
    """A category value equal to a fixed suffix must resolve as one-hot, not as that suffix.

    Column *status* with a value *freq* produces ``status_freq``; a suffix-first scan would misread
    it as frequency encoding of *status*. One-hot is matched first, so it resolves correctly.
    """
    meta = SparkFeatureMetadata(
        column_infos=[{"name": "status", "category": "categorical"}],
        categorical_frequency_maps={},
        onehot_categories={"status": ["freq", "open"]},
        engineered_feature_names=["status_freq", "status_open"],
    )
    assert source_column("status_freq", meta) == "status"
    assert human_label("status_freq", meta) == "status = freq"


def test_numeric_column_named_like_a_suffix_is_identity_not_frequency():
    """A numeric column literally named ``revenue_freq`` must resolve to itself.

    Stripping ``_freq`` yields *revenue*, which is not a source column, so the suffix match is
    rejected and numeric identity wins.
    """
    meta = SparkFeatureMetadata(
        column_infos=[{"name": "revenue_freq", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["revenue_freq"],
    )
    assert source_column("revenue_freq", meta) == "revenue_freq"
    assert human_label("revenue_freq", meta) == "revenue_freq"


def test_unknown_feature_resolves_to_none_and_labels_unchanged(metadata: SparkFeatureMetadata):
    """A feature from a convention this version does not know never crashes and is never hidden."""
    assert source_column("mystery_feature", metadata) is None
    assert human_label("mystery_feature", metadata) == "mystery_feature"


# ── provenance is read from the recorded transform, not from spelling ────────────────────────────────


def _ungrouped_with_a_literal_suffix_column() -> SparkFeatureMetadata:
    """A schema feature engineering permits: the caller's own ``amount_rel_baseline`` column, no grouping.

    Legal because ``validate_generated_feature_names`` only refuses a collision when the transform would
    actually generate that name, and with no ``baseline_by`` nothing does.
    """
    return SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric"},
            {"name": "amount_rel_baseline", "category": "numeric"},
            {"name": "latency_rel_time", "category": "numeric"},
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["amount", "amount_rel_baseline", "latency_rel_time"],
        baseline_by=[],
        baseline_over_time="",
    )


def test_a_literal_column_ending_in_a_suffix_resolves_to_itself():
    """The reported defect: the reverse map decomposed a real column and credited a different one.

    ``amount_rel_baseline`` is the caller's own metric here. Resolving it to ``amount`` attributes one
    column's contribution to another, and it makes redaction of ``amount`` sweep up an unrelated column.
    """
    ungrouped = _ungrouped_with_a_literal_suffix_column()

    assert source_column("amount_rel_baseline", ungrouped) == "amount_rel_baseline"
    assert source_column("latency_rel_time", ungrouped) == "latency_rel_time"


def test_a_literal_column_ending_in_a_suffix_is_labelled_as_itself():
    """It was labelled "amount vs its group baseline" in a model with no group baseline at all."""
    ungrouped = _ungrouped_with_a_literal_suffix_column()

    assert human_label("amount_rel_baseline", ungrouped) == "amount_rel_baseline"
    assert human_label("latency_rel_time", ungrouped) == "latency_rel_time"


def test_redacting_one_column_does_not_sweep_up_a_similarly_named_one():
    """The same defect seen from the redaction side, which is the one with a privacy consequence."""
    ungrouped = _ungrouped_with_a_literal_suffix_column()

    assert engineered_from("amount", ungrouped) == frozenset({"amount"})


def test_a_genuine_derived_feature_still_resolves_when_its_basis_ran(metadata: SparkFeatureMetadata):
    """The other direction, so the gate cannot be satisfied by refusing everything.

    The shared fixture records both bases, so both suffixes must still decompose.
    """
    assert source_column("amount_rel_baseline", metadata) == "amount"
    assert source_column("amount_rel_time", metadata) == "amount"
    assert human_label("amount_rel_baseline", metadata) == "amount vs its group baseline"


def test_source_blocks_group_every_feature_under_exactly_one_source(metadata: SparkFeatureMetadata):
    """What block attribution indexes with: every engineered feature in exactly one block, order kept."""
    blocks = source_blocks(metadata)

    assert blocks["amount"] == ["amount", "amount_rel_baseline", "amount_rel_time"]
    assert blocks["country"] == ["country_US", "country_DE", "country_is_null"]
    flattened = [name for names in blocks.values() for name in names]
    assert sorted(flattened) == sorted(metadata.engineered_feature_names)
    assert len(flattened) == len(set(flattened))


def test_source_blocks_keep_an_unresolvable_feature_as_its_own_block():
    """A model trained by a newer DQX may carry a convention this version cannot parse. It must still be
    explainable rather than silently dropped from the map."""
    future = SparkFeatureMetadata(
        column_infos=[{"name": "amount", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["amount", "amount_some_future_transform"],
    )

    assert source_blocks(future)["amount_some_future_transform"] == ["amount_some_future_transform"]
