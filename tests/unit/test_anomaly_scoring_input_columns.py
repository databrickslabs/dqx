"""Unit pins for the columns a scoring run must hand the feature transform (no Spark).

The frame reaching `apply_feature_engineering_from_metadata` at scoring time is deliberately narrow: a
model trained on four features should not be fed a hundred-column table. But the transform reads more
than it produces, and every comparison basis added to the feature set has had to be added here too. Both
times it was missed, the symptom was the same and only visible on a workspace: an unresolved column at
scoring time, from a query plan that looked fine until Spark tried to resolve it.

These are cheap because the rule is pure -- names in, names out.
"""

from databricks.labs.dqx.anomaly.feature_prep import scoring_input_columns
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata

ROW_ID = "__dqx_row_id__"


def _metadata(**kwargs) -> SparkFeatureMetadata:
    """Metadata carrying only the fields this rule reads; the rest are structurally required."""
    return SparkFeatureMetadata(
        column_infos=[],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
        **kwargs,
    )


def test_the_plain_case_is_features_plus_the_row_id():
    """Nothing conditioned: no basis to carry, so the list is exactly what the model needs plus the join key."""
    selected = scoring_input_columns(["amount", "quantity"], [ROW_ID], _metadata())

    assert selected == ["amount", "quantity", ROW_ID]


def test_grouping_columns_are_carried_even_though_they_are_not_features():
    """The group-relative transform computes a deviation from the row's own group, so it needs the key."""
    selected = scoring_input_columns(["amount"], [ROW_ID], _metadata(baseline_by=["region", "product"]))

    assert selected == ["amount", "region", "product", ROW_ID]


def test_the_time_column_is_carried_even_though_it_is_not_a_feature():
    """The regression this test exists for.

    The fitted expectation is a function of time, so scoring has to evaluate it at the row's own timestamp.
    Without the column on this list the query fails to resolve rather than scoring badly.
    """
    selected = scoring_input_columns(["amount"], [ROW_ID], _metadata(baseline_over_time="event_ts"))

    assert "event_ts" in selected


def test_both_bases_are_carried_together():
    """The two compose: the temporal fit runs on the group-relative value, so both are read."""
    selected = scoring_input_columns(
        ["amount"], [ROW_ID], _metadata(baseline_by=["region"], baseline_over_time="event_ts")
    )

    assert selected == ["amount", "region", "event_ts", ROW_ID]


def test_an_unset_time_column_adds_nothing():
    """Inertness, the same guarantee the feature list itself carries: an empty basis is byte-identical."""
    assert scoring_input_columns(["amount"], [ROW_ID], _metadata(baseline_over_time="")) == ["amount", ROW_ID]


def test_passthrough_columns_come_last():
    """The packed original row is appended, so restoring it after scoring reads a stable position."""
    selected = scoring_input_columns(["amount"], [ROW_ID], _metadata(), passthrough_columns=["__dqx_orig_abc"])

    assert selected[-1] == "__dqx_orig_abc"


def test_a_column_serving_two_roles_is_selected_once():
    """Spark rejects a duplicated name in a select, and a grouping column may also be a merge column."""
    selected = scoring_input_columns(["amount"], ["region"], _metadata(baseline_by=["region"]))

    assert selected == ["amount", "region"]
