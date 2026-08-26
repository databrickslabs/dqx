"""Redacting a column must redact the features derived from it.

SHAP contributions are keyed by *engineered* feature name, and redaction filters those keys by exact
match against the caller's ``redact_columns``. Baseline conditioning adds a derived feature per
metric, so redacting ``amount`` left ``amount_rel_baseline`` -- a signed log-ratio of the same
column -- flowing into an LLM prompt sent to an external serving endpoint. A caller naming a column
sensitive means everything computed from it is sensitive.
"""

from databricks.labs.dqx.anomaly.anomaly_llm_explainer import redaction_set
from databricks.labs.dqx.anomaly.transformers import BASELINE_RELATIVE_SUFFIX, SparkFeatureMetadata


def test_redacting_a_column_also_redacts_its_baseline_relative_feature():
    """The hole this closed: the derived feature is the leak, and it is the interesting one."""
    assert redaction_set(("amount",)) == {"amount", f"amount{BASELINE_RELATIVE_SUFFIX}"}


def test_redaction_covers_every_named_column():
    result = redaction_set(("amount", "salary"))

    assert "amount" in result and f"amount{BASELINE_RELATIVE_SUFFIX}" in result
    assert "salary" in result and f"salary{BASELINE_RELATIVE_SUFFIX}" in result


def test_no_redaction_stays_empty():
    """An empty set is what lets the caller skip the filter entirely, so it must not grow entries."""
    assert not redaction_set(())


def test_similar_prefixes_are_not_swept_up():
    """Expansion is by exact suffix, not prefix matching.

    Redacting ``amount`` must not silently drop ``amount_paid``, which is a different column the
    caller did not name. Prefix matching would, and would quietly reduce what the explanation can
    talk about.
    """
    result = redaction_set(("amount",))

    assert "amount_paid" not in result
    assert f"amount_paid{BASELINE_RELATIVE_SUFFIX}" not in result


def _metadata() -> SparkFeatureMetadata:
    """Feature metadata with a categorical column expanded to one-hot + frequency + null-indicator,
    alongside a numeric column with a baseline-relative feature."""
    return SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric"},
            {"name": "country", "category": "categorical"},
        ],
        categorical_frequency_maps={"country": {"US": 0.7, "DE": 0.3}},
        onehot_categories={"country": ["US", "DE"]},
        engineered_feature_names=[
            "amount",
            "country_US",
            "country_DE",
            "country_freq",
            "country_is_null",
            "amount_rel_baseline",
        ],
    )


def test_metadata_driven_redaction_covers_onehot_and_frequency():
    """The gap this closed: one-hot and frequency features carry the same information as the raw
    column, but their names cannot be reconstructed from the column alone. With metadata they are
    enumerated exactly and dropped.
    """
    result = redaction_set(("country",), _metadata())

    assert result == {"country", "country_US", "country_DE", "country_freq", "country_is_null"}


def test_metadata_driven_redaction_covers_numeric_identity_and_baseline():
    result = redaction_set(("amount",), _metadata())

    assert result == {"amount", "amount_rel_baseline"}


def test_metadata_driven_redaction_leaves_other_columns_untouched():
    """Redacting one column must not sweep up another column's features."""
    result = redaction_set(("country",), _metadata())

    assert "amount" not in result
    assert "amount_rel_baseline" not in result
