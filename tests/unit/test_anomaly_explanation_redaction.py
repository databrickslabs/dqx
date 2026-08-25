"""Redacting a column must redact the features derived from it.

SHAP contributions are keyed by *engineered* feature name, and redaction filters those keys by exact
match against the caller's ``redact_columns``. Baseline conditioning adds a derived feature per
metric, so redacting ``amount`` left ``amount_rel_baseline`` -- a signed log-ratio of the same
column -- flowing into an LLM prompt sent to an external serving endpoint. A caller naming a column
sensitive means everything computed from it is sensitive.
"""

from databricks.labs.dqx.anomaly.anomaly_llm_explainer import redaction_set
from databricks.labs.dqx.anomaly.transformers import BASELINE_RELATIVE_SUFFIX


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
