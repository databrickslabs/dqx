"""The baseline key: one group identifier, computed identically in Python and in Spark."""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F


# Separator between group column values in a composite group key. ASCII unit separator: a
# control character, so ordinary categorical data (names, codes, countries) cannot contain it and
# two different group tuples cannot collide onto one key.
#
# Known limitation: this is a separator, not an escaping scheme. Data that genuinely contains
# \x1f inside a group value can still collide — ("x\x1fy", "z") and ("x", "y\x1fz") produce the
# same key. Deliberately not solved by length-prefixing: the Python and Spark halves of this key
# must agree exactly, and adding length arithmetic to that contract buys protection against
# pathological data at the cost of widening the one invariant most expensive to get wrong.
BASELINE_KEY_SEPARATOR = "\x1f"

# Stand-in for a NULL group value. NULLs must map to a real key rather than propagating,
# or every row in a group with a missing dimension silently loses its baseline.
#
# A control character rather than a word, for the same reason the separator above is one: this was
# "MISSING", which a group column can genuinely contain, and then a NULL region and a region literally
# named MISSING shared one persisted key and one baseline without any complaint. \x00 is not a value a
# categorical dimension carries in practice, so the two are now distinguishable.
#
# This changes the persisted key format. It is done in the same release as the configuration-hash change
# that already forces every model to be retrained, so it costs no additional retrain -- deferring it would
# have made it a second forced retrain later.
BASELINE_KEY_NULL = "\x00"

# The single column every stage reads the group key from. See :func:`with_baseline_key`.
BASELINE_KEY_COLUMN = "__dqx_baseline_key"


def _as_spark_string(value: Any) -> str:
    """Render *value* the way Spark's ``cast("string")`` does.

    Python's ``str()`` is not a drop-in for Spark's cast, and the differences are silent:

    * **Booleans.** Spark renders ``true``/``false``; Python renders ``True``/``False``. Found by
      the integration test that pins Python/Spark key agreement, and it is exactly the failure this
      contract exists to catch — a capitalised key persisted at training would miss every lookup at
      scoring and fall back to the global baseline without raising.
    * Integrals, strings and dates already agree, which is why
      :func:`~databricks.labs.dqx.anomaly.validation.validate_baseline_columns` permits those types and
      rejects floating-point and decimal ones, where the two renderings diverge in ways no small
      shim can reconcile.
    """
    if value is None:
        return BASELINE_KEY_NULL
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def build_baseline_key(group_values: Mapping[str, Any] | None) -> str:
    """Build the group key for a row, in Python.

    Must agree exactly with :func:`baseline_key_column`, which computes the same key in Spark.
    Training persists baselines under keys produced by one and scoring looks them up with the
    other, so a disagreement does not fail — it silently misses every lookup and falls back to
    the global baseline, producing a model that appears to work and conditions on nothing.
    Their agreement is pinned by an integration test, which is how the boolean rendering
    difference handled in :func:`_as_spark_string` was found.

    Values are ordered by column name so the key does not depend on the order the caller
    happened to list the group columns in.
    """
    if not group_values:
        return ""
    ordered = sorted(group_values.items(), key=lambda item: str(item[0]))
    return BASELINE_KEY_SEPARATOR.join(_as_spark_string(value) for _key, value in ordered)


def baseline_key_column(baseline_by: list[str]) -> Column:
    """Build the group key for every row, in Spark.

    The Spark half of the contract described on :func:`build_baseline_key`. This side is the source of
    truth — it is what runs at scoring time — so :func:`_as_spark_string` is written to match
    ``cast("string")`` rather than the other way round.
    """
    parts = [F.coalesce(F.col(name).cast("string"), F.lit(BASELINE_KEY_NULL)) for name in sorted(baseline_by, key=str)]
    return F.concat_ws(BASELINE_KEY_SEPARATOR, *parts)


def with_baseline_key(df: DataFrame, baseline_by: list[str]) -> DataFrame:
    """Ensure *df* carries :data:`BASELINE_KEY_COLUMN`, computing it only when absent.

    Idempotent on purpose, and the point is *availability*, not agreement: every stage calls
    :func:`baseline_key_column`, so two Spark-side computations cannot disagree with each other.
    What they can do is run on a frame that no longer has the columns to compute from.

    Feature engineering drops the raw group columns so they cannot reach the sklearn pipeline or
    the inferred MLflow signature. Stages downstream of it therefore have the grouping only if
    something carried it forward. Training-time severity calibration is downstream — it runs on the
    scored engineered frame — and rebuilding the key there raised
    ``UNRESOLVED_COLUMN`` on the first grouped model ever trained against real Spark.
    """
    if not baseline_by or BASELINE_KEY_COLUMN in df.columns:
        return df
    return df.withColumn(BASELINE_KEY_COLUMN, baseline_key_column(baseline_by))
