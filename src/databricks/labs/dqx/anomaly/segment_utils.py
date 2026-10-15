"""The baseline key: one group identifier, computed identically in Python and in Spark."""

from collections.abc import Iterable, Mapping
from typing import Any

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from databricks.labs.dqx.utils import quote_column_name
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.anomaly.column_names import names_resolve_same, reserve_temporary_column


# The preferred name, used only when it does not conflict with source fields.
BASELINE_KEY_COLUMN = "__dqx_baseline_key"


def _as_spark_string(value: Any) -> str | None:
    """Render *value* the way Spark's ``cast("string")`` does.

    Python's ``str()`` is not a drop-in for Spark's cast, and the differences are silent:

    * **Booleans.** Spark renders ``true``/``false``; Python renders ``True``/``False``. Found by
      the integration test that pins Python/Spark key agreement, and it is exactly the failure this
      contract exists to catch — a capitalised key persisted at training would miss every lookup at
      scoring and fall back to the global baseline without raising.
    * Integrals, strings and dates already agree, which is why
      *validate_baseline_columns* permits those types and
      rejects floating-point and decimal ones, where the two renderings diverge in ways no small
      shim can reconcile.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def build_baseline_key(group_values: Mapping[str, Any] | None) -> str:
    """Build a deterministic group identifier for the supported baseline-column types.

    The Python and Spark implementations must agree exactly so persisted baselines, calibration,
    and known-group membership refer to the same population at training and scoring.

    Values are ordered by column name. Each non-null value is prefixed by its UTF-8 byte
    length and a colon; null is encoded as -1:. Framing by length preserves tuple boundaries
    even when values contain control characters, colons, or text resembling another frame.
    A literal empty string is 0:, distinct from null and from an empty grouping.
    """
    if not group_values:
        return ""
    ordered = sorted(group_values.items(), key=lambda item: str(item[0]))
    parts = [_as_spark_string(value) for _key, value in ordered]
    return "".join("-1:" if part is None else f"{len(part.encode('utf-8'))}:{part}" for part in parts)


def baseline_key_column(baseline_by: list[str]) -> Column:
    """Build the group key for every row, in Spark.

    The Spark half of the contract described on *build_baseline_key*. This side is the source of
    truth — it is what runs at scoring time — so *_as_spark_string* is written to match
    ``cast("string")`` rather than the other way round.
    """
    values = [F.col(quote_column_name(name)).cast("string") for name in sorted(baseline_by, key=str)]
    parts = [
        F.when(value.isNull(), F.lit("-1:")).otherwise(
            F.concat(F.octet_length(value).cast("string"), F.lit(":"), value)
        )
        for value in values
    ]
    return F.concat(*parts) if parts else F.lit("")


def allocate_baseline_key_column(columns: Iterable[str]) -> str:
    """Choose a transient key name without replacing a source field, regardless of case."""
    occupied = set(columns)
    return reserve_temporary_column(BASELINE_KEY_COLUMN, occupied)


def with_baseline_key(df: DataFrame, baseline_by: list[str], *, key_column: str) -> DataFrame:
    """Compute the group key into an explicitly owned, unoccupied transient column.

    A source field can have any name, including the preferred internal key name. Callers allocate
    a free target and carry that name explicitly through feature engineering and calibration;
    an existing source field is never accepted as evidence that DQX already computed its key.
    """
    if not baseline_by:
        return df
    if any(names_resolve_same(key_column, name) for name in df.columns):
        raise InvalidParameterError("The transient baseline key conflicts with an input column.")
    return df.withColumn(key_column, baseline_key_column(baseline_by))
