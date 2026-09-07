"""Reverse mapping from an engineered feature name back to the source column it came from.

Feature engineering (see *transformers.py*) expands each source column into one or more engineered
features by a fixed set of naming conventions: one-hot ``{{col}}_{{value}}``, frequency ``{{col}}_freq``,
null indicator ``{{col}}_is_null``, boolean ``{{col}}_bool``, the datetime cyclicals
``{{col}}_hour_sin`` / ``_hour_cos`` / ``_dow_sin`` / ``_dow_cos`` / ``_month_sin`` / ``_month_cos`` /
``_is_weekend``, numeric identity (the feature *is* the column), and baseline-relative
``{{metric}}_rel_baseline``. This module inverts those conventions so that:

- redaction can drop *every* feature derived from a redacted column, not just the column itself --
  a one-hot or frequency feature leaks the same information as the raw column, and
- a raw contribution key such as ``event_count_rel_baseline`` can be rendered for a person as
  ``event_count vs its group baseline``.

Pure functions over *SparkFeatureMetadata*: no Spark, no I/O, deterministic.

Resolution order is deliberate. One-hot names are matched first, against the recorded
``onehot_categories``, because a category *value* may itself end in a fixed suffix (a column *status*
with a value *freq* produces ``status_freq``, which a suffix-first scan would misread as frequency
encoding of *status*).

**An exact source-column match comes next, before suffix decomposition.** Both can match the same name
at once: a caller may pass their own ``amount_rel_baseline`` column alongside ``amount``, which feature
engineering permits when no grouping is configured. Decomposing that name attributed one user column's
contribution to a different one and labelled it "amount vs its group baseline" in a model with no group
baseline. Identity cannot be wrong here -- a name in ``column_infos`` is a column that was analysed --
and it cannot mask a genuine derived feature, because ``validation.validate_generated_feature_names``
refuses a schema where a generated name collides with a real column.

An earlier version of this note said the order must not be reordered, justified by a numeric column
named ``revenue_freq`` where *revenue* does not exist. That case resolves correctly under either order,
so it never argued for suffix-first.

Fixed suffixes are tried last, accepted only when the remainder is a known source column **and** the
transform that appends the suffix actually ran for this model -- see ``_basis_ran``.
"""

from databricks.labs.dqx.anomaly.transformers import (
    BASELINE_RELATIVE_SUFFIX,
    TEMPORAL_RELATIVE_SUFFIX,
    SparkFeatureMetadata,
)

# Fixed suffixes appended by the non-one-hot transforms, paired with a human-phrase template applied
# to the recovered source column. Order within this tuple does not affect correctness -- a suffix is
# only accepted when the remainder is a known source column, and no two suffixes strip the same name
# to the same valid column -- but the baseline-relative suffix is listed first because it is the one
# a reader is most likely to see in a contribution.
_SUFFIX_LABELS: tuple[tuple[str, str], ...] = (
    (BASELINE_RELATIVE_SUFFIX, "{col} vs its group baseline"),
    # "vs its expected level" and not "unusual value": a time-relative contribution can be large while
    # the metric's own value sits well inside its normal range, and an LLM handed the raw suffix has
    # nothing to stop it reporting the latter. That mistake was already made once on this feature for
    # correlation breaks; the fix is the same one, applied where the name is rendered for a reader.
    (TEMPORAL_RELATIVE_SUFFIX, "{col} vs its expected level at that time"),
    ("_is_weekend", "{col} is a weekend"),
    ("_hour_sin", "{col} hour"),
    ("_hour_cos", "{col} hour"),
    ("_dow_sin", "{col} day of week"),
    ("_dow_cos", "{col} day of week"),
    ("_month_sin", "{col} month"),
    ("_month_cos", "{col} month"),
    ("_is_null", "{col} is null"),
    ("_freq", "{col} frequency"),
    ("_bool", "{col}"),
)


def _source_column_names(metadata: SparkFeatureMetadata) -> frozenset[str]:
    """Names of the analysed source columns, from the persisted column_infos."""
    return frozenset(info["name"] for info in metadata.column_infos if "name" in info)


def _match_onehot(engineered_name: str, metadata: SparkFeatureMetadata) -> tuple[str, str] | None:
    """Return (source_column, value) if *engineered_name* is a recorded one-hot feature.

    Matched against ``onehot_categories`` rather than by splitting on ``_``, because a value may
    contain an underscore and the column name may too, so no split point is reliable.
    """
    for col, values in metadata.onehot_categories.items():
        for value in values:
            if engineered_name == f"{col}_{value}":
                return col, value
    return None


def _basis_ran(suffix: str, metadata: SparkFeatureMetadata) -> bool:
    """Whether the transform that appends *suffix* actually ran for this model.

    A comparison suffix only means a derived feature when its basis is recorded: without ``baseline_by``
    nothing appends ``_rel_baseline``, and without ``baseline_over_time`` nothing appends ``_rel_time``.
    Checking this is what stops a user's own column named ``amount_rel_baseline`` being reported as
    "amount vs its group baseline" in a model that has no group baseline at all.
    """
    if suffix == BASELINE_RELATIVE_SUFFIX:
        return bool(metadata.baseline_by)
    if suffix == TEMPORAL_RELATIVE_SUFFIX:
        return bool(metadata.baseline_over_time)
    return True


def _match_suffix(
    engineered_name: str, source_names: frozenset[str], metadata: SparkFeatureMetadata
) -> tuple[str, str] | None:
    """Return (source_column, label_template) if *engineered_name* ends in a known fixed suffix,
    the remainder is a real source column, and the transform that appends that suffix ran."""
    for suffix, template in _SUFFIX_LABELS:
        if engineered_name.endswith(suffix) and _basis_ran(suffix, metadata):
            col = engineered_name[: -len(suffix)]
            if col in source_names:
                return col, template
    return None


def source_column(engineered_name: str, metadata: SparkFeatureMetadata) -> str | None:
    """The source column an engineered feature came from, or None if it cannot be resolved.

    Args:
        engineered_name: A feature name as it appears in *engineered_feature_names* / a SHAP key.
        metadata: The persisted feature metadata for the model that produced the feature.

    Returns:
        The source column name, or None when the feature matches no known convention (a model
        trained by a newer DQX with a convention this version does not know).
    """
    onehot = _match_onehot(engineered_name, metadata)
    if onehot is not None:
        return onehot[0]

    source_names = _source_column_names(metadata)
    if engineered_name in source_names:
        # A feature that *is* an analysed column is that column, whatever it is spelled like. Checked
        # before suffix decomposition because both can match at once: a caller may pass their own
        # ``amount_rel_baseline`` column alongside ``amount``, and decomposing it would attribute one
        # user column's contribution to a different one.
        return engineered_name

    suffix = _match_suffix(engineered_name, source_names, metadata)
    if suffix is not None:
        return suffix[0]
    return None


def human_label(engineered_name: str, metadata: SparkFeatureMetadata) -> str:
    """A short human phrase for an engineered feature, for display in prompts and info structs.

    Falls back to the engineered name unchanged when the feature matches no known convention, so
    this never raises and never hides a driver from a reader.

    Args:
        engineered_name: A feature name as it appears in *engineered_feature_names* / a SHAP key.
        metadata: The persisted feature metadata for the model that produced the feature.
    """
    onehot = _match_onehot(engineered_name, metadata)
    if onehot is not None:
        col, value = onehot
        return f"{col} = {value}"

    source_names = _source_column_names(metadata)
    if engineered_name in source_names:
        return engineered_name

    suffix = _match_suffix(engineered_name, source_names, metadata)
    if suffix is not None:
        col, template = suffix
        return template.format(col=col)

    if engineered_name in source_names:
        return engineered_name
    return engineered_name


def engineered_from(source: str, metadata: SparkFeatureMetadata) -> frozenset[str]:
    """Every engineered feature derived from *source*.

    Defined in terms of *source_column* over the recorded feature list, so the forward and reverse
    directions can never disagree: a feature is in this set exactly when it resolves back to
    *source*. This is what lets redaction of a source column drop all of its derived features --
    one-hot, frequency, null indicator, baseline-relative and the rest.

    Args:
        source: A source column name.
        metadata: The persisted feature metadata for the model.
    """
    return frozenset(name for name in metadata.engineered_feature_names if source_column(name, metadata) == source)


def source_blocks(metadata: SparkFeatureMetadata) -> dict[str, list[str]]:
    """Each source column mapped to the engineered features derived from it, in feature order.

    The forward view of :func:`source_column`, and the grouping attribution needs: explaining one
    engineered feature at a time is unsound when several of them carry the same source. Dropping one view
    of a metric leaves another copy behind, so the measured loss is small for every view, and normalising
    those small numbers hands almost all of the apparent blame to an unrelated metric. Measured on a
    correlated pair, adding one affine duplicate of *x* moved the reported cause from 99.8% *x* to 99.9%
    *y* while the score did not move.

    A feature that resolves to no source becomes its own single-member block, so every engineered feature
    belongs to exactly one block and nothing is silently dropped from an explanation.

    Order is preserved twice over: blocks appear in first-appearance order, and features within a block
    keep their positional order, because ``engineered_feature_names`` is positional and callers index
    into the feature matrix with it.

    Args:
        metadata: The persisted feature metadata for the model.

    Returns:
        source column -> its engineered feature names.
    """
    blocks: dict[str, list[str]] = {}
    for name in metadata.engineered_feature_names:
        blocks.setdefault(source_column(name, metadata) or name, []).append(name)
    return blocks


def source_block_indices(metadata: SparkFeatureMetadata) -> dict[str, list[int]]:
    """:func:`source_blocks` as positions into the feature matrix, which is what an estimator needs.

    Computed on the driver once per scoring run and closed over by the UDF, rather than per row or per
    partition: it is a pure function of the persisted metadata.
    """
    positions = {name: index for index, name in enumerate(metadata.engineered_feature_names)}
    return {
        source: [positions[name] for name in names if name in positions]
        for source, names in source_blocks(metadata).items()
    }
