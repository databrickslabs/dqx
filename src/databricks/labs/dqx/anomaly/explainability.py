"""Feature attribution for row anomaly detection: which columns made a row look unusual.

Two detectors supply attribution by different means -- TreeSHAP for a tree model, an exact decomposition
for the correlation-aware one -- and one invariant lets everything downstream treat them alike:
**attribution is oriented so that a larger value means more responsible for the anomaly.** TreeSHAP does
not arrive that way, because it explains a path length that *shrinks* as a row becomes more isolated, so
:func:`_oriented_towards_anomaly` flips it.

Attribution stays signed for as long as it is being combined. Averaging across ensemble members and
summing a source column's engineered views both depend on SHAP's exact additivity, and a value arguing
the row is normal has to be able to cancel one arguing it is not. :func:`format_shap_contributions` is
the single place the sign is dropped, producing the public map: non-negative shares of the
anomaly-driving evidence, totalling 100, keyed by the columns the caller passed.

Requires the 'anomaly' extras: pip install databricks-labs-dqx[anomaly]
"""

import logging
from collections.abc import Sequence
from typing import Any

import mlflow.sklearn as mlflow_sklearn
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, MapType, StringType, StructField, StructType
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.anomaly.scoring_config import TAIL_ANCHOR_PERCENTILE, TAIL_RATE_PERCENTILE
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.reporting_columns import DefaultColumnNames

try:
    import shap as _shap  # type: ignore

    SHAP = _shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP = None
    SHAP_AVAILABLE = False

logger = logging.getLogger(__name__)


def _oriented_towards_anomaly(shap_values: np.ndarray) -> np.ndarray:
    """Flip TreeSHAP's sign so that larger means *more* responsible for the anomaly.

    TreeSHAP on an isolation forest explains the ensemble's **average path length**, not a score: its
    base value is a path length (around 12 on a few thousand rows), and a row is anomalous when it is
    isolated in *few* splits. So a negative SHAP value shortens the path and drives the anomaly, while a
    positive one lengthens it and argues the row is ordinary. Negating puts this branch on the same
    footing as the correlation-aware detector's own attribution, whose values are already non-negative
    with larger meaning more responsible -- one orientation for both, which is what lets everything
    downstream average and group attributions without asking where they came from.

    The sign is easy to get backwards, and backwards is not a subtle error. Measured on 200 rows each
    carrying exactly one deliberately anomalous feature, the feature with the largest value was the true
    culprit 199 times out of 200 under this orientation, 199 out of 200 using the magnitude alone, and
    **0 out of 200** using the un-negated positive part -- which reliably names the most *ordinary*
    feature instead. Magnitude alone was the previous behaviour and was very nearly right, because on
    rows anomalous enough to be shown at all the evidence is overwhelmingly anomaly-driving: 97.8% of it
    at a severity of 95, 99.5% at 99. It was still wrong in kind, adding evidence of normality to
    evidence of anomaly and reporting the sum as one number.

    **Deliberately does not clip.** Negative values -- features arguing the row is normal -- have to
    survive until after any averaging across ensemble members or summing across a source column's
    engineered views, because those operations rely on SHAP's exact additivity and the negatives are
    what makes them cancel correctly. Clipping first would overstate: two views at +3 and -1 net to 2,
    but clipping first sums to 3, and two ensemble members at +4 and -2 average to 1 rather than 2.
    :func:`format_shap_contributions` is the one place the clip happens, once, at the very end.

    Args:
        shap_values: Raw TreeSHAP values, shape ``(n_rows, n_features)``.

    Returns:
        Signed array of the same shape, oriented so larger means more responsible for the anomaly.
    """
    return -shap_values


def format_shap_contributions(
    attribution: np.ndarray,
    valid_indices: np.ndarray,
    num_rows: int,
    keys: list[str],
) -> list[dict[str, float | None]]:
    """Normalise an attribution matrix into per-row percentage maps.

    Takes attribution oriented so that larger means more responsible for the anomaly -- TreeSHAP via
    :func:`_oriented_towards_anomaly`, or the correlation-aware detector's own decomposition, which is
    already oriented that way and already non-negative.

    **This is the one place the clip happens, and it has to be here rather than earlier.** Negative
    values are features arguing the row is *normal*, and a normalising feature is not a driver, so they
    earn no share. But they must survive averaging across ensemble members and summing across a source
    column's engineered views first, because both rely on SHAP's exact additivity and the negatives are
    what makes them cancel: two views at +3 and -1 net to 2, while clipping first sums to 3. Clipping
    last keeps every intermediate step an honest decomposition and still yields a non-negative public
    map. The correlation-aware branch is unaffected, since its values are non-negative to begin with.

    A row whose attribution is entirely non-positive gets the all-``None`` map. It used to be given
    ``1 / n`` for every key, which reads as "every feature contributed equally" when what is true is that
    nothing is known -- invented rather than measured, and the more misleading of the two because the
    numbers look ordinary. The all-``None`` shape is already what a row with a null feature produces, so
    every consumer already handles it: *_pattern_spark_expr* and *_format_contributions_sql* both drop
    null entries and fall back to ``'unknown'``. Rare in practice -- no such row among 200 measured at a
    severity of 95 -- but "could not judge" has to stay distinguishable from "judged".

    Args:
        attribution: Per-key attribution, shape ``(n_valid_rows, len(keys))``, oriented so larger means
            more responsible. May contain negatives; they are dropped here.
        valid_indices: Boolean mask over the original rows, marking which reached the attribution.
        num_rows: Row count of the caller's frame, so the returned list aligns with it.
        keys: Names for the columns of *attribution* -- source columns, or engineered feature names.

    Returns:
        One map per row: percentages summing to 100 where there was anomaly-driving evidence, all-``None``
        otherwise.
    """
    num_keys = len(keys)
    contributions: list[dict[str, float | None]] = [{key: None for key in keys} for _ in range(num_rows)]

    if attribution.size == 0 or num_keys == 0:
        return contributions

    magnitudes = np.maximum(attribution, 0.0)
    totals = magnitudes.sum(axis=1, keepdims=True)
    normalized = np.divide(magnitudes, totals, out=np.zeros_like(magnitudes), where=totals > 0)
    has_attribution = totals.squeeze(axis=1) > 0

    valid_row_idx = 0
    for i in range(num_rows):
        if valid_indices[i]:
            if has_attribution[valid_row_idx]:
                contributions[i] = {
                    keys[j]: round(float(normalized[valid_row_idx, j] * 100.0), 1) for j in range(num_keys)
                }
            valid_row_idx += 1

    return contributions


def compute_row_attributions(
    model_local: Any,
    feature_matrix: pd.DataFrame,
    engineered_feature_cols: list[str],
    blocks: dict[str, list[int]] | None = None,
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Per-feature attribution for each row, from whichever estimator the model wraps.

    Two sources, one output shape. A tree model goes through ``SHAP.TreeExplainer``, which is
    approximate and the only SHAP explainer fast enough to be worth running here. An estimator that
    exposes ``feature_contributions`` supplies its own *exact* attribution instead -- the Mahalanobis
    detector's leave-one-out decomposition, which needs no SHAP at all.

    Dispatch is by duck typing rather than an ``isinstance`` check on purpose: this module is imported
    at rule-registration time and by both scorers, so importing a concrete estimator here would drag it
    into all of them and make the dependency direction harder to reason about.

    Whatever the source, the values feed the same *format_shap_contributions*, so the emitted map has
    identical scaling and null handling either way.

    *blocks* switches to **source-block** attribution, keyed by the source column rather than by engineered
    feature. That is not cosmetic: explaining engineered features one at a time is unsound when several of
    them share a source, because one view's evidence says nothing about how much the *column* mattered.
    Both detectors need it and both get it, by different arithmetic.

    For the correlation-aware detector the block value is an exact joint marginalisation, because dropping
    one view leaves another copy of the same information behind and the per-view drops are therefore
    almost nothing. Measured: adding one affine duplicate of a metric moved the reported cause from 99.8%
    that metric to 99.9% an unrelated one, while the score did not move; blocking restores 99.7%/0.3%.

    For a tree model the block value is a plain **sum** of the oriented values within the block, which is
    exact because SHAP is additive -- each value is that feature's share of the path length, so a source
    column's share is the sum over its views, with any view that argued the row was normal correctly
    cancelling part of the others. This is why the orientation must not clip before here. The failure it
    fixes is different in shape from the correlation-aware one: the evidence is *split* rather than
    misdirected, so a metric's true 100% shows up as 49.3% and 50.7% across two views. That still changes
    the answer, because DQX gives a numeric column up to three views: measured on a column whose true share
    was 74.5%, splitting it three ways left each view near 25% and handed the top spot to a *derived view*
    of it, with an unrelated single-view column tied alongside.

    Returns:
        ``(attribution, valid_indices, keys)`` where *keys* names the columns of *attribution* -- source
        columns when blocked, engineered features otherwise.
    """
    scaler = getattr(model_local, "named_steps", {}).get("scaler")
    estimator = getattr(model_local, "named_steps", {}).get("model", model_local)

    feature_values = scaler.transform(feature_matrix) if scaler else feature_matrix.values
    valid_indices = ~pd.isna(feature_values).any(axis=1)

    blocked = bool(blocks) and len(engineered_feature_cols) > 1
    keys = list(blocks) if blocked and blocks is not None else engineered_feature_cols

    attribution = np.array([])
    if valid_indices.any():
        attribution = _attribute(
            estimator,
            feature_values[valid_indices],
            len(engineered_feature_cols),
            [blocks[key] for key in keys] if blocked and blocks is not None else None,
        )

    return attribution, valid_indices, keys


def _attribute(
    estimator: Any, rows: np.ndarray, num_features: int, block_indices: list[list[int]] | None
) -> np.ndarray:
    """Attribution from whichever estimator this is, already grouped into blocks if blocks were asked for.

    Dispatch order matters. An estimator offering ``block_contributions`` supplies an exact joint
    marginalisation, which is the only correct grouping for a non-additive attribution; a tree model gets
    the plain sum, which is correct because SHAP is additive. A model with a single feature has nothing to
    decompose, so that feature takes the whole share.

    Args:
        estimator: The bare estimator, already unwrapped from any pipeline.
        rows: Feature values for the rows to attribute, scaled if the model carries a scaler.
        num_features: Width of the engineered feature space.
        block_indices: Feature positions per block, or ``None`` to attribute per feature.

    Returns:
        Signed attribution, oriented so larger means more responsible, one column per block when blocked.
    """
    if num_features == 1:
        return np.ones((len(rows), 1))
    if block_indices is not None and hasattr(estimator, "block_contributions"):
        return np.asarray(estimator.block_contributions(rows, block_indices))
    if hasattr(estimator, "feature_contributions"):
        return np.asarray(estimator.feature_contributions(rows))

    per_feature = _oriented_towards_anomaly(np.asarray(SHAP.TreeExplainer(estimator).shap_values(rows)))
    return _sum_within_blocks(per_feature, block_indices) if block_indices is not None else per_feature


def _sum_within_blocks(attribution: np.ndarray, block_indices: list[list[int]]) -> np.ndarray:
    """Collapse per-feature attribution into one column per block by summing within each.

    Correct only for an additive attribution, which TreeSHAP is: each value is that feature's signed share
    of the explained quantity, so a group's share is the plain sum over its members. An estimator whose
    attribution is *not* additive -- the correlation-aware detector's leave-one-out drops, where the views
    of one column each measure almost nothing on their own -- must not come through here; it supplies
    ``block_contributions`` instead.

    Args:
        attribution: Oriented per-feature attribution, shape ``(n_rows, n_features)``.
        block_indices: Feature positions per block, in the order the blocks' keys are reported.

    Returns:
        Array of shape ``(n_rows, len(block_indices))``. Signed, like its input: the clip belongs to
        :func:`format_shap_contributions`, after this.
    """
    return np.column_stack([attribution[:, indices].sum(axis=1) for indices in block_indices])


def mean_row_attributions(
    models: Sequence[Any],
    feature_matrix: pd.DataFrame,
    engineered_feature_cols: list[str],
    blocks: dict[str, list[int]] | None = None,
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Attribution for the *aggregate* an ensemble reports, rather than for one of its members.

    An ensemble's reported score is the mean over members, and so is its ``confidence_std``, but the
    explanation used to come from ``models[0]`` alone. Members differ only by random seed, which makes
    member zero an arbitrary choice, not a representative one -- and they disagree far more than that
    framing suggests: measured on 200 rows with the default three members, the pair that agreed least
    named a different top driver on 130 of them, and the closest pair still differed on 92. So the row
    was flagged by a committee and explained by whichever member happened to be trained first.

    Averaging is the aggregate that matches what the score does. SHAP is additive per member, so the mean
    of the per-feature values decomposes the mean predicted path length exactly -- which is why
    :func:`_oriented_towards_anomaly` must not clip before this runs.

    One honest caveat. The reported score is ``mean(-score_samples)``, and ``score_samples`` is a strictly
    monotone but *nonlinear* transform of path length, so this decomposes the mean path length rather than
    the mean score. That is acceptable because nothing reads these numbers as quantities: they are
    normalised to shares and consumed only as a ranking, and since the transform is strictly decreasing, a
    feature that shortens the path in every member also raises the score in every member. Only the
    weighting would differ.

    Averaging also does not make the explanation *stable*, only unbiased between members -- with three
    members it remains noisy, and the fix for that is more members, not a different aggregate.

    Args:
        models: The models behind the reported score, in any order. A single-element sequence returns the
            single-model attribution untouched, with no averaging.
        feature_matrix: Rows to attribute, already engineered.
        engineered_feature_cols: Feature names, positionally matching *feature_matrix*.
        blocks: Optional source-column grouping, forwarded to :func:`compute_row_attributions`.

    Returns:
        ``(attribution, valid_indices, keys)``, matching :func:`compute_row_attributions`.

    Raises:
        InvalidParameterError: If *models* is empty, or if members disagree on the attribution's shape or
            keys. Averaging those would blend columns naming different features into a confident,
            plausible-looking, wrong explanation -- the same failure this function exists to remove, so it
            fails loudly instead of falling back to one member.
    """
    if not models:
        raise InvalidParameterError("At least one model is required to attribute a row.")

    attribution, valid_indices, keys = compute_row_attributions(
        models[0], feature_matrix, engineered_feature_cols, blocks
    )
    if len(models) == 1 or attribution.size == 0:
        return attribution, valid_indices, keys

    members = [attribution]
    for model in models[1:]:
        member, member_valid, member_keys = compute_row_attributions(
            model, feature_matrix, engineered_feature_cols, blocks
        )
        if member_keys != keys or member.shape != attribution.shape or not np.array_equal(member_valid, valid_indices):
            raise InvalidParameterError(
                f"Ensemble members produced attributions that cannot be averaged: {len(keys)} keys with "
                f"shape {attribution.shape} against {len(member_keys)} keys with shape {member.shape}. "
                "Averaging them would mix different feature layouts."
            )
        members.append(member)

    return np.mean(np.stack(members), axis=0), valid_indices, keys


# Severity-gating margin for in-UDF SHAP computation. The UDF recomputes severity from raw
# scores with numpy while the authoritative severity is a Spark expression over the same
# quantile points; the epsilon makes the UDF-side gate slightly over-inclusive so floating-point
# drift between the two implementations can never leave an anomalous row without contributions.
_SEVERITY_GATE_EPSILON = 1e-6


def severity_from_scores(scores: np.ndarray, quantile_points: list[tuple[float, float]]) -> np.ndarray:
    """Map raw anomaly scores to severity percentiles: linear up to p95, then an exponential tail.

    Numpy counterpart of *add_severity_percentile_column*, and it has to stay one: this function decides
    which rows get SHAP inside the scoring UDF, so a disagreement between the two would drop contributions
    from precisely the rows that were flagged.

    The tail matches *_tail_severity_expr* term for term, and for the reason given there: linear
    interpolation between p95, p99 and the training maximum is the wrong shape for a score quantile
    function, so a threshold between those knots fired on well under the share of rows it promised.
    """
    points = sorted(quantile_points, key=lambda p: p[0])
    by_percentile = dict(points)
    anchor = by_percentile.get(TAIL_ANCHOR_PERCENTILE)
    rate = by_percentile.get(TAIL_RATE_PERCENTILE)

    # Without both anchors there is no tail to fit, and a degenerate tail has no width to interpolate over.
    # Either way every point stays a knot, which is the behaviour that predates the tail.
    if anchor is None or rate is None or rate <= anchor:
        percentiles = np.array([float(p) for p, _ in points])
        score_knots = np.array([float(q) for _, q in points])
        return np.interp(scores, score_knots, percentiles)

    body = [(p, q) for p, q in points if p <= TAIL_ANCHOR_PERCENTILE]
    values = np.asarray(scores, dtype=float)
    severity = np.interp(
        values,
        np.array([float(q) for _, q in body]),
        np.array([float(p) for p, _ in body]),
    )

    head_tail_probability = 100.0 - TAIL_ANCHOR_PERCENTILE
    base = head_tail_probability / (100.0 - TAIL_RATE_PERCENTILE)
    above = values > anchor
    severity[above] = 100.0 - head_tail_probability * np.power(base, -(values[above] - anchor) / (rate - anchor))
    return severity


def compute_gated_shap_contributions(
    models: Sequence[Any],
    feature_matrix: pd.DataFrame,
    engineered_feature_cols: list[str],
    scores: np.ndarray,
    quantile_points: list[tuple[float, float]] | None,
    threshold: float | None,
    blocks: dict[str, list[int]] | None = None,
) -> list[dict[str, float | None] | None]:
    """Attribute only the rows whose severity reaches the anomaly threshold.

    TreeSHAP costs an order of magnitude more than scoring itself, and contributions are only
    surfaced for anomalous rows, so computing SHAP for the typically tiny anomalous subset
    instead of every row removes most of the contributions cost. Rows below the threshold get
    ``None`` (a null map). When *quantile_points* or *threshold* is unavailable, attribution runs
    for all rows (previous behaviour).

    *models* is the set of models behind the reported score, not one model: for an ensemble that is every
    member, and the attribution is their mean via :func:`mean_row_attributions`. The parameter is a
    sequence rather than a single model precisely so the ensemble mistake this replaced -- scoring with a
    committee and explaining with whichever member trained first -- cannot be made again by omission. A
    single-element sequence behaves exactly as passing that model alone did. Note that an sklearn
    ``Pipeline`` is itself indexable, so passing one bare where a sequence is expected is not a type error;
    it fails a frame later inside the explainer rather than silently attributing a pipeline step.

    Cost of the ensemble aggregate is modest because the gate does the heavy lifting. Attribution runs on
    the rows above the threshold, typically well under 1% of them, at roughly ten times scoring cost --
    about a tenth of one scoring pass. Scoring an N-member ensemble already costs N passes, so the total
    moves from about ``N + 0.1`` to ``N + 0.1N``: near 7% more at the default three members. That is the
    reason there is no setting for how many members to attribute. Such a knob would offer a choice between
    a correct explanation and a few percent of runtime, and the honest lever for anyone who does not want
    the cost is *enable_contributions=False*, which already exists.
    """
    num_rows = len(feature_matrix)
    if not quantile_points or threshold is None:
        attribution, valid_indices, keys = mean_row_attributions(
            models, feature_matrix, engineered_feature_cols, blocks
        )
        return list(format_shap_contributions(attribution, valid_indices, num_rows, keys))

    severity = severity_from_scores(np.asarray(scores, dtype=float), quantile_points)
    anomalous_positions = np.flatnonzero(severity >= (float(threshold) - _SEVERITY_GATE_EPSILON))
    contributions: list[dict[str, float | None] | None] = [None] * num_rows
    if anomalous_positions.size:
        subset = feature_matrix.iloc[anomalous_positions]
        attribution, valid_indices, keys = mean_row_attributions(models, subset, engineered_feature_cols, blocks)
        subset_contributions = format_shap_contributions(attribution, valid_indices, len(subset), keys)
        for position, contribution in zip(anomalous_positions.tolist(), subset_contributions):
            contributions[position] = contribution
    return contributions


def format_contributions_map(contributions_map: dict[str, float | None] | None, top_n: int) -> str:
    """Format contributions map as string for top N contributors.

    Args:
        contributions_map: Dictionary mapping feature names to contribution values (0-100 range)
        top_n: Number of top contributors to include

    Features with no share are omitted rather than rendered as ``name (0%)``. Since attribution stopped
    crediting features that argued the row was *normal*, an exact zero is now common -- and naming one as a
    contributor states that it contributed, which is what the reader takes from seeing it in this list.

    Returns:
        Formatted string like "amount (85%), quantity (10%), discount (5%)"
        Empty string if contributions_map is None, empty, or holds nothing that contributed

    Example:
        >>> format_contributions_map(dict(amount=85.0, quantity=10.0), 2)
        'amount (85%), quantity (10%)'
    """
    if not contributions_map:
        return ""

    contributed = [(col, val) for col, val in contributions_map.items() if val is not None and val > 0.0]
    # Rank by share, descending. Values are non-negative shares of the anomaly-driving evidence.
    top_contribs = sorted(contributed, key=lambda item: item[1], reverse=True)[:top_n]

    # Format as string: "amount (85%), quantity (10%), discount (5%)"
    return ", ".join(f"{col} ({val:.0f}%)" for col, val in top_contribs)


def create_optimal_tree_explainer(tree_model: Any) -> Any:
    """Create TreeSHAP explainer for the given tree model.

    Uses SHAP's TreeExplainer, which provides efficient SHAP value computation
    for tree-based models via optimized C++ implementations.

    Args:
        tree_model: Trained tree-based model (e.g., IsolationForest)

    Returns:
        Configured SHAP TreeExplainer
    """
    return SHAP.TreeExplainer(tree_model)


def compute_contributions_for_matrix(
    model_local: Any, feature_matrix: np.ndarray, columns: list[str]
) -> list[dict[str, float | None]]:
    """Compute normalised contributions for a raw feature matrix, one row at a time.

    Shares the semantics of the scoring path rather than reimplementing them: values are oriented via
    :func:`_oriented_towards_anomaly`, the side arguing the row is normal earns no share, and a row with
    no anomaly-driving evidence gets all-``None`` instead of an invented uniform split. Keeping the two in
    step matters more than the small duplication -- one module giving two different answers to "what does a
    negative SHAP value mean" is how the original defect survived as long as it did.

    Unlike the scoring path, contributions here are fractions of 1 rather than percentages, which is the
    existing contract of this function and its caller.
    """
    # If model is a Pipeline (due to feature scaling), extract components
    # SHAP's TreeExplainer only supports tree models, not pipelines
    # A Pipeline no longer necessarily contains a scaler: DQX fits the forest without one, since an
    # affine per-feature transform cannot change axis-parallel splits. Models trained before that
    # still carry a RobustScaler, so the step is looked up rather than assumed -- indexing
    # named_steps["scaler"] directly would raise KeyError on anything trained by this version.
    if isinstance(model_local, Pipeline):
        scaler = model_local.named_steps.get("scaler")
        tree_model = model_local.named_steps["model"]
        needs_scaling = scaler is not None
    else:
        scaler = None
        tree_model = model_local
        needs_scaling = False

    explainer = SHAP.TreeExplainer(tree_model)

    # Scale the data if the model uses a scaler
    if needs_scaling:
        feature_matrix = scaler.transform(feature_matrix)

    # Handle NaN values (SHAP can't process them)
    has_nan = pd.isna(feature_matrix).any(axis=1)

    contributions_list: list[dict[str, float | None]] = []
    for i in range(len(feature_matrix)):
        if has_nan[i]:
            contributions_list.append({col: None for col in columns})
            continue

        driving = np.maximum(
            _oriented_towards_anomaly(np.asarray(explainer.shap_values(feature_matrix[i : i + 1]))[0]), 0.0
        )
        total = driving.sum()

        contributions: dict[str, float | None]
        if total > 0:
            normalized = driving / total
            contributions = {col: float(normalized[j]) for j, col in enumerate(columns)}
        else:
            contributions = {col: None for col in columns}

        contributions_list.append(contributions)

    return contributions_list


def compute_feature_contributions(
    model_uri: str,
    df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """
    Compute per-row feature contributions using TreeSHAP.

    TreeSHAP provides exact feature attributions from the IsolationForest model,
    showing which features contributed most to each anomaly score.

    Args:
        model_uri: MLflow model URI to load sklearn IsolationForest.
        df: DataFrame with data to explain.
        columns: Feature columns used for training.

    Returns:
        DataFrame with additional 'anomaly_contributions' map column containing
        normalized SHAP values (absolute contributions summing to 1.0 per row).
    """
    return_schema = StructType([StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)])

    @pandas_udf(return_schema)  # type: ignore[call-overload]
    def compute_shap_udf(feature_struct: pd.Series) -> pd.DataFrame:
        """Compute SHAP values for each row using TreeExplainer."""
        model_local = mlflow_sklearn.load_model(model_uri)

        # feature_struct is already a DataFrame with struct fields as columns
        feature_matrix = feature_struct.values
        contributions_list = compute_contributions_for_matrix(model_local, feature_matrix, columns)

        # Return as a DataFrame so the StructType schema is satisfied
        return pd.DataFrame({"anomaly_contributions": contributions_list})

    # Combine feature columns into struct, then apply UDF
    result = df.withColumn("anomaly_contributions", compute_shap_udf(F.struct(*[F.col(c) for c in columns])))

    return result


def add_top_contributors_to_message(df: DataFrame, threshold: float, top_n: int = 3) -> DataFrame:
    """
    Enhance error messages with top feature contributors from SHAP values.

    Args:
        df: DataFrame with anomaly_score and anomaly_contributions.
        threshold: Score threshold for anomalies.
        top_n: Number of top contributors to include in message.

    Returns:
        DataFrame with enhanced messages including top contributing features.
    """
    format_udf = F.udf(lambda m: format_contributions_map(m, top_n), StringType())

    info_col = DefaultColumnNames.INFO.value
    if "severity_percentile" in df.columns:
        severity_col = F.col("severity_percentile")
    elif info_col in df.columns:
        # Info column is array<struct<...>>; use element_at(_, 1) to avoid Spark Connect getItem(0) resolution bug
        first_info = F.element_at(F.col(info_col), 1)
        severity_col = first_info.getField("anomaly").getField("severity_percentile")
    else:
        raise InvalidParameterError(
            "severity_percentile is required to determine top contributors. "
            "Ensure scoring adds severity_percentile before calling this helper."
        )

    return df.withColumn(
        "_top_contributors",
        F.when(severity_col >= threshold, format_udf(F.col("anomaly_contributions"))).otherwise(F.lit("")),
    )
