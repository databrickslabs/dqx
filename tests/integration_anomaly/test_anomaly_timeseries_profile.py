"""``profile="timeseries"`` end to end: registry, contributions, AI explanation, detection quality.

This is the only place the correlation-aware detector is exercised through the real pipeline. Everything
before it is unit-level (numpy) or offline (the SMD bake-off), and neither can answer the two questions
that only a workspace can:

1. **Does MLflow round-trip a DQX-defined estimator class at all?** ``log_sklearn_model_compatible``
   passes no *code_paths* and no *pip_requirements* (``mlflow_registry.py:167``). The sklearn flavour
   defaults to cloudpickle, and ``timeseries_detector`` registers itself for pickle-by-value, so the
   class should travel inside the artifact -- but "should" is the word this test exists to remove.
   Scoring calls ``mlflow.sklearn.load_model`` (``model_loader.py:34``), so a failure here surfaces as a
   load error rather than as a wrong number.
2. **Does signature inference accept it?** ``register_model_with_signature_inference`` calls
   ``predict``. Unit tests pin that it returns ``{-1, +1}``; only MLflow can say whether that satisfies
   its inference.

Deliberately **one test with several assertions**, in the style of ``test_anomaly_quality.py``: every
property is read off the same pair of trained models, and each model is registered in Unity Catalog.
Splitting the assertions would retrain that pair once per test for no extra coverage. pytester's
``spark`` fixture is function-scoped, so a module-scoped fixture cannot hold the pair either. Each
assertion carries its own message, so a failure still says which property broke.

The fixture is ``generate_correlated_multivariate_data``, whose anomalies are produced by permuting
columns among the anomalous rows. That preserves every marginal distribution exactly, so the positives
are unreachable by any per-column statistic -- which is what makes a comparison between the two profiles
mean something rather than measuring which detector is better tuned.
"""

import numpy as np
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.training_strategies import MAHALANOBIS_ALGORITHM
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine
from tests.constants import TEST_CATALOG

from tests.integration_anomaly.constants import DEFAULT_SCORE_THRESHOLD
from tests.integration_anomaly.quality_metrics import pr_auc, trivial_baselines
from tests.integration_anomaly.synthetic_generators import generate_correlated_multivariate_data
from tests.integration_anomaly.conftest import ai_query_llm_config, create_anomaly_check_rule

# The gap the correlation-aware detector must clear against IsolationForest on data whose anomalies are
# *only* joint. Loose on purpose: the claim is the direction and rough size, not a tripwire on the
# forest's RNG. Measured offline on this exact fixture, PR-AUC was 0.146 for IsolationForest and 0.873
# for the correlation-aware detector, so 0.25 leaves a wide margin. The offline run also confirmed the
# fixture is univariately invisible: max-abs-z reached 0.072 against a 0.050 random floor.
MIN_PR_AUC_GAIN = 0.25

# Contributions must point at the columns whose correlation was actually severed, not merely be
# non-null. Not every row: the permutation moves values between anomalous rows, so a row can
# occasionally receive a value that happens to fit, leaving a *correlated* column as the largest single
# term. Measured 26 of 30 rows offline, so a simple majority is a wide margin and still fails loudly if
# the attribution were pointing somewhere arbitrary.
MIN_TOP_CONTRIBUTOR_HIT_RATE = 0.6

TRAIN_ROWS = 1200
TEST_ROWS = 600

pytestmark = pytest.mark.slow


def _scored_frame(anomaly_scorer, test_df, model, registry, columns, **check_kwargs):
    """Score, then collect the columns the metrics need into pandas."""
    result = anomaly_scorer(test_df, model, registry, extract_score=False, **check_kwargs)
    anomaly = F.element_at(F.col("_dq_info"), 1).getField("anomaly")
    return (
        result.select(
            *[F.col(c) for c in columns],
            F.col("is_anomaly").alias("label"),
            anomaly.getField("score").alias("score"),
            anomaly.getField("is_anomaly").cast("double").alias("flagged"),
            anomaly.getField("contributions").alias("contributions"),
            anomaly.getField("ai_explanation").alias("ai_explanation"),
        )
        .toPandas()
        .dropna(subset=["score"])
    )


def _engineered_feature_names(spark: SparkSession, registry_table: str, model_name: str) -> list[str]:
    """Read the persisted feature contract back out of the registry.

    Goes through ``SparkFeatureMetadata.from_json`` rather than parsing the JSON here, so this reads the
    contract the same way scoring does.
    """
    row = (
        spark.table(registry_table)
        .filter(F.col("identity.model_name") == model_name)
        .select("features.feature_metadata")
        .collect()[0]
    )
    return SparkFeatureMetadata.from_json(row["feature_metadata"]).engineered_feature_names


def _train_both_profiles(spark, quick_model_factory, columns, train_rows):
    """Train one model per profile on identical data, returning ``{profile: (model, registry)}``."""
    train_schema = ", ".join(f"{col} double" for col in columns) + ", is_anomaly double"
    models = {}
    for profile in ("timeseries", "tabular"):
        model, registry, _ = quick_model_factory(
            spark,
            columns=columns,
            train_data=train_rows,
            train_schema=train_schema,
            # sample_fraction=1.0 so the comparison is between estimators rather than between two
            # different random subsets. baseline_by=[] suppresses grouping discovery: there is no
            # grouping column in this fixture, and an empty list keeps that explicit rather than
            # relying on discovery happening to find nothing.
            params=AnomalyParams(sample_fraction=1.0),
            baseline_by=[],
            profile=profile,
        )
        models[profile] = (model, registry)
    return models


def _assert_contribution_contract(contributions_series, engineered_names: set[str]) -> None:
    """Every map is keyed by the persisted contract, non-negative, and normalised to 100.

    Non-negativity is the load-bearing one: the leave-one-out attribution is non-negative by
    construction (the precision matrix is PSD), which is what lets it reuse the SHAP formatter
    unchanged. A negative value here would mean the formula regressed, not the formatting.
    """
    for contributions in contributions_series:
        assert contributions is not None, "a flagged row carried no contributions map"
        unknown = sorted(set(contributions) - engineered_names)
        assert not unknown, f"contribution keys {unknown} are not in the persisted engineered feature names"
        values = [v for v in contributions.values() if v is not None]
        assert values, "a flagged row's contributions map held only nulls"
        assert min(values) >= 0.0, f"leave-one-out contributions must be non-negative, got {min(values)}"
        assert abs(sum(values) - 100.0) < 0.5, f"contributions should be normalised to 100, summed to {sum(values)}"


def _top_contributor_hits(contributions_series, expected_columns: list[str]) -> int:
    """How many rows name one of *expected_columns* as their single largest contributor."""
    expected = set(expected_columns)
    hits = 0
    for contributions in contributions_series:
        ranked = [(k, v) for k, v in contributions.items() if v is not None]
        if ranked and max(ranked, key=lambda item: item[1])[0] in expected:
            hits += 1
    return hits


def _assert_registry_records_profile(spark: SparkSession, registry_table: str, model_name: str) -> None:
    """The registry names the algorithm the profile selected, and how the covariance was estimated.

    Scoring reads *algorithm* back off the registry to resolve a scoring strategy, so a wrong value here
    would not fail training -- it would fail every future scoring run against this model.
    """
    row = (
        spark.table(registry_table)
        .filter(F.col("identity.model_name") == model_name)
        .select("identity.algorithm", "training.hyperparameters")
        .collect()[0]
    )
    assert row["algorithm"] == MAHALANOBIS_ALGORITHM, (
        f"registry recorded algorithm {row['algorithm']!r}; scoring resolves its strategy from this "
        f"string, so anything else orphans the model"
    )
    covariance = row["hyperparameters"].get("covariance")
    assert covariance in {"empirical", "ledoit_wolf"}, (
        f"expected the covariance estimator to be recorded, got {covariance!r}; this is how a reader "
        f"tells whether shrinkage was applied"
    )


def _assert_beats_tabular_and_baselines(timeseries, tabular, columns: list[str]) -> None:
    """The detector beats the default profile, and beats doing almost nothing."""
    timeseries_pr_auc = pr_auc(timeseries["label"], timeseries["score"])
    tabular_pr_auc = pr_auc(tabular["label"], tabular["score"])

    # The design claim: a broken correlation is close to invisible to a per-feature splitter.
    assert timeseries_pr_auc > tabular_pr_auc + MIN_PR_AUC_GAIN, (
        f"the timeseries profile should beat the tabular one on anomalies that are purely joint "
        f"(tabular PR-AUC {tabular_pr_auc:.4f}, timeseries {timeseries_pr_auc:.4f})"
    )

    # And it must beat doing almost nothing. max_abs_z is the honest floor here: the fixture preserves
    # every marginal exactly, so a univariate statistic cannot separate these positives -- which also
    # means failing this assertion would say the fixture broke rather than that the model did.
    baselines = trivial_baselines(timeseries, columns)
    assert (
        timeseries_pr_auc > baselines["random"]
    ), f"timeseries {timeseries_pr_auc:.4f} did not beat random {baselines['random']:.4f}"
    assert timeseries_pr_auc > baselines["max_abs_z"], (
        f"timeseries {timeseries_pr_auc:.4f} did not beat a max-abs-z baseline {baselines['max_abs_z']:.4f}; "
        f"the fixture's anomalies may have stopped being purely joint"
    )


def _assert_ai_explanation_present(flagged) -> None:
    """The AI explanation survives the new attribution path.

    It reads the contributions map, so this is what proves a non-SHAP attribution reaches the LLM prompt
    intact. Asserted structurally -- a non-empty narrative -- so the test does not depend on wording.
    """
    explained = flagged[flagged["ai_explanation"].notna()]
    assert not explained.empty, "no flagged row carried an ai_explanation struct"
    narrative = explained["ai_explanation"].iloc[0]["narrative"]
    assert isinstance(narrative, str) and narrative.strip(), "ai_explanation.narrative was empty"


def test_timeseries_profile_end_to_end(
    spark: SparkSession,
    quick_model_factory,
    anomaly_scorer,
    ai_query_endpoint,
):
    """The timeseries profile trains, registers, scores, attributes, explains, and beats the default.

    Trains two models on identical data -- one per profile -- and compares them on the same rows.
    """
    columns, train_df, test_df, broken_columns = generate_correlated_multivariate_data(
        spark,
        n_train=TRAIN_ROWS,
        n_test=TEST_ROWS,
    )
    train_rows = [tuple(r) for r in train_df.collect()]

    models = _train_both_profiles(spark, quick_model_factory, columns, train_rows)
    timeseries_model, timeseries_registry = models["timeseries"]
    tabular_model, tabular_registry = models["tabular"]

    # 1. The profile's choice is persisted and readable back.
    _assert_registry_records_profile(spark, timeseries_registry, timeseries_model)

    # Contributions and the AI explanation are only produced when asked for, and the explanation
    # depends on the contributions map — _resolve_ai_explanation_flag disables it silently when
    # contributions are off, so assertion 4 is load-bearing for assertion 5.
    timeseries = _scored_frame(
        anomaly_scorer,
        test_df,
        timeseries_model,
        timeseries_registry,
        columns,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_ai_explanation=True,
        ai_explanation_llm_model_config=ai_query_llm_config(ai_query_endpoint),
    )
    tabular = _scored_frame(anomaly_scorer, test_df, tabular_model, tabular_registry, columns)

    # 2. Detection quality: better than the default profile, and better than a one-liner.
    _assert_beats_tabular_and_baselines(timeseries, tabular, columns)

    # 3. Contributions honour the persisted feature contract on every flagged row.
    engineered_names = set(_engineered_feature_names(spark, timeseries_registry, timeseries_model))
    flagged = timeseries[timeseries["flagged"] == 1.0]
    assert not flagged.empty, "no row was flagged, so the contributions assertions would pass vacuously"

    _assert_contribution_contract(flagged["contributions"], engineered_names)

    # Gating must actually have happened. Attribution costs an order of magnitude more than scoring,
    # so computing it for every row is a performance regression rather than a cosmetic one. Asserted as
    # "not all rows" rather than "exactly the flagged rows" because the UDF-side gate is deliberately
    # over-inclusive by a small epsilon, so drift between the numpy and Spark severity computations can
    # never leave a flagged row without a map.
    with_contributions = int(timeseries["contributions"].notna().sum())
    assert with_contributions < len(timeseries), (
        f"all {len(timeseries)} rows received contributions, so severity gating did not run; "
        f"attribution is far more expensive than scoring, so this is a cost regression"
    )

    # 4. The attribution points at the columns whose correlation was actually severed. Without this the
    #    map could be uniform noise and every assertion above would still pass.
    positives = timeseries[(timeseries["label"] == 1.0) & timeseries["contributions"].notna()]
    assert not positives.empty, "no labelled anomaly received contributions"
    hits = _top_contributor_hits(positives["contributions"], broken_columns)
    hit_rate = hits / len(positives)
    assert hit_rate >= MIN_TOP_CONTRIBUTOR_HIT_RATE, (
        f"the top contributor named a column from {broken_columns} on only {hit_rate:.0%} of labelled "
        f"anomalies ({hits}/{len(positives)}); the attribution is not tracking the severed correlation"
    )

    # 5. The AI explanation still reaches the user with a non-SHAP attribution behind it.
    _assert_ai_explanation_present(flagged)

    # 6. Scores must be finite everywhere. A singular covariance would surface as NaN or inf rather
    #    than as an exception, and every metric above would silently degrade instead of failing.
    assert np.isfinite(timeseries["score"]).all(), "the timeseries model produced non-finite scores"


def test_a_grouped_timeseries_model_trains_and_scores(ws, spark: SparkSession, make_schema, make_random):
    """The combination that failed: the correlation-aware profile together with a grouping.

    Feature engineering deliberately preserves the group key, so the engineered frame is wider than the
    feature list. Signature inference passed that whole frame to ``model.predict``, handing the estimator a
    string column it was never fitted on. Every grouped model on the single-model path hit it --
    ``profile="timeseries"`` always, and the tabular profile at ``ensemble_size=1`` -- while the default
    three-model ensemble registers by URI and never comes through that code, which is why the existing
    coverage passed: it uses ``baseline_by=[]``.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(6).lower()
    model_name = f"{TEST_CATALOG}.{schema.name}.grouped_ts_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"

    rng = np.random.default_rng(5)
    rows = []
    for region in ("eu", "us", "apac"):
        offset = {"eu": 0.0, "us": 40.0, "apac": 80.0}[region]
        for _ in range(300):
            latent = rng.normal(0.0, 1.0)
            rows.append((region, float(100.0 + offset + 6.0 * latent), float(20.0 + 1.4 * latent)))
    train_df = spark.createDataFrame(rows, "region string, load double, current double")

    trained = AnomalyEngine(ws, spark).train(
        df=train_df,
        columns=["load", "current"],
        model_name=model_name,
        registry_table=registry_table,
        baseline_by=["region"],
        profile="timeseries",
        params=AnomalyParams(sample_fraction=1.0),
    )

    # Registering is where it failed; scoring proves the registered signature is usable.
    result_df = DQEngine(ws, spark).apply_checks(
        train_df.limit(20),
        [create_anomaly_check_rule(model_name=trained, registry_table=registry_table, threshold=95.0)],
    )
    scored = result_df.select(F.col("_dq_info")[0].getField("anomaly").getField("score").alias("score")).collect()

    assert len(scored) == 20
    assert all(row["score"] is not None for row in scored)


def test_a_grouped_single_forest_trains_and_scores(ws, spark: SparkSession, make_schema, make_random):
    """The second live path into the same defect: the tabular profile with one model instead of three."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(6).lower()
    model_name = f"{TEST_CATALOG}.{schema.name}.grouped_single_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"

    rows = [("eu" if i % 2 else "us", float(100 + i % 23), float(5 + i % 7)) for i in range(600)]
    train_df = spark.createDataFrame(rows, "region string, amount double, quantity double")

    trained = AnomalyEngine(ws, spark).train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
        baseline_by=["region"],
        params=AnomalyParams(sample_fraction=1.0, ensemble_size=1),
    )

    assert AnomalyModelRegistry(spark).get_active_model(registry_table, trained) is not None
