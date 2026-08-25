# Anomaly conditioning experiment

Measures whether conditioning row anomaly detection on a group actually detects better, and which
mechanism to use. This is the evidence behind [#1484](https://github.com/databrickslabs/dqx/issues/1484)
and behind the decision to remove the heterogeneity gate rather than keep it.

Not part of `make test`, `make integration`, or the nightly. It is a manual harness: it downloads
third-party datasets, takes minutes to hours, and produces a correlation rather than a pass/fail.
The assertions that guard the same claims in CI live in
`tests/unit/test_anomaly_relative_feature_separability.py` (mechanism, numpy, under five seconds)
and `tests/integration_anomaly/test_anomaly_quality.py` (the real DQX pipeline through Spark and
MLflow).

## Running it

```shell
# Synthetic sweep only: no network, no Databricks workspace, a few minutes.
uv run python benchmarks/anomaly_conditioning/run_experiment.py --seeds 5

# Add the real datasets. Downloads ~250 MB on first run, cached in ~/.cache/dqx-benchmarks.
uv run python benchmarks/anomaly_conditioning/run_experiment.py --seeds 5 \
    --datasets synthetic smd nslkdd
```

Results are written to `results/YYYY-MM-DD-<sha>.md` and `.json`, stamped with the DQX commit they
came from. The markdown is meant to be readable on its own; the JSON holds every cell for reanalysis.

## What it compares

Three configurations, each fitted and scored on the same rows:

| config | what it is | in DQX |
|---|---|---|
| `pooled` | one model over the raw metrics | DQX before #1484 |
| `relative` | one model, raw metrics **plus** each metric's deviation from its own group's baseline | `baseline_by` |
| `per_group` | one model per group | the legacy `segment_by` |

The comparison is **paired by seed** and tested with Wilcoxon signed-rank. The seed drives both the
data draw and the forest and dominates the between-cell variance, so unpaired means would mostly
measure the seed.

## Why the metrics are what they are

**PR-AUC is primary.** These datasets are heavily imbalanced and ROC-AUC flatters a detector that
merely ranks the majority class well.

**No point-adjusted F1, anywhere.** Under the point-adjust protocol — crediting a whole labelled
anomaly segment when any single point inside it is detected — a *random* score achieves
state-of-the-art F1 (Kim et al., *Towards a Rigorous Evaluation of Time-series Anomaly Detection*,
AAAI 2022). Numbers produced that way are uninterpretable, so none are computed. If you are about to
add one, read that paper first.

**Worst-group false-positive rate, not the average.** A per-group model's failure mode is one
group's calibration collapsing while the rest look fine, and an average over groups is exactly what
hides it.

**Trivial baselines are reported.** Kim et al.'s other recommendation: state the improvement over
doing almost nothing, not an absolute number.

## Reading the numbers honestly

DQX scores **rows independently**. It is not a sequence model. PR-AUC around 0.15 on SMD against
published sequence-model results above 0.80 is a *different task*, not a worse implementation —
those models consume a window of history per prediction, and DQX deliberately does not. Any table
lifted out of here needs that caveat attached, or it reads as a failure.

The harness also reimplements the relative transform in numpy rather than calling DQX. That is
deliberate: it makes a sweep of a few thousand fits possible without a Spark session, at the cost of
measuring the *mechanism* rather than DQX's implementation of it. Pipeline fidelity is a separate
question, asserted in `tests/integration_anomaly/test_anomaly_quality.py`.

## Datasets, licences, citations

Everything is downloaded at run time and cached. **Nothing is vendored into this repository**, which
is what keeps the licensing position simple — DQX redistributes none of it.

| dataset | licence | grouping candidates | citation |
|---|---|---|---|
| Server Machine Dataset (SMD) | MIT, via [`NetManAIOps/OmniAnomaly`](https://github.com/NetManAIOps/OmniAnomaly) | server entity (28), machine family (3) | Su et al., *Robust Anomaly Detection for Multivariate Time Series through Stochastic Recurrent Neural Networks*, KDD 2019 |
| NSL-KDD | redistributable with citation | `service`, `protocol_type`, `flag` | Tavallaee et al., *A detailed analysis of the KDD CUP 99 data set*, CISDA 2009 |

**SMAP and MSL are deliberately excluded.** Their data files carry "© Original Authors" with no
permissive licence, so they cannot be used even under download-at-runtime.

**ADBench is not used** despite being the obvious benchmark suite: it ships pre-processed `.npz`
numeric matrices, so categorical column identity is gone and there is no group left to condition on.
Only raw datasets retain what this experiment measures.

Two adjustments are made to the real data, both documented at their definitions in `datasets/real.py`:
SMD is capped at 4,000 rows per entity so a run takes minutes rather than hours, and NSL-KDD's
attacks are downsampled from ~46% to 2% so the task is anomaly detection rather than classification.
Every configuration sees identical rows, so neither affects the comparison.

## The heterogeneity gate question

DQX briefly had `MIN_GROUP_HETEROGENEITY = 0.10`: conditioning was skipped when eta-squared — the
share of variance explained by the grouping — fell below it, on the theory that a grouping which
explains little contributes noise.

The counter-argument, and the reason it was removed rather than kept pending: at low heterogeneity
every group median approaches the global median, so `signed_log(x) - signed_log(median)` becomes a
monotone transform of `x` — a near-duplicate of an informative column, not noise. Redundancy, not
misdirection.

That is a falsifiable prediction, and the decision rules are fixed in `run_experiment.py` **before**
any run, so a reader can check the conclusion against the criterion rather than against a narrative
written afterwards:

- **No gate needed** if the worst delta below eta-squared 0.10 stays above −0.01.
- **Gate needed** if any such cell reaches −0.02, in which case refit the threshold from the sweep
  rather than reinstating 0.10 by inheritance.

The harness also reports Spearman ρ(eta-squared, delta) with a bootstrap CI, and regresses
`delta ~ eta_squared + is_contextual`, because the gate's premise requires eta-squared to carry
signal *after* controlling for which mechanism produced the anomaly. A correlation that disappears
under that control was the mechanism all along.
