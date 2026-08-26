"""SMD bake-off: what would it take to be in the ballpark on time-series anomaly detection?

An earlier experiment asked "do lag and rolling-window features help?" and answered no: +9% PR-AUC for
190 extra features, with ROC-AUC regressing, because IsolationForest degrades as the feature count
grows. This asks the harder question — **what does it actually take** — across four axes at once, and
its answer is why DQX gained a second algorithm: the estimator was the problem, not the features.

Why this script exists rather than a comparison against published numbers: SMD's headline results
(~0.80 F1) are computed with *point adjustment*, under which a random score reaches state of the art
(Kim et al., AAAI 2022). They are not a target and cannot be scaled to "80% of". So this script builds
its own honest reference — the best windowed reconstruction detector it can — and everything else is
measured against that.

Axes:

1. **Unit of analysis.** DQX scores one row at a time. SMD anomalies are subsequences, so a single row
   is the wrong unit. ``win_stats`` makes the trailing window the sample: current value plus trailing
   mean, standard deviation, min and max. Strictly trailing, so no leakage.
2. **Estimator.** Isolation Forest (what ships) against PCA reconstruction error, an MLP
   autoencoder, and Mahalanobis distance. PCA and the autoencoder are the interesting candidates
   because reconstruction degrades gracefully as the feature count grows, which is where Isolation
   Forest was measured to fail.
3. **Scope.** Pooled across all 28 machines (what DQX does) versus one model per machine (what the SMD
   literature does). Per-entity here uses a **global** score threshold rather than per-entity
   contamination — the latter is what made an all-normal entity flag its own most-unusual rows and
   produced the 56% false-alarm result in earlier work.
4. **Metric.** Point-wise PR-AUC is reported throughout, and it is harsh: an anomaly is a *range*, so a
   detector that fires two timesteps late scores as a miss plus a false positive. Alongside it,
   **event recall at a fixed alert budget**: of the true anomaly ranges, how many contain at least one
   alerted row, when the alert budget is capped. Precision stays strictly point-wise, so this is not
   point adjustment — it is the operational question a data-quality user actually has ("of the
   incidents, how many did I surface inside my review queue?").

Run:  uv run python benchmarks/anomaly_conditioning/smd_bakeoff.py --seeds 2
"""

import argparse
import json
import time

import numpy as np

from datasets.real import SMD_ENTITIES, load_smd_split
from metrics import pr_auc, precision_at_n, roc_auc
from sklearn.covariance import LedoitWolf
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler

N_TREES = 200
CONTAMINATION = 0.02
# Trailing window length in rows. SMD is one-minute cadence, so 60 rows is the last hour: long enough
# to characterise "normal recently" without so much lag that a short incident is averaged away.
WINDOW = 60


# --------------------------------------------------------------------------------------
# Axis 1: the unit of analysis
# --------------------------------------------------------------------------------------


def window_stats(values: np.ndarray, window: int = WINDOW) -> np.ndarray:
    """Current value plus trailing mean, stddev, min and max over the preceding *window* rows.

    Feature count is ``5 x n_metrics`` regardless of window length, unlike flattening the window,
    which multiplies by the window itself. Strictly trailing: the statistics cover rows
    ``[i-window, i-1]``, so the current row never contributes to its own baseline and nothing is
    computed from the future.

    Warm-up rows (fewer than two rows of history) fall back to the current value with zero spread,
    which is the honest neutral: with no history there is no deviation to report. A production
    implementation should mark them unscoreable instead, the way an unseen baseline group already is.
    """
    n_rows, n_cols = values.shape
    padded = np.vstack([np.zeros((1, n_cols)), values])
    csum = np.cumsum(padded, axis=0)
    csum_sq = np.cumsum(padded**2, axis=0)

    mean = np.zeros_like(values)
    std = np.zeros_like(values)
    lo = np.zeros_like(values)
    hi = np.zeros_like(values)

    for i in range(n_rows):
        start = max(0, i - window)
        count = i - start
        if count < 2:
            mean[i], std[i], lo[i], hi[i] = values[i], 0.0, values[i], values[i]
            continue
        total = csum[i] - csum[start]
        total_sq = csum_sq[i] - csum_sq[start]
        m = total / count
        mean[i] = m
        std[i] = np.sqrt(np.maximum(total_sq / count - m**2, 0.0))
        chunk = values[start:i]
        lo[i] = chunk.min(axis=0)
        hi[i] = chunk.max(axis=0)

    return np.hstack([values, mean, std, lo, hi])


FEATURISERS = {
    "raw": lambda v: v,
    "win_stats": window_stats,
}


# --------------------------------------------------------------------------------------
# Axis 2: estimators. Each returns test scores, higher = more anomalous.
# --------------------------------------------------------------------------------------


def score_iforest(train: np.ndarray, test: np.ndarray, seed: int) -> np.ndarray:
    model = IsolationForest(n_estimators=N_TREES, contamination=CONTAMINATION, random_state=seed, n_jobs=-1)
    model.fit(train)
    return -model.score_samples(test)


def score_pca_recon(train: np.ndarray, test: np.ndarray, seed: int) -> np.ndarray:
    """Reconstruction error after projecting onto the dominant linear subspace of *train*.

    A linear autoencoder converges to exactly this, so it is the honest cheap stand-in for one, and it
    is the candidate most likely to survive a wide feature matrix: extra correlated columns add to the
    subspace rather than diluting a random split.
    """
    scaler = StandardScaler().fit(train)
    tr, te = scaler.transform(train), scaler.transform(test)
    n_components = max(1, min(int(0.5 * tr.shape[1]), tr.shape[1] - 1))
    pca = PCA(n_components=n_components, random_state=seed).fit(tr)
    return np.sum((te - pca.inverse_transform(pca.transform(te))) ** 2, axis=1)


def score_mlp_autoencoder(train: np.ndarray, test: np.ndarray, seed: int) -> np.ndarray:
    """Non-linear reconstruction error from a small MLP trained to reproduce its input.

    Stands in for the deep autoencoders the SMD literature uses; no torch in this environment, so this
    is the closest available reference for "what a learned reconstruction achieves". Deliberately
    small and capped in iterations — this is a reference point, not a tuned model.
    """
    scaler = StandardScaler().fit(train)
    tr, te = scaler.transform(train), scaler.transform(test)
    width = tr.shape[1]
    bottleneck = max(2, width // 4)
    model = MLPRegressor(
        hidden_layer_sizes=(max(4, width // 2), bottleneck, max(4, width // 2)),
        random_state=seed,
        max_iter=60,
        early_stopping=False,
        learning_rate_init=0.005,
    )
    model.fit(tr, tr)
    return np.sum((te - model.predict(te)) ** 2, axis=1)


def _mahalanobis_sq(train: np.ndarray, test: np.ndarray, *, shrinkage: str | float, ridge: float) -> np.ndarray:
    """Squared Mahalanobis distance from the training centre, scaled and regularised.

    Two facts govern every variant below, and they are easy to get wrong.

    **The scaler is a no-op in exact arithmetic.** Mahalanobis distance is invariant under any
    invertible linear map: standardising *x* and using the standardised covariance gives bit-identical
    distances to using the raw covariance. So ``StandardScaler`` cannot change the answer directly --
    it changes it only *through the regulariser*, because a shrinkage target is not affine-equivariant.
    A fixed ``1e-3`` ridge is negligible against a column of variance 1e6 and dominant against one of
    variance 1e-3; standardising first makes one ridge value mean the same thing for every column. This
    is the mirror image of the argument in ``core.py`` for why RobustScaler was removed for
    IsolationForest: there the scaler was measured to be a genuine no-op, here it earns its place only
    by fixing the regulariser's basis.

    **Ledoit-Wolf minimises the error of the covariance, not the conditioning of its inverse.** So a
    ridge floor is still applied on top, expressed relative to the average variance so it is
    scale-free.
    """
    scaler = StandardScaler().fit(train)
    tr, te = scaler.transform(train), scaler.transform(test)

    if shrinkage == "ledoit_wolf":
        cov = LedoitWolf(assume_centered=False).fit(tr).covariance_
    else:
        cov = np.atleast_2d(np.cov(tr - tr.mean(axis=0), rowvar=False))
        if shrinkage:  # explicit convex shrink toward the average-variance diagonal
            target = np.eye(cov.shape[0]) * (np.trace(cov) / cov.shape[0])
            cov = (1.0 - float(shrinkage)) * cov + float(shrinkage) * target

    cov = np.atleast_2d(cov)
    cov = cov + np.eye(cov.shape[0]) * ridge * (np.trace(cov) / cov.shape[0])
    delta = te - tr.mean(axis=0)
    # Cholesky solve rather than an explicit inverse: same answer, better conditioned.
    factor = np.linalg.cholesky(cov)
    solved = np.linalg.solve(factor, delta.T)
    return np.sum(solved**2, axis=0)


def score_mahalanobis(train: np.ndarray, test: np.ndarray, seed: int) -> np.ndarray:
    """The candidate under test: StandardScaler + Ledoit-Wolf + a scale-free ridge floor (G1)."""
    del seed
    return _mahalanobis_sq(train, test, shrinkage="ledoit_wolf", ridge=1e-6)


def score_mahalanobis_ridge(train: np.ndarray, test: np.ndarray, seed: int) -> np.ndarray:
    """Same, with no Ledoit-Wolf: the fallback if data-estimated shrinkage over-regularises (G1)."""
    del seed
    return _mahalanobis_sq(train, test, shrinkage=0.0, ridge=1e-6)


ESTIMATORS = {
    "iforest": score_iforest,
    "pca_recon": score_pca_recon,
    "mlp_ae": score_mlp_autoencoder,
    "mahalanobis": score_mahalanobis,
    "maha_ridge": score_mahalanobis_ridge,
}


# --------------------------------------------------------------------------------------
# Axis 4: an honest event-level metric
# --------------------------------------------------------------------------------------


def event_recall_at_budget(labels: np.ndarray, scores: np.ndarray, budget_frac: float = 0.01) -> tuple[float, int]:
    """Fraction of true anomaly *ranges* containing at least one alerted row, within an alert budget.

    Why this is not point adjustment: point adjustment rewrites every point of a detected range as a
    true positive, which inflates precision and is why a random scorer reaches state of the art under
    it. Here precision is never touched — the alert budget is fixed at *budget_frac* of all rows, and
    only recall is counted per event. A detector cannot game it by firing everywhere, because the
    budget caps how much it may fire.

    Returns ``(event_recall, n_events)``.
    """
    flags = np.zeros(len(labels), dtype=bool)
    budget = max(1, int(len(labels) * budget_frac))
    flags[np.argsort(scores)[::-1][:budget]] = True

    # Contiguous runs of label == 1 are events.
    padded = np.concatenate([[0], (labels > 0).astype(int), [0]])
    edges = np.diff(padded)
    starts = np.flatnonzero(edges == 1)
    ends = np.flatnonzero(edges == -1)
    if len(starts) == 0:
        return float("nan"), 0
    detected = sum(1 for s, e in zip(starts, ends, strict=False) if flags[s:e].any())
    return detected / len(starts), len(starts)


# --------------------------------------------------------------------------------------
# Axis 3: scope, and the driver
# --------------------------------------------------------------------------------------


def contaminate(
    train: dict[str, np.ndarray],
    test: dict[str, np.ndarray],
    labels: dict[str, np.ndarray],
    rate: float,
    seed: int = 0,
) -> dict[str, np.ndarray]:
    """Return a copy of *train* with anomalous rows mixed in, at approximately *rate*.

    Without this, the whole exercise measures the wrong thing. SMD ships a **clean** train split, but
    DQX fits on ``sample_df`` -- a random sample of the user's table, anomalies included. Sample mean and
    covariance are non-robust, so a few extreme rows inflate the covariance *along the anomaly
    direction*, which is precisely the direction that must stay tight. That is masking, and it is the
    difference between a benchmark number and a number a user will see.

    IsolationForest resists this (it has ``contamination`` and subsampling); a moment-based estimator
    does not. Anomalous rows are drawn from each entity's own test split so they are realistic
    anomalies for that machine rather than synthetic noise.
    """
    rng = np.random.default_rng(seed)
    out = {}
    for entity, rows in train.items():
        anomalous = test[entity][labels[entity] > 0]
        n_inject = int(len(rows) * rate)
        if len(anomalous) == 0 or n_inject == 0:
            out[entity] = rows
            continue
        picks = rng.choice(len(anomalous), size=min(n_inject, len(anomalous)), replace=n_inject > len(anomalous))
        out[entity] = np.vstack([rows, anomalous[picks]])
    return out


def run_cell(
    train: dict[str, np.ndarray],
    test: dict[str, np.ndarray],
    labels: dict[str, np.ndarray],
    featuriser: str,
    estimator: str,
    per_entity: bool,
    seed: int,
) -> dict:
    """One configuration: featurise per entity, then fit either one model or one per entity."""
    started = time.perf_counter()
    fx = FEATURISERS[featuriser]
    scorer = ESTIMATORS[estimator]

    tr_by_entity = {e: np.nan_to_num(fx(train[e]), nan=0.0, posinf=0.0, neginf=0.0) for e in SMD_ENTITIES}
    te_by_entity = {e: np.nan_to_num(fx(test[e]), nan=0.0, posinf=0.0, neginf=0.0) for e in SMD_ENTITIES}

    if per_entity:
        # One model per machine. Scores are z-normalised per entity against that entity's own *training*
        # score distribution, so the numbers are comparable across models and a single global threshold
        # applies -- this is what stops an all-normal entity from flagging its own quietest rows.
        score_parts = []
        for entity in SMD_ENTITIES:
            tr, te = tr_by_entity[entity], te_by_entity[entity]
            raw_test = scorer(tr, te, seed)
            raw_train = scorer(tr, tr, seed)
            centre, spread = float(np.mean(raw_train)), float(np.std(raw_train)) or 1.0
            score_parts.append((raw_test - centre) / spread)
        scores = np.concatenate(score_parts)
        n_models = len(SMD_ENTITIES)
    else:
        tr_all = np.vstack([tr_by_entity[e] for e in SMD_ENTITIES])
        te_all = np.vstack([te_by_entity[e] for e in SMD_ENTITIES])
        scores = scorer(tr_all, te_all, seed)
        n_models = 1

    y = np.concatenate([labels[e] for e in SMD_ENTITIES])
    recall, n_events = event_recall_at_budget(y, scores)
    return {
        "pr_auc": pr_auc(y, scores),
        "roc_auc": roc_auc(y, scores),
        "precision_at_n": precision_at_n(y, scores),
        "event_recall_at_1pct": recall,
        "n_events": n_events,
        "n_features": int(next(iter(tr_by_entity.values())).shape[1]),
        "n_models": n_models,
        "seconds": round(time.perf_counter() - started, 1),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=2)
    parser.add_argument("--estimators", nargs="+", default=list(ESTIMATORS), choices=list(ESTIMATORS))
    parser.add_argument("--skip-per-entity", action="store_true", help="pooled scope only (faster)")
    parser.add_argument(
        "--contaminate",
        type=float,
        default=0.0,
        help="Mix this fraction of anomalous rows into the TRAIN split (G3: masking under contamination)",
    )
    args = parser.parse_args()

    print("Loading SMD (cached)...", flush=True)
    train, test, labels = load_smd_split()
    if args.contaminate:
        train = contaminate(train, test, labels, args.contaminate)
        print(f"  TRAIN CONTAMINATED at {args.contaminate:.1%} (G3: does the estimator survive masking?)")
    y = np.concatenate([labels[e] for e in SMD_ENTITIES])
    _, n_events = event_recall_at_budget(y, np.random.default_rng(0).random(len(y)))
    print(f"  28 entities | {len(y):,} test rows | {y.mean():.2%} anomalous points | {n_events} events")
    print("  protocol: fit on SMD train split, score test split (chronological), no point adjustment")
    print(f"  event_recall_at_1pct = of {n_events} events, share with >=1 row in a 1%-of-rows alert budget\n")

    scopes = [False] if args.skip_per_entity else [False, True]
    header = f"{'featuriser':11s} {'estimator':12s} {'scope':11s} {'PR-AUC':>8s} {'ROC':>7s} {'P@n':>7s} {'EvRec@1%':>9s} {'feat':>5s} {'s':>5s}"
    print(header)
    print("-" * len(header))

    results: list[dict] = []
    for featuriser in FEATURISERS:
        for estimator in args.estimators:
            for per_entity in scopes:
                per_seed = [
                    run_cell(train, test, labels, featuriser, estimator, per_entity, seed) for seed in range(args.seeds)
                ]
                agg = {
                    "featuriser": featuriser,
                    "estimator": estimator,
                    "scope": "per_entity" if per_entity else "pooled",
                    **{
                        k: float(np.mean([m[k] for m in per_seed]))
                        for k in ("pr_auc", "roc_auc", "precision_at_n", "event_recall_at_1pct")
                    },
                    "n_features": per_seed[0]["n_features"],
                    "n_models": per_seed[0]["n_models"],
                    "seconds": float(np.mean([m["seconds"] for m in per_seed])),
                }
                results.append(agg)
                print(
                    f"{featuriser:11s} {estimator:12s} {agg['scope']:11s} "
                    f"{agg['pr_auc']:8.4f} {agg['roc_auc']:7.4f} {agg['precision_at_n']:7.4f} "
                    f"{agg['event_recall_at_1pct']:9.3f} {agg['n_features']:5d} {agg['seconds']:5.0f}",
                    flush=True,
                )

    shipped = next(
        (r for r in results if r["featuriser"] == "raw" and r["estimator"] == "iforest" and r["scope"] == "pooled"),
        None,
    )
    best_pr = max(results, key=lambda r: r["pr_auc"])
    best_ev = max(results, key=lambda r: r["event_recall_at_1pct"])

    print("\n--- reading ---")
    if shipped:
        print(f"shipped today       : PR-AUC {shipped['pr_auc']:.4f}   EvRec@1% {shipped['event_recall_at_1pct']:.3f}")
    print(
        f"best PR-AUC         : {best_pr['pr_auc']:.4f}  "
        f"({best_pr['featuriser']}/{best_pr['estimator']}/{best_pr['scope']}, {best_pr['n_features']} feat)"
    )
    print(
        f"best event recall   : {best_ev['event_recall_at_1pct']:.3f}  "
        f"({best_ev['featuriser']}/{best_ev['estimator']}/{best_ev['scope']})"
    )
    if shipped and shipped["pr_auc"]:
        print(f"headroom on PR-AUC  : {best_pr['pr_auc'] / shipped['pr_auc']:.2f}x over what ships today")
    print("\nThe best row here IS the ballpark: published SMD figures use point adjustment and are not")
    print("a valid target. '80% of the ballpark' means 80% of the best honest configuration above.")

    suffix = f"-contaminated-{args.contaminate:g}" if args.contaminate else ""
    out = f"benchmarks/anomaly_conditioning/results/smd-bakeoff{suffix}.json"
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(
            {"window": WINDOW, "seeds": args.seeds, "contaminate": args.contaminate, "results": results},
            handle,
            indent=2,
        )
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
