"""Run the conditioning experiment and write a dated results page.

    python benchmarks/anomaly_conditioning/run_experiment.py --seeds 5
    python benchmarks/anomaly_conditioning/run_experiment.py --seeds 5 --datasets synthetic smd nslkdd

Synthetic runs need no network and no Databricks workspace. Real datasets are downloaded at run
time and cached; see ``datasets/__init__.py`` for why none of them are vendored.

The decision rules below were fixed *before* the first run, and are printed alongside the result so
a reader can check the conclusion against the criterion rather than against a narrative written
afterwards.
"""

import argparse
import datetime as dt
import json
import pathlib
import subprocess
import sys
from dataclasses import dataclass

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from conditioning import CONFIGS, eta_squared  # noqa: E402
from datasets import synthetic  # noqa: E402
from metrics import (  # noqa: E402
    CellMetrics,
    macro_average,
    per_group_pr_auc,
    pr_auc,
    precision_at_n,
    roc_auc,
    spearman_with_bootstrap_ci,
    trivial_baselines,
    wilcoxon_paired,
    worst_group_false_positive_rate,
)

RESULTS_DIR = pathlib.Path(__file__).resolve().parent / "results"

# Decision rules, fixed in advance. See the plan for #1484.
#
# The gate being tested is the removed MIN_GROUP_HETEROGENEITY = 0.10: conditioning used to be
# skipped when eta-squared fell below it, on the theory that a grouping which explains little
# variance contributes noise. The counter-argument is that at low heterogeneity every group median
# approaches the global median, so the relative feature degenerates into a monotone transform of
# the raw metric -- a near-duplicate of an informative column, not noise. That is a prediction, and
# NO_GATE_FLOOR is where it gets tested.
LOW_ETA = 0.10
NO_GATE_FLOOR = -0.01  # relative never materially worse than pooled below LOW_ETA
GATE_NEEDED_DELTA = -0.02  # a real cost, concentrated below LOW_ETA


@dataclass
class Cell:
    """One measured configuration."""

    dataset: str
    grouping: str
    config: str
    seed: int
    mechanism: str
    level_spread: float
    metrics: CellMetrics

    def as_dict(self) -> dict:
        record = {
            "dataset": self.dataset,
            "grouping": self.grouping,
            "config": self.config,
            "seed": self.seed,
            "mechanism": self.mechanism,
            "level_spread": self.level_spread,
        }
        record.update(self.metrics.as_dict())
        return record


def measure(values: np.ndarray, labels: np.ndarray, groups: np.ndarray, config: str, seed: int) -> CellMetrics:
    """Fit one configuration and compute every metric for it."""
    result = CONFIGS[config](values, groups, seed)
    scores = result.scores
    return CellMetrics(
        pr_auc=pr_auc(labels, scores),
        roc_auc=roc_auc(labels, scores),
        precision_at_n=precision_at_n(labels, scores),
        macro_pr_auc=macro_average(per_group_pr_auc(labels, scores, groups)),
        worst_group_fpr=worst_group_false_positive_rate(labels, scores, groups),
        n_models=result.n_models,
        seconds=result.seconds,
        eta_squared=eta_squared(values, groups),
    )


def run_synthetic(seeds: int) -> list[Cell]:
    """The two-factor sweep: spread x mechanism x config x seed."""
    cells: list[Cell] = []
    points = synthetic.sweep_points()
    total = len(points) * len(CONFIGS) * seeds
    done = 0
    for level_spread, mechanism in points:
        for seed in range(seeds):
            values, labels, groups = synthetic.generate(seed=seed, level_spread=level_spread, mechanism=mechanism)
            for config in CONFIGS:
                cells.append(
                    Cell(
                        dataset="synthetic",
                        grouping=f"spread={level_spread:.3f}",
                        config=config,
                        seed=seed,
                        mechanism=mechanism,
                        level_spread=level_spread,
                        metrics=measure(values, labels, groups, config, seed),
                    )
                )
                done += 1
                print(f"\r  {done}/{total} cells", end="", flush=True)
    print()
    return cells


def run_real(real_module, name: str, seeds: int) -> list[Cell]:
    """Every candidate grouping of a real dataset, across configs and seeds.

    *mechanism* is recorded as ``"real"``: which mechanism produced a real anomaly is not knowable,
    which is exactly why the synthetic sweep carries the mechanism contrast and these datasets
    check that its conclusion survives.
    """
    cells: list[Cell] = []
    for grouping, values, labels, groups in real_module.load_groupings(name):
        print(f"  {grouping}: {values.shape[0]} rows, {len(np.unique(groups))} groups")
        for seed in range(seeds):
            for config in CONFIGS:
                cells.append(
                    Cell(
                        dataset=name,
                        grouping=grouping,
                        config=config,
                        seed=seed,
                        mechanism="real",
                        level_spread=float("nan"),
                        metrics=measure(values, labels, groups, config, seed),
                    )
                )
        print(f"    done ({seeds} seeds x {len(CONFIGS)} configs)")
    return cells


def run_tabular(seeds: int, names: list[str] | None = None) -> tuple[list[Cell], list[dict]]:
    """The plain-tabular regime: no grouping, so only the pooled configuration is meaningful.

    Returns ``(cells, baselines)``. This does not compare configurations -- there is nothing to
    condition on -- it characterises absolute quality on the benchmarks the field actually reports,
    and pins each result against its own base rate and its own trivial baselines. Reported per
    dataset and never pooled into one headline: PR-AUC is not comparable across base rates.
    """
    from datasets import tabular  # noqa: PLC0415 - downloads data, so only imported when asked for

    cells: list[Cell] = []
    baselines: list[dict] = []
    for name, values, labels in tabular.iter_datasets(names):
        base_rate = float(labels.mean())
        print(f"  {name}: {values.shape[0]} rows, {values.shape[1]} features, base rate {base_rate:.4%}")
        groups = np.zeros(len(values), dtype=str)  # one group: pooled is the only configuration
        for seed in range(seeds):
            cells.append(
                Cell(
                    dataset=f"tabular:{name}",
                    grouping="none",
                    config="pooled",
                    seed=seed,
                    mechanism="tabular",
                    level_spread=float("nan"),
                    metrics=measure(values, labels, groups, "pooled", seed),
                )
            )
        trivial = trivial_baselines(values, labels)
        baselines.append(
            {
                "dataset": name,
                "n_rows": int(values.shape[0]),
                "n_features": int(values.shape[1]),
                "base_rate": base_rate,
                "dqx_pr_auc": float(np.median([c.metrics.pr_auc for c in cells if c.dataset.endswith(name)])),
                **trivial,
            }
        )
    return cells, baselines


def tabular_table(baselines: list[dict]) -> list[str]:
    """Per-dataset absolute quality against the floor, with the base rate alongside."""
    if not baselines:
        return []
    lines = [
        "## Plain tabular benchmarks (no grouping)",
        "",
        "No grouping exists in these datasets, so conditioning is not applicable and only the pooled",
        "configuration runs. This characterises the mechanism on the benchmarks the field reports, and",
        "is the regression check that adding conditioning did not disturb the ungrouped path.",
        "",
        "PR-AUC is **not comparable across rows** — it moves with the base rate — so each dataset is",
        "read against its own random floor. `lift` is DQX PR-AUC divided by the random floor.",
        "",
        "| dataset | rows | features | base rate | DQX PR-AUC | random | max-abs-z | lift vs random | beats max-abs-z |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in baselines:
        lift = row["dqx_pr_auc"] / row["random"] if row["random"] else float("nan")
        beats = "yes" if row["dqx_pr_auc"] > row["max_abs_z"] else "**no**"
        lines.append(
            f"| {row['dataset']} | {row['n_rows']} | {row['n_features']} | {row['base_rate']:.2%} | "
            f"{row['dqx_pr_auc']:.4f} | {row['random']:.4f} | {row['max_abs_z']:.4f} | "
            f"{lift:.1f}x | {beats} |"
        )
    lines.append("")
    return lines


def paired_deltas(cells: list[Cell], left: str, right: str, metric: str = "pr_auc") -> list[dict]:
    """``metric(left) - metric(right)`` for every (dataset, grouping, seed) the two share.

    Paired rather than averaged: the seed drives both the data draw and the forest, and dominates
    the variance between cells.
    """
    # The key must identify a cell uniquely. Mechanism belongs in it: the synthetic sweep runs both
    # mechanisms at the same spread and seed, so a key without it collapses each pair of cells onto
    # one entry and silently discards half the grid — which is what it did, until the absent
    # "global" row in the summary table gave it away.
    index: dict[tuple[str, str, str, int], dict[str, Cell]] = {}
    for cell in cells:
        index.setdefault((cell.dataset, cell.grouping, cell.mechanism, cell.seed), {})[cell.config] = cell

    out = []
    for (dataset, grouping, _mechanism, seed), by_config in sorted(index.items()):
        if left not in by_config or right not in by_config:
            continue
        left_cell, right_cell = by_config[left], by_config[right]
        left_value = getattr(left_cell.metrics, metric)
        right_value = getattr(right_cell.metrics, metric)
        out.append(
            {
                "dataset": dataset,
                "grouping": grouping,
                "seed": seed,
                "mechanism": left_cell.mechanism,
                "eta_squared": left_cell.metrics.eta_squared,
                "delta": left_value - right_value,
                "left": left_value,
                "right": right_value,
            }
        )
    return out


def regress_delta_on_eta(deltas: list[dict]) -> dict[str, float]:
    """Least squares of ``delta ~ 1 + eta_squared + is_contextual``.

    The gate's premise needs eta-squared to carry signal *after* controlling for the anomaly
    mechanism. If the eta coefficient collapses once the mechanism dummy is present, the apparent
    correlation was the mechanism all along.

    Synthetic rows only. On real data the mechanism behind each anomaly is unknown, so folding those
    rows in would silently code them as "global" and bias the very coefficient being tested.
    """
    rows = [
        d
        for d in deltas
        if np.isfinite(d["delta"]) and np.isfinite(d["eta_squared"]) and d["mechanism"] in synthetic.MECHANISMS
    ]
    if len(rows) < 4:
        return {}
    design = np.column_stack(
        [
            np.ones(len(rows)),
            np.array([r["eta_squared"] for r in rows]),
            np.array([1.0 if r["mechanism"] == "contextual" else 0.0 for r in rows]),
        ]
    )
    target = np.array([r["delta"] for r in rows])
    coeffs, *_ = np.linalg.lstsq(design, target, rcond=None)
    residual = target - design @ coeffs
    ss_res = float((residual**2).sum())
    ss_tot = float(((target - target.mean()) ** 2).sum())
    return {
        "intercept": float(coeffs[0]),
        "eta_squared": float(coeffs[1]),
        "is_contextual": float(coeffs[2]),
        "r_squared": 1.0 - ss_res / ss_tot if ss_tot else float("nan"),
        "n": float(len(rows)),
    }


def decide(deltas: list[dict]) -> tuple[str, str, dict[str, float]]:
    """Apply the pre-registered rules, and a variance-robust companion.

    Returns ``(pre_registered_verdict, robust_verdict, evidence)``.

    The pre-registered rule is a **minimum over single cells**, which makes it maximally sensitive
    to the variance of the estimator it is applied to — and Isolation Forest on a 3-group,
    68k-row dataset is high variance. It fired on the first run against real data, on one seed of
    ``nslkdd/protocol_type`` whose other seeds were **+0.1003** and **+0.0989**.

    That is a mis-specification, not a finding: the rule's stated intent was that conditioning is
    never *materially worse* on homogeneous data, which is a claim about the distribution rather
    than about the unluckiest single draw. So the per-(dataset, grouping) **median** delta is
    reported beside it, and both verdicts are published. The pre-registered rule is deliberately
    left in place rather than replaced, so that a reader can see the criterion that was set in
    advance, the answer it gave, and why a second statistic was added.
    """
    low = [d for d in deltas if np.isfinite(d["delta"]) and d["eta_squared"] < LOW_ETA]
    if not low:
        return "inconclusive: nothing fell below the eta-squared threshold", "inconclusive", {}

    worst_cell = min(d["delta"] for d in low)
    harmful = [d for d in low if d["delta"] <= GATE_NEEDED_DELTA]

    # Median per (dataset, grouping): the distributional form of the same question.
    by_grouping: dict[tuple[str, str], list[float]] = {}
    for d in low:
        by_grouping.setdefault((str(d["dataset"]), str(d["grouping"])), []).append(float(d["delta"]))
    medians = {key: float(np.median(values)) for key, values in by_grouping.items()}
    worst_median = min(medians.values())
    harmful_groupings = [key for key, value in medians.items() if value <= GATE_NEEDED_DELTA]

    evidence = {
        "n_low_eta_cells": float(len(low)),
        "n_low_eta_groupings": float(len(medians)),
        "worst_delta_single_cell": worst_cell,
        "n_harmful_cells": float(len(harmful)),
        "worst_median_delta_per_grouping": worst_median,
        "n_harmful_groupings": float(len(harmful_groupings)),
        "median_delta_below_threshold": float(np.median([d["delta"] for d in low])),
    }

    def verdict(worst: float, any_harmful: bool) -> str:
        if worst > NO_GATE_FLOOR:
            return "no gate needed"
        if any_harmful:
            return "gate needed; refit the threshold from this sweep"
        return "borderline: below the floor but above the harm threshold"

    return verdict(worst_cell, bool(harmful)), verdict(worst_median, bool(harmful_groupings)), evidence


def git_sha() -> str:
    """Short SHA of the tree these numbers came from. Results without it are unreproducible."""
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=pathlib.Path(__file__).resolve().parent,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def summarise(deltas: list[dict], label: str) -> list[str]:
    """Median, IQR and a paired test for one comparison, split by anomaly mechanism."""
    lines = [f"### {label}", ""]
    lines.append("| mechanism | n | median delta | IQR | Wilcoxon p |")
    lines.append("|---|---|---|---|---|")
    present = sorted({str(d["mechanism"]) for d in deltas})
    for mechanism in [*present, "*all*"]:
        subset = deltas if mechanism == "*all*" else [d for d in deltas if d["mechanism"] == mechanism]
        values = [d["delta"] for d in subset if np.isfinite(d["delta"])]
        if not values:
            continue
        q25, q75 = np.percentile(values, [25, 75])
        _, p_value = wilcoxon_paired(values)
        p_text = "n/a" if not np.isfinite(p_value) else f"{p_value:.2e}"
        lines.append(
            f"| {mechanism} | {len(values)} | {np.median(values):+.4f} | " f"{q25:+.4f} to {q75:+.4f} | {p_text} |"
        )
    lines.append("")
    return lines


def low_eta_table(deltas: list[dict]) -> list[str]:
    """Every low-eta grouping, seed by seed.

    The table the two verdicts have to be read against: a grouping whose seeds straddle zero is
    telling you about variance, and one whose seeds agree is telling you about the mechanism.
    """
    low = [d for d in deltas if np.isfinite(d["delta"]) and d["eta_squared"] < LOW_ETA]
    if not low:
        return []

    by_grouping: dict[tuple[str, str], list[dict]] = {}
    for d in low:
        by_grouping.setdefault((str(d["dataset"]), str(d["grouping"])), []).append(d)

    lines = [
        f"### Every grouping below eta-squared {LOW_ETA}, seed by seed",
        "",
        "| dataset | grouping | eta-squared | median delta | min | max | seeds agree? |",
        "|---|---|---|---|---|---|---|",
    ]
    for (dataset, grouping), group in sorted(by_grouping.items()):
        values = [g["delta"] for g in group]
        agree = "yes, all negative" if max(values) < 0 else ("yes, none negative" if min(values) >= 0 else "**no**")
        lines.append(
            f"| {dataset} | {grouping} | {group[0]['eta_squared']:.4f} | {np.median(values):+.4f} | "
            f"{min(values):+.4f} | {max(values):+.4f} | {agree} |"
        )
    lines.append("")
    return lines


def write_report(
    cells: list[Cell], seeds: int, datasets: list[str], tabular_baselines: list[dict] | None = None
) -> pathlib.Path:
    """Write the dated markdown and JSON results, and return the markdown path."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.date.today().isoformat()
    sha = git_sha()
    stem = f"{stamp}-{sha}"

    rel_vs_pooled = paired_deltas(cells, "relative", "pooled")
    rel_vs_per_group = paired_deltas(cells, "relative", "per_group")
    pre_registered_verdict, robust_verdict, evidence = decide(rel_vs_pooled)
    rho, lo, hi = spearman_with_bootstrap_ci(
        [d["eta_squared"] for d in rel_vs_pooled], [d["delta"] for d in rel_vs_pooled]
    )
    regression = regress_delta_on_eta(rel_vs_pooled)

    lines = [
        f"# Anomaly conditioning experiment — {stamp}",
        "",
        f"DQX `{sha}` · datasets: {', '.join(datasets)} · seeds per cell: {seeds} · " f"cells: {len(cells)}",
        "",
        "PR-AUC is the primary metric. No point-adjusted F1 appears anywhere; see `metrics.py`.",
        "DQX scores rows independently, so figures on time-series data are not comparable with",
        "published sequence-model results — that is a different task, not a worse implementation.",
        "",
        "## Does removing the heterogeneity gate cost anything?",
        "",
        f"Rules fixed before running: **no gate** if the worst delta below eta-squared "
        f"{LOW_ETA} exceeds {NO_GATE_FLOOR:+.2f}; **gate needed** if any such cell reaches "
        f"{GATE_NEEDED_DELTA:+.2f}.",
        "",
        f"- **Pre-registered rule (worst single cell): {pre_registered_verdict}**",
        f"- **Variance-robust companion (worst per-grouping median): {robust_verdict}**",
        "",
        "The pre-registered rule takes a minimum over individual cells, so it is maximally "
        "sensitive to estimator variance. It is reported unchanged, alongside the median form of "
        "the same question, so the criterion set in advance and the answer it gave are both "
        "visible. Where the two disagree, the per-grouping deltas below show why.",
        "",
    ]
    if evidence:
        lines.append("| statistic | value |")
        lines.append("|---|---|")
        for key, value in evidence.items():
            lines.append(f"| {key} | {value:+.4f} |" if "delta" in key else f"| {key} | {value:.0f} |")
        lines.append("")

    lines += [
        "### Does eta-squared predict the benefit at all?",
        "",
        f"Spearman rho(eta-squared, delta) = **{rho:+.3f}** (bootstrap 95% CI {lo:+.3f} to {hi:+.3f}).",
        "",
    ]
    if regression:
        lines += [
            "Least squares `delta ~ 1 + eta_squared + is_contextual`, which asks whether "
            "eta-squared still carries signal once the anomaly mechanism is controlled for:",
            "",
            "| term | coefficient |",
            "|---|---|",
            f"| intercept | {regression['intercept']:+.4f} |",
            f"| eta_squared | {regression['eta_squared']:+.4f} |",
            f"| is_contextual | {regression['is_contextual']:+.4f} |",
            f"| R-squared | {regression['r_squared']:.3f} (n={regression['n']:.0f}) |",
            "",
        ]

    lines += tabular_table(tabular_baselines or [])
    lines += low_eta_table(rel_vs_pooled)
    lines += ["## Paired comparisons", ""]
    lines += summarise(rel_vs_pooled, "baseline-relative minus pooled (PR-AUC)")
    lines += summarise(rel_vs_per_group, "baseline-relative minus per-group (PR-AUC)")

    lines += ["## Cost", "", "| config | median models | median seconds |", "|---|---|---|"]
    for config in CONFIGS:
        subset = [c for c in cells if c.config == config]
        if subset:
            lines.append(
                f"| {config} | {np.median([c.metrics.n_models for c in subset]):.0f} | "
                f"{np.median([c.metrics.seconds for c in subset]):.2f} |"
            )
    lines.append("")

    md_path = RESULTS_DIR / f"{stem}.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (RESULTS_DIR / f"{stem}.json").write_text(
        json.dumps(
            {
                "generated": stamp,
                "git_sha": sha,
                "seeds": seeds,
                "datasets": datasets,
                "verdict_pre_registered": pre_registered_verdict,
                "verdict_robust": robust_verdict,
                "evidence": evidence,
                "spearman": {"rho": rho, "ci_low": lo, "ci_high": hi},
                "regression": regression,
                # Included so emit_docs.py can refresh the published tables without re-running the
                # sweep, and so a results file is self-contained.
                "tabular_baselines": tabular_baselines or [],
                "cells": [c.as_dict() for c in cells],
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return md_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=5, help="seeds per cell (paired across configs)")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["synthetic"],
        choices=["synthetic", "smd", "nslkdd", "tabular"],
        help="synthetic needs no network; the others download at run time",
    )
    args = parser.parse_args()

    cells: list[Cell] = []
    tabular_baselines: list[dict] = []
    if "tabular" in args.datasets:
        print("plain tabular benchmarks:")
        tab_cells, tabular_baselines = run_tabular(args.seeds)
        cells += tab_cells
    if "synthetic" in args.datasets:
        print("synthetic sweep:")
        cells += run_synthetic(args.seeds)

    real_names = [n for n in ("smd", "nslkdd") if n in args.datasets]
    if real_names:
        # Imported lazily: it downloads data, so a synthetic-only run stays offline.
        from datasets import real  # noqa: PLC0415

        for name in real_names:
            print(f"{name}:")
            cells += run_real(real, name, args.seeds)

    if not cells:
        print("no cells measured", file=sys.stderr)
        return 1

    path = write_report(cells, args.seeds, args.datasets, tabular_baselines)
    print(f"\nwrote {path}")
    print(path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
