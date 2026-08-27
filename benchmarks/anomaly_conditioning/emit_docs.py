"""Refresh the generated tables in the user-facing quality page from a results JSON.

    python benchmarks/anomaly_conditioning/emit_docs.py results/2026-08-25-abc1234.json \
        [--bakeoff results/smd-bakeoff.json results/smd-bakeoff-contaminated-0.033.json]

The conditioning sweep and the estimator bake-off are separate runs over different datasets, so the
profile table is fed from its own file(s) rather than from the conditioning results. Pass the clean
run first and the contaminated run second: contaminated training is what DQX actually does (it fits a
random sample of the user's table, anomalies included), so publishing only the clean number would
overstate what a user gets.

Only the regions between marker comments are replaced, so the hand-written prose around them --
which is most of the page, and the part that tells a reader what to do about a number -- survives
regeneration. That is the difference from ``docs/dqx/docs/reference/benchmarks.mdx``, which
``tests/perf/generate_md_report.py`` rewrites wholesale and where narrative therefore cannot live.

Deliberately not routed through pytest-benchmark's ``extra_info``. The nightly merges baseline.json
with keep-old-on-conflict semantics, so a benchmark that already exists keeps its previous entry and
its extra_info is frozen at first observation. Quality published that way would never refresh.
"""

import json
import pathlib
import sys

MARKERS = {
    "conditioning": ("<!-- generated:conditioning:start -->", "<!-- generated:conditioning:end -->"),
    "per_group": ("<!-- generated:per-group:start -->", "<!-- generated:per-group:end -->"),
    "tabular": ("<!-- generated:tabular:start -->", "<!-- generated:tabular:end -->"),
    "cost": ("<!-- generated:cost:start -->", "<!-- generated:cost:end -->"),
    "profile": ("<!-- generated:profile:start -->", "<!-- generated:profile:end -->"),
}

# The estimator each profile ships. `maha_ridge` is the bake-off name for the configuration in
# ``anomaly/timeseries_detector.py``: standardised internally, with a small ridge floor on the
# covariance. Kept as a mapping rather than inlined so the table cannot silently start reporting a
# configuration DQX does not ship.
PROFILE_ESTIMATORS = {"tabular": "iforest", "timeseries": "maha_ridge"}

_DOCS = pathlib.Path(__file__).resolve().parents[2] / "docs" / "dqx" / "docs"
PAGE = _DOCS / "reference" / "anomaly_detection_quality.mdx"
# The user guide carries the same profile table, in a shorter form. Generated rather than copied: a
# hand-typed duplicate of a measured number drifts the first time the measurement is refreshed.
GUIDE = _DOCS / "guide" / "row_anomaly_detection" / "index.mdx"

MECHANISM_LABELS = {
    "contextual": "**contextual** — ordinary for the table, wrong for their group",
    "global": "**globally extreme** — unusual against the whole table",
    "real": "mixed or unknown (real-world datasets)",
}
ADVICE = {
    "contextual": "use `baseline_by`",
    "global": "costs nothing; leave it on",
    "real": "leave it on",
}


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def _paired_medians(cells: list[dict], left: str, right: str) -> dict[str, float]:
    """Median per-mechanism delta between two configurations, paired on the cell they share."""
    index: dict[tuple, dict[str, dict]] = {}
    for cell in cells:
        key = (cell["dataset"], cell["grouping"], cell["mechanism"], cell["seed"])
        index.setdefault(key, {})[cell["config"]] = cell

    by_mechanism: dict[str, list[float]] = {}
    for (_dataset, _grouping, mechanism, _seed), configs in index.items():
        if left not in configs or right not in configs:
            continue
        delta = configs[left]["pr_auc"] - configs[right]["pr_auc"]
        if delta == delta:  # skip NaN
            by_mechanism.setdefault(mechanism, []).append(delta)
    return {m: _median(v) for m, v in by_mechanism.items()}


def conditioning_table(cells: list[dict]) -> str:
    medians = _paired_medians(cells, "relative", "pooled")
    rows = [
        "| your anomalies are... | median ΔPR-AUC with `baseline_by` | what to do |",
        "|---|---|---|",
    ]
    for mechanism in ("contextual", "global", "real"):
        if mechanism not in medians:
            continue
        value = medians[mechanism]
        emphasis = f"**{value:+.4f}**" if value > 0.01 else f"{value:+.4f}"
        rows.append(f"| {MECHANISM_LABELS[mechanism]} | {emphasis} | {ADVICE[mechanism]} |")
    return "\n".join(rows)


def per_group_table(cells: list[dict]) -> str:
    medians = _paired_medians(cells, "relative", "per_group")
    rows = ["| your anomalies are... | median ΔPR-AUC vs one-model-per-group |", "|---|---|"]
    labels = {"contextual": "contextual", "global": "globally extreme", "real": "mixed or unknown"}
    for mechanism in ("contextual", "global", "real"):
        if mechanism not in medians:
            continue
        value = medians[mechanism]
        emphasis = f"**{value:+.4f}**" if value > 0.01 else f"{value:+.4f}"
        rows.append(f"| {labels[mechanism]} | {emphasis} |")
    return "\n".join(rows)


def cost_table(cells: list[dict]) -> str:
    per_config: dict[str, tuple[list[int], list[float]]] = {}
    for cell in cells:
        models, seconds = per_config.setdefault(cell["config"], ([], []))
        models.append(cell["n_models"])
        seconds.append(cell["seconds"])

    rows = ["| approach | models trained | median fit time |", "|---|---|---|"]
    if "relative" in per_config:
        models, seconds = per_config["relative"]
        rows.append(f"| `baseline_by` | 1 | {_median(seconds):.2f}s |")
    if "per_group" in per_config:
        models, seconds = per_config["per_group"]
        rows.append(
            f"| `segment_by` | one per group ({int(_median([float(m) for m in models]))} in this sweep) "
            f"| {_median(seconds):.2f}s |"
        )
    return "\n".join(rows)


def tabular_table(baselines: list[dict]) -> str:
    rows = [
        "| dataset | rows | features | base rate | DQX PR-AUC | random | max-abs-z | lift vs random "
        "| beats max-abs-z |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    # Best first: a reader scanning for "is this any good" should meet the strong cases before the weak.
    for row in sorted(baselines, key=lambda r: -r["dqx_pr_auc"]):
        lift = row["dqx_pr_auc"] / row["random"] if row["random"] else float("nan")
        beats = "yes" if row["dqx_pr_auc"] > row["max_abs_z"] else "**no**"
        score = f"**{row['dqx_pr_auc']:.4f}**" if lift > 10 else f"{row['dqx_pr_auc']:.4f}"
        rows.append(
            f"| {row['dataset']} | {row['n_rows']} | {row['n_features']} | {row['base_rate']:.2%} | "
            f"{score} | {row['random']:.4f} | {row['max_abs_z']:.4f} | {lift:.1f}x | {beats} |"
        )
    return "\n".join(rows)


def profile_table(runs: list[tuple[str, list[dict]]]) -> str:
    """Incident coverage per profile, on raw features and pooled scope, across the runs given.

    Reports **event recall at a 1%-of-rows alert budget**, not PR-AUC, and not point-adjusted F1.
    SMD's 3,732 anomalous rows sit in 39 incidents of median length 6 and maximum length 1041, so
    point-wise PR-AUC is dominated by "did you find the one huge incident" -- the best-PR-AUC
    configuration in this sweep covers 2 of 39 incidents. Counting incidents inside a fixed budget
    keeps precision point-wise and answers the operational question instead. Point adjustment is
    excluded on purpose: Kim et al. (AAAI 2022) showed random scores reach state-of-the-art under it,
    which is why published SMD figures near 0.80 F1 are not a comparable target.
    """
    header = ["| `profile` | detector |"]
    divider = ["|---|---|"]
    for label, _ in runs:
        header.append(f" incidents surfaced ({label}) |")
        divider.append("---|")
    rows = ["".join(header), "".join(divider)]

    for profile, estimator in PROFILE_ESTIMATORS.items():
        detector = "Isolation Forest" if profile == "tabular" else "correlation-aware"
        cells = [f"| `\"{profile}\"` | {detector} |"]
        for _, results in runs:
            match = [
                r
                for r in results
                if r["estimator"] == estimator and r["featuriser"] == "raw" and r["scope"] == "pooled"
            ]
            if not match:
                cells.append(" n/a |")
                continue
            recall = match[0]["event_recall_at_1pct"]
            emphasis = f"**{recall:.0%}**" if profile == "timeseries" else f"{recall:.0%}"
            cells.append(f" {emphasis} |")
        rows.append("".join(cells))
    return "\n".join(rows)


def guide_profile_table(runs: list[tuple[str, list[dict]]]) -> str:
    """The same measurement, trimmed for the page where a user chooses.

    Shows only the contaminated-training column -- the number a user actually gets, since DQX fits a
    random sample of their table with anomalies still in it. The clean-split upper bound and the
    methodology stay on the reference page, where a reader has come to interrogate the numbers rather
    than to make a choice.
    """
    preferred = [r for label, r in runs if "contaminated" in label] or [runs[-1][1]]
    results = preferred[0]
    rows = ["| `profile` | Incidents surfaced |", "|---|---|"]
    for profile, estimator in PROFILE_ESTIMATORS.items():
        match = [
            r for r in results if r["estimator"] == estimator and r["featuriser"] == "raw" and r["scope"] == "pooled"
        ]
        recall = f"{match[0]['event_recall_at_1pct']:.0%}" if match else "n/a"
        emphasis = f"**{recall}**" if profile == "timeseries" else recall
        rows.append(f'| `"{profile}"` | {emphasis} |')
    return "\n".join(rows)


def replace_region(text: str, name: str, body: str, page: pathlib.Path = PAGE) -> str:
    start, end = MARKERS[name]
    if start not in text or end not in text:
        raise SystemExit(f"marker pair for {name!r} missing from {page.name}; add {start} / {end}")
    head = text[: text.index(start) + len(start)]
    tail = text[text.index(end) :]
    return f"{head}\n{body}\n{tail}"


def main() -> int:
    argv = sys.argv[1:]
    bakeoff_paths: list[str] = []
    if "--bakeoff" in argv:
        cut = argv.index("--bakeoff")
        bakeoff_paths = argv[cut + 1 :]
        argv = argv[:cut]
    if not argv:
        raise SystemExit(f"usage: {pathlib.Path(sys.argv[0]).name} <results.json> [--bakeoff <smd.json> ...]")
    data = json.loads(pathlib.Path(argv[0]).read_text(encoding="utf-8"))
    cells = data["cells"]
    baselines = data.get("tabular_baselines") or []

    text = PAGE.read_text(encoding="utf-8")
    text = replace_region(text, "conditioning", conditioning_table(cells))
    text = replace_region(text, "per_group", per_group_table(cells))
    text = replace_region(text, "cost", cost_table(cells))
    if bakeoff_paths:
        runs = []
        for raw_path in bakeoff_paths:
            bakeoff = json.loads(pathlib.Path(raw_path).read_text(encoding="utf-8"))
            # `contaminate` is null in a run that trained on data containing anomalies, and 0.0 in one
            # that trained on the clean split. Label from the file rather than from argument order, so a
            # mislabelled column cannot outlive a typo on the command line.
            contaminate = bakeoff.get("contaminate")
            label = "clean training split" if contaminate == 0.0 else "contaminated training"
            runs.append((label, bakeoff["results"]))
        text = replace_region(text, "profile", profile_table(runs))

        guide_text = GUIDE.read_text(encoding="utf-8")
        guide_text = replace_region(guide_text, "profile", guide_profile_table(runs), GUIDE)
        GUIDE.write_text(guide_text, encoding="utf-8")
        print(f"refreshed the profile table in {GUIDE}")
    if baselines:
        text = replace_region(text, "tabular", tabular_table(baselines))
    else:
        # Loud, not silent: a run without --datasets tabular leaves that table at whatever the last
        # run published, and a stale table that looks fresh is worse than an obvious gap.
        print(
            "warning: results file carries no tabular_baselines, so the plain-tabular table was left "
            "untouched. Re-run with '--datasets ... tabular' to refresh it.",
            file=sys.stderr,
        )
    PAGE.write_text(text, encoding="utf-8")
    print(f"refreshed generated tables in {PAGE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
