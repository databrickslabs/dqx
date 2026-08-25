"""Refresh the generated tables in the user-facing quality page from a results JSON.

    python benchmarks/anomaly_conditioning/emit_docs.py results/2026-08-25-abc1234.json

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
}

PAGE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "docs"
    / "dqx"
    / "docs"
    / "reference"
    / "anomaly_detection_quality.mdx"
)

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


def replace_region(text: str, name: str, body: str) -> str:
    start, end = MARKERS[name]
    if start not in text or end not in text:
        raise SystemExit(f"marker pair for {name!r} missing from {PAGE.name}; add {start} / {end}")
    head = text[: text.index(start) + len(start)]
    tail = text[text.index(end) :]
    return f"{head}\n{body}\n{tail}"


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit(f"usage: {pathlib.Path(sys.argv[0]).name} <results.json>")
    data = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    cells = data["cells"]
    baselines = data.get("tabular_baselines") or []

    text = PAGE.read_text(encoding="utf-8")
    text = replace_region(text, "conditioning", conditioning_table(cells))
    text = replace_region(text, "per_group", per_group_table(cells))
    text = replace_region(text, "cost", cost_table(cells))
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
