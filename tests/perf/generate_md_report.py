import json
from pathlib import Path


baseline_path = Path(__file__).parent / ".benchmarks" / "baseline.json"
report_path = Path(__file__).parent.parent.parent / "docs" / "dqx" / "docs" / "reference" / "benchmarks.mdx"

data = json.loads(baseline_path.read_text())

lines = []
lines.append("---\n")
lines.append("title: Benchmarks\n")
lines.append("sidebar_position: 13\n")
lines.append("---\n")
lines.append("# Performance Benchmarks Report\n")
lines.append(
    "| Test | Mean (s) | Median (s) | Min (s) | Max (s) | Stddev (s) | iqr (s) | q1 (s) | q3 (s) | Rounds | iqr outliers | stddev outliers | Ops/s |"
)
lines.append(
    "|------|----------|------------|---------|---------|------------|---------|--------|--------|--------|--------------|-----------------|-------|"
)

for bench in data["benchmarks"]:
    stats = bench["stats"]
    lines.append(
        f"| {bench['name']} "
        f"| {stats['mean']:.6f} "
        f"| {stats['median']:.6f} "
        f"| {stats['min']:.6f} "
        f"| {stats['max']:.6f} "
        f"| {stats['stddev']:.6f} "
        f"| {stats['iqr']:.6f} "
        f"| {stats['q1']:.6f} "
        f"| {stats['q3']:.6f} "
        f"| {stats['rounds']} "
        f"| {stats['iqr_outliers']} "
        f"| {stats['stddev_outliers']} "
        f"| {stats['ops']:.2f} |"
    )

lines.append("\nThe benchmarks are created using Databricks Serverless cluster.")
lines.append("\nSee the tests implementation [here](https://github.com/databrickslabs/dqx/blob/v0.9.1/tests/perf/).\n")

# overwrite the report
report_path.write_text("\n".join(lines))
print(f"REPORT_PATH={report_path.resolve()}")
