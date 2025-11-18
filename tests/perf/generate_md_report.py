"""
This script generates a markdown report from the benchmark results stored in
`tests/perf/.benchmarks/baseline.json` and writes it to
`docs/dqx/docs/reference/benchmarks.mdx`.

The report includes:
- Benchmark specifications.
- Performance results in a tabular format.
- A link to the test implementation.

Usage:
    Run the script directly to generate the report.
"""

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

lines.append("## Specification")
lines.append("* 100 million rows are used for each test.")
lines.append(
    "* DQX rules are executed in parallel and in distributed manner using Spark. To minimize test variability, each type of check is executed sequentially in this benchmark. Specific tests, such as `test_benchmark_apply_checks_all_dataset_checks` and `test_benchmark_apply_checks_all_row_checks`, execute all types of checks in parallel simultaneously."
)
lines.append("* Benchmarks are created using Databricks Serverless cluster.")
lines.append(
    "* The provided benchmarks are indicative. You should always consider benchmarking results in the context of your own data and environment.\n"
)

lines.append("## Results")

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

# overwrite the report
report_path.write_text("\n".join(lines))
print(f"REPORT_PATH={report_path.resolve()}")
