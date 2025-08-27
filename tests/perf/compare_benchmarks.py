import json
from pathlib import Path
import sys

# Paths to benchmark JSON files
baseline_path = Path("tests/perf/.benchmarks/baseline.json")
current_path = Path("tests/perf/.benchmarks/current.json")

# Load benchmark data
with open(baseline_path) as f:
    baseline_data = json.load(f)

with open(current_path) as f:
    current_data = json.load(f)

def map_means(data):
    """Map benchmark names to mean execution times"""
    return {bench["name"]: bench["stats"]["mean"] for bench in data.get("benchmarks", [])}


baseline_means = map_means(baseline_data)
current_means = map_means(current_data)

# Compare and report
failed = False
print(f"{'Benchmark':50} {'Baseline(s)':>12} {'Current(s)':>12} {'% Change':>10}")
print("-"*90)
for name, baseline_mean in baseline_means.items():
    current_mean = current_means.get(name)
    if current_mean is None:
        print(f"{name:50} MISSING in current results")
        failed = True
        continue
    percent_change = ((current_mean - baseline_mean) / baseline_mean) * 100
    print(f"{name:50} {baseline_mean:12.6f} {current_mean:12.6f} {percent_change:10.2f}%")
    if percent_change > 20:
        failed = True

if failed:
    print("\nERROR: Some benchmarks degraded by more than 20%!")
    sys.exit(1)
else:
    print("\nAll benchmarks within acceptable range.")
    sys.exit(0)
