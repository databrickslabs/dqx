"""Benchmarks for semantic-aware profiling.

Compares *DQProfiler.profile* runs with and without a *SemanticRegistry* to
quantify the overhead of the default semantic-detection chain (enum → key →
measurement → text) on worst-case columns — inputs shaped so the classifier
walks the full chain before matching the trailing fallback detector:

* **Integer, worst-case measurement** — random ints in a wide range so
  distinctness is ~1.0 but density (*count_distinct / (max - min + 1)*) is
  well below the key threshold. Chain traversal: enum rejects (cardinality
  ≫ *max_in_count*), key rejects (density below threshold), measurement
  matches.

* **String, worst-case text** — variable-length free-form words so
  distinctness is ~1.0 but length stability (*min_len / max_len*) is well
  below the key threshold. Chain traversal: enum rejects, key runs a Spark
  aggregate for min/max length and then rejects on low stability, text
  matches. The key detector's aggregate is the primary added cost this
  benchmark surfaces.
"""

import pytest

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.semantic import SemanticRegistry


# 100K rows is smaller than the DEFAULT_ROWS used elsewhere in tests/perf because the legacy
# *is_in* profile builder does an unconditional *df.select(col).distinct().collect()* on every
# column. Our worst-case fixtures deliberately produce ~1.0 distinctness, so the collect returns
# roughly BENCHMARK_ROWS rows to the driver — at DEFAULT_ROWS this exceeds the Spark Connect
# message limit / driver memory and the no-registry baseline fails with PythonException. 100K
# keeps the collect under a few MB while still letting the semantic-detection chain exercise
# its full worst-case path (enum + key aggregates fire, then fall through to the trailing
# fallback detector).
BENCHMARK_ROWS = 100_000
BENCHMARK_COLUMNS = 4

# Wide-range random integers force key-density rejection: cardinality is ~BENCHMARK_ROWS but
# max-min+1 is 2*10^9, so density = BENCHMARK_ROWS / 2*10^9 ≪ KEY_MIN_DENSITY_RATIO and the
# chain falls through to measurement. The upper bound stays below int32 max (2_147_483_647)
# because dbldatagen samples via double then casts to int; a larger max triggers CAST_OVERFLOW.
INT_WIDE_RANGE_OPTS = {"minValue": 1, "maxValue": 2_000_000_000, "random": True}

PROFILE_OPTIONS = {
    # sample_fraction=None profiles the full dataset so numbers reflect the real cost,
    # not sample-driven noise.
    "sample_fraction": None,
    # LLM primary-key detection is orthogonal to the semantic chain and would dominate the timing.
    "llm_primary_key_detection": False,
}

SEMANTIC_REGISTRY_PARAMS = [
    pytest.param(None, id="no_semantic_registry"),
    pytest.param(SemanticRegistry.default(), id="default_semantic_registry"),
]


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": BENCHMARK_ROWS, "n_columns": BENCHMARK_COLUMNS, "opts": INT_WIDE_RANGE_OPTS}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.parametrize("semantic_registry", SEMANTIC_REGISTRY_PARAMS)
@pytest.mark.benchmark(group="test_benchmark_profile_semantic_integer_measurement")
def test_benchmark_profile_semantic_integer_measurement(benchmark, ws, generated_integer_df, semantic_registry):
    """Profile wide-range random integers — worst-case measurement path."""
    _columns, df, n_rows = generated_integer_df
    profiler = DQProfiler(workspace_client=ws, semantic_registry=semantic_registry)
    _stats, profiles = benchmark(lambda: profiler.profile(df, options=PROFILE_OPTIONS))
    assert profiles, f"expected profiler to emit at least one profile for {n_rows} rows"


@pytest.mark.parametrize(
    "generated_string_df",
    [
        {
            "n_rows": BENCHMARK_ROWS,
            "n_columns": BENCHMARK_COLUMNS,
        }
    ],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.parametrize("semantic_registry", SEMANTIC_REGISTRY_PARAMS)
@pytest.mark.benchmark(group="test_benchmark_profile_semantic_string_text")
def test_benchmark_profile_semantic_string_text(benchmark, ws, generated_string_df, semantic_registry):
    """Profile variable-length random text — worst-case text path (key detector triggers Spark aggregate)."""
    _columns, df, n_rows = generated_string_df
    profiler = DQProfiler(workspace_client=ws, semantic_registry=semantic_registry)
    _stats, profiles = benchmark(lambda: profiler.profile(df, options=PROFILE_OPTIONS))
    assert profiles, f"expected profiler to emit at least one profile for {n_rows} rows"
