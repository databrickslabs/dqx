"""Constants for DQProfiler option keys and default option values."""

from typing import Any

PROFILE_OPTION_ROUND = "round"
"""Round min/max boundary values away from the mean when generating *min_max* rules."""

PROFILE_OPTION_MAX_IN_COUNT = "max_in_count"
"""Maximum number of distinct values allowed for an *is_in* rule to be emitted."""

PROFILE_OPTION_DISTINCT_RATIO = "distinct_ratio"
"""Maximum ratio of distinct-to-total rows (0–1) for an *is_in* rule to be emitted."""

PROFILE_OPTION_MAX_NULL_RATIO = "max_null_ratio"
"""Maximum acceptable fraction of null values for a *is_not_null* rule to be emitted."""

PROFILE_OPTION_MAX_EMPTY_RATIO = "max_empty_ratio"
"""Maximum acceptable fraction of empty-string values for an *is_not_empty* rule to be emitted."""

PROFILE_OPTION_REMOVE_OUTLIERS = "remove_outliers"
"""Whether to cap *min_max* boundaries using ``mean ± num_sigmas * stddev`` outlier removal."""

PROFILE_OPTION_OUTLIER_COLUMNS = "outlier_columns"
"""Explicit list of column names on which to apply outlier removal; empty list means all columns."""

PROFILE_OPTION_NUM_SIGMAS = "num_sigmas"
"""Number of standard deviations used to cap outlier boundaries."""

PROFILE_OPTION_TRIM_STRINGS = "trim_strings"
"""Whether to trim leading/trailing whitespace before analysing string columns."""

PROFILE_OPTION_SAMPLE_FRACTION = "sample_fraction"
"""Fraction of rows to sample (float) or per-stratum fractions (dict) before profiling."""

PROFILE_OPTION_SAMPLE_SEED = "sample_seed"
"""Random seed for reproducible sampling; *None* means non-deterministic."""

PROFILE_OPTION_SAMPLE_BY_COLUMN = "sample_by_column"
"""Column name used to stratify the sample; requires *PROFILE_OPTION_SAMPLE_FRACTION*."""

PROFILE_OPTION_SAMPLE_BY_VALUES_LIMIT = "sample_by_values_limit"
"""Maximum number of distinct stratum values collected for uniform stratified sampling."""

PROFILE_OPTION_LIMIT = "limit"
"""Hard row limit applied after sampling via ``DataFrame.limit``; *None* disables it."""

PROFILE_OPTION_FILTER = "filter"
"""SQL expression used to pre-filter the DataFrame before profiling."""

PROFILE_OPTION_LLM_PRIMARY_KEY_DETECTION = "llm_primary_key_detection"
"""Whether to invoke the LLM engine to detect primary-key columns and emit an *is_unique* rule."""

PROFILE_OPTION_OUTLIERS_RATIO = "outliers_ratio"
"""Outliers percentage threshold to consider generate `has_no_outliers` profile for"""

DEFAULT_PROFILE_OPTIONS: dict[str, Any] = {
    PROFILE_OPTION_ROUND: True,
    PROFILE_OPTION_MAX_IN_COUNT: 10,
    PROFILE_OPTION_DISTINCT_RATIO: 0.05,
    PROFILE_OPTION_MAX_NULL_RATIO: 0.01,
    PROFILE_OPTION_REMOVE_OUTLIERS: True,
    PROFILE_OPTION_OUTLIER_COLUMNS: [],
    PROFILE_OPTION_NUM_SIGMAS: 3,
    PROFILE_OPTION_TRIM_STRINGS: True,
    PROFILE_OPTION_MAX_EMPTY_RATIO: 0.01,
    PROFILE_OPTION_SAMPLE_FRACTION: 0.3,
    PROFILE_OPTION_SAMPLE_SEED: None,
    PROFILE_OPTION_SAMPLE_BY_COLUMN: None,
    PROFILE_OPTION_SAMPLE_BY_VALUES_LIMIT: 1000,
    PROFILE_OPTION_LIMIT: 1000,
    PROFILE_OPTION_FILTER: None,
    PROFILE_OPTION_LLM_PRIMARY_KEY_DETECTION: True,
    PROFILE_OPTION_OUTLIERS_RATIO: 0.01,
}
"""Default values for all DQProfiler options."""
