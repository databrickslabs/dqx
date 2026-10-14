"""Thresholds governing baseline conditioning.

Named in one place so the policy is reviewable. These bound a *statistical* requirement, not a cost
one: baseline conditioning trains a single model however many groups exist. Group-specific medians
and score quantiles need sufficient usable observations; smaller groups use the pooled statistics.

The segmented thresholds that used to live here went with the ``segment_by`` path they guarded. They
were the opposite shape — few groups, hundreds of rows each — because a per-segment model had to
train a forest on every group. Applying them to baseline conditioning was a measured defect: on a
dataset grouped by country x event_type x product (90 groups) discovery selected the single
lowest-cardinality column, giving 3 groups, and conditioning barely engaged.

See https://github.com/databrickslabs/dqx/issues/1484 for the measurements behind them.
"""

# Minimum usable values for a group median, or usable scores for group calibration. Smaller known
# groups use pooled statistics. This floor is a support safeguard, not an accuracy guarantee for
# extreme percentiles such as p99. Discovery projects support; fitting verifies the actual counts.
MIN_ROWS_PER_BASELINE_GROUP = 30

# Ceiling on total baseline groups. Not a training cost — it bounds what gets persisted in the
# feature metadata and broadcast at scoring. Well above the 200 keys at which unseen-group marking
# switches from an isin to a broadcast join, so both strategies stay viable.
MAX_BASELINE_GROUPS = 5000

# Per-column distinct-value ceiling for a baseline column. Higher than MAX_AUTO_GROUP_COUNT since
# breadth is affordable here, but still below the profiler's high-cardinality warning at 50, so an
# identifier-like column is never mistaken for a dimension.
MAX_BASELINE_COLUMN_CARDINALITY = 50
