"""Thresholds governing per-segment model training.

These were previously inline literals spread across the profiler and the training service. Naming
them in one place makes the policy reviewable.

All of them now guard the legacy ``segment_by`` path — the only path that trains one model per
group, and therefore the only one whose cost grows with the group count. ``baseline_by`` trains a
single pooled model however many groups there are, so it needs no ceiling.

See https://github.com/databrickslabs/dqx/issues/1484 for the measurements behind them.
"""

# Hard ceiling on the number of per-segment models a single training run will attempt. Segmented
# training does not ensemble, so cost is linear in the segment count: 90 segments measured roughly
# 88 minutes, and a 90-segment run was cancelled after 70 minutes without finishing. Overridable
# via ``AnomalyParams.max_segment_models``.
MAX_SEGMENT_MODELS = 50

# Below this many rows per segment on average, a per-segment model is calibrated on too small a
# sample to be trustworthy. Severity is a percentile of each model's own training scores, so a thin
# sample yields a fragile threshold rather than an obviously bad one. Used by the profiler to warn.
MIN_ROWS_PER_SEGMENT = 100

# Segment count above which training logs a slow-training warning. Retained at its historical value
# so existing runs keep warning where they used to; MAX_SEGMENT_MODELS is the ceiling that actually
# stops a run.
SEGMENT_COUNT_WARN_THRESHOLD = 100

# A segment with fewer rows than this is skipped and gets no model at all, so its rows come back
# unscored. Distinct from MIN_ROWS_PER_SEGMENT, which is the average below which per-segment
# modelling is merely discouraged.
MIN_ROWS_TO_TRAIN_SEGMENT = 10

# Upper bound on the distinct values a column may have to be *recommended* as a grouping by
# auto-discovery on the legacy segmented path. Conservative on purpose: each distinct value there
# becomes its own model.
MAX_AUTO_GROUP_COUNT = 20


# --- baseline conditioning ----------------------------------------------------------------------
#
# These deliberately differ from the segmented thresholds above, because the cost model differs.
# A per-segment model must *train a forest* on its group, so it needs hundreds of rows and the group
# count is a direct cost. A baseline group only has to yield a *median*, and there is one model
# however many groups exist — so the constraint is statistical, not economic.
#
# Applying the segmented thresholds to baseline_by was a real defect: on a dataset whose natural
# grouping was country x event_type x product (90 groups), discovery selected the single
# lowest-cardinality column — 3 groups — and conditioning barely engaged, scoring PR-AUC 0.1302
# against 0.1176 unconditioned. Finer grouping is what makes a baseline tight.

# Rows per group needed for a representative median. Well below MIN_ROWS_PER_SEGMENT because a
# median is a far cheaper statistic than a fitted forest: percentile_approx over a few dozen rows is
# a usable centre, where a forest over the same rows is not a usable model.
MIN_ROWS_PER_BASELINE_GROUP = 30

# Ceiling on total baseline groups. Not a training cost — it bounds what gets persisted in the
# feature metadata and broadcast at scoring. Well above the 200 keys at which unseen-group marking
# switches from an isin to a broadcast join, so both strategies stay viable.
MAX_BASELINE_GROUPS = 5000

# Per-column distinct-value ceiling for a baseline column. Higher than MAX_AUTO_GROUP_COUNT since
# breadth is affordable here, but still below the profiler's high-cardinality warning at 50, so an
# identifier-like column is never mistaken for a dimension.
MAX_BASELINE_COLUMN_CARDINALITY = 50
