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
# auto-discovery. Conservative on purpose: a wide grouping is fine for baseline_by, which trains one
# model, but auto-discovery's recommendation is also what the legacy segmented path would consume.
MAX_AUTO_GROUP_COUNT = 20
