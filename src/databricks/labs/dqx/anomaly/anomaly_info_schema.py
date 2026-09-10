"""Schema for the anomaly info struct stored in _dq_info (array of structs)."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Schema for the AI explanation sub-struct inside the anomaly info struct.
# narrative / business_impact / action are LLM-generated; top_features is deterministic
# (the sorted top-2 contributing features that define the group, as engineered names, so it
# stays stable for grouping and tooling). top_drivers is the same drivers rendered as human labels
# with their weights, e.g. 'amount vs its group baseline (74%), quantity (12%)', for display.
# group_size / group_avg_severity describe the pattern group this row belongs to.
ai_explanation_struct_schema = StructType(
    [
        StructField("narrative", StringType(), True),
        StructField("business_impact", StringType(), True),
        StructField("top_features", StringType(), True),
        StructField("top_drivers", StringType(), True),
        StructField("action", StringType(), True),
        StructField("group_size", LongType(), True),
        StructField("group_avg_severity", DoubleType(), True),
        # Appended last, and it must stay last: add_info_column casts this struct positionally, so schema
        # order and construction order have to match. _dq_info therefore grows wider, and appending to a
        # Delta table that already holds the narrower shape needs mergeSchema.
        #
        # How much of the contributing evidence the explanation was allowed to show, as a coarse state
        # rather than a share -- a share would disclose by proportion what a redacted name discloses by
        # identity. Machine-readable so a consumer can filter or escalate on it instead of parsing prose.
        StructField("evidence_scope", StringType(), True),
    ]
)

# Inner struct: one row's anomaly metadata from a single has_no_row_anomalies check.
# After merge, _dq_info is array<struct<anomaly: ...>>; each element uses this schema.
anomaly_info_struct_schema = StructType(
    [
        StructField("check_name", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("severity_percentile", DoubleType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("model", StringType(), True),
        StructField("contributions", MapType(StringType(), DoubleType()), True),
        StructField("confidence_std", DoubleType(), True),
        StructField("ai_explanation", ai_explanation_struct_schema, True),
        # True when the row's group was never seen in training, so its score and severity are
        # null rather than guessed. Appended at the end: existing named-field queries such as
        # _dq_info[0].anomaly.score keep working, but a Delta table already holding _dq_info
        # needs mergeSchema on append because the struct is now wider.
        StructField("is_new_baseline", BooleanType(), True),
        StructField("new_baseline_key", StringType(), True),
        # True when the row's timestamp lies beyond the window a temporal baseline was fitted on, so
        # its expected level is an extrapolation. Unlike an unseen group, the score is still produced:
        # a fitted function does extrapolate, and measured, accuracy one training window past the
        # boundary is 3.4% false flags against 2.8% at the boundary itself. Nulling that would discard
        # a usable verdict. It degrades with distance though -- 6.4% five windows out, 89.6% at
        # twenty-five -- so the flag says "this is extrapolation, and here is where the evidence ran
        # out". Both null when no temporal baseline was fitted. Appended for the same reason as the
        # two above: named-field queries keep working, a wider struct needs mergeSchema on append.
        StructField("is_stale_baseline", BooleanType(), True),
        StructField("stale_baseline_horizon", StringType(), True),
    ]
)
