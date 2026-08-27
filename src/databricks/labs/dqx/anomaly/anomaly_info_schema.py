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
    ]
)
