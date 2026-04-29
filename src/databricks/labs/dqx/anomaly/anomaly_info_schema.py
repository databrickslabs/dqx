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
# narrative / business_impact / action are LLM-generated; pattern is deterministic.
# group_size / group_avg_severity describe the (segment, pattern) group this row belongs to.
ai_explanation_struct_schema = StructType(
    [
        StructField("narrative", StringType(), True),
        StructField("business_impact", StringType(), True),
        StructField("pattern", StringType(), True),
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
        StructField("segment", MapType(StringType(), StringType()), True),
        StructField("contributions", MapType(StringType(), DoubleType()), True),
        StructField("confidence_std", DoubleType(), True),
        StructField("ai_explanation", ai_explanation_struct_schema, True),
    ]
)
