"""Schema for the anomaly info struct stored in _dq_info (array of structs)."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
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
    ]
)
