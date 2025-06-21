from pyspark.sql.types import StructType, StructField, ArrayType, StringType, TimestampType, MapType

dq_result_item_schema = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("message", StringType(), nullable=True),
        StructField("columns", ArrayType(StringType()), nullable=True),
        StructField("filter", StringType(), nullable=True),
        StructField("function", StringType(), nullable=True),
        StructField("run_time", TimestampType(), nullable=True),
        StructField("user_metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)

dq_result_schema = ArrayType(dq_result_item_schema)
