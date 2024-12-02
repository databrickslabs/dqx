import pyspark.sql.types as T
from databricks.labs.dqx.profiler.profiler import DQProfiler


def test_get_columns_or_fields():
    inp = T.StructType(
        [
            T.StructField("ts1", T.IntegerType()),
            T.StructField(
                "ss1",
                T.StructType(
                    [
                        T.StructField("ns1", T.TimestampType()),
                        T.StructField(
                            "s2",
                            T.StructType([T.StructField("ns2", T.StringType()), T.StructField("ns3", T.DateType())]),
                        ),
                    ]
                ),
            ),
        ]
    )
    fields = DQProfiler.get_columns_or_fields(inp.fields)
    expected = [
        T.StructField("ts1", T.IntegerType()),
        T.StructField("ss1.ns1", T.TimestampType()),
        T.StructField("ss1.s2.ns2", T.StringType()),
        T.StructField("ss1.s2.ns3", T.DateType()),
    ]
    assert fields == expected
