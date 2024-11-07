from databricks.labs.dqx.profiler.profiler import (
    T,
    get_columns_or_fields,
)


def test_get_columns_or_fields():
    inp = T.StructType(
        [
            T.StructField("t1", T.IntegerType()),
            T.StructField(
                "s1",
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
    fields = get_columns_or_fields(inp.fields)
    expected = [
        T.StructField("t1", T.IntegerType()),
        T.StructField("s1.ns1", T.TimestampType()),
        T.StructField("s1.s2.ns2", T.StringType()),
        T.StructField("s1.s2.ns3", T.DateType()),
    ]
    assert fields == expected
