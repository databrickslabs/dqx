from datetime import date, datetime

from pyspark.sql import SparkSession

from databricks.labs.dqx.profiler.profiler import (
    DQProfile,
    T,
    get_columns_or_fields,
    profile,
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


def test_profiler(spark_session: SparkSession):
    inp_schema = T.StructType(
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
    inp_df = spark_session.createDataFrame(
        [
            [
                1,
                {
                    "ns1": datetime.fromisoformat("2023-01-08T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-08")},
                },
            ],
            [
                2,
                {
                    "ns1": datetime.fromisoformat("2023-01-07T10:00:11+00:00"),
                    "s2": {"ns2": "test2", "ns3": date.fromisoformat("2023-01-07")},
                },
            ],
            [
                3,
                {
                    "ns1": datetime.fromisoformat("2023-01-06T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-06")},
                },
            ],
        ],
        schema=inp_schema,
    )
    stats, rules = profile(inp_df)
    # pprint.pprint(stats)
    # pprint.pprint(rules)
    expected_rules = [
        DQProfile(name="is_not_null", column="t1", description=None, parameters=None),
        DQProfile(
            name="min_max", column="t1", description="Real min/max values were used", parameters={"min": 1, "max": 3}
        ),
        DQProfile(name="is_not_null", column="s1.ns1", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.ns1",
            description="Real min/max values were used",
            parameters={"min": datetime(2023, 1, 6, 0, 0), "max": datetime(2023, 1, 9, 0, 0)},
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None),
        DQProfile(name="is_not_null_or_empty", column="s1.s2.ns2", description=None, parameters={"trim_strings": True}),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(2023, 1, 8)},
        ),
    ]

    assert rules == expected_rules


def test_profiler_empty_df(spark_session: SparkSession):
    test_df = spark_session.createDataFrame([], "data: string")

    actual_summary_stats, actual_dq_rule = profile(test_df)

    assert len(actual_dq_rule) == 0
