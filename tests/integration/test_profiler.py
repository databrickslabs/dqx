from datetime import date, datetime
from decimal import Decimal

import pyspark.sql.types as T
from databricks.labs.dqx.profiler.profiler import DQProfiler, DQProfile


def test_profiler(spark, ws):
    inp_schema = T.StructType(
        [
            T.StructField("t1", T.IntegerType()),
            T.StructField("d1", T.DecimalType(10, 2)),
            T.StructField("t2", T.StringType()),
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
    inp_df = spark.createDataFrame(
        [
            [
                1,
                Decimal("1.23"),
                " test ",
                {
                    "ns1": datetime.fromisoformat("2023-01-08T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-08")},
                },
            ],
            [
                2,
                Decimal("2.41"),
                "test2",
                {
                    "ns1": datetime.fromisoformat("2023-01-07T10:00:11+00:00"),
                    "s2": {"ns2": "test2", "ns3": date.fromisoformat("2023-01-07")},
                },
            ],
            [
                3,
                Decimal("333323.0"),
                None,
                {
                    "ns1": datetime.fromisoformat("2023-01-06T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-06")},
                },
            ],
        ],
        schema=inp_schema,
    )

    profiler = DQProfiler(ws)
    stats, rules = profiler.profile(inp_df)

    expected_rules = [
        DQProfile(name="is_not_null", column="t1", description=None, parameters=None),
        DQProfile(
            name="min_max", column="t1", description="Real min/max values were used", parameters={"min": 1, "max": 3}
        ),
        DQProfile(name='is_not_null', column='d1', description=None, parameters=None),
        DQProfile(
            name='min_max',
            column='d1',
            description='Real min/max values were used',
            parameters={'max': Decimal('333323.00'), 'min': Decimal('1.23')},
        ),
        DQProfile(name='is_not_null_or_empty', column='t2', description=None, parameters={'trim_strings': True}),
        DQProfile(name="is_not_null", column="s1.ns1", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.ns1",
            description="Real min/max values were used",
            parameters={"min": datetime(2023, 1, 6, 0, 0), "max": datetime(2023, 1, 9, 0, 0)},
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(2023, 1, 8)},
        ),
    ]
    print(stats)
    assert len(stats.keys()) > 0
    assert rules == expected_rules


def test_profiler_non_default_profile_options(spark, ws):
    inp_schema = T.StructType(
        [
            T.StructField("t1", T.IntegerType()),
            T.StructField("t2", T.StringType()),
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
    inp_df = spark.createDataFrame(
        [
            [
                1,
                " test ",
                {
                    "ns1": datetime.fromisoformat("2023-01-08T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-08")},
                },
            ],
            [
                2,
                " ",
                {
                    "ns1": datetime.fromisoformat("2023-01-07T10:00:11+00:00"),
                    "s2": {"ns2": "test2", "ns3": date.fromisoformat("2023-01-07")},
                },
            ],
            [
                3,
                None,
                {
                    "ns1": datetime.fromisoformat("2023-01-06T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-06")},
                },
            ],
        ],
        schema=inp_schema,
    )

    profiler = DQProfiler(ws)

    profiler.default_profile_options = {
        "round": False,
        "max_in_count": 1,
        "distinct_ratio": 0.01,
        "max_null_ratio": 0.01,  # Generate is_null if we have less than 1 percent of nulls
        "remove_outliers": False,
        "outlier_columns": ["t1", "s1"],  # remove outliers in all columns of appropriate type
        "num_sigmas": 1,  # number of sigmas to use when remove_outliers is True
        "trim_strings": False,  # trim whitespace from strings
        "max_empty_ratio": 0.01,
    }

    stats, rules = profiler.profile(inp_df)

    expected_rules = [
        DQProfile(name="is_not_null", column="t1", description=None, parameters=None),
        DQProfile(
            name="min_max", column="t1", description="Real min/max values were used", parameters={"min": 1, "max": 3}
        ),
        DQProfile(name='is_not_null_or_empty', column='t2', description=None, parameters={'trim_strings': False}),
        DQProfile(name="is_not_null", column="s1.ns1", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.ns1",
            description="Real min/max values were used",
            parameters={'max': datetime(2023, 1, 8, 10, 0, 11), 'min': datetime(2023, 1, 6, 10, 0, 11)},
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(2023, 1, 8)},
        ),
    ]
    print(stats)
    assert len(stats.keys()) > 0
    assert rules == expected_rules


def test_profiler_empty_df(spark, ws):
    test_df = spark.createDataFrame([], "data: string")

    profiler = DQProfiler(ws)
    actual_summary_stats, actual_dq_rules = profiler.profile(test_df)

    assert len(actual_summary_stats.keys()) > 0
    assert len(actual_dq_rules) == 0


def test_profiler_when_numeric_field_is_empty(spark, ws):
    schema = "col1: int, col2: int, col3: int, col4 int"
    input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], schema)

    profiler = DQProfiler(ws)
    stats, rules = profiler.profile(input_df)

    expected_rules = [
        DQProfile(name='is_not_null', column='col1', description=None, parameters=None),
        DQProfile(
            name='min_max', column='col1', description='Real min/max values were used', parameters={'max': 2, 'min': 1}
        ),
        DQProfile(
            name='min_max', column='col2', description='Real min/max values were used', parameters={'max': 3, 'min': 2}
        ),
        DQProfile(name='is_not_null', column='col3', description=None, parameters=None),
        DQProfile(
            name='min_max', column='col3', description='Real min/max values were used', parameters={'max': 4, 'min': 3}
        ),
        DQProfile(name='is_not_null', column='col4', description=None, parameters=None),
        DQProfile(
            name='min_max', column='col4', description='Real min/max values were used', parameters={'max': 4, 'min': 1}
        ),
    ]

    assert len(stats.keys()) > 0
    assert rules == expected_rules
