from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
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
            T.StructField("b1", T.ByteType()),
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
                0,
            ],
            [
                2,
                Decimal("2.41"),
                "test2",
                {
                    "ns1": datetime.fromisoformat("2023-01-07T10:00:11+00:00"),
                    "s2": {"ns2": "test2", "ns3": date.fromisoformat("2023-01-07")},
                },
                1,
            ],
            [
                3,
                Decimal("333323.0"),
                None,
                {
                    "ns1": datetime.fromisoformat("2023-01-06T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-06")},
                },
                0,
            ],
        ],
        schema=inp_schema,
    )

    profiler = DQProfiler(ws)
    stats, rules = profiler.profile(inp_df, options={"sample_fraction": None})

    expected_rules = [
        DQProfile(name="is_not_null", column="t1", description=None, parameters=None, filter=None),
        DQProfile(
            name="min_max", column="t1", description="Real min/max values were used", parameters={"min": 1, "max": 3}, filter=None
        ),
        DQProfile(name='is_not_null', column='d1', description=None, parameters=None, filter=None),
        DQProfile(
            name='min_max',
            column='d1',
            description='Real min/max values were used',
            parameters={'max': Decimal('333323.00'), 'min': Decimal('1.23')},
            filter=None
        ),
        DQProfile(name='is_not_null_or_empty', column='t2', description=None, parameters={'trim_strings': True},filter=None),
        DQProfile(name="is_not_null", column="s1.ns1", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.ns1",
            description="Real min/max values were used",
            parameters={
                "min": datetime(2023, 1, 6, 0, 0, tzinfo=timezone.utc),
                "max": datetime(2023, 1, 9, 0, 0, tzinfo=timezone.utc),
            },
            filter=None
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None,filter=None),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None,filter=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(2023, 1, 8)},
            filter=None
        ),
        DQProfile(name="is_not_null", column="b1", description=None, parameters=None,filter=None),
    ]
    print(stats)
    assert len(stats.keys()) > 0
    assert rules == expected_rules


def test_profiler_rounding_midnight_behavior(spark, ws):
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
            T.StructField("b1", T.ByteType()),
        ]
    )
    inp_df = spark.createDataFrame(
        [
            [
                1,
                Decimal("1.23"),
                " test ",
                {
                    "ns1": datetime.fromisoformat("2023-01-08T00:00:00+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-08")},
                },
                0,
            ],
            [
                2,
                Decimal("2.41"),
                "test2",
                {
                    "ns1": datetime.fromisoformat("2023-01-07T10:00:11+00:00"),
                    "s2": {"ns2": "test2", "ns3": date.fromisoformat("2023-01-07")},
                },
                1,
            ],
            [
                3,
                Decimal("333323.0"),
                None,
                {
                    "ns1": datetime.fromisoformat("2023-01-06T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("2023-01-06")},
                },
                0,
            ],
        ],
        schema=inp_schema,
    )

    profiler = DQProfiler(ws)
    stats, rules = profiler.profile(inp_df, options={"sample_fraction": None})

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
            parameters={
                "min": datetime(2023, 1, 6, 0, 0, tzinfo=timezone.utc),
                "max": datetime(2023, 1, 8, 0, 0, tzinfo=timezone.utc),
            },
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(2023, 1, 8)},
        ),
        DQProfile(name="is_not_null", column="b1", description=None, parameters=None),
    ]
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

    profile_options = {
        "round": False,  # do not round the min/max values
        "max_in_count": 1,  # generate is_in if we have less than 1 percent of distinct values
        "distinct_ratio": 0.01,  # generate is_in if we have less than 1 percent of distinct values
        "remove_outliers": False,  # do not remove outliers
        "outlier_columns": ["t1", "s1"],  # remove outliers in all columns of appropriate type
        "num_sigmas": 1,  # number of sigmas to use when remove_outliers is True
        "trim_strings": False,  # trim whitespace from strings
        "max_empty_ratio": 0.01,  # generate is_not_null_or_empty rule if we have less than 1 percent of empty strings
        "sample_fraction": 1.0,  # fraction of data to sample
        "sample_seed": None,  # seed for sampling
        "limit": 1000,  # limit the number of samples
    }

    stats, rules = profiler.profile(inp_df, columns=inp_df.columns, options=profile_options)

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


def test_profiler_non_default_profile_options_remove_outliers_no_outlier_columns(spark, ws):
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
                    "ns1": datetime.fromisoformat("9999-12-31T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("9999-12-31")},
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

    profile_options = {
        "round": False,  # do not round the min/max values
        "max_in_count": 1,  # generate is_in if we have less than 1 percent of distinct values
        "distinct_ratio": 0.01,  # generate is_in if we have less than 1 percent of distinct values
        "remove_outliers": True,  # remove outliers
        "num_sigmas": 1,  # number of sigmas to use when remove_outliers is True
        "trim_strings": False,  # trim whitespace from strings
        "max_empty_ratio": 0.01,  # generate is_not_null_or_empty rule if we have less than 1 percent of empty strings
        "sample_fraction": 1.0,  # fraction of data to sample
        "sample_seed": None,  # seed for sampling
        "limit": 1000,  # limit the number of samples
    }

    stats, rules = profiler.profile(inp_df, columns=inp_df.columns, options=profile_options)

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
            parameters={
                'max': datetime(9999, 12, 31, 10, 0, 11, tzinfo=timezone.utc),
                'min': datetime(2023, 1, 6, 10, 0, 11, tzinfo=timezone.utc),
            },
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(9999, 12, 31)},
        ),
    ]
    assert len(stats.keys()) > 0
    assert rules == expected_rules


def test_profiler_non_default_profile_options_with_rounding_enabled(spark, ws):
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
                    "ns1": datetime.fromisoformat("9999-12-31T10:00:11+00:00"),
                    "s2": {"ns2": "test", "ns3": date.fromisoformat("9999-12-31")},
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

    profile_options = {
        "round": True,  # round the min/max values
        "max_in_count": 1,  # generate is_in if we have less than 1 percent of distinct values
        "distinct_ratio": 0.01,  # generate is_in if we have less than 1 percent of distinct values
        "remove_outliers": False,  # do not remove outliers
        "outlier_columns": ["t1", "s1"],  # remove outliers in all columns of appropriate type
        "num_sigmas": 1,  # number of sigmas to use when remove_outliers is True
        "trim_strings": False,  # trim whitespace from strings
        "max_empty_ratio": 0.01,  # generate is_not_null_or_empty rule if we have less than 1 percent of empty strings
        "sample_fraction": 1.0,  # fraction of data to sample
        "sample_seed": None,  # seed for sampling
        "limit": 1000,  # limit the number of samples
    }

    stats, rules = profiler.profile(inp_df, columns=inp_df.columns, options=profile_options)

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
            parameters={'max': datetime.max, 'min': datetime(2023, 1, 6)},
        ),
        DQProfile(name="is_not_null", column="s1.s2.ns2", description=None, parameters=None),
        DQProfile(name="is_not_null", column="s1.s2.ns3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="s1.s2.ns3",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 6), "max": date(9999, 12, 31)},
        ),
    ]
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
    stats, rules = profiler.profile(input_df, options={"sample_fraction": None})

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


def test_profiler_sampling(spark, ws):
    schema = "col1: int, col2: int, col3: int, col4 int"
    input_df = spark.createDataFrame(
        [
            [1, 3, 3, 1],
            [2, None, 4, 1],
            [10, 67, 3, 51],
            [100, 14, 3, 13],
            [-1, 45, None, 42],
            [3, 22, 3, 4],
            [63, 2, 3, 4],
            [15, None, 3, 41],
            [2, 62, 3, 85],
            [1, 24, 31, None],
        ],
        schema,
    )

    profiler = DQProfiler(ws)
    profiler_opts = {"sample_seed": 44, "limit": 7}  # default sample_fraction is 0.3
    cols = ["col1", "col2", "col4"]
    stats, rules = profiler.profile(input_df, columns=cols, options=profiler_opts)
    stats2, rules2 = profiler.profile(input_df, columns=cols, options=profiler_opts)

    assert len(stats.keys()) == 3
    assert len(rules) > 0
    assert stats == stats2
    assert rules == rules2


def test_profile_table(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("amount", T.DecimalType(10, 2)),
            T.StructField("created_date", T.DateType()),
            T.StructField("is_active", T.BooleanType()),
        ]
    )
    input_df = spark.createDataFrame(
        [
            [1, "Alice", Decimal("100.50"), date(2023, 1, 1), True],
            [2, "Bob", Decimal("250.75"), date(2023, 1, 2), False],
            [3, "Charlie", Decimal("175.25"), date(2023, 1, 3), True],
            [4, None, Decimal("300.00"), date(2023, 1, 4), True],
        ],
        schema=input_schema,
    )
    input_df.write.format("delta").saveAsTable(table_name)

    profiler = DQProfiler(ws)
    stats, rules = profiler.profile_table(table_name, options={"sample_fraction": None})
    expected_rules = [
        DQProfile(name="is_not_null", column="id", description=None, parameters=None),
        DQProfile(
            name="min_max", column="id", description="Real min/max values were used", parameters={"min": 1, "max": 4}
        ),
        DQProfile(name="is_not_null_or_empty", column="name", description=None, parameters={"trim_strings": True}),
        DQProfile(name="is_not_null", column="amount", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="amount",
            description="Real min/max values were used",
            parameters={"min": Decimal("100.50"), "max": Decimal("300.00")},
        ),
        DQProfile(name="is_not_null", column="created_date", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="created_date",
            description="Real min/max values were used",
            parameters={"min": date(2023, 1, 1), "max": date(2023, 1, 4)},
        ),
        DQProfile(name="is_not_null", column="is_active", description=None, parameters=None),
    ]

    assert len(stats.keys()) > 0
    assert stats["id"]["count"] == 4  # Verify we got all records
    assert rules == expected_rules


def test_profile_table_non_default_opts(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema = "category: string, value: int"
    input_df = spark.createDataFrame(
        [
            [None, 1],
            ["B", 2],
            ["C", 3],
            [None, 4],
            ["E", 5],
            ["F", 6],
            ["G", 7],
            ["H", 8],
            ["I", 9],
            ["J", 100],
        ],
        input_schema,
    )
    input_df.write.format("delta").saveAsTable(table_name)

    profiler = DQProfiler(ws)
    custom_opts = {
        "sample_fraction": 1.0,
        "max_null_ratio": 0.5,
        "remove_outliers": False,
        "trim_strings": False,
    }
    stats, rules = profiler.profile_table(table_name, options=custom_opts)
    expected_rules = [
        DQProfile(
            name="is_not_null",
            column="category",
            description="Column category has 20.0% of null values (allowed 50.0%)",
            parameters=None,
        ),
        DQProfile(name="is_not_null", column="value", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="value",
            description="Real min/max values were used",
            parameters={"min": 1, "max": 100},
        ),
    ]

    assert len(stats.keys()) > 0
    assert stats["category"]["count"] == 10
    assert rules == expected_rules


def test_profile_table_with_column_selection(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema = "col1: int, col2: string, col3: double, col4: boolean"
    input_df = spark.createDataFrame(
        [
            [1, "test1", 10.5, True],
            [2, "test2", 20.5, False],
            [3, "test3", 30.5, True],
        ],
        input_schema,
    )
    input_df.write.format("delta").saveAsTable(table_name)

    profiler = DQProfiler(ws)
    selected_cols = ["col1", "col3"]  # Only profile these columns
    stats, rules = profiler.profile_table(table_name, columns=selected_cols, options={"sample_fraction": None})
    expected_rules = [
        DQProfile(name="is_not_null", column="col1", description=None, parameters=None),
        DQProfile(
            name="min_max", column="col1", description="Real min/max values were used", parameters={"min": 1, "max": 3}
        ),
        DQProfile(name="is_not_null", column="col3", description=None, parameters=None),
        DQProfile(
            name="min_max",
            column="col3",
            description="Real min/max values were used",
            parameters={"min": 10.5, "max": 30.5},
        ),
    ]

    assert len(stats.keys()) == 2  # Only selected columns should be in stats
    assert "col1" in stats
    assert "col3" in stats
    assert "col2" not in stats  # Should not be included
    assert "col4" not in stats  # Should not be included
    assert rules == expected_rules


def test_profile_tables(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema1 = "col1: int, col2: int, col3: int, col4 int"
    input_df1 = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = "col1: string, col2: string, col3: int"
    input_df2 = spark.createDataFrame([["a", "b", 1], ["b", "c", 2], ["c", "d", 3]], input_schema2)
    input_df2.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    options = [
        {"table": table1_name, "options": {"sample_fraction": None}},
        {"table": table2_name, "options": {"sample_fraction": None}},
    ]
    profiles = profiler.profile_tables(tables=[table1_name, table2_name], options=options)
    expected_rules = {
        table1_name: [
            DQProfile(name='is_not_null', column='col1', description=None, parameters=None),
            DQProfile(
                name='min_max',
                column='col1',
                description='Real min/max values were used',
                parameters={'max': 2, 'min': 1},
            ),
            DQProfile(
                name='min_max',
                column='col2',
                description='Real min/max values were used',
                parameters={'max': 3, 'min': 2},
            ),
            DQProfile(name='is_not_null', column='col3', description=None, parameters=None),
            DQProfile(
                name='min_max',
                column='col3',
                description='Real min/max values were used',
                parameters={'max': 4, 'min': 3},
            ),
            DQProfile(name='is_not_null', column='col4', description=None, parameters=None),
            DQProfile(
                name='min_max',
                column='col4',
                description='Real min/max values were used',
                parameters={'max': 4, 'min': 1},
            ),
        ],
        table2_name: [
            DQProfile(name="is_not_null", column="col1", description=None, parameters=None),
            DQProfile(name="is_not_null", column="col2", description=None, parameters=None),
            DQProfile(name="is_not_null", column="col3", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="col3",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 3},
            ),
        ],
    }
    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_include_patterns(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    known_random = f"_data_{make_random(6).lower()}"
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}" + known_random
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema1 = "col1: int, col2: int, col3: int, col4 int"
    input_df1 = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = "col1: string, col2: string, col3: int"
    input_df2 = spark.createDataFrame([["a", "b", 1], ["b", "c", 2], ["c", "d", 3]], input_schema2)
    input_df2.write.format("delta").saveAsTable(table2_name)

    options = [
        {"table": f"*{known_random}", "options": {"sample_fraction": None}},
        {"table": table2_name, "options": {"sample_fraction": None}},
    ]
    profiles = DQProfiler(ws).profile_tables(
        patterns=[f"{catalog_name}.{schema_name}.*{known_random}"], options=options
    )
    expected_rules = {
        table1_name: [
            DQProfile(name='is_not_null', column='col1', description=None, parameters=None),
            DQProfile(
                name='min_max',
                column='col1',
                description='Real min/max values were used',
                parameters={'max': 2, 'min': 1},
            ),
            DQProfile(
                name='min_max',
                column='col2',
                description='Real min/max values were used',
                parameters={'max': 3, 'min': 2},
            ),
            DQProfile(name='is_not_null', column='col3', description=None, parameters=None),
            DQProfile(
                name='min_max',
                column='col3',
                description='Real min/max values were used',
                parameters={'max': 4, 'min': 3},
            ),
            DQProfile(name='is_not_null', column='col4', description=None, parameters=None),
            DQProfile(
                name='min_max',
                column='col4',
                description='Real min/max values were used',
                parameters={'max': 4, 'min': 1},
            ),
        ],
    }

    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_no_pattern_match(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema = "col1: int, col2: string"
    input_df = spark.createDataFrame([[1, "test"], [2, "data"]], input_schema)
    input_df.write.format("delta").saveAsTable(table1_name)

    no_match_pattern = "nonexistent_catalog.*"
    profiler = DQProfiler(ws)
    options = [{"table": table1_name, "options": {"sample_fraction": None}}]
    with pytest.raises(ValueError, match="No tables found matching include or exclude criteria"):
        profiler.profile_tables(patterns=[no_match_pattern], options=options)


def test_profile_tables_with_no_options(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema1 = "col1: int, col2: int, col3: int, col4 int"
    input_df1 = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = "col1: string, col2: string, col3: int"
    input_df2 = spark.createDataFrame([["a", "b", 1], ["b", "c", 2], ["c", "d", 3]], input_schema2)
    input_df2.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    options = [
        {"table": table1_name, "options": {}},
        {"table": table2_name, "options": None},
    ]
    profiles = profiler.profile_tables(tables=[table1_name, table2_name], options=options)

    for table_name, (stats, _) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        # not asserting rules here because of default sampling which creates non-deterministic results


def test_profile_tables_with_no_matched_options(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema1 = "col1: string, col2: string, col3: string"
    input_df1 = spark.createDataFrame([["1", None, "3"], ["2", None, "4"], ["1", None, "3"]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = "col1: string, col2: string, col3: string"
    input_df2 = spark.createDataFrame([["a", "b", "c"], ["b", "c", "d"], ["c", "d", "e"]], input_schema2)
    input_df2.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    options = [
        {"table": "unmatched_catalog.*", "options": {"max_null_ratio": 1.0}},
        {"table": f"{catalog_name}.unmatched_schema.*", "options": {"max_null_ratio": 1.0}},
        {"table": table1_name, "options": {"sample_fraction": 1.0}},
        {"table": table2_name, "options": {"sample_fraction": 1.0}},
    ]
    profiles = profiler.profile_tables(tables=[table1_name, table2_name], options=options)
    expected_rules = {
        table1_name: [
            DQProfile(name='is_not_null', column='col1', description=None, parameters=None),
            DQProfile(name="is_not_null_or_empty", column="col2", description=None, parameters={"trim_strings": True}),
            DQProfile(name='is_not_null', column='col3', description=None, parameters=None),
        ],
        table2_name: [
            DQProfile(name="is_not_null", column="col1", description=None, parameters=None),
            DQProfile(name="is_not_null", column="col2", description=None, parameters=None),
            DQProfile(name="is_not_null", column="col3", description=None, parameters=None),
        ],
    }
    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_with_common_opts(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema = "category: string, value: int"
    input_df = spark.createDataFrame(
        [
            [None, 1],
            ["B", 2],
            ["C", 3],
            [None, 4],
            ["E", 5],
            ["F", 6],
            ["G", 7],
            ["H", 8],
            ["I", 9],
            ["J", 100],
        ],
        input_schema,
    )
    input_df.write.format("delta").saveAsTable(table1_name)
    input_df.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    options = [
        {
            "table": "*",  # Matches all tables
            "options": {
                "max_null_ratio": 0.5,
                "remove_outliers": False,
                "sample_fraction": 1.0,
                "trim_strings": False,
            },
        }
    ]
    profiles = profiler.profile_tables(tables=[table1_name, table2_name], options=options)
    expected_rules = {
        table1_name: [
            DQProfile(
                name="is_not_null",
                column="category",
                description="Column category has 20.0% of null values (allowed 50.0%)",
                parameters=None,
            ),
            DQProfile(
                name="is_not_null",
                column="value",
                description=None,
                parameters=None,
            ),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 100},
            ),
        ],
        table2_name: [
            DQProfile(
                name="is_not_null",
                column="category",
                description="Column category has 20.0% of null values (allowed 50.0%)",
                parameters=None,
            ),
            DQProfile(name="is_not_null", column="value", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 100},
            ),
        ],
    }

    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_with_different_opts(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_prefix = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table1_name = f"{table_prefix}_001"
    table2_name = f"{table_prefix}_002"

    input_schema = "category: string, value: int"
    input_df = spark.createDataFrame(
        [
            [None, 1],
            ["B", 2],
            ["C", 3],
            [None, 4],
            ["E", 5],
            ["F", 6],
            ["G", 7],
            ["H", 8],
            ["I", 9],
            ["J", 100],
        ],
        input_schema,
    )
    input_df.write.format("delta").saveAsTable(table1_name)
    input_df.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    table_opts = [
        {
            "table": table1_name,
            "options": {
                "remove_outliers": False,
                "max_null_ratio": 0.5,
                "sample_fraction": 1.0,
                "trim_strings": False,
            },
        },
        {
            "table": f"{table_prefix}*",
            "options": {
                "remove_outliers": False,
                "sample_fraction": 1.0,
            },
        },
    ]

    profiles = profiler.profile_tables(
        patterns=[f"{table_prefix}*"],  # we can use patterns or provide table names (does not matter for the test)
        options=table_opts,
    )

    expected_rules = {
        table1_name: [
            DQProfile(
                name="is_not_null",
                column="category",
                description="Column category has 20.0% of null values (allowed 50.0%)",
                parameters=None,
            ),
            DQProfile(name="is_not_null", column="value", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 100},
            ),
        ],
        table2_name: [
            DQProfile(
                name="is_not_null_or_empty",
                column="category",
                description=None,
                parameters={"trim_strings": True},
            ),
            DQProfile(name="is_not_null", column="value", description=None),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 100},
            ),
        ],
    }

    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_with_partial_opts_match(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}_001"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}_002"

    input_schema = "category: string, value: int"
    input_df = spark.createDataFrame(
        [
            [None, 1],
            ["B", 2],
            ["C", 3],
            [None, 4],
            ["E", 5],
            ["F", 6],
            ["G", 7],
            ["H", 8],
            ["I", 9],
            ["J", 100],
        ],
        input_schema,
    )
    input_df.write.format("delta").saveAsTable(table1_name)
    input_df.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    table_opts = [
        {
            "table": table1_name,
            "options": {
                "remove_outliers": False,
                "max_null_ratio": 0.5,
                "sample_fraction": 1.0,
                "trim_strings": False,
            },
        },
        {
            "table": f"{catalog_name}.{schema_name}.*",
            "options": {
                "remove_outliers": False,
                "sample_fraction": 1.0,
                "trim_strings": True,
            },
        },
    ]
    profiles = profiler.profile_tables(tables=[table1_name, table2_name], options=table_opts)
    expected_rules = {
        table1_name: [
            DQProfile(
                name="is_not_null",
                column="category",
                description="Column category has 20.0% of null values (allowed 50.0%)",
                parameters=None,
            ),
            DQProfile(name="is_not_null", column="value", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 100},
            ),
        ],
        table2_name: [
            DQProfile(
                name="is_not_null_or_empty", column="category", description=None, parameters={"trim_strings": True}
            ),
            DQProfile(name="is_not_null", column="value", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 100},
            ),
        ],
    }

    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0, f"Stats did not match expected for {table_name}"
        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_with_selected_columns(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}_tbl1"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}_tbl2"

    input_schema1 = "col1: int, col2: string, col3: double, col4: boolean"
    input_df1 = spark.createDataFrame(
        [
            [1, "test1", 10.5, True],
            [2, "test2", 20.5, False],
            [3, "test3", 30.5, True],
        ],
        input_schema1,
    )
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = "id: int, name: string, value: int, active: boolean"
    input_df2 = spark.createDataFrame(
        [
            [100, "Alice", 500, True],
            [200, "Bob", 600, False],
            [300, "Charlie", 700, True],
        ],
        input_schema2,
    )
    input_df2.write.format("delta").saveAsTable(table2_name)

    profiler = DQProfiler(ws)
    table_columns = {
        table1_name: ["col1", "col3"],  # Only profile numeric columns
        table2_name: ["id", "value"],  # Only profile numeric columns
    }
    table_options = [
        {"table": table1_name, "options": {"sample_fraction": None}},
        {"table": table2_name, "options": {"sample_fraction": None}},
    ]

    profiles = profiler.profile_tables(tables=[table1_name, table2_name], columns=table_columns, options=table_options)
    expected_rules = {
        table1_name: [
            DQProfile(name="is_not_null", column="col1", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="col1",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 3},
            ),
            DQProfile(name="is_not_null", column="col3", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="col3",
                description="Real min/max values were used",
                parameters={"min": 10.5, "max": 30.5},
            ),
        ],
        table2_name: [
            DQProfile(name="is_not_null", column="id", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="id",
                description="Real min/max values were used",
                parameters={"min": 100, "max": 300},
            ),
            DQProfile(name="is_not_null", column="value", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="value",
                description="Real min/max values were used",
                parameters={"min": 500, "max": 700},
            ),
        ],
    }

    for table_name, (stats, rules) in profiles.items():
        if table_name == table1_name:
            assert len(stats.keys()) == 2
            assert "col1" in stats
            assert "col3" in stats
            assert "col2" not in stats
            assert "col4" not in stats
        elif table_name == table2_name:
            assert len(stats.keys()) == 2
            assert "id" in stats
            assert "value" in stats
            assert "name" not in stats
            assert "active" not in stats

        assert rules == expected_rules[table_name], f"Rules did not match expected for {table_name}"


def test_profile_tables_no_tables_or_patterns(ws):
    profiler = DQProfiler(ws)

    with pytest.raises(ValueError, match="Either 'tables' or 'patterns' must be provided"):
        profiler.profile_tables()


def test_profile_with_dataset_filter(spark, ws):
    schema = T.StructType(
        [
            T.StructField("machine_id", T.StringType(), False),
            T.StructField("maintenance_type", T.StringType(), True),
            T.StructField("maintenance_date", T.DateType(), True),
            T.StructField("cost", T.DecimalType(10, 2), True),
            T.StructField("next_scheduled_date", T.DateType(), True),
            T.StructField("safety_check_passed", T.BooleanType(), True),
        ]
    )
    maintenance_data = [
        (
            "MCH-001",
            "preventive",
            date(2025, 4, 1),
            Decimal("450.00"),
            date(2025, 7, 1),
            True,
        ),
        (
            "MCH-002",
            "corrective",
            date(2025, 4, 15),
            Decimal("1200.50"),
            date(2026, 4, 1),
            False,
        ),
        (
            "MCH-003",
            None,
            date(2025, 4, 20),
            Decimal("-500.00"),
            date(2024, 4, 20),
            None,
        ),
        (
            "MCH-001",
            "predictive",
            date(2025, 4, 25),
            Decimal("800.00"),
            date(2025, 10, 1),
            True,
        ),
        (
            "MCH-002",
            "preventive",
            date(2025, 4, 29),
            Decimal("300.50"),
            date(2025, 7, 15),
            True,
        ),
        (
            "MCH-003",
            "corrective",
            date(2025, 4, 30),
            Decimal("150.00"),
            date(2025, 8, 1),
            False,
        ),
        (
            "MCH-002",
            "preventive",
            date(2025, 5, 30),
            Decimal("150.00"),
            date(2025, 9, 1),
            True,
        ),
        (
            "MCH-002",
            "preventive",
            date(2025, 7, 30),
            Decimal("100.00"),
            date(2025, 12, 1),
            True,
        ),
    ]

    input_df = spark.createDataFrame(maintenance_data, schema=schema)

    custom_options = {
        "sample_fraction": None,
        "round":False,
        "limit": None,
        "filter": "machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
    }

    profiler = DQProfiler(ws)
    filtered_df = profiler._sample(input_df, opts=custom_options)

    stats, rules = profiler.profile(input_df, options=custom_options)

    expected_rules = [
        DQProfile(
            name="is_not_null",
            column="machine_id",
            description=None,            
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"
            
        ),
        DQProfile(
            name="is_not_null",
            column="maintenance_type",
            description=None,
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"
        ),
        DQProfile(
            name="is_not_null",
            column="maintenance_date",
            description=None,
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"
        ),
        DQProfile(
            name="min_max",
            column="maintenance_date",
            description="Real min/max values were used",
            parameters={
                "min": date(2025, 4, 29),
                "max": date(2025, 7, 30)},
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"   
            ),
        DQProfile(
            name="is_not_null",
            column="cost",
            description=None,
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"
        ),
        DQProfile(
            name="min_max",
            column="cost",
            parameters={
                "min": Decimal('100.00'),
                "max": Decimal('300.50'),
            },
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
            description="Real min/max values were used",
        ),
        DQProfile(
            name="is_not_null",
            column="next_scheduled_date",
            description=None,
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"
        ),
        DQProfile(
            name="min_max",
            column="next_scheduled_date",
            description="Real min/max values were used",
            parameters={
                "min": date(2025, 7, 15),
                "max": date(2025, 12, 1)},
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'"
        ),
        DQProfile(
            name="is_not_null",
            column="safety_check_passed",
            description=None,
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
        ),
    ]

    assert filtered_df.count() == 3
    assert len(stats.keys()) > 0
    assert rules == expected_rules
