import re
from datetime import date, datetime
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
    stats, rules = profiler.profile(inp_df, opts={"sample_fraction": None})

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
        DQProfile(name="is_not_null", column="b1", description=None, parameters=None),
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

    profile_options = {
        "round": False,  # do not round the min/max values
        "max_in_count": 1,  # generate is_in if we have less than 1 percent of distinct values
        "distinct_ratio": 0.01,  # generate is_distinct if we have less than 1 percent of distinct values
        "remove_outliers": False,  # do not remove outliers
        "outlier_columns": ["t1", "s1"],  # remove outliers in all columns of appropriate type
        "num_sigmas": 1,  # number of sigmas to use when remove_outliers is True
        "trim_strings": False,  # trim whitespace from strings
        "max_empty_ratio": 0.01,  # generate is_empty if we have less than 1 percent of empty strings
        "sample_fraction": 1.0,  # fraction of data to sample
        "sample_seed": None,  # seed for sampling
        "limit": 1000,  # limit the number of samples
    }

    stats, rules = profiler.profile(inp_df, cols=inp_df.columns, opts=profile_options)

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
    stats, rules = profiler.profile(input_df, opts={"sample_fraction": None})

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
    stats, rules = profiler.profile(input_df, cols=cols, opts=profiler_opts)
    stats2, rules2 = profiler.profile(input_df, cols=cols, opts=profiler_opts)

    assert len(stats.keys()) == 3
    assert len(rules) > 0
    assert stats == stats2
    assert rules == rules2


def test_profile_tables(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    # Create the tables:
    input_schema1 = "col1: int, col2: int, col3: int, col4 int"
    input_df1 = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = T.StructType(
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
    input_df2 = spark.createDataFrame(
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
        schema=input_schema2,
    )
    input_df2.write.format("delta").saveAsTable(table2_name)

    # Profile the tables:
    profiler = DQProfiler(ws)
    profiles = profiler.profile_tables(tables=[table1_name, table2_name], opts={"sample_fraction": None})

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
            DQProfile(name="is_not_null", column="t1", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="t1",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 3},
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
            DQProfile(name="is_not_null", column="b1", description=None, parameters=None),
        ],
    }
    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0
        assert rules == expected_rules[table_name]


def test_profile_tables_include_patterns(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    # Create the tables:
    input_schema1 = "col1: int, col2: int, col3: int, col4 int"
    input_df1 = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = T.StructType(
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
    input_df2 = spark.createDataFrame(
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
        schema=input_schema2,
    )
    input_df2.write.format("delta").saveAsTable(table2_name)

    # Profile the tables:
    profiles = DQProfiler(ws).profile_tables(patterns=[f".*{schema_name}.*"], opts={"sample_fraction": None})

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
            DQProfile(name="is_not_null", column="t1", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="t1",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 3},
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
            DQProfile(name="is_not_null", column="b1", description=None, parameters=None),
        ],
    }
    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0
        assert rules == expected_rules[table_name]


def test_profile_tables_exclude_patterns(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table3_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    # Create the tables:
    input_schema1 = "col1: int, col2: int, col3: int, col4 int"
    input_df1 = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1], [1, 2, 3, 4]], input_schema1)
    input_df1.write.format("delta").saveAsTable(table1_name)

    input_schema2 = T.StructType(
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
    input_df2 = spark.createDataFrame(
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
        schema=input_schema2,
    )
    input_df2.write.format("delta").saveAsTable(table2_name)

    input_schema3 = "col1: int, col2: string"
    input_df3 = spark.createDataFrame([1, "a"], [2, "b"], [3, "c"], input_schema3)
    input_df3.write.format("delta").saveAsTable(table3_name)

    # Profile the tables:
    profiles = DQProfiler(ws).profile_tables(
        patterns=[re.escape(table3_name)], exclude_matched=True, opts={"sample_fraction": None}
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
        table2_name: [
            DQProfile(name="is_not_null", column="t1", description=None, parameters=None),
            DQProfile(
                name="min_max",
                column="t1",
                description="Real min/max values were used",
                parameters={"min": 1, "max": 3},
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
            DQProfile(name="is_not_null", column="b1", description=None, parameters=None),
        ],
    }
    for table_name, (stats, rules) in profiles.items():
        assert len(stats.keys()) > 0
        assert rules == expected_rules[table_name]


def test_profile_tables_no_pattern_match(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_schema = "col1: int, col2: string"
    input_df = spark.createDataFrame([[1, "test"], [2, "data"]], input_schema)
    input_df.write.format("delta").saveAsTable(table1_name)

    no_match_pattern = "nonexistent_catalog\\..*"
    profiler = DQProfiler(ws)
    with pytest.raises(ValueError, match="No tables found matching include or exclude criteria"):
        profiler.profile_tables(patterns=[no_match_pattern])
