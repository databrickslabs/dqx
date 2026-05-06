import pytest
from pyspark.testing.utils import assertDataFrameEqual
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.geo.check_funcs import are_polygons_mutually_disjoint, is_within_polygon_precise


def test_are_polygons_mutually_disjoint_pass(skip_if_runtime_not_geo_compatible, spark):

    input_schema = "geom: string"
    # Three completely disjoint polygons
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],  # Square 1: [0,1]x[0,1]
            ["POLYGON((2 0, 3 0, 3 1, 2 1, 2 0))"],  # Square 2: [2,3]x[0,1]
            ["POLYGON((0 2, 1 2, 1 3, 0 3, 0 2))"],  # Square 3: [0,1]x[2,3]
            ["not-a-polygon"],  # Invalid geometry should be ignored
            [None],  # Should ignore nulls
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", None],
            ["POLYGON((2 0, 3 0, 3 1, 2 1, 2 0))", None],
            ["POLYGON((0 2, 1 2, 1 3, 0 3, 0 2))", None],
            ["not-a-polygon", None],
            [None, None],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_fail(skip_if_runtime_not_geo_compatible, spark):

    input_schema = "geom: string"
    # Polygons where two intersect
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],  # Square 1: [0,2]x[0,2]
            ["POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))"],  # Square 2: [1,3]x[1,3] (Intersects Square 1)
            ["POLYGON((4 4, 5 4, 5 5, 4 5, 4 4))"],  # Square 3: [4,5]x[4,5] (Disjoint)
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [
                "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))",
                "value `POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "value `POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))` in column `geom` intersects with at least one other polygon",
            ],
            ["POLYGON((4 4, 5 4, 5 5, 4 5, 4 4))", None],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_touching(skip_if_runtime_not_geo_compatible, spark):
    """Polygons sharing an edge should be flagged as intersecting because they share points in common."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],  # Square 1: [0,1]x[0,1]
            ["POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))"],  # Square 2: [1,2]x[0,1] (shares edge x=1)
            ["POLYGON((0 1, 1 1, 1 2, 0 2, 0 1))"],  # Square 3: [0,1]x[1,2] (shares edge y=1 with Square 1)
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "value `POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))",
                "value `POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "POLYGON((0 1, 1 1, 1 2, 0 2, 0 1))",
                "value `POLYGON((0 1, 1 1, 1 2, 0 2, 0 1))` in column `geom` intersects with at least one other polygon",
            ],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_with_row_filter(skip_if_runtime_not_geo_compatible, spark):
    """When row_filter is applied, only matching rows should be evaluated for intersection."""
    input_schema = "category: string, geom: string"
    test_df = spark.createDataFrame(
        [
            ["A", "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],  # Intersects with row 2
            ["A", "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))"],  # Intersects with row 1
            ["B", "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],  # Would intersect with row 1 and 2, but filtered out
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom", row_filter="category = 'A'")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("category", "geom", condition)

    checked_schema = "category: string, geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [
                "A",
                "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))",
                "value `POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "A",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "value `POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))` in column `geom` intersects with at least one other polygon",
            ],
            ["B", "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))", None],  # Not evaluated due to filter
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_single_polygon(skip_if_runtime_not_geo_compatible, spark):
    """A single valid polygon cannot intersect with anything."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", None],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_empty_df(skip_if_runtime_not_geo_compatible, spark):
    """An empty DataFrame should produce no violations."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame([], input_schema)

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame([], checked_schema)

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_all_overlap(skip_if_runtime_not_geo_compatible, spark):
    """When all polygons overlap each other, all should be flagged."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))"],  # Overlaps with all
            ["POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))"],  # Overlaps with all
            ["POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))"],  # Overlaps with all
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [
                "POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))",
                "value `POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))",
                "value `POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))",
                "value `POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))` in column `geom` intersects with at least one other polygon",
            ],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_duplicates(skip_if_runtime_not_geo_compatible, spark):
    """Two identical polygons should both be flagged as intersecting."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],  # Identical to row 1
        ],
        input_schema,
    )

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "value `POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "value `POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_row_filter_no_violations(skip_if_runtime_not_geo_compatible, spark):
    """Filter out overlapping rows so that no violations remain."""
    input_schema = "category: string, geom: string"
    test_df = spark.createDataFrame(
        [
            ["A", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
            ["B", "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Overlaps with A
            ["A", "POLYGON((2 0, 3 0, 3 1, 2 1, 2 0))"],
        ],
        input_schema,
    )

    # Only rows with category='A' are considered; they are disjoint.
    condition, apply_method = are_polygons_mutually_disjoint("geom", row_filter="category = 'A'")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("category", "geom", condition)

    checked_schema = "category: string, geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            ["A", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", None],
            ["B", "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))", None],
            ["A", "POLYGON((2 0, 3 0, 3 1, 2 1, 2 0))", None],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_row_filter_complex(skip_if_runtime_not_geo_compatible, spark):
    """Use complex filter logic (multiple conditions)."""
    input_schema = "id: int, active: boolean, geom: string"
    test_df = spark.createDataFrame(
        [
            [1, True, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
            [2, False, "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Overlaps with 1
            [3, True, "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Overlaps with 1
        ],
        input_schema,
    )

    # Filter: active = True AND id > 1 -> only row 3. Single row always passes.
    condition, apply_method = are_polygons_mutually_disjoint("geom", row_filter="active = True AND id > 1")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("id", "geom", condition)

    checked_schema = "id: int, geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [1, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", None],
            [2, "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))", None],
            [3, "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))", None],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_row_filter_numeric(skip_if_runtime_not_geo_compatible, spark):
    """Use numeric comparison in row_filter."""
    input_schema = "val: double, geom: string"
    test_df = spark.createDataFrame(
        [
            [10.0, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],
            [20.0, "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))"],  # Overlaps with 10.0
            [5.0, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],  # Also overlaps
        ],
        input_schema,
    )

    # Filter: val >= 10.0 -> rows with 10.0 and 20.0. They overlap.
    condition, apply_method = are_polygons_mutually_disjoint("geom", row_filter="val >= 10.0")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("val", "geom", condition)

    checked_schema = "val: double, geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [
                10.0,
                "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))",
                "value `POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))` in column `geom` intersects with at least one other polygon",
            ],
            [
                20.0,
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "value `POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))` in column `geom` intersects with at least one other polygon",
            ],
            [5.0, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))", None],
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


def test_are_polygons_mutually_disjoint_via_engine_api(skip_if_runtime_not_geo_compatible, spark, ws: WorkspaceClient):
    """Test through the DQX engine API with metadata-based rule definition."""
    input_schema = "id: int, geom: string"
    test_df = spark.createDataFrame(
        [
            [1, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],  # Intersects with id=2
            [2, "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))"],  # Intersects with id=1
            [3, "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))"],  # Disjoint
        ],
        input_schema,
    )

    engine = DQEngine(ws)
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "are_polygons_mutually_disjoint",
                "arguments": {"column": "geom"},
            },
        }
    ]

    checked_df = engine.apply_checks_by_metadata(test_df, checks)

    # Verify _errors column was added with correct violations
    assert "_errors" in checked_df.columns
    error_rows = checked_df.filter("_errors is not null").select("id", "geom", "_errors").collect()
    assert len(error_rows) == 2
    assert {row.id for row in error_rows} == {1, 2}

    # Verify id=3 has no error
    clean_rows = checked_df.filter("_errors is null").select("id").collect()
    assert [row.id for row in clean_rows] == [3]


def test_are_polygons_mutually_disjoint_multipartition_determinism(skip_if_runtime_not_geo_compatible, spark):
    """Test determinism with multiple partitions to ensure hash-based IDs are stable."""
    input_schema = "id: int, category: string, geom: string"
    # Create test data with deliberately varied polygon coordinates
    test_df = spark.createDataFrame(
        [
            [1, "A", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
            [2, "A", "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Intersects 1
            [3, "B", "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"],
            [4, "B", "POLYGON((2.5 2.5, 3.5 2.5, 3.5 3.5, 2.5 3.5, 2.5 2.5))"],  # Intersects 3
            [5, "C", "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))"],  # Disjoint from all
        ],
        input_schema,
    ).repartition(
        5
    )  # Force 5 partitions to increase chance of re-evaluation

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_df = apply_method(df=test_df)
    actual = actual_df.select("id", "category", condition).collect()

    # Expected violations: ids 1,2 and 3,4
    violations = {row.id for row in actual if getattr(row, list(row.__fields__)[-1]) is not None}
    assert violations == {1, 2, 3, 4}, f"Expected violations for ids 1,2,3,4 but got {violations}"

    # id=5 should not be flagged
    clean = {row.id for row in actual if getattr(row, list(row.__fields__)[-1]) is None}
    assert 5 in clean


def test_are_polygons_mutually_disjoint_row_filter_with_partitions(skip_if_runtime_not_geo_compatible, spark):
    """Test row_filter respects partition boundaries correctly."""
    input_schema = "category: string, geom: string"
    test_df = spark.createDataFrame(
        [
            ["A", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
            ["A", "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Intersects A1
            ["B", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],  # Same geom as A1 but different category
            ["B", "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Same as A2
            ["C", "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))"],
        ],
        input_schema,
    ).repartition(3)

    # Only evaluate category='A' rows
    condition, apply_method = are_polygons_mutually_disjoint("geom", row_filter="category = 'A'")

    actual_df = apply_method(df=test_df)
    actual = actual_df.select("category", condition).collect()

    # Count violations per category
    violations_by_category = {}
    for row in actual:
        cat = row.category
        has_error = getattr(row, list(row.__fields__)[-1]) is not None
        violations_by_category[cat] = violations_by_category.get(cat, 0) + (1 if has_error else 0)

    # A: 2 violations (A1 and A2 intersect)
    # B: 0 violations (filtered out, so not evaluated)
    # C: 0 violations (disjoint)
    assert violations_by_category.get("A", 0) == 2
    assert violations_by_category.get("B", 0) == 0
    assert violations_by_category.get("C", 0) == 0


def test_are_polygons_mutually_disjoint_large_partition_count(skip_if_runtime_not_geo_compatible, spark):
    """Stress test with many small partitions to trigger re-evaluation edge cases."""
    input_schema = "id: int, geom: string"
    rows = [
        [1, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"],  # Overlaps with id=2
        [2, "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))"],  # Overlaps with id=1
        [3, "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))"],  # Disjoint
        [4, "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))"],  # Disjoint
        [5, "POLYGON((15 15, 16 15, 16 16, 15 16, 15 15))"],  # Disjoint
    ]

    test_df = spark.createDataFrame(rows, input_schema).repartition(10)  # Many small partitions

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_df = apply_method(df=test_df)
    actual = actual_df.select("id", condition).collect()

    violations = {row.id for row in actual if getattr(row, list(row.__fields__)[-1]) is not None}
    assert violations == {1, 2}, f"Expected ids 1 and 2 to violate, got {violations}"


def test_are_polygons_mutually_disjoint_row_filter_with_duplicates(skip_if_runtime_not_geo_compatible, spark):
    """Verify that row_filter properly excludes rows from the polygon intersection evaluation.

    Rows that fail the filter should not be evaluated for intersections, even if they
    would form part of an intersection pair in the unfiltered dataset.
    """
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],  # Row 1: would intersect with intersecting polygons
            ["POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"],  # Row 2: intersects with row 1
        ],
        input_schema,
    )

    # Filter to include only the first polygon (excludes the intersecting one)
    condition, apply_method = are_polygons_mutually_disjoint(
        "geom", row_filter="geom = 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'"
    )

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", None],  # Single row after filter, no violations
            ["POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))", None],  # Filtered out, not evaluated
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)


@pytest.mark.parametrize(
    "column_value,column_type,column_representation,reference_polygon_value,reference_polygon_type,reference_polygon_representation",
    [
        (
            "POINT(4.90 52.37)",
            "GEOMETRY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "WKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOMETRY",
            "WKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "EWKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOMETRY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "WKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOMETRY",
            "WKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "EWKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOMETRY",
            "EWKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "WKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOMETRY",
            "EWKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "EWKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOMETRY",
            "EWKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "WKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOMETRY",
            "EWKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "EWKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "WKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "EWKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "WKT",
        ),
        (
            "POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "EWKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "WKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "EWKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "WKT",
        ),
        (
            "SRID=4326;POINT(4.90 52.37)",
            "GEOGRAPHY",
            "WKT",
            "SRID=4326;POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "EWKT",
        ),
    ],
)
def test_is_within_polygon_precise_inside(
    column_value,
    column_type,
    column_representation,
    reference_polygon_value,
    reference_polygon_type,
    reference_polygon_representation,
    skip_if_runtime_not_geo_compatible,
    spark,
):
    """Test for `is_within_polygon_precise` function when column point is inside a polygon."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame([[column_value], [None]], input_schema)

    # Filter to include only the first polygon (excludes the intersecting one)
    condition, apply_method = is_within_polygon_precise(
        "geom",
        reference_polygon_value,
        column_type,
        column_representation,
        reference_polygon_type,
        reference_polygon_representation,
        topological_relationship="WITHIN",
    )

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame(
        [
            [column_value, None],  # Single row after filter, no violations
            [None, None],  # Evaluation skipped due to the null value
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)

@pytest.mark.parametrize(
    "column_value,column_type,column_representation,reference_polygon_value,reference_polygon_type,reference_polygon_representation",
    [
        (
            "POINT(4.73 52.28)",
            "GEOMETRY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "WKT",
        ),
        (
            "POINT(4.73 52.28)",
            "GEOGRAPHY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "WKT",
        )
    ],
)
def test_is_within_polygon_precise_edge(
    column_value,
    column_type,
    column_representation,
    reference_polygon_value,
    reference_polygon_type,
    reference_polygon_representation,
    skip_if_runtime_not_geo_compatible,
    spark,
):
    """Test for `is_within_polygon_precise` function when column point on an edge of polygon."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame([[column_value], [None]], input_schema)

    # Filter to include only the first polygon (excludes the intersecting one)
    condition, apply_method = is_within_polygon_precise(
        "geom",
        reference_polygon_value,
        column_type, # noqa
        column_representation, # noqa
        reference_polygon_type, # noqa
        reference_polygon_representation, # noqa
        topological_relationship="WITHIN",
    )

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string"
    expected = spark.createDataFrame(
        [
            [column_value, None],  # Single row after filter, no violations
            [None, None],  # Evaluation skipped due to the null value
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)

@pytest.mark.parametrize(
    "column_value,column_type,column_representation,reference_polygon_value,reference_polygon_type,reference_polygon_representation",
    [
        (
            "POINT(4.48 51.92)",
            "GEOMETRY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOMETRY",
            "WKT",
        ),
        (
            "POINT(4.48 51.92)",
            "GEOGRAPHY",
            "WKT",
            "POLYGON((4.73 52.28, 5.05 52.28, 5.05 52.43, 4.73 52.43, 4.73 52.28))",
            "GEOGRAPHY",
            "WKT",
        )
    ],
)
def test_is_within_polygon_precise_outside(
    column_value,
    column_type,
    column_representation,
    reference_polygon_value,
    reference_polygon_type,
    reference_polygon_representation,
    skip_if_runtime_not_geo_compatible,
    spark,
):
    """Test for `is_within_polygon_precise` function when column outside a polygon."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame([[column_value], [None]], input_schema)

    # Filter to include only the first polygon (excludes the intersecting one)
    condition, apply_method = is_within_polygon_precise(
        "geom",
        reference_polygon_value,
        column_type,  # noqa
        column_representation,  # noqa
        reference_polygon_type,  # noqa
        reference_polygon_representation,  # noqa
        topological_relationship="WITHIN",
    )

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string"
    expected = spark.createDataFrame(
        [
            [column_value, None],  # Single row after filter, no violations
            [None, None],  # Evaluation skipped due to the null value
        ],
        checked_schema,
    )

    assertDataFrameEqual(actual, expected, checkRowOrder=False)
