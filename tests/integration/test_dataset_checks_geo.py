from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.geo.check_funcs import are_polygons_mutually_disjoint


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_are_polygons_mutually_disjoint_empty_df(skip_if_runtime_not_geo_compatible, spark):
    """An empty DataFrame should produce no violations."""
    input_schema = "geom: string"
    test_df = spark.createDataFrame([], input_schema)

    condition, apply_method = are_polygons_mutually_disjoint("geom")

    actual_apply_df = apply_method(df=test_df)
    actual = actual_apply_df.select("geom", condition)

    checked_schema = "geom: string, geom_not_mutually_disjoint: string"
    expected = spark.createDataFrame([], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


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

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
