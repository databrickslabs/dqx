from databricks.labs.dqx.telemetry import (
    is_dlt_pipeline,
    get_tables_from_spark_plan,
    get_paths_from_spark_plan,
    get_spark_plan_as_string,
)


class DummySparkConf:
    def __init__(self, value=None, raise_exc=False):
        self.value = value
        self.raise_exc = raise_exc

    def get(self, key, default=None):
        if self.raise_exc:
            raise RuntimeError("Config error")
        if key == "pipelines.id":
            return self.value
        return default


class DummySparkSession:
    def __init__(self, conf):
        self.conf = conf


class MockDataFrameWithPlan:
    """Mock DataFrame that prints a specific plan when explain() is called."""

    def __init__(self, plan_output):
        self.plan_output = plan_output

    def explain(self, _extended=False):
        print(self.plan_output, end='')


class MockDataFrameWithException:
    """Mock DataFrame that raises an exception when explain() is called."""

    def __init__(self, exception):
        self.exception = exception

    def explain(self, extended=False):
        raise self.exception


def test_is_dlt_pipeline_true():
    dummy_spark = DummySparkSession(DummySparkConf(value="dlt-123"))
    assert is_dlt_pipeline(dummy_spark) is True


def test_is_dlt_pipeline_false_missing_id():
    dummy_spark = DummySparkSession(DummySparkConf(value=None))
    assert is_dlt_pipeline(dummy_spark) is False


def test_is_dlt_pipeline_false_exception():
    dummy_spark = DummySparkSession(DummySparkConf(raise_exc=True))
    assert is_dlt_pipeline(dummy_spark) is False


def test_get_tables_with_missing_analyzed_plan_section():
    """Test that get_tables_from_spark_plan returns 0 when plan has no Analyzed Logical Plan section."""
    # Create a plan string without the "== Analyzed Logical Plan ==" section
    plan_without_analyzed_section = """== Physical Plan ==
Some physical plan details here
== Optimized Logical Plan ==
Some optimized plan details here
"""
    tables = get_tables_from_spark_plan(plan_without_analyzed_section)

    assert len(tables) == 0, f"Expected 0 tables when Analyzed Logical Plan section is missing, but found {len(tables)}"


def test_get_spark_plan_exception():
    """Test that get_tables_from_spark_plan returns 0 when df.explain() raises an exception."""
    mock_df = MockDataFrameWithException(RuntimeError("Failed to explain plan"))

    tables = get_spark_plan_as_string(mock_df)

    assert len(tables) == 0, f"Expected 0 tables when explain() raises exception, but found {len(tables)}"


def test_get_paths_with_single_path():
    """Test that get_paths_from_spark_plan extracts a single path from Physical Plan."""
    plan_with_single_path = """== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [vendor_id#1080600] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[/Volumes/catalog/schema/volume/nyctaxi_2019], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<vendor_id:string>

== Photon Explanation ==
"""

    paths = get_paths_from_spark_plan(plan_with_single_path)

    assert len(paths) == 1, f"Expected 1 path, but found {len(paths)}"
    assert "/Volumes/catalog/schema/volume/nyctaxi_2019" in paths


def test_get_paths_with_multiple_paths():
    """Test that get_paths_from_spark_plan extracts multiple paths from Physical Plan."""
    plan_with_multiple_paths = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   ColumnarToRow
   +- PhotonResultStage
      +- PhotonProject [vendor_id#1081424]
         +- PhotonShuffledHashJoin [vendor_id#1081424], [vendor_id#1081478], Inner, BuildLeft
            :- PhotonShuffleExchangeSource
            :  +- PhotonShuffleMapStage
            :     +- PhotonShuffleExchangeSink hashpartitioning(vendor_id#1081424, 200)
            :        +- PhotonScan parquet [vendor_id#1081424] DataFilters: [isnotnull(vendor_id#1081424)], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[/Volumes/catalog/schema/volume1/nyctaxi_2019], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<vendor_id:string>
            +- PhotonShuffleExchangeSource
               +- PhotonShuffleMapStage
                  +- PhotonShuffleExchangeSink hashpartitioning(vendor_id#1081478, 200)
                     +- PhotonScan parquet users.marcin_wojtyczka.nyctaxi[vendor_id#1081478] DataFilters: [isnotnull(vendor_id#1081478)], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b1], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<vendor_id:string>

== Photon Explanation ==
"""

    paths = get_paths_from_spark_plan(plan_with_multiple_paths)

    assert len(paths) == 2, f"Expected 2 paths, but found {len(paths)}"
    assert "/Volumes/catalog/schema/volume1/nyctaxi_2019" in paths
    assert "s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b1" in paths


def test_get_paths_with_missing_physical_plan_section():
    """Test that get_paths_from_spark_plan returns 0 when plan has no Physical Plan section."""
    plan_without_physical_section = """== Analyzed Logical Plan ==
Some analyzed plan details here
== Optimized Logical Plan ==
Some optimized plan details here
"""

    paths = get_paths_from_spark_plan(plan_without_physical_section)

    assert len(paths) == 0, f"Expected 0 paths when Physical Plan section is missing, but found {len(paths)}"


def test_get_paths_with_no_location_patterns():
    """Test that get_paths_from_spark_plan returns 0 when Physical Plan has no Location patterns."""
    plan_without_locations = """== Physical Plan ==
*(1) ColumnarToRow
+- Some plan without Location patterns
== Photon Explanation ==
"""

    paths = get_paths_from_spark_plan(plan_without_locations)

    assert len(paths) == 0, f"Expected 0 paths when no Location patterns found, but found {len(paths)}"


def test_get_paths_with_different_fileindex_types():
    """Test that get_paths_from_spark_plan extracts paths from different FileIndex types."""
    plan_with_various_fileindex = """== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [id#100] DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/data/dataset1], ReadSchema: struct<id:int>
   +- PhotonScan parquet [id#200] DataFilters: [], Format: parquet, Location: ParquetFileIndex(1 paths)[s3://bucket/path/dataset2], ReadSchema: struct<id:int>
   +- PhotonScan parquet [id#300] DataFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[/Volumes/catalog/schema/volume/dataset3], ReadSchema: struct<id:int>

== Photon Explanation ==
"""

    paths = get_paths_from_spark_plan(plan_with_various_fileindex)

    assert len(paths) == 3, f"Expected 3 paths from different FileIndex types, but found {len(paths)}"
    assert "file:/tmp/data/dataset1" in paths, "Expected to find InMemoryFileIndex path"
    assert "s3://bucket/path/dataset2" in paths, "Expected to find ParquetFileIndex path"
    assert "/Volumes/catalog/schema/volume/dataset3" in paths, "Expected to find PreparedDeltaFileIndex path"


def test_get_paths_excludes_table_paths():
    """Test that get_paths_from_spark_plan excludes paths associated with tables when requested."""
    plan_with_table_and_file = """== Analyzed Logical Plan ==
vendor_id: string, pickup_datetime: timestamp
Project [vendor_id#1081424, pickup_datetime#1081425]
+- Join Inner, (vendor_id#1081424 = vendor_id#1081478)
   :- Relation [vendor_id#1081424,pickup_datetime#1081425] parquet
   +- SubqueryAlias users.marcin_wojtyczka.nyctaxi
      +- Relation users.marcin_wojtyczka.nyctaxi[vendor_id#1081478,pickup_datetime#1081479] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- PhotonProject [vendor_id#1081424, pickup_datetime#1081425]
   +- PhotonShuffledHashJoin [vendor_id#1081424], [vendor_id#1081478], Inner, BuildLeft
      :- PhotonScan parquet [vendor_id#1081424] DataFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[/Volumes/catalog/schema/volume/nyctaxi_2019], ReadSchema: struct<vendor_id:string>
      +- PhotonScan parquet users.marcin_wojtyczka.nyctaxi[vendor_id#1081478] DataFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b1], ReadSchema: struct<vendor_id:string>

== Photon Explanation ==
"""

    table_names = {"users.marcin_wojtyczka.nyctaxi"}

    # Without excluding table paths
    paths_all = get_paths_from_spark_plan(plan_with_table_and_file)
    assert len(paths_all) == 2, f"Expected 2 paths without exclusion, but found {len(paths_all)}"

    # With excluding table paths
    paths_filtered = get_paths_from_spark_plan(plan_with_table_and_file, table_names)
    assert len(paths_filtered) == 1, f"Expected 1 path with table exclusion, but found {len(paths_filtered)}"
    assert "/Volumes/catalog/schema/volume/nyctaxi_2019" in paths_filtered, "Expected to find the file-based path"
    assert (
        "s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b1" not in paths_filtered
    ), "Table path should be excluded"
