from unittest.mock import Mock, PropertyMock

import pytest

from databricks.labs.dqx.telemetry import (
    is_dlt_pipeline,
    get_tables_from_spark_plan,
    get_paths_from_spark_plan,
    get_spark_plan_as_string,
    log_telemetry,
    log_dataframe_telemetry,
    reset_telemetry_cache,
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


def test_get_tables_handles_none_input():
    """Test that get_tables_from_spark_plan handles None input gracefully."""
    tables = get_tables_from_spark_plan(None)
    assert len(tables) == 0, "Expected empty set for None input"


def test_get_tables_handles_empty_string():
    """Test that get_tables_from_spark_plan handles empty string gracefully."""
    tables = get_tables_from_spark_plan("")
    assert len(tables) == 0, "Expected empty set for empty string"


def test_get_paths_handles_none_input():
    """Test that get_paths_from_spark_plan handles None input gracefully."""
    paths = get_paths_from_spark_plan(None)
    assert len(paths) == 0, "Expected empty set for None input"


def test_get_paths_handles_empty_string():
    """Test that get_paths_from_spark_plan handles empty string gracefully."""
    paths = get_paths_from_spark_plan("")
    assert len(paths) == 0, "Expected empty set for empty string"


def test_get_tables_handles_regex_exception():
    """Test that get_tables_from_spark_plan handles exceptions during regex processing."""
    # Create a pathological string that could cause regex issues
    # Use a very large string to potentially trigger catastrophic backtracking
    large_plan = "== Analyzed Logical Plan ==\n" + "SubqueryAlias " + "a" * 100000 + "\n=="

    # Should not raise exception, just return empty or partial results
    tables = get_tables_from_spark_plan(large_plan)
    assert isinstance(tables, set), "Expected set return type even with problematic input"


def test_get_paths_handles_malformed_location_pattern():
    """Test that get_paths_from_spark_plan handles malformed Location patterns gracefully."""
    # Malformed pattern with unbalanced brackets and special characters
    malformed_plan = """== Physical Plan ==
+- PhotonScan Location: PreparedDeltaFileIndex(1 paths)[[[unclosed
+- PhotonScan Location: FileIndex(broken)[path1, path2
+- Normal line without location
"""

    # Should not raise exception
    paths = get_paths_from_spark_plan(malformed_plan)
    assert isinstance(paths, set), "Expected set return type even with malformed input"


def test_get_tables_with_unicode_and_special_chars():
    """Test that get_tables_from_spark_plan handles unicode and special characters gracefully."""
    plan_with_unicode = """== Analyzed Logical Plan ==
SubqueryAlias 表名_with_中文
SubqueryAlias table.with.dots
SubqueryAlias table-with-dashes
SubqueryAlias table$with$dollars
SubqueryAlias table@with@symbols
+- Other content 🚀 with emojis
== Next Section ==
"""

    # Should handle gracefully and extract what it can
    tables = get_tables_from_spark_plan(plan_with_unicode)
    assert isinstance(tables, set), "Expected set return type with unicode input"
    # At least some tables should be extracted
    assert len(tables) >= 1, "Should extract at least some table names"


def test_get_paths_with_unicode_paths():
    """Test that get_paths_from_spark_plan handles unicode in paths gracefully."""
    plan_with_unicode_paths = """== Physical Plan ==
+- PhotonScan Location: PreparedDeltaFileIndex(1 paths)[/Volumes/目录/schema/卷/数据]
+- PhotonScan Location: ParquetFileIndex(1 paths)[s3://bucket/path/🚀data]
== Photon Explanation ==
"""

    # Should handle gracefully
    paths = get_paths_from_spark_plan(plan_with_unicode_paths)
    assert isinstance(paths, set), "Expected set return type with unicode paths"


# --- log_telemetry: best-effort, non-blocking, deduplicated ------------------------------------


class _FakeConfig:
    """Minimal stand-in for databricks.sdk.config.Config supporting the calls log_telemetry makes."""

    def __init__(self):
        self.user_agent_extra: list[tuple[str, str]] = []
        self.retry_timeout_seconds = None
        self.http_timeout_seconds = None

    def copy(self) -> "_FakeConfig":
        clone = _FakeConfig()
        clone.user_agent_extra = list(self.user_agent_extra)
        return clone

    def with_user_agent_extra(self, key: str, value: str) -> "_FakeConfig":
        self.user_agent_extra.append((key, value))
        return self


class _FakeClusters:
    def __init__(self, on_call):
        self._on_call = on_call

    def select_spark_version(self):
        self._on_call()


class _FakeWorkspaceClient:
    """Re-instantiable fake so `type(ws)(config=new_config)` in log_telemetry works without a network."""

    call_count = 0
    raise_exc: BaseException | None = None
    last_config: _FakeConfig | None = None

    def __init__(self, config: _FakeConfig | None = None):
        self.config = config or _FakeConfig()

    @property
    def clusters(self) -> _FakeClusters:
        return _FakeClusters(self._record_call)

    def _record_call(self):
        type(self).call_count += 1
        type(self).last_config = self.config
        exc = type(self).raise_exc
        if exc is not None:
            raise exc


@pytest.fixture(autouse=True)
def _reset_telemetry_state():
    """Reset the process-level dedup cache and fake-client state around each test."""
    reset_telemetry_cache()
    _FakeWorkspaceClient.call_count = 0
    _FakeWorkspaceClient.raise_exc = None
    _FakeWorkspaceClient.last_config = None
    yield
    reset_telemetry_cache()


def test_log_telemetry_sends_once_and_sets_bounded_timeouts():
    log_telemetry(_FakeWorkspaceClient(), "check", "is_not_null")

    assert _FakeWorkspaceClient.call_count == 1
    cfg = _FakeWorkspaceClient.last_config
    assert ("check", "is_not_null") in cfg.user_agent_extra
    # The control-plane ping must be bounded (a few seconds) so a brownout cannot stall the caller
    # for minutes; assert the behavior (a small positive timeout) rather than the exact constant.
    assert cfg.retry_timeout_seconds is not None and 0 < cfg.retry_timeout_seconds <= 60
    assert cfg.http_timeout_seconds is not None and 0 < cfg.http_timeout_seconds <= 60


def test_log_telemetry_deduplicates_same_key_value():
    ws = _FakeWorkspaceClient()
    for _ in range(5):
        log_telemetry(ws, "check", "is_not_null")

    # Same (key, value) sent five times -> control plane pinged at most once per process.
    assert _FakeWorkspaceClient.call_count == 1


def test_log_telemetry_distinct_keys_each_send_once():
    ws = _FakeWorkspaceClient()
    log_telemetry(ws, "check", "is_not_null")
    log_telemetry(ws, "check", "is_in_range")
    log_telemetry(ws, "streaming", "true")
    log_telemetry(ws, "check", "is_not_null")  # duplicate of the first

    assert _FakeWorkspaceClient.call_count == 3


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("Timed out after 0:05:00"),  # the exact SDK retry-exhaustion case from issue #1355
        RuntimeError("boom"),
        ValueError("unexpected"),
    ],
)
def test_log_telemetry_never_propagates_exceptions(exc):
    _FakeWorkspaceClient.raise_exc = exc
    # Must not raise: telemetry is best-effort and must never reach the data path.
    log_telemetry(_FakeWorkspaceClient(), "check", "is_not_null")
    assert _FakeWorkspaceClient.call_count == 1


def test_log_telemetry_failed_send_is_not_retried():
    _FakeWorkspaceClient.raise_exc = TimeoutError("Timed out")
    ws = _FakeWorkspaceClient()
    for _ in range(3):
        log_telemetry(ws, "check", "is_not_null")

    # Marked sent on attempt (not only on success), so a brownout cannot re-stall every micro-batch.
    assert _FakeWorkspaceClient.call_count == 1


def test_log_dataframe_telemetry_never_throws_when_is_streaming_raises():
    """log_dataframe_telemetry must honor its 'never throw' contract even if df.isStreaming raises
    (the failure mode from issue #1001) — the outer guard must swallow it and continue."""
    df = Mock()
    type(df).isStreaming = PropertyMock(side_effect=RuntimeError("Spark Connect: isStreaming unavailable"))

    spark = DummySparkSession(DummySparkConf(value=None))
    # Must not raise, and must not reach the control-plane ping (it fails before any log_telemetry send).
    log_dataframe_telemetry(_FakeWorkspaceClient(), spark, df)
    assert _FakeWorkspaceClient.call_count == 0


def test_log_telemetry_cache_is_bounded_and_evicts_oldest(monkeypatch):
    """The dedup cache is hard-bounded: past the cap the oldest signal is evicted (FIFO), so it can be
    re-sent later. Prevents unbounded growth for a long-lived process reading ever-new inputs."""
    monkeypatch.setattr("databricks.labs.dqx.telemetry._TELEMETRY_CACHE_MAX_SIZE", 3)
    ws = _FakeWorkspaceClient()

    # Fill to capacity with distinct signals (each pings once).
    for i in range(3):
        log_telemetry(ws, "input_table", f"h{i}")
    assert _FakeWorkspaceClient.call_count == 3

    # One more distinct signal exceeds the cap and evicts the oldest ("h0").
    log_telemetry(ws, "input_table", "h3")
    assert _FakeWorkspaceClient.call_count == 4

    # The evicted "h0" is no longer cached, so re-sending it pings again (proves eviction happened).
    log_telemetry(ws, "input_table", "h0")
    assert _FakeWorkspaceClient.call_count == 5

    # A still-cached signal ("h3") is not re-sent.
    log_telemetry(ws, "input_table", "h3")
    assert _FakeWorkspaceClient.call_count == 5
