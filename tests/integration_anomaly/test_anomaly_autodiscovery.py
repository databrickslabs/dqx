"""Integration tests for auto-discovery of anomaly detection columns and segments."""

import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly.profiler import auto_discover_columns, suggest_baseline_columns
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.constants import SEGMENT_REGIONS
from tests.integration_anomaly.conftest import qualify_model_name

#: A Monday, so the weekday and weekend calendar features the advisory warns about are meaningful.
START = datetime.datetime(2025, 1, 6)


def test_auto_discover_numeric_columns(spark: SparkSession):
    """Test auto-discovery selects numeric columns with variance."""

    df = spark.createDataFrame(
        [
            (1, 100.0, 50.0, "2024-01-01", "id123"),
            (2, 101.0, 51.0, "2024-01-02", "id456"),
            (3, 102.0, 52.0, "2024-01-03", "id789"),
            (4, 103.0, 53.0, "2024-01-04", "id101"),
        ],
        "row_id int, amount double, discount double, date_col string, user_id string",
    )

    profile = auto_discover_columns(df)

    # Should select amount and discount (numeric with variance)
    # Should exclude: row_id (pattern match), date_col (not numeric), user_id (pattern match)
    assert "amount" in profile.recommended_columns
    assert "discount" in profile.recommended_columns
    assert "row_id" not in profile.recommended_columns
    assert "user_id" not in profile.recommended_columns


def test_auto_discover_segments(spark: SparkSession):
    """Test auto-discovery identifies categorical columns for segmentation."""
    # Create data with good segment candidates
    data = []
    for region in SEGMENT_REGIONS:
        for i in range(1500):  # >1000 rows per segment
            data.append((region, 100.0 + i))

    df = spark.createDataFrame(data, "region string, amount double")

    profile = auto_discover_columns(df)

    # Should select region as segment (3 distinct values, >1000 rows per segment)
    assert "region" in profile.recommended_segments
    assert profile.segment_count == 3


def test_auto_discover_segments_uses_exact_distinct(spark: SparkSession):
    """Ensure segment counts use exact distinct values (4 regions -> segment_count 4)."""
    data = []
    for region in ("North", "South", "East", "West"):
        for i in range(1200):
            data.append((region, 100.0 + i))

    df = spark.createDataFrame(data, "region string, amount double")

    profile = auto_discover_columns(df)

    assert "region" in profile.recommended_segments
    assert profile.segment_count == 4


def test_auto_discover_excludes_high_cardinality(spark: SparkSession):
    """Test that high-cardinality columns are excluded from segmentation."""
    # Create data with too many distinct values
    data = [(f"category_{i}", 100.0 + i) for i in range(100)]
    df = spark.createDataFrame(data, "category string, amount double")

    profile = auto_discover_columns(df)

    # Should not recommend category for segmentation (high cardinality)
    assert "category" not in profile.recommended_segments
    # Should warn about high cardinality (excludes columns with "id" in name)
    assert any("category" in w for w in profile.warnings)


def test_zero_config_training(spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Zero-config training discovers the metrics *and* a grouping, and conditions on it.

    A discovered grouping produces **one** model that judges each metric against its own group's
    baseline, not one model per group. It used to produce one per group, which is the configuration
    that measured worst on the Server Machine Dataset — worse than pooling, with one entity emitting
    15,963 false positives on 28,392 normal rows — while also being the only configuration whose
    cost grows with group count. See databrickslabs/dqx#1484.
    """
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Create data with clear numeric and segment columns
    data = []
    for region in ("US", "EU"):
        for i in range(200):
            base = 100 if region == "US" else 200
            data.append((region, base + i * 0.5, base * 0.8 + i * 0.3))

    df = spark.createDataFrame(data, "region string, amount double, discount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.auto_train_test_{suffix}"
    df.write.saveAsTable(table_name)

    # Train with zero config (should auto-discover columns and segments)
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    model_uri = anomaly_engine.train(
        df=spark.table(table_name),
        model_name=qualify_model_name(f"test_auto_{suffix}", registry_table),
        registry_table=registry_table,
    )

    # Verify models were created
    assert model_uri is not None

    registry = spark.table(registry_table)
    models = registry.filter("identity.status = 'active'").collect()

    # One conditioned model, not one per region: the discovered grouping becomes baseline_by.
    assert len(models) == 1
    assert models[0].grouping.baseline_by == ["region"]

    # Verify auto-discovered columns (amount and discount)
    for model in models:
        assert set(model.training.columns) == {"amount", "discount"}


def test_explicit_columns_keep_the_pooled_comparison_but_warn(
    spark: SparkSession, make_schema, make_random, anomaly_engine, caplog
):
    """Naming the feature columns keeps the whole-table comparison, and says so.

    Explicit configuration wins: DQX does not add engineered baseline features the caller never
    asked for, because a discovered grouping is data-dependent and a retrain after a cardinality
    shift would silently move every score. What it does instead is name the grouping it found, so
    the caller can opt in. Silence was the actual defect here, not the pooling.
    """
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # region is a strong baseline candidate (2 values, 200 rows each)
    data = []
    for region in ("US", "EU"):
        for i in range(200):
            data.append((region, 100.0 + i))

    df = spark.createDataFrame(data, "region string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.explicit_cols_test_{suffix}"
    df.write.saveAsTable(table_name)

    # Explicit feature column, no explicit grouping: pooled, with an advisory naming what it found.
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.anomaly.training_service"):
        anomaly_engine.train(
            df=spark.table(table_name),
            columns=["amount"],
            model_name=qualify_model_name(f"test_explicit_{suffix}", registry_table),
            registry_table=registry_table,
        )

    registry = spark.table(registry_table)
    models = registry.filter("identity.status = 'active'").collect()
    assert len(models) == 1
    assert models[0].training.columns == ["amount"]
    assert not models[0].grouping.baseline_by

    advisories = [r.message for r in caplog.records if "looks like a grouping" in r.message]
    assert advisories, "an explicit-columns train over groupable data should name the grouping"
    # The message has to carry the fix, not just the diagnosis, or the caller has to go and read docs.
    assert "region" in advisories[0]
    assert "baseline_by=['region']" in advisories[0]
    assert "baseline_by=[]" in advisories[0]


def test_no_advisory_when_the_data_has_no_grouping(
    spark: SparkSession, make_schema, make_random, anomaly_engine, caplog
):
    """The advisory stays quiet when there is nothing to act on.

    This is what keeps it worth reading: a warning that fires on every explicit-columns call is one
    callers learn to filter out, and then it is worth nothing when it does matter.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Numeric metrics only, so there is no categorical column to group on.
    df = spark.createDataFrame([(100.0 + i, 5.0 + i % 7) for i in range(400)], "amount double, discount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.no_grouping_{suffix}"
    df.write.saveAsTable(table_name)

    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.anomaly.training_service"):
        anomaly_engine.train(
            df=spark.table(table_name),
            columns=["amount", "discount"],
            model_name=qualify_model_name(f"test_nogroup_{suffix}", registry_table),
            registry_table=registry_table,
        )

    assert not [r for r in caplog.records if "looks like a grouping" in r.message]


def test_suggestion_never_offers_a_column_the_caller_measures(spark: SparkSession):
    """A column cannot be both what is measured and what it is measured against.

    ``validate_baseline_columns`` would reject the overlap anyway, so suggesting it would be advice
    that fails if taken.
    """
    df = spark.createDataFrame(
        [("US", "retail", 100.0 + i) for i in range(300)] + [("EU", "retail", 100.0 + i) for i in range(300)],
        "region string, channel string, amount double",
    )

    suggestion = suggest_baseline_columns(df, exclude=["region"])

    assert suggestion is None or "region" not in suggestion.columns


def test_suggestion_reports_the_shape_the_warning_quotes(spark: SparkSession):
    """The advisory quotes a group count and rows per group, so those have to be right."""
    df = spark.createDataFrame(
        [(region, 100.0 + i) for region in ("US", "EU", "APAC") for i in range(200)],
        "region string, amount double",
    )

    suggestion = suggest_baseline_columns(df, exclude=["amount"])

    assert suggestion is not None
    assert suggestion.columns == ["region"]
    assert suggestion.group_count == 3
    assert suggestion.rows_per_group == 200


def test_autodiscovery_excludes_high_null_numeric_columns(spark: SparkSession):
    """Test that numeric columns with ≥50% nulls are excluded (lines 82, 90)."""
    # Create DataFrame with varying null rates
    df = spark.createDataFrame(
        [
            (100.0, None, 50.0),
            (101.0, None, None),
            (102.0, None, None),
            (None, 1.0, None),
        ],
        "amount double, high_null_col double, very_high_null double",
    )

    profile = auto_discover_columns(df)

    # amount has 25% nulls and variance - should be selected
    assert "amount" in profile.recommended_columns

    # high_null_col has 50% nulls (exactly at threshold) - should be excluded
    assert "high_null_col" not in profile.recommended_columns

    # very_high_null has 75% nulls - should be excluded
    assert "very_high_null" not in profile.recommended_columns


def test_autodiscovery_with_boolean_columns(spark: SparkSession):
    """Test boolean column analysis with various null rates (lines 106-108)."""
    # Create DataFrame with boolean columns at different null rates
    data = []
    for i in range(100):
        is_active = i % 2 == 0
        high_null_bool = None if i < 60 else (i % 2 == 0)  # 60% nulls
        data.append((is_active, high_null_bool, 100.0 + i))

    df = spark.createDataFrame(data, "is_active boolean, high_null_bool boolean, amount double")

    profile = auto_discover_columns(df)

    # is_active has 0% nulls - should be selected
    assert "is_active" in profile.recommended_columns
    assert profile.column_types is not None
    assert profile.column_types["is_active"] == "boolean"

    # high_null_bool has 60% nulls - should be excluded
    assert "high_null_bool" not in profile.recommended_columns


def test_autodiscovery_with_datetime_columns(spark: SparkSession):
    """Test datetime column analysis with various null rates (lines 116-118)."""
    # Create DataFrame with datetime columns at different null rates
    data = []
    for i in range(100):
        event_date = f"2024-01-{(i % 28) + 1:02d}"
        high_null_date = None if i < 60 else f"2024-01-{(i % 28) + 1:02d}"  # 60% nulls
        data.append((event_date, high_null_date, 100.0 + i))

    df = spark.createDataFrame(data, "event_date string, high_null_date string, amount double")
    df = df.withColumn("event_date", F.to_date(F.col("event_date"))).withColumn(
        "high_null_date", F.to_date(F.col("high_null_date"))
    )

    profile = auto_discover_columns(df)

    # event_date has 0% nulls - should be selected
    assert "event_date" in profile.recommended_columns
    assert profile.column_types is not None
    assert profile.column_types["event_date"] == "datetime"

    # high_null_date has 60% nulls - should be excluded
    assert "high_null_date" not in profile.recommended_columns


def test_autodiscovery_with_various_cardinality_strings(spark: SparkSession):
    """Test string column analysis with low/medium/high cardinality (lines 130-148)."""
    # 1,000 rows so `category` clears the *effective* rows-per-group floor. Discovery runs before sampling,
    # so a candidate needs MIN_ROWS_PER_BASELINE_GROUP / (sample_fraction * train_ratio) rows per group on the
    # full table for the fit to see the minimum it promises. At 200 rows this fixture gave `category` 40 rows
    # per group, which is 9.6 by the time anything is fitted, so it is correctly no longer a grouping
    # candidate. 1,000 rows gives it 200, and ~48 after sampling.
    data = []
    for i in range(1000):
        data.append(
            (
                f"cat_{i % 5}",  # Low cardinality (5 distinct), 200 rows/group
                f"user_{i % 50}",  # Medium cardinality (50 distinct)
                f"tx_{i}",  # High cardinality (1000 distinct) - avoid "id" pattern
                100.0 + i,
            )
        )

    df = spark.createDataFrame(data, "category string, user_code string, transaction_ref string, amount double")

    profile = auto_discover_columns(df)

    # category has 5 distinct values - low cardinality makes it a baseline grouping column,
    # not a feature. Under the baseline-aware policy it is routed to recommended_segments and
    # removed from recommended_columns (baseline columns are not features).
    assert "category" in profile.recommended_segments
    assert "category" not in profile.recommended_columns
    assert profile.column_types is not None
    assert profile.column_types["category"] == "categorical"

    # user_code has 50 distinct values - stays a feature: as a second baseline column it would give
    # 5 * 50 = 250 groups over 1,000 rows, 4 rows each, far under the floor, so it is not selected.
    assert "user_code" in profile.recommended_columns
    assert profile.column_types["user_code"] == "categorical"

    # transaction_ref has 1000 distinct values (>100) - should be excluded with warning (lines 144-148)
    assert "transaction_ref" not in profile.recommended_columns
    warnings_text = " ".join(profile.warnings)
    assert "transaction_ref" in warnings_text
    assert "200 distinct values" in warnings_text
    assert "too high cardinality" in warnings_text

    # amount should be selected as numeric
    assert "amount" in profile.recommended_columns
    assert profile.column_types["amount"] == "numeric"


def test_autodiscovery_with_many_columns(spark):
    """Test column selection limit when >10 suitable columns exist (lines 169-173)."""
    # Create DataFrame with 15 numeric columns (exceeds max of 10)
    # Use different multipliers to ensure variance in all columns
    data = []
    for j in range(100):
        row = [float(100 + j * (i + 1)) for i in range(15)]  # +1 to avoid zero variance
        data.append(tuple(row))

    columns = [f"col_{i}" for i in range(15)]
    schema = ", ".join([f"{col} double" for col in columns])
    df = spark.createDataFrame(data, schema)

    profile = auto_discover_columns(df)

    # Should select exactly 10 columns (max limit)
    assert len(profile.recommended_columns) == 10

    # Should have warning about too many columns (lines 169-173)
    # Note: The actual count may be 14 or 15 depending on variance filtering
    warnings_text = " ".join(profile.warnings)
    assert "suitable columns" in warnings_text and "selected top 10" in warnings_text
    assert "Priority: numeric" in warnings_text or "by priority" in warnings_text


def test_autodiscovery_with_no_suitable_columns(spark: SparkSession):
    """Test warning when no suitable columns found (lines 176-180)."""
    # Create DataFrame with only unsuitable columns:
    # - ID columns (excluded by pattern)
    # - Constant values (no variance)
    # - Unsupported types (array)
    df = spark.createDataFrame(
        [
            ("id1", 100, [1, 2, 3]),
            ("id2", 100, [4, 5, 6]),
            ("id3", 100, [7, 8, 9]),
        ],
        "user_id string, constant_val int, array_col array<int>",
    )

    profile = auto_discover_columns(df)

    # Should have no recommended columns
    assert len(profile.recommended_columns) == 0

    # Should have warning about no suitable columns
    assert any("No suitable columns found" in w for w in profile.warnings)


def test_autodiscovery_many_segments_warning(spark: SparkSession):
    """Test warning when total segments exceed 50 (lines 240-244)."""
    # Create 15 segments - within the 2-20 range for segment selection (line 292)
    # but when checking total segment count will trigger >50 warning if we had multiple segment columns
    # For single column, we need >50 distinct values, but that's outside 2-20 range
    # So this test actually triggers the high cardinality warning (line 302)
    data = []
    for region in [f"region_{i}" for i in range(60)]:
        for j in range(100):  # 100 rows per segment
            data.append((region, 100.0 + j))

    df = spark.createDataFrame(data, "region string, amount double")

    profile = auto_discover_columns(df)

    # region has 60 distinct values - exceeds segment threshold of 20 (line 292)
    # Should NOT be selected as segment, but should get high cardinality warning
    assert "region" not in profile.recommended_segments

    # Should have warning about high cardinality for segmentation (line 302, lines 211-213)
    warnings_text = " ".join(profile.warnings)
    assert "region" in warnings_text
    assert "60 distinct values" in warnings_text
    assert "too high cardinality" in warnings_text or "excluding from auto-selection" in warnings_text

    # region should still be added as a feature column (line 139-141: 20 < 60 <= 100)
    # But wait - if distinct > 20 for string, it goes to line 139. But line 139 checks <= 100
    # Actually 60 > 20, so it goes to line 139-141 which should add it
    # However, it might be filtered by the segment high cardinality check
    # Let's verify it's included as a feature
    assert "region" in profile.recommended_columns


def test_autodiscovery_selects_multi_value_grouping(spark: SparkSession):
    """Five groups of 150 rows clear the 30-rows-per-group floor and are selected as the grouping.

    The baseline policy bounds groups by rows-per-group, not by a fixed minimum count, and emits no
    "too granular" warning once the floor is met — one model serves every group.
    """
    data = []
    for region in [f"region_{i}" for i in range(5)]:
        for j in range(150):  # 150 rows per group, well above MIN_ROWS_PER_BASELINE_GROUP
            data.append((region, 100.0 + j))

    df = spark.createDataFrame(data, "region string, amount double")

    profile = auto_discover_columns(df)

    assert "region" in profile.recommended_segments
    assert profile.segment_count == 5


def test_segment_column_explicitly_removed_from_features(spark: SparkSession):
    """Test that segment columns are removed from feature columns (lines 488-489)."""
    # Create DataFrame where 'region' qualifies as both feature and segment
    data = []
    for region in ("US", "EU", "APAC"):
        for i in range(1500):  # >1000 rows per segment
            data.append((region, 100.0 + i, 50.0 + i * 0.5))

    df = spark.createDataFrame(data, "region string, amount double, discount double")

    profile = auto_discover_columns(df)

    # CRITICAL: Verify segment removal from features (line 489)
    assert "region" in profile.recommended_segments, "region should be selected as segment"
    assert "region" not in profile.recommended_columns, "region must be REMOVED from features (line 489)"

    # Verify other feature columns remain
    assert "amount" in profile.recommended_columns
    assert "discount" in profile.recommended_columns

    # Verify region was analyzed (has a type) but then removed
    assert profile.column_types is not None


def test_the_calendar_advisory_fires_when_a_timestamp_reaches_the_feature_list(
    spark: SparkSession, make_schema, make_random, anomaly_engine, caplog
):
    """A datetime column in *columns* becomes seven cyclical features, and the caller has to be told.

    This is the advisory with the most to say for itself, because the mistake it names is the *default*:
    auto-discovery includes any timestamp it finds. Measured on the transactions demo, the same model with a
    meaningless timestamp added took recall at a fixed alert budget from 100% to 79% while raising exactly as
    many alerts, so a caller who never sees the warning silently loses a quarter of their detections.

    The message must carry both escapes, since the right one depends on what the column means: exclude it
    when the clock is irrelevant, or name it as an axis when the level moves over time.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    rows = [(START + datetime.timedelta(hours=i), float(100.0 + i % 17), 5.0 + i % 3) for i in range(400)]
    df = spark.createDataFrame(rows, "event_ts timestamp, amount double, discount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.calendar_advisory_{suffix}"
    df.write.saveAsTable(table_name)

    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.anomaly.training_service"):
        anomaly_engine.train(
            df=spark.table(table_name),
            columns=["amount", "discount", "event_ts"],
            model_name=qualify_model_name(f"test_calendar_{suffix}", registry_table),
            registry_table=registry_table,
            baseline_by=[],
        )

    advisories = [r.message for r in caplog.records if "cyclical calendar features" in r.message]
    assert advisories, "a datetime column among the features must be called out"
    message = advisories[0]
    assert "event_ts" in message
    # Both escapes, and each naming the actual column rather than a placeholder, so either is copy-pasteable.
    assert "exclude_columns=['event_ts']" in message
    assert "baseline_over_time='event_ts'" in message


def test_the_calendar_advisory_stays_quiet_when_the_timestamp_is_the_axis(
    spark: SparkSession, make_schema, make_random, anomaly_engine, caplog
):
    """Naming a column as the time axis is the fix the advisory recommends, so it must not then fire.

    An advisory that keeps warning after you have followed it is one callers learn to filter out, and then it
    is worth nothing on the run where it matters.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    rows = [(START + datetime.timedelta(hours=i), float(100.0 + 0.05 * i), 5.0 + i % 3) for i in range(400)]
    df = spark.createDataFrame(rows, "event_ts timestamp, amount double, discount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.calendar_axis_{suffix}"
    df.write.saveAsTable(table_name)

    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.anomaly.training_service"):
        anomaly_engine.train(
            df=spark.table(table_name),
            columns=["amount", "discount"],
            model_name=qualify_model_name(f"test_axis_{suffix}", registry_table),
            registry_table=registry_table,
            baseline_by=[],
            baseline_over_time="event_ts",
        )

    advisories = [r.message for r in caplog.records if "cyclical calendar features" in r.message]
    assert not advisories, f"the axis column must not be reported as a feature: {advisories}"
