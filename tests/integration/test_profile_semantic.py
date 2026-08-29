import uuid

import pyspark.sql.types as T

from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.semantic import (
    DEFAULT_ENUM_DETECTOR,
    DQSemanticType,
    DQSemanticTypeDetector,
    SemanticRegistry,
    default_semantic_detectors,
)

from tests.constants import TEST_CATALOG


DEMO_ROW_COUNT = 60


def _make_demo_df(spark):
    """Grounded design fixture: vehicle_type/cargo_weight/deal_value/user_id/order_id/user_name/work_description."""
    schema = T.StructType(
        [
            T.StructField("vehicle_type", T.StringType()),
            T.StructField("cargo_weight", T.DoubleType()),
            T.StructField("deal_value", T.DoubleType()),
            T.StructField("user_id", T.LongType()),
            T.StructField("order_id", T.StringType()),
            T.StructField("user_name", T.StringType()),
            T.StructField("work_description", T.StringType()),
        ]
    )
    kinds = ["car", "truck", "van"]
    rows = []
    for i in range(DEMO_ROW_COUNT):
        rows.append(
            (
                kinds[i % 3],
                100.0 + float(i) * 50.0,
                1000.0 + float(i) * 25.0,
                i + 1,
                # 36-char UUID-shaped keys — length-stability = 1.0
                str(uuid.UUID(int=i)),
                # Free-form names 3..30 chars — length_stability ~= 0.1
                ("A" * (3 + (i % 27))),
                "The quick brown fox jumps over the lazy dog " * (1 + i),
            )
        )
    return spark.createDataFrame(rows, schema=schema)


def _profile_by_column(profiles):
    grouped: dict[str, list] = {}
    for profile in profiles:
        grouped.setdefault(profile.column, []).append(profile)
    return grouped


def test_profile_without_semantic_registry_matches_pre_feature_output(spark, ws):
    """No registry → profiler output is identical to the pre-feature behaviour.

    Every DQProfile.semantic_type must be None because detection did not run.
    """
    df = _make_demo_df(spark)
    profiler = DQProfiler(ws)
    _stats, profiles = profiler.profile(df, options={"sample_fraction": None, "llm_primary_key_detection": False})
    assert profiles, "expected the pre-feature profiler to emit at least one profile"
    tagged = [(p.column, p.name, p.semantic_type) for p in profiles if p.semantic_type is not None]
    assert not tagged, f"expected all profiles to have semantic_type=None, got tagged: {tagged}"


def test_default_semantic_registry_classifies_grounded_columns(spark, ws):
    df = _make_demo_df(spark)
    profiler = DQProfiler(ws, semantic_registry=SemanticRegistry.default())
    _stats, profiles = profiler.profile(df, options={"sample_fraction": None, "llm_primary_key_detection": False})

    by_column = _profile_by_column(profiles)

    vehicle_names = {p.name for p in by_column.get("vehicle_type", [])}
    assert "is_in" in vehicle_names, f"expected 'is_in' profile on vehicle_type, got: {vehicle_names}"
    assert "min_max" not in vehicle_names, f"unexpected 'min_max' profile on vehicle_type, got: {vehicle_names}"
    vehicle_is_in_semantic_types = [p.semantic_type for p in by_column.get("vehicle_type", []) if p.name == "is_in"]
    assert all(
        st == "enum" for st in vehicle_is_in_semantic_types
    ), f"expected all vehicle_type is_in profiles tagged semantic_type='enum', got: {vehicle_is_in_semantic_types}"

    for measurement_col in ("cargo_weight", "deal_value"):
        names = {profile.name for profile in by_column.get(measurement_col, [])}
        assert "min_max" in names, f"expected 'min_max' profile on {measurement_col}, got: {names}"
        assert "is_in" not in names, f"unexpected 'is_in' profile on {measurement_col}, got: {names}"
        min_max_semantic_types = [
            profile.semantic_type for profile in by_column.get(measurement_col, []) if profile.name == "min_max"
        ]
        assert all(st == "measurement" for st in min_max_semantic_types), (
            f"expected all {measurement_col} min_max profiles tagged semantic_type='measurement', "
            f"got: {min_max_semantic_types}"
        )

    user_id_names = {p.name for p in by_column.get("user_id", [])}
    assert "min_max" not in user_id_names, f"unexpected 'min_max' profile on user_id key column, got: {user_id_names}"
    assert "is_in" not in user_id_names, f"unexpected 'is_in' profile on user_id key column, got: {user_id_names}"

    order_id_names = {p.name for p in by_column.get("order_id", [])}
    assert "is_in" not in order_id_names, f"unexpected 'is_in' profile on order_id key column, got: {order_id_names}"
    assert (
        "min_max" not in order_id_names
    ), f"unexpected 'min_max' profile on order_id key column, got: {order_id_names}"

    # user_name has variable length → falls through to text (not key). text emits no additional
    # rules; only null_or_empty candidates remain.
    user_name_profiles = by_column.get("user_name", [])
    user_name_names = {p.name for p in user_name_profiles}
    assert "is_in" not in user_name_names, f"unexpected 'is_in' profile on user_name, got: {user_name_names}"
    user_name_semantic_types = {p.semantic_type for p in user_name_profiles if p.semantic_type is not None}
    assert user_name_semantic_types <= {
        "text"
    }, f"expected user_name profiles tagged only with 'text' semantic_type, got: {user_name_semantic_types}"

    work_desc_profiles = by_column.get("work_description", [])
    work_desc_names = {p.name for p in work_desc_profiles}
    assert "is_in" not in work_desc_names, f"unexpected 'is_in' profile on work_description, got: {work_desc_names}"
    work_desc_semantic_types = {p.semantic_type for p in work_desc_profiles if p.semantic_type is not None}
    assert work_desc_semantic_types <= {
        "text"
    }, f"expected work_description profiles tagged only with 'text' semantic_type, got: {work_desc_semantic_types}"


def test_registry_without_enum_falls_through_to_measurement(spark, ws):
    df = _make_demo_df(spark)
    chain_without_enum = default_semantic_detectors()[1:]  # key → measurement → text (drops enum)
    profiler = DQProfiler(ws, semantic_registry=SemanticRegistry(detectors=chain_without_enum))
    _stats, profiles = profiler.profile(df, options={"sample_fraction": None, "llm_primary_key_detection": False})

    by_column = _profile_by_column(profiles)
    vehicle_names = {p.name for p in by_column.get("vehicle_type", [])}
    # vehicle_type is StringType, so it now falls through to text (no min_max, no is_in).
    assert "is_in" not in vehicle_names, f"unexpected 'is_in' profile on vehicle_type, got: {vehicle_names}"
    assert "min_max" not in vehicle_names, f"unexpected 'min_max' profile on vehicle_type, got: {vehicle_names}"


def test_prepend_custom_detector_takes_precedence(spark, ws):
    """A prepended detector wins over the default enum detector."""

    def _always_text(_ctx):
        return DQSemanticType(name="text", description="forced")

    forced = DQSemanticTypeDetector(name="forced_text", detect=_always_text)
    registry = SemanticRegistry.default().prepend(forced)
    profiler = DQProfiler(ws, semantic_registry=registry)
    df = _make_demo_df(spark)
    _stats, profiles = profiler.profile(df, options={"sample_fraction": None, "llm_primary_key_detection": False})
    by_column = _profile_by_column(profiles)
    vehicle_names = {p.name for p in by_column.get("vehicle_type", [])}
    # forced_text emits "text" — same gate as DEFAULT_TEXT_DETECTOR — so is_in and min_max are skipped
    assert (
        "is_in" not in vehicle_names
    ), f"unexpected 'is_in' profile on vehicle_type when forced_text prepended, got: {vehicle_names}"
    assert (
        "min_max" not in vehicle_names
    ), f"unexpected 'min_max' profile on vehicle_type when forced_text prepended, got: {vehicle_names}"


def test_profile_table_populates_metadata_from_unity_catalog(spark, ws, make_schema, make_random):
    """profile_table fetches UC metadata and threads it into ctx.metadata."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.t{make_random(10).lower()}"

    schema = T.StructType(
        [
            T.StructField("vehicle_type", T.StringType(), metadata={"comment": "kind of vehicle"}),
            T.StructField("cargo_weight", T.DoubleType()),
        ]
    )
    data = [("car", 1.0), ("truck", 2.0), ("van", 3.0)] * 20
    spark.createDataFrame(data, schema).write.format("delta").saveAsTable(table_name)
    spark.sql(f"COMMENT ON TABLE {table_name} IS 'demo table with vehicle types'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN vehicle_type COMMENT 'kind of vehicle'")

    captured: list[dict] = []

    def _spy_detect(ctx):
        captured.append(dict(ctx.metadata))

    spy = DQSemanticTypeDetector(name="spy", detect=_spy_detect)
    registry = SemanticRegistry(detectors=(spy, *default_semantic_detectors()))
    profiler = DQProfiler(ws, semantic_registry=registry)
    profiler.profile_table(
        input_config=InputConfig(location=table_name),
        options={"sample_fraction": None, "llm_primary_key_detection": False},
    )

    vehicle_metadata = next((m for m in captured if m.get("column_comment") == "kind of vehicle"), None)
    assert (
        vehicle_metadata is not None
    ), f"expected captured metadata for vehicle_type with column_comment='kind of vehicle', got: {captured}"
    assert (
        vehicle_metadata.get("table_name") == table_name
    ), f"expected table_name={table_name!r}, got: {vehicle_metadata.get('table_name')!r}"
    assert (
        vehicle_metadata.get("table_comment") == "demo table with vehicle types"
    ), f"expected table_comment='demo table with vehicle types', got: {vehicle_metadata.get('table_comment')!r}"


def test_enum_detector_values_reused_by_is_in_builder(spark, ws):
    """The is_in builder must reuse the enum detector's values — no double distinct-collect."""
    df = _make_demo_df(spark)

    invocations: list[str] = []
    original = DEFAULT_ENUM_DETECTOR.detect

    def _tracing_detect(ctx):
        if ctx.column_name == "vehicle_type":
            invocations.append(ctx.column_name)
        return original(ctx)

    tracing = DQSemanticTypeDetector(name="enum", detect=_tracing_detect)
    registry = SemanticRegistry.default().replace([tracing, *default_semantic_detectors()[1:]])
    profiler = DQProfiler(ws, semantic_registry=registry)
    _stats, profiles = profiler.profile(df, options={"sample_fraction": None, "llm_primary_key_detection": False})

    assert invocations == [
        "vehicle_type"
    ], f"expected enum detector invoked exactly once for vehicle_type, got invocations: {invocations}"
    is_in_profiles = [p for p in profiles if p.column == "vehicle_type" and p.name == "is_in"]
    assert (
        len(is_in_profiles) == 1
    ), f"expected exactly one 'is_in' profile for vehicle_type, got {len(is_in_profiles)}: {is_in_profiles}"
    is_in = is_in_profiles[0]
    values = set(is_in.parameters["in"])
    assert values == {"car", "truck", "van"}, f"expected is_in values {{car, truck, van}}, got: {values}"
