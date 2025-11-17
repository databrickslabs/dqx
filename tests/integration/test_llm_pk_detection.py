from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.profiler_runner import ProfilerRunner
from tests.conftest import TEST_CATALOG


def test_detect_primary_keys_simple_table(ws, spark, make_schema, make_table, installation_ctx):
    """Test primary key detection on a simple table with clear primary key."""
    # Create test table with obvious primary key
    schema = make_schema(catalog_name=TEST_CATALOG)
    test_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES "
        "(1, 'Alice', 'alice@example.com'), "
        "(2, 'Bob', 'bob@example.com'), "
        "(3, 'Charlie', 'charlie@example.com'), "
        "(4, 'Diana', 'diana@example.com'), "
        "(5, 'Eve', 'eve@example.com') "
        "AS users(user_id, username, email)",
    )

    # Setup profiler and generator with LLM
    profiler = DQProfiler(ws, spark)
    dq_engine = DQEngine(ws, spark)
    generator = DQGenerator(ws, spark, llm_model_config=LLMModelConfig())
    runner = ProfilerRunner(ws, spark, dq_engine, installation_ctx.installation, profiler)

    # Profile the table
    input_config = InputConfig(location=test_table.full_name)
    summary_stats, _ = profiler.profile(input_config)

    # Run PK detection
    runner.detect_primary_keys_using_llm(generator, input_config, summary_stats)

    # Assertions
    assert "primary_keys" in summary_stats, "Primary keys should be detected"
    pk_info = summary_stats["primary_keys"]

    # Verify structure of PK result
    if "error" not in pk_info:
        assert "columns" in pk_info, "PK result should contain columns"
        assert isinstance(pk_info["columns"], list), "Columns should be a list"
        assert "confidence" in pk_info, "PK result should contain confidence"

        # user_id should be detected as primary key
        assert len(pk_info["columns"]) > 0, "At least one primary key column should be detected"
        assert "user_id" in pk_info["columns"], "user_id should be detected as primary key"


def test_detect_primary_keys_composite(ws, spark, make_schema, make_table, installation_ctx):
    """Test primary key detection on a table with composite primary key."""
    # Create test table with composite key
    schema = make_schema(catalog_name=TEST_CATALOG)
    test_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES "
        "(1, 101, 'Order A', 10.50), "
        "(1, 102, 'Order B', 20.00), "
        "(2, 101, 'Order C', 15.75), "
        "(2, 103, 'Order D', 30.25), "
        "(3, 101, 'Order E', 25.00) "
        "AS orders(customer_id, order_id, order_name, amount)",
    )

    # Setup profiler and generator with LLM
    profiler = DQProfiler(ws, spark)
    dq_engine = DQEngine(ws, spark)
    generator = DQGenerator(ws, spark, llm_model_config=LLMModelConfig())
    runner = ProfilerRunner(ws, spark, dq_engine, installation_ctx.installation, profiler)

    # Profile the table
    input_config = InputConfig(location=test_table.full_name)
    summary_stats, _ = profiler.profile(input_config)

    # Run PK detection
    runner.detect_primary_keys_using_llm(generator, input_config, summary_stats)

    # Assertions
    assert "primary_keys" in summary_stats, "Primary keys should be detected"
    pk_info = summary_stats["primary_keys"]

    # Verify structure of PK result
    if "error" not in pk_info:
        assert "columns" in pk_info, "PK result should contain columns"
        assert isinstance(pk_info["columns"], list), "Columns should be a list"
        # Composite key should be detected
        assert len(pk_info["columns"]) >= 1, "At least one primary key column should be detected"


def test_detect_primary_keys_no_clear_key(ws, spark, make_schema, make_table, installation_ctx):
    """Test primary key detection on a table with no clear primary key."""
    # Create test table without clear primary key (log-style data)
    schema = make_schema(catalog_name=TEST_CATALOG)
    test_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES "
        "('2025-01-01 10:00:00', 'INFO', 'Application started'), "
        "('2025-01-01 10:01:00', 'DEBUG', 'Connection established'), "
        "('2025-01-01 10:02:00', 'WARN', 'High memory usage'), "
        "('2025-01-01 10:03:00', 'ERROR', 'Connection timeout'), "
        "('2025-01-01 10:04:00', 'INFO', 'Application stopped') "
        "AS logs(timestamp, level, message)",
    )

    # Setup profiler and generator with LLM
    profiler = DQProfiler(ws, spark)
    dq_engine = DQEngine(ws, spark)
    generator = DQGenerator(ws, spark, llm_model_config=LLMModelConfig())
    runner = ProfilerRunner(ws, spark, dq_engine, installation_ctx.installation, profiler)

    # Profile the table
    input_config = InputConfig(location=test_table.full_name)
    summary_stats, _ = profiler.profile(input_config)

    # Run PK detection
    runner.detect_primary_keys_using_llm(generator, input_config, summary_stats)

    # Assertions
    assert "primary_keys" in summary_stats, "Primary keys result should be present"
    pk_info = summary_stats["primary_keys"]

    # Should either detect "none" or have an error, but result should exist
    assert isinstance(pk_info, dict), "PK result should be a dictionary"


def test_detect_primary_keys_with_duplicates(ws, spark, make_schema, make_table, installation_ctx):
    """Test primary key detection on a table with duplicate values."""
    # Create test table with duplicates
    schema = make_schema(catalog_name=TEST_CATALOG)
    test_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES "
        "(1, 'Product A', 100), "
        "(2, 'Product B', 200), "
        "(1, 'Product A Duplicate', 150), "
        "(3, 'Product C', 300), "
        "(2, 'Product B Duplicate', 250) "
        "AS products(product_id, product_name, price)",
    )

    # Setup profiler and generator with LLM
    profiler = DQProfiler(ws, spark)
    dq_engine = DQEngine(ws, spark)
    generator = DQGenerator(ws, spark, llm_model_config=LLMModelConfig())
    runner = ProfilerRunner(ws, spark, dq_engine, installation_ctx.installation, profiler)

    # Profile the table
    input_config = InputConfig(location=test_table.full_name)
    summary_stats, _ = profiler.profile(input_config)

    # Run PK detection (with duplicate validation)
    runner.detect_primary_keys_using_llm(generator, input_config, summary_stats)

    # Assertions
    assert "primary_keys" in summary_stats, "Primary keys should be detected"
    pk_info = summary_stats["primary_keys"]

    # Should detect duplicates if PK detection succeeds
    if "error" not in pk_info and "has_duplicates" in pk_info:
        # If product_id is identified as PK, should flag duplicates
        if "product_id" in pk_info.get("columns", []):
            assert pk_info.get("has_duplicates", False), "Should detect duplicates in product_id"
            assert pk_info.get("duplicate_count", 0) > 0, "Should count duplicate records"


def test_compare_datasets_with_llm_wrapper(ws, spark, make_schema, make_table, skip_if_llm_not_available):
    """Test compare_datasets_with_llm wrapper with auto PK detection."""
    # Use the fixture to ensure LLM is available
    _ = skip_if_llm_not_available

    schema = make_schema(catalog_name=TEST_CATALOG)

    # Create source table
    source_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie') AS data(id, name)",
    )

    # Create reference table (same data)
    ref_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie') AS data(id, name)",
    )

    # Load both DataFrames
    source_df = spark.table(source_table.full_name)
    ref_df = spark.table(ref_table.full_name)

    # Register reference as temp view
    ref_df.createOrReplaceTempView("ref_table_for_test")

    # Test the wrapper with auto PK detection
    # pylint: disable=import-outside-toplevel
    from databricks.labs.dqx.check_funcs import compare_datasets_with_llm

    _, apply_func = compare_datasets_with_llm(
        source_table=source_table.full_name,
        ref_table=ref_table.full_name,  # For PK detection
        ref_df_name="ref_table_for_test",  # For comparison
    )

    # Apply the comparison using apply_func with all required arguments
    result_df = apply_func(source_df, spark, {"ref_table_for_test": ref_df})

    # Assertions - all rows should match (no differences)
    results = result_df.collect()
    assert len(results) == 3, "Should have 3 rows"

    # Get the compare status column (has generated UUID in name)
    compare_col = [col for col in result_df.columns if col.startswith("__compare_status")][0]

    # Check that all rows match
    for row in results:
        check_val = row[compare_col]
        assert check_val.matched is True, "All rows should match between identical datasets"
