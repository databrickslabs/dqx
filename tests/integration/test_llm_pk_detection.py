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
    summary_stats, _ = profiler.profile_table(InputConfig(test_table.full_name))

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
    summary_stats, _ = profiler.profile_table(InputConfig(test_table.full_name))

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
    summary_stats, _ = profiler.profile_table(InputConfig(test_table.full_name))

    # Run PK detection
    runner.detect_primary_keys_using_llm(generator, input_config, summary_stats)

    # Assertions
    assert "primary_keys" in summary_stats, "Primary keys result should be present"
    pk_info = summary_stats["primary_keys"]

    # Should either detect "none" or have an error, but result should exist
    assert isinstance(pk_info, dict), "PK result should be a dictionary"
