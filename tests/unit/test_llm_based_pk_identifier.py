"""
Simple unit tests for LLM-based primary key identifier.
"""

import pytest
from unittest.mock import Mock, patch

# Handle optional LLM dependencies gracefully
try:
    from databricks.labs.dqx.llm.pk_identifier import DatabricksPrimaryKeyDetector
    HAS_LLM_DEPS = True
except ImportError:
    HAS_LLM_DEPS = False
    DatabricksPrimaryKeyDetector = None


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
@patch('databricks.labs.dqx.llm.pk_identifier.configure_with_tracing')
@patch('databricks.labs.dqx.llm.pk_identifier.configure_databricks_llm')
def test_detect_primary_key_simple(mock_configure_llm, mock_tracing):
    """Test simple primary key detection with mocked table schema and LLM."""

    # Mock table definition and metadata
    mock_table_definition = """
    CREATE TABLE customers (
        customer_id BIGINT NOT NULL,
        first_name STRING,
        last_name STRING,
        email STRING,
        created_at TIMESTAMP
    )
    """
    mock_metadata = "Table: customers, Columns: 5, Primary constraints: None"

    # Mock LLM response object
    mock_result = Mock()
    mock_result.primary_key_columns = "customer_id"
    mock_result.reasoning = "customer_id is a unique identifier, non-nullable bigint typically used as primary key"

    with (
        patch('databricks.labs.dqx.llm.pk_identifier.SparkManager') as mock_spark_manager_class,
        patch('databricks.labs.dqx.llm.pk_identifier.dspy.ChainOfThought') as mock_chain,
    ):

        # Setup SparkManager mock
        mock_manager = Mock()
        mock_manager.get_table_definition.return_value = mock_table_definition
        mock_manager.get_table_metadata_info.return_value = mock_metadata
        mock_spark_manager_class.return_value = mock_manager

        # Setup LLM mock
        mock_detector = Mock()
        mock_detector.return_value = mock_result
        mock_chain.return_value = mock_detector

        # Create detector instance
        detector = DatabricksPrimaryKeyDetector(
            table_name="customers", schema="sales", catalog="main", endpoint="mock-endpoint", validate_duplicates=False
        )

        # Test primary key detection
        result = detector.detect_primary_key_from_table_name()

        # Assertions
        assert result["success"] is True
        assert result["primary_key_columns"] == ["customer_id"]
        assert "customer_id" in result["reasoning"]


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
@patch('databricks.labs.dqx.llm.pk_identifier.configure_with_tracing')
@patch('databricks.labs.dqx.llm.pk_identifier.configure_databricks_llm')
def test_detect_primary_key_composite(mock_configure_llm, mock_tracing):
    """Test detection of composite primary key."""

    # Mock table definition and metadata
    mock_table_definition = """
    CREATE TABLE order_items (
        order_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        quantity INT,
        price DECIMAL
    )
    """
    mock_metadata = "Table: order_items, Columns: 4, Primary constraints: None"

    # Mock LLM response object
    mock_result = Mock()
    mock_result.primary_key_columns = "order_id, product_id"
    mock_result.reasoning = "Combination of order_id and product_id forms composite primary key for order items"

    with (
        patch('databricks.labs.dqx.llm.pk_identifier.SparkManager') as mock_spark_manager_class,
        patch('databricks.labs.dqx.llm.pk_identifier.dspy.ChainOfThought') as mock_chain,
    ):

        # Setup SparkManager mock
        mock_manager = Mock()
        mock_manager.get_table_definition.return_value = mock_table_definition
        mock_manager.get_table_metadata_info.return_value = mock_metadata
        mock_spark_manager_class.return_value = mock_manager

        # Setup LLM mock
        mock_detector = Mock()
        mock_detector.return_value = mock_result
        mock_chain.return_value = mock_detector

        # Create detector instance
        detector = DatabricksPrimaryKeyDetector(
            table_name="order_items",
            schema="sales",
            catalog="main",
            endpoint="mock-endpoint",
            validate_duplicates=False,
        )

        # Test primary key detection
        result = detector.detect_primary_key_from_table_name()

        # Assertions
        assert result["success"] is True
        assert result["primary_key_columns"] == ["order_id", "product_id"]


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
@patch('databricks.labs.dqx.llm.pk_identifier.configure_with_tracing')
@patch('databricks.labs.dqx.llm.pk_identifier.configure_databricks_llm')
def test_detect_primary_key_no_clear_key(mock_configure_llm, mock_tracing):
    """Test when LLM cannot identify a clear primary key."""

    # Mock table definition and metadata
    mock_table_definition = """
    CREATE TABLE application_logs (
        timestamp TIMESTAMP,
        level STRING,
        message STRING,
        source STRING
    )
    """
    mock_metadata = "Table: application_logs, Columns: 4, Primary constraints: None"

    # Mock LLM response object
    mock_result = Mock()
    mock_result.primary_key_columns = "none"
    mock_result.reasoning = (
        "No clear primary key identified - all columns are nullable and none appear to be unique identifiers"
    )

    with (
        patch('databricks.labs.dqx.llm.pk_identifier.SparkManager') as mock_spark_manager_class,
        patch('databricks.labs.dqx.llm.pk_identifier.dspy.ChainOfThought') as mock_chain,
    ):

        # Setup SparkManager mock
        mock_manager = Mock()
        mock_manager.get_table_definition.return_value = mock_table_definition
        mock_manager.get_table_metadata_info.return_value = mock_metadata
        mock_spark_manager_class.return_value = mock_manager

        # Setup LLM mock
        mock_detector = Mock()
        mock_detector.return_value = mock_result
        mock_chain.return_value = mock_detector

        # Create detector instance
        detector = DatabricksPrimaryKeyDetector(
            table_name="application_logs",
            schema="logs",
            catalog="main",
            endpoint="mock-endpoint",
            validate_duplicates=False,
        )

        # Test primary key detection
        result = detector.detect_primary_key_from_table_name()

        # Assertions
        assert result["success"] is True
        assert result["primary_key_columns"] == ["none"]


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
@patch('databricks.labs.dqx.llm.pk_identifier.configure_with_tracing')
@patch('databricks.labs.dqx.llm.pk_identifier.configure_databricks_llm')
def test_detect_primary_key_error_handling(mock_configure_llm, mock_tracing):
    """Test error handling when schema retrieval fails."""

    with patch('databricks.labs.dqx.llm.pk_identifier.SparkManager') as mock_spark_manager_class:

        # Setup SparkManager mock to raise exception
        mock_manager = Mock()
        mock_manager.get_table_definition.side_effect = Exception("Table not found")
        mock_spark_manager_class.return_value = mock_manager

        # Create detector instance
        detector = DatabricksPrimaryKeyDetector(
            table_name="nonexistent_table", schema="test", catalog="main", endpoint="mock-endpoint"
        )

        # Test primary key detection
        result = detector.detect_primary_key_from_table_name()

        # Assertions
        assert result["success"] is False
        assert "Table not found" in result["error"]
