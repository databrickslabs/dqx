"""
Simple unit tests for LLM-based primary key identifier.
"""

import pytest
from unittest.mock import Mock

# Use centralized LLM dependency checking
from databricks.labs.dqx.llm import is_llm_available

HAS_LLM_DEPS = is_llm_available()

if HAS_LLM_DEPS:
    from databricks.labs.dqx.llm.pk_identifier import DatabricksPrimaryKeyDetector, SparkManager
else:
    DatabricksPrimaryKeyDetector = None  # type: ignore
    SparkManager = None  # type: ignore


# Test helper classes
class MockSparkManager:
    """Test double for SparkManager."""

    def __init__(self, table_definition="", metadata_info="", should_raise=False):
        self.table_definition = table_definition
        self.metadata_info = metadata_info
        self.should_raise = should_raise

    def get_table_definition(self, table_name, catalog=None, schema=None):
        if self.should_raise:
            raise Exception("Table not found")
        return self.table_definition

    def get_table_metadata_info(self, table_name, catalog=None, schema=None):
        if self.should_raise:
            raise Exception("Metadata not available")
        return self.metadata_info

    def check_duplicates(self, table_name, pk_columns, catalog=None, schema=None, sample_size=10000):
        # Default: no duplicates found
        return False, 0


class MockDetector:
    """Test double for DSPy detector."""

    def __init__(self, primary_key_columns="", confidence="high", reasoning=""):
        self.primary_key_columns = primary_key_columns
        self.confidence = confidence
        self.reasoning = reasoning

    def __call__(self, **kwargs):
        result = Mock()
        result.primary_key_columns = self.primary_key_columns
        result.confidence = self.confidence
        result.reasoning = self.reasoning
        return result


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
def test_detect_primary_key_simple():
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

    # Create detector instance with dependency injection
    detector = DatabricksPrimaryKeyDetector(
        table_name="customers",
        schema="sales",
        catalog="main",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
    )

    # Inject test doubles
    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="customer_id",
        confidence="high",
        reasoning="customer_id is a unique identifier, non-nullable bigint typically used as primary key",
    )

    # Test primary key detection
    result = detector.detect_primary_key_from_table_name()

    # Assertions
    assert result["success"] is True
    assert result["primary_key_columns"] == ["customer_id"]
    assert "customer_id" in result["reasoning"]


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
def test_detect_primary_key_composite():
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

    # Create detector instance with dependency injection
    detector = DatabricksPrimaryKeyDetector(
        table_name="order_items",
        schema="sales",
        catalog="main",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
    )

    # Inject test doubles
    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="order_id, product_id",
        confidence="high",
        reasoning="Combination of order_id and product_id forms composite primary key for order items",
    )

    # Test primary key detection
    result = detector.detect_primary_key_from_table_name()

    # Assertions
    assert result["success"] is True
    assert result["primary_key_columns"] == ["order_id", "product_id"]


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
def test_detect_primary_key_no_clear_key():
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

    # Create detector instance with dependency injection
    detector = DatabricksPrimaryKeyDetector(
        table_name="application_logs",
        schema="logs",
        catalog="main",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
    )

    # Inject test doubles
    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="none",
        confidence="low",
        reasoning="No clear primary key identified - all columns are nullable and none appear to be unique identifiers",
    )

    # Test primary key detection
    result = detector.detect_primary_key_from_table_name()

    # Assertions
    assert result["success"] is True
    assert result["primary_key_columns"] == ["none"]


@pytest.mark.skipif(not HAS_LLM_DEPS, reason="LLM dependencies (dspy, databricks_langchain) not installed")
def test_detect_primary_key_error_handling():
    """Test error handling when schema retrieval fails."""

    # Create detector instance with dependency injection
    detector = DatabricksPrimaryKeyDetector(
        table_name="nonexistent_table",
        schema="test",
        catalog="main",
        endpoint="mock-endpoint",
        show_live_reasoning=False,
    )

    # Inject test double that raises exception
    detector.spark_manager = MockSparkManager(should_raise=True)

    # Test primary key detection
    result = detector.detect_primary_key_from_table_name()

    # Assertions
    assert result["success"] is False
    assert "Table not found" in result["error"]
