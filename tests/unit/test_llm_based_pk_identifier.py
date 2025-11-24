"""
LLM-based primary key identifier.
"""

from unittest.mock import Mock

# Import LLM dependencies - fixture will handle availability checking
from databricks.labs.dqx.llm.pk_identifier import DatabricksPrimaryKeyDetector


# Test helper classes
class MockSparkManager:
    """Test double for SparkManager."""

    def __init__(self, table_definition="", metadata_info="", should_raise=False):
        self.table_definition = table_definition
        self.metadata_info = metadata_info
        self.should_raise = should_raise

    def get_table_definition(self, _table_name, _catalog=None, _schema=None):
        if self.should_raise:
            raise ValueError("Table not found")
        return self.table_definition

    def get_table_metadata_info(self, _table_name, _catalog=None, _schema=None):
        if self.should_raise:
            raise ValueError("Metadata not available")
        return self.metadata_info

    def check_duplicates(self, _table_name, _pk_columns, _catalog=None, _schema=None):
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


def test_detect_primary_key_simple(skip_if_llm_not_available):
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

    # Create mock ChatDatabricks class
    mock_chat_databricks = Mock()

    # Create detector instance with injected mock
    detector = DatabricksPrimaryKeyDetector(
        table="customers",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
        spark_session=None,  # Explicitly pass None to avoid Spark session creation
        chat_databricks_cls=mock_chat_databricks,
    )

    # Inject test doubles
    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="customer_id",
        confidence="high",
        reasoning="customer_id is a unique identifier, non-nullable bigint typically used as primary key",
    )

    # Test primary key detection
    result = detector.detect_primary_keys()

    # Assertions
    assert result["success"] is True
    assert result["primary_key_columns"] == ["customer_id"]
    assert "customer_id" in result["reasoning"]


def test_detect_primary_key_composite(skip_if_llm_not_available):
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

    # Create mock ChatDatabricks class
    mock_chat_databricks = Mock()

    # Create detector instance with injected mock
    detector = DatabricksPrimaryKeyDetector(
        table="order_items",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
        spark_session=None,  # Explicitly pass None to avoid Spark session creation
        chat_databricks_cls=mock_chat_databricks,
    )

    # Inject test doubles
    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="order_id, product_id",
        confidence="high",
        reasoning="Combination of order_id and product_id forms composite primary key for order items",
    )

    # Test primary key detection
    result = detector.detect_primary_keys()

    # Assertions
    assert result["success"] is True
    assert result["primary_key_columns"] == ["order_id", "product_id"]


def test_detect_primary_key_no_clear_key(skip_if_llm_not_available):
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

    # Create mock ChatDatabricks class
    mock_chat_databricks = Mock()

    # Create detector instance with injected mock
    detector = DatabricksPrimaryKeyDetector(
        table="application_logs",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
        spark_session=None,  # Explicitly pass None to avoid Spark session creation
        chat_databricks_cls=mock_chat_databricks,
    )

    # Inject test doubles
    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="none",
        confidence="low",
        reasoning="No clear primary key identified - all columns are nullable and none appear to be unique identifiers",
    )

    # Test primary key detection
    result = detector.detect_primary_keys()

    # Assertions
    assert result["success"] is True
    assert result["primary_key_columns"] == ["none"]
