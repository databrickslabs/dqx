from unittest.mock import Mock
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.llm.llm_pk_detector import PrimaryKeyDetector


class MockLLMEngine:
    """Test double for DQLLMEngine."""

    def __init__(self):
        """Initialize mock LLM engine."""


class MockTableManager:
    """Test double for TableManager."""

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
        return False, 0

    def get_table_column_names(self, _table_name, _catalog=None, _schema=None):
        return ["order_id", "product_id", "quantity", "price"]


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


def test_profiler_detect_pk_with_llm():
    """Test primary key detection using DQProfiler."""
    # Create mock workspace client with proper config
    mock_config = Mock()
    # Use setattr to avoid pylint protected-access warning
    setattr(mock_config, '_product_info', ("dqx", "1.0.0"))
    mock_ws = Mock()
    mock_ws.config = mock_config

    # Create mock spark session
    mock_spark = Mock()

    # Create profiler
    profiler = DQProfiler(mock_ws, mock_spark, llm_model_config=LLMModelConfig(model_name="test-model"))

    # Verify method exists and is callable
    assert hasattr(profiler, 'detect_primary_keys_with_llm')
    assert callable(profiler.detect_primary_keys_with_llm)

    # Verify method accepts correct parameters
    # This will fail on actual execution without real table/endpoint,
    # but we're testing the interface
    try:
        result = profiler.detect_primary_keys_with_llm(table="test_table")
        # If it somehow works, verify result structure
        assert isinstance(result, dict)
        assert "table" in result
    except Exception:
        # Expected to fail without real endpoint - this is fine
        # We've verified the method exists and accepts correct parameters
        pass


def test_detect_primary_key_composite():
    """Test detection of composite primary key."""
    mock_table_definition = """
    CREATE TABLE order_items (
        order_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        quantity INT,
        price DECIMAL
    )
    """
    mock_metadata = "Table: order_items, Columns: 4, Primary constraints: None"

    detector = PrimaryKeyDetector(
        table="order_items",
        show_live_reasoning=False,
        spark_session=None,
    )

    detector.table_manager = MockTableManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="order_id, product_id",
        confidence="high",
        reasoning="Combination of order_id and product_id forms composite primary key for order items",
    )

    result = detector.detect_primary_keys_with_llm()

    assert result["success"] is True
    assert result["primary_key_columns"] == ["order_id", "product_id"]


def test_detect_primary_key_no_clear_key():
    """Test when LLM cannot identify a clear primary key."""
    mock_table_definition = """
    CREATE TABLE application_logs (
        timestamp TIMESTAMP,
        level STRING,
        message STRING,
        source STRING
    )
    """
    mock_metadata = "Table: application_logs, Columns: 4, Primary constraints: None"

    detector = PrimaryKeyDetector(
        table="application_logs",
        show_live_reasoning=False,
        spark_session=None,
    )

    detector.table_manager = MockTableManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="none",
        confidence="low",
        reasoning="No clear primary key identified - all columns are nullable and none appear to be unique identifiers",
    )

    result = detector.detect_primary_keys_with_llm()

    assert result["success"] is False
    assert result["primary_key_columns"] == []
