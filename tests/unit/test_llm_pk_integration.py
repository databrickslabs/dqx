"""
Unit tests for LLM-based primary key detection integration in profiler.
"""

from unittest.mock import Mock

import pytest

from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.profiler.profiler_runner import ProfilerRunner

try:
    from databricks.labs.dqx.llm.llm_pk_detector import DatabricksPrimaryKeyDetector

    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False


@pytest.fixture
def skip_if_llm_not_available():
    """Skip test if LLM dependencies are not installed."""
    if not LLM_AVAILABLE:
        pytest.skip("LLM dependencies not installed")


class MockLLMEngine:
    """Test double for DQLLMEngine."""

    def __init__(self):
        """Initialize mock LLM engine."""


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
    mock_chat_databricks = Mock()

    detector = DatabricksPrimaryKeyDetector(
        table="customers",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
        spark_session=None,
        chat_databricks_cls=mock_chat_databricks,
    )

    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="customer_id",
        confidence="high",
        reasoning="customer_id is a unique identifier, non-nullable bigint typically used as primary key",
    )

    result = detector.detect_primary_keys()

    assert result["success"] is True
    assert result["primary_key_columns"] == ["customer_id"]
    assert "customer_id" in result["reasoning"]


def test_detect_primary_key_composite(skip_if_llm_not_available):
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
    mock_chat_databricks = Mock()

    detector = DatabricksPrimaryKeyDetector(
        table="order_items",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
        spark_session=None,
        chat_databricks_cls=mock_chat_databricks,
    )

    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="order_id, product_id",
        confidence="high",
        reasoning="Combination of order_id and product_id forms composite primary key for order items",
    )

    result = detector.detect_primary_keys()

    assert result["success"] is True
    assert result["primary_key_columns"] == ["order_id", "product_id"]


def test_detect_primary_key_no_clear_key(skip_if_llm_not_available):
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
    mock_chat_databricks = Mock()

    detector = DatabricksPrimaryKeyDetector(
        table="application_logs",
        endpoint="mock-endpoint",
        validate_duplicates=False,
        show_live_reasoning=False,
        spark_session=None,
        chat_databricks_cls=mock_chat_databricks,
    )

    detector.spark_manager = MockSparkManager(mock_table_definition, mock_metadata)
    detector.detector = MockDetector(
        primary_key_columns="none",
        confidence="low",
        reasoning="No clear primary key identified - all columns are nullable and none appear to be unique identifiers",
    )

    result = detector.detect_primary_keys()

    assert result["success"] is True
    assert result["primary_key_columns"] == ["none"]


def test_profiler_runner_integration():
    """Test ProfilerRunner integration with PK detection."""
    runner = ProfilerRunner(
        ws=Mock(),
        spark=Mock(),
        dq_engine=Mock(),
        installation=Mock(),
        profiler=Mock(),
    )

    mock_generator = Mock()
    mock_generator.llm_engine = MockLLMEngine()
    mock_generator.detect_primary_keys_with_llm.return_value = {
        'table': 'test_table',
        'success': True,
        'primary_key_columns': ['id'],
        'confidence': 'high',
        'reasoning': 'id is primary key',
        'has_duplicates': False,
        'duplicate_count': 0,
    }

    input_config = InputConfig(location="test_table")
    summary_stats = {}

    runner.detect_primary_keys_using_llm(mock_generator, input_config, summary_stats)

    assert 'primary_keys' in summary_stats
    assert summary_stats['primary_keys']['columns'] == ['id']
    assert summary_stats['primary_keys']['confidence'] == 'high'
    assert summary_stats['primary_keys']['has_duplicates'] is False
    assert summary_stats['primary_keys']['duplicate_count'] == 0
    mock_generator.detect_primary_keys_with_llm.assert_called_once_with(input_config)


def test_profiler_runner_llm_not_enabled():
    """Test ProfilerRunner when LLM is not enabled."""
    runner = ProfilerRunner(
        ws=Mock(),
        spark=Mock(),
        dq_engine=Mock(),
        installation=Mock(),
        profiler=Mock(),
    )

    mock_generator = Mock()
    mock_generator.llm_engine = None

    input_config = InputConfig(location="test_table")
    summary_stats = {}

    runner.detect_primary_keys_using_llm(mock_generator, input_config, summary_stats)

    assert 'primary_keys' not in summary_stats
    mock_generator.detect_primary_keys_with_llm.assert_not_called()


def test_profiler_runner_pk_detection_error():
    """Test ProfilerRunner when PK detection fails."""
    runner = ProfilerRunner(
        ws=Mock(),
        spark=Mock(),
        dq_engine=Mock(),
        installation=Mock(),
        profiler=Mock(),
    )

    mock_generator = Mock()
    mock_generator.llm_engine = MockLLMEngine()
    mock_generator.detect_primary_keys_with_llm.return_value = {
        'table': 'test_table',
        'success': False,
        'error': 'Table not found',
    }

    input_config = InputConfig(location="test_table")
    summary_stats = {}

    runner.detect_primary_keys_using_llm(mock_generator, input_config, summary_stats)

    assert 'primary_keys' in summary_stats
    assert 'error' in summary_stats['primary_keys']
    assert summary_stats['primary_keys']['error'] == 'Table not found'
