from unittest.mock import Mock
import pytest
import pandas as pd  # type: ignore
from databricks.labs.dqx.llm.llm_pk_detector import (
    DspyLMAdapter,
    TableManager,
)
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


def test_adapter_and_configuration():
    """Test DspyLMAdapter initialization, calls, error handling, and configuration."""
    mock_chat_cls = Mock()
    mock_chat_instance = Mock()
    mock_chat_cls.return_value = mock_chat_instance
    mock_response = Mock()
    mock_response.content = "response_content"
    mock_chat_instance.invoke.return_value = mock_response

    adapter = DspyLMAdapter(endpoint="test-endpoint", max_tokens=500, chat_databricks_cls=mock_chat_cls)
    assert adapter.endpoint == "test-endpoint"
    assert adapter(prompt="test prompt") == ["response_content"]
    assert adapter(messages=[Mock()]) == ["response_content"]

    # Test all error types
    mock_chat_instance.invoke = Mock(side_effect=ConnectionError("Connection failed"))
    assert adapter(prompt="test") == ["Error: Connection failed"]
    mock_chat_instance.invoke = Mock(side_effect=TimeoutError("Timeout"))
    assert adapter(prompt="test") == ["Error: Timeout"]
    mock_chat_instance.invoke = Mock(side_effect=ValueError("Invalid"))
    assert adapter(prompt="test") == ["Error: Invalid"]
    mock_chat_instance.invoke = Mock(side_effect=RuntimeError("Runtime"))
    assert adapter(prompt="test") == ["Unexpected error: Runtime"]
    mock_chat_instance.invoke = Mock(side_effect=AttributeError("Attr"))
    assert "Unexpected error" in adapter(prompt="test")[0]

    adapter2 = DspyLMAdapter(endpoint="databricks/model", chat_databricks_cls=mock_chat_cls)
    assert adapter2.endpoint == "databricks/model"


def test_table_manager(mock_spark):
    """Test TableManager initialization, operations, and metadata."""
    manager = TableManager(spark_session=mock_spark)
    assert manager.spark == mock_spark

    manager_none = TableManager(spark_session=None)
    with pytest.raises(ValueError, match="Spark session not available"):
        manager_none.get_table_definition("test")
    assert "No metadata available" in manager_none.get_table_metadata_info("test")

    # Test successful operations
    mock_df = pd.DataFrame({'col_name': ['id', 'name'], 'data_type': ['bigint', 'string'], 'comment': ['', '']})
    mock_result = Mock()
    mock_result.toPandas.return_value = mock_df
    mock_pk = Mock()
    mock_pk.toPandas.return_value = pd.DataFrame({'key': ['delta.constraints.primary_key'], 'value': ['id']})
    mock_spark.sql = Mock(side_effect=[mock_result, mock_pk])
    definition = manager.get_table_definition("test")
    assert 'id bigint' in definition and 'Existing Primary Key: id' in definition

    mock_spark.sql = Mock(side_effect=ValueError("Not found"))
    with pytest.raises(ValueError):
        manager.get_table_definition("test")
    mock_spark.sql = Mock(side_effect=TypeError("Type error"))
    with pytest.raises(RuntimeError, match="Failed to retrieve table definition"):
        manager.get_table_definition("test")

    props_df = pd.DataFrame({'key': ['delta.numRows', 'rawdatasize'], 'value': ['1000', '5000']})
    cols_df = pd.DataFrame(
        {'col_name': ['id', 'amount', 'name', 'date'], 'data_type': ['int', 'decimal', 'varchar', 'date']}
    )
    mock_props = Mock()
    mock_props.toPandas.return_value = props_df
    mock_cols = Mock()
    mock_cols.toPandas.return_value = cols_df
    mock_spark.sql = Mock(side_effect=[mock_props, mock_cols])
    metadata = manager.get_table_metadata_info("test")
    assert 'numRows' in metadata or 'Metadata' in metadata


def test_profiler_detect_pk_with_llm(mock_workspace_client, mock_spark):
    """Test primary key detection using DQProfiler."""
    # Create mock workspace client with proper config
    mock_config = Mock()
    # Use setattr to avoid pylint protected-access warning
    setattr(mock_config, '_product_info', ("dqx", "1.0.0"))
    mock_workspace_client.config = mock_config

    # Create profiler
    profiler = DQProfiler(mock_workspace_client, mock_spark, llm_model_config=LLMModelConfig(model_name="test-model"))

    # Verify method accepts correct parameters
    result = profiler.detect_primary_keys_with_llm(table="test_table")

    # If it somehow works, verify result structure
    assert isinstance(result, dict)
    assert "table" in result


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


def _create_mock_detector_result(pk_cols, conf, reasoning):
    """Helper to create mock detector result."""
    mock_det = Mock()
    mock_det.primary_key_columns, mock_det.confidence, mock_det.reasoning = pk_cols, conf, reasoning
    return mock_det


def _create_mock_spark_results():
    """Helper to create reusable mock Spark results."""
    col_df = pd.DataFrame({'col_name': ['id'], 'data_type': ['bigint'], 'comment': ['']})
    col_res, pk_res, dup_res = Mock(), Mock(), Mock()
    col_res.toPandas.return_value = col_df
    pk_res.toPandas.return_value = pd.DataFrame()
    dup_res.toPandas.return_value = pd.DataFrame()
    return col_res, pk_res, dup_res
