from unittest.mock import Mock
import pytest
import pandas as pd  # type: ignore
from databricks.labs.dqx.llm.llm_pk_detector import (
    DspyLMAdapter,
    TableManager,
)


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


def test_table_manager():
    """Test TableManager initialization, operations, and metadata."""
    mock_spark = Mock()
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
