"""
Comprehensive unit tests for LLM-based primary key detection.
"""

from unittest.mock import Mock
import pytest

try:
    import pandas as pd  # type: ignore
    from databricks.labs.dqx.llm.llm_pk_detector import (
        DspyLMAdapter,
        TableManager,
        PrimaryKeyDetector,
        configure_databricks_llm,
        configure_with_tracing,
    )

    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE, pd = False, None  # type: ignore

try:
    from databricks.labs.dqx.config import LLMModelConfig, InputConfig
    from databricks.labs.dqx.profiler.profiler import DQProfiler
    from databricks.labs.dqx.profiler.generator import DQGenerator
    from databricks.labs.dqx.profiler.profiler_runner import ProfilerRunner
    from databricks.labs.dqx.errors import MissingParameterError
except ImportError:
    pass


pytestmark = pytest.mark.skipif(not LLM_AVAILABLE, reason="LLM dependencies not installed")


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

    # Test configuration
    language_model = configure_databricks_llm(LLMModelConfig(model_name="test"), chat_databricks_cls=mock_chat_cls)
    assert language_model.endpoint == "test"
    assert configure_with_tracing() is True


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


def test_primary_key_detector():
    """Test PrimaryKeyDetector duplicate checking, detection flow, and error handling."""
    mock_chat, mock_spark = Mock(), Mock()
    mock_response = Mock(content="test")
    mock_instance = Mock(invoke=Mock(return_value=mock_response))
    mock_chat.return_value = mock_instance

    detector = PrimaryKeyDetector(
        table="test",
        model_config=LLMModelConfig(model_name="model"),
        spark_session=mock_spark,
        context="ctx",
        max_retries=3,
        chat_databricks_cls=mock_chat,
        detector_cls=Mock(),
    )
    assert detector.table == "test" and detector.max_retries == 3

    detector_no_spark = PrimaryKeyDetector(
        table="test", spark_session=None, chat_databricks_cls=mock_chat, detector_cls=Mock()
    )
    with pytest.raises(ValueError, match="Spark session not available"):
        detector_no_spark.check_duplicates("test", ["id"])

    mock_result = Mock(toPandas=Mock(return_value=pd.DataFrame()))
    mock_spark.sql = Mock(return_value=mock_result)
    has_dup, count = detector.check_duplicates("test", ["id"])
    assert not has_dup and count == 0

    mock_result.toPandas.return_value = pd.DataFrame({'id': [1, 2], 'duplicate_count': [2, 3]})
    has_dup, count = detector.check_duplicates("test", ["id", "name"])
    assert has_dup and count == 2

    for error in (ValueError("Error"), TypeError("Type"), AttributeError("Attr")):
        mock_spark.sql = Mock(side_effect=error)
        has_dup, count = detector.check_duplicates("test", ["id"])
        assert not has_dup and count == 0

    for error in (OSError("IO"), AttributeError("Attr")):
        mock_spark.sql = Mock(side_effect=error)
        result = detector.detect_primary_keys()
        assert not result['success']

    col_res, pk_res, dup_res = _create_mock_spark_results()
    mock_spark.sql = Mock(side_effect=(col_res, pk_res, pk_res, col_res, dup_res))

    result = PrimaryKeyDetector(
        table="test",
        spark_session=mock_spark,
        chat_databricks_cls=mock_chat,
        detector_cls=Mock(
            return_value=_create_mock_detector_result("id", "high", "Step 1\nThink\n- Point\n• Point\nanalyze")
        ),
        show_live_reasoning=False,
        max_retries=1,
    ).detect_primary_keys()
    assert result['success'] and result['primary_key_columns'] == ['id']

    dup_data = Mock(toPandas=Mock(return_value=pd.DataFrame({'id': [1], 'duplicate_count': [2]})))
    dup_empty = Mock(toPandas=Mock(return_value=pd.DataFrame()))
    mock_spark.sql = Mock(side_effect=(col_res, pk_res, pk_res, col_res, dup_data, dup_empty))

    assert (
        'primary_key_columns'
        in PrimaryKeyDetector(
            table="test",
            spark_session=mock_spark,
            chat_databricks_cls=mock_chat,
            detector_cls=Mock(return_value=_create_mock_detector_result("id, name", "medium", "")),
            show_live_reasoning=False,
            max_retries=2,
        ).detect_primary_keys()
    )

    dup_always = Mock(toPandas=Mock(return_value=pd.DataFrame({'id': [1], 'duplicate_count': [5]})))
    mock_spark.sql = Mock(side_effect=(col_res, pk_res, pk_res, col_res, dup_always, dup_always, dup_always))

    assert not PrimaryKeyDetector(
        table="test",
        spark_session=mock_spark,
        chat_databricks_cls=mock_chat,
        detector_cls=Mock(return_value=_create_mock_detector_result("id", "low", "test")),
        max_retries=2,
    ).detect_primary_keys()['success']

    for error_cls in (ValueError, ImportError):
        mock_spark.sql = Mock(side_effect=(col_res, pk_res, pk_res, col_res))
        detector_err = PrimaryKeyDetector(
            table="test",
            spark_session=mock_spark,
            chat_databricks_cls=mock_chat,
            detector_cls=Mock(side_effect=error_cls("Error")),
            max_retries=0,
        )
        assert not detector_err.detect_primary_keys()['success']

    PrimaryKeyDetector.print_pk_detection_summary(
        {
            'table': 'test',
            'success': True,
            'primary_key_columns': ['id', 'uid'],
            'confidence': 'medium',
            'retries_attempted': 2,
            'validation_performed': True,
            'has_duplicates': False,
            'duplicate_count': 0,
            'final_status': 'success',
            'all_attempts': (
                {'primary_key_columns': ['id'], 'confidence': 'low'},
                {'primary_key_columns': ['id', 'uid'], 'confidence': 'medium'},
            ),
        }
    )

    PrimaryKeyDetector.print_pk_detection_summary(
        {
            'table': 'test',
            'success': False,
            'primary_key_columns': ['id'],
            'confidence': 'low',
            'retries_attempted': 3,
            'validation_performed': False,
            'has_duplicates': True,
            'duplicate_count': 5,
            'final_status': 'max_retries_reached_with_duplicates',
            'all_attempts': ({'primary_key_columns': ['id'], 'confidence': 'low'},),
        }
    )


def test_integration_profiler():
    """Test DQProfiler and DQGenerator integration."""
    rule = DQGenerator.dq_generate_is_unique(
        column="id", level="error", columns=["id"], confidence="high", reasoning="unique"
    )
    assert rule['check']['function'] == 'is_unique'
    assert rule['name'] == 'primary_key_id_validation'

    rule2 = DQGenerator.dq_generate_is_unique(
        column="id,uid", level="warning", columns=["id", "uid"], confidence="medium", llm_detected=True
    )
    assert rule2['user_metadata']['llm_based_detection'] is True

    mock_ws = Mock()
    mock_config = Mock()
    setattr(mock_config, '_product_info', ("dqx", "1.0.0"))
    mock_ws.config = mock_config
    profiler = DQProfiler(mock_ws, Mock())

    with pytest.raises(MissingParameterError):
        profiler.detect_primary_keys_with_llm(None)
    with pytest.raises(MissingParameterError):
        profiler.detect_primary_keys_with_llm(InputConfig(location=""))

    try:
        profiler.detect_primary_keys_with_llm(InputConfig(location="test"))
    except Exception:
        pass


def test_profiler_runner():
    """Test ProfilerRunner with LLM integration scenarios."""
    mock_profiler = Mock()
    runner = ProfilerRunner(ws=Mock(), spark=Mock(), dq_engine=Mock(), installation=Mock(), profiler=mock_profiler)

    mock_gen = Mock()
    mock_gen.llm_engine = None
    stats = {}
    runner.detect_primary_keys_using_llm(mock_gen, InputConfig(location="test"), stats)
    assert 'primary_keys' not in stats
    mock_profiler.detect_primary_keys_with_llm.assert_not_called()

    mock_profiler.detect_primary_keys_with_llm.return_value = {
        'table': 'test',
        'success': True,
        'primary_key_columns': ['id'],
        'confidence': 'high',
        'reasoning': 'unique',
        'has_duplicates': False,
        'duplicate_count': 0,
    }
    mock_gen.llm_engine = Mock()
    stats = {}
    runner.detect_primary_keys_using_llm(mock_gen, InputConfig(location="test"), stats)
    assert stats['primary_keys']['columns'] == ['id']
    assert stats['primary_keys']['confidence'] == 'high'

    mock_profiler.detect_primary_keys_with_llm.return_value = {'table': 'test', 'success': False, 'error': 'Not found'}
    stats = {}
    runner.detect_primary_keys_using_llm(mock_gen, InputConfig(location="test"), stats)
    assert stats['primary_keys']['error'] == 'Not found'

    mock_profiler.detect_primary_keys_with_llm.side_effect = Exception("Unexpected")
    stats = {}
    runner.detect_primary_keys_using_llm(mock_gen, InputConfig(location="test"), stats)
    assert 'Unexpected' in stats['primary_keys']['error']
