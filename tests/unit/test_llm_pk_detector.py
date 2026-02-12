from unittest.mock import Mock
import pytest
import pandas as pd  # type: ignore
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.labs.dqx.llm.llm_pk_detector import LLMPrimaryKeyDetector, PredictionResult
from databricks.labs.dqx.table_manager import TableManager
from databricks.labs.dqx.profiler.profiler import DQProfiler


class MockLLMEngine:
    """Test double for DQLLMEngine."""

    def __init__(self):
        """Initialize mock LLM engine."""


class MockRepository:
    """Mock implementation of TableDataProvider protocol for testing."""

    def __init__(self, should_raise=False):
        """
        Initialize mock repository.

        Args:
            should_raise: If True, methods will raise exceptions.
        """
        self.should_raise = should_raise

    def get_table_columns(self, _table: str) -> pd.DataFrame:
        """Mock get_table_columns."""
        if self.should_raise:
            raise ValueError("Table not found")
        return pd.DataFrame(
            {
                'col_name': ['order_id', 'product_id', 'quantity', 'price'],
                'data_type': ['bigint', 'bigint', 'int', 'decimal'],
                'comment': ['', '', '', ''],
            }
        )

    def get_existing_primary_key(self, _table: str) -> str | None:
        """Mock get_existing_primary_key."""
        return None

    def get_table_properties(self, _table: str) -> pd.DataFrame:
        """Mock get_table_properties."""
        if self.should_raise:
            raise ValueError("Properties not available")
        return pd.DataFrame({'key': ['delta.numRows'], 'value': ['1000']})

    def get_column_statistics(self, _table: str) -> pd.DataFrame:
        """Mock get_column_statistics."""
        if self.should_raise:
            raise ValueError("Stats not available")
        return pd.DataFrame(
            {
                'col_name': ['order_id', 'product_id', 'quantity', 'price'],
                'data_type': ['bigint', 'bigint', 'int', 'decimal'],
            }
        )

    def get_table_column_names(self, _table: str) -> list[str]:
        """Mock get_table_column_names."""
        return ["order_id", "product_id", "quantity", "price"]

    def execute_query(self, _query: str):
        """Mock execute_query that returns an empty result (no duplicates)."""
        mock_result = Mock()
        mock_result.toPandas.return_value = pd.DataFrame()
        return mock_result


class MockTableManager(TableManager):
    """Test double for TableManager."""

    def __init__(self, spark: SparkSession, table_definition="", metadata_info="", should_raise=False):
        """
        Initialize mock table manager.

        Args:
            spark: SparkSession (for compatibility, not used in mock).
            table_definition: Mock table definition to return.
            metadata_info: Mock metadata info to return.
            should_raise: If True, methods will raise exceptions.
        """
        self.table_definition = table_definition
        self.metadata_info = metadata_info
        self.should_raise = should_raise

        # Use mock repository for testing
        mock_repo = MockRepository(should_raise=should_raise)
        super().__init__(spark=None, repository=mock_repo)

    def get_table_definition(self, _table: str):
        """Override to return mock data."""
        if self.should_raise:
            raise ValueError("Table not found")
        return self.table_definition

    def get_table_metadata_info(self, _table: str):
        """Override to return mock data."""
        if self.should_raise:
            raise ValueError("Metadata not available")
        return self.metadata_info

    def check_duplicates(self, _table: str, _pk_columns: list[str]):
        """Mock check_duplicates (legacy method, not in new design)."""
        return False, 0

    def get_table_column_names(self, _table: str):
        """Override to return mock data."""
        return ["order_id", "product_id", "quantity", "price"]

    def run_sql(self, _query: str):
        """Override to return mock data."""
        mock_result = Mock()
        # Return empty dataframe (no duplicates found)
        mock_result.toPandas.return_value = pd.DataFrame()
        return mock_result


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


class MockPredictor:
    """Test double for PrimaryKeyPredictor."""

    def __init__(self, primary_key_columns: list[str], confidence="high", reasoning=""):
        self.primary_key_columns = primary_key_columns
        self.confidence = confidence
        self.reasoning = reasoning

    def predict(self, table, _table_definition, _context, _previous_attempts, _metadata_info) -> PredictionResult:
        """Mock prediction that returns fixed results."""
        return PredictionResult(
            table=table,
            columns=self.primary_key_columns,
            confidence=self.confidence,
            reasoning=self.reasoning,
            success=True,
        )


def test_table_manager(mock_spark):
    """Test TableManager initialization, operations, and metadata."""
    table = "test"
    manager = TableManager(spark=mock_spark)
    # Verify repository was created with the spark session
    assert manager.repository is not None
    assert manager.repository.spark == mock_spark

    # Test successful operations
    mock_df = pd.DataFrame({'col_name': ['id', 'name'], 'data_type': ['bigint', 'string'], 'comment': ['', '']})
    mock_result = Mock()
    mock_result.toPandas.return_value = mock_df
    mock_pk = Mock()
    mock_pk.toPandas.return_value = pd.DataFrame({'key': ['delta.constraints.primary_key'], 'value': ['id']})
    mock_spark.sql = Mock(side_effect=[mock_result, mock_pk])
    definition = manager.get_table_definition(table=table)
    assert 'id bigint' in definition and 'Existing Primary Key: id' in definition

    mock_spark.sql = Mock(side_effect=ValueError("Not found"))
    with pytest.raises(ValueError):
        manager.get_table_definition(table=table)
    mock_spark.sql = Mock(side_effect=TypeError("Type error"))
    with pytest.raises(TypeError, match="Type error"):
        manager.get_table_definition(table=table)

    props_df = pd.DataFrame({'key': ['delta.numRows', 'rawdatasize'], 'value': ['1000', '5000']})
    cols_df = pd.DataFrame(
        {'col_name': ['id', 'amount', 'name', 'date'], 'data_type': ['int', 'decimal', 'varchar', 'date']}
    )
    mock_props = Mock()
    mock_props.toPandas.return_value = props_df
    mock_cols = Mock()
    mock_cols.toPandas.return_value = cols_df
    mock_spark.sql = Mock(side_effect=[mock_props, mock_cols])
    metadata = manager.get_table_metadata_info(table=table)
    assert 'numRows' in metadata or 'Metadata' in metadata


def test_profiler_detect_pk_with_llm(mock_workspace_client, mock_spark):
    # Create profiler
    profiler = DQProfiler(mock_workspace_client, mock_spark, llm_model_config=LLMModelConfig(model_name="test-model"))

    # Verify method accepts correct parameters
    result = profiler.detect_primary_keys_with_llm(input_config=InputConfig(location="test_table"))

    # verify the basic structure
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["table"] == "test_table"


def test_detect_primary_key_composite(mock_spark):
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

    mock_predictor = MockPredictor(
        primary_key_columns=["order_id", "product_id"],
        confidence="high",
        reasoning="Combination of order_id and product_id forms composite primary key for order items",
    )

    detector = LLMPrimaryKeyDetector(
        table_manager=MockTableManager(mock_spark, mock_table_definition, mock_metadata),
        predictor=mock_predictor,
        show_live_reasoning=False,
    )

    result = detector.detect_primary_keys_with_llm(table="order_items")

    assert result["success"] is True
    assert result["primary_key_columns"] == ["order_id", "product_id"]


def test_detect_primary_key_no_clear_key(mock_spark):
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

    mock_predictor = MockPredictor(
        primary_key_columns=["none"],
        confidence="low",
        reasoning="No clear primary key identified - all columns are nullable and none appear to be unique identifiers",
    )

    detector = LLMPrimaryKeyDetector(
        table_manager=MockTableManager(mock_spark, mock_table_definition, mock_metadata),
        predictor=mock_predictor,
        show_live_reasoning=False,
    )

    result = detector.detect_primary_keys_with_llm(table="application_logs")

    assert result["success"] is False
    # The test expects empty list but we'll get ["none"] which will fail validation
    # since "none" is not a valid column name
    assert "error" in result


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
