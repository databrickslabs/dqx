"""
Integration tests for primary key detection in the DQX profiler.
"""

from unittest.mock import Mock, patch
import pytest
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import ProfilerConfig, LLMConfig
from databricks.labs.dqx.profiler.profiler import DQProfiler, DQProfile
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.runner import ProfilerRunner
from databricks.labs.dqx.config import InputConfig


@pytest.fixture
def mock_workspace_client():
    """Mock WorkspaceClient for testing."""
    return Mock(spec=WorkspaceClient)


@pytest.fixture
def mock_spark():
    """Mock Spark session for testing."""
    return Mock(spec=SparkSession)


@pytest.fixture
def mock_installation():
    """Mock installation for testing."""
    return Mock()


@pytest.fixture
def profiler_config_with_llm_pk():
    """ProfilerConfig with LLM-based PK detection enabled."""
    return ProfilerConfig(
        llm_config=LLMConfig(
            enable_pk_detection=True,
            pk_detection_endpoint="databricks-meta-llama-3-1-8b-instruct",
        ),
        sample_fraction=0.1,
        limit=100,
    )


@pytest.fixture
def profiler_config_without_llm_pk():
    """ProfilerConfig with LLM-based PK detection disabled (default)."""
    return ProfilerConfig(
        llm_config=LLMConfig(enable_pk_detection=False),  # Explicit disable (though False is default)
        sample_fraction=0.1, 
        limit=100
    )


class TestPrimaryKeyDetectionIntegration:
    """Test primary key detection integration with the profiler."""

    def test_profiler_config_has_llm_pk_options(self, profiler_config_with_llm_pk):
        """Test that ProfilerConfig includes LLM-based PK detection options."""
        assert profiler_config_with_llm_pk.llm_config.enable_pk_detection is True
        assert profiler_config_with_llm_pk.llm_config.pk_detection_endpoint == "databricks-meta-llama-3-1-8b-instruct"

    def test_profiler_config_defaults_no_llm(self):
        """Test that ProfilerConfig defaults to no LLM-based PK detection."""
        config = ProfilerConfig()
        assert config.llm_config.enable_pk_detection is False  # Should default to False

    @patch('databricks.labs.dqx.llm.pk_identifier.DatabricksPrimaryKeyDetector')
    def test_profiler_detect_primary_key_success(self, mock_detector_class, mock_workspace_client, mock_spark):
        """Test successful primary key detection in profiler."""
        # Setup mock detector
        mock_detector = Mock()
        mock_detector.detect_primary_key_from_table_name.return_value = {
            "success": True,
            "primary_key_columns": ["customer_id"],
            "confidence": "high",
            "reasoning": "customer_id is a unique identifier",
            "has_duplicates": False,
            "validation_performed": True,
        }
        mock_detector.print_pk_detection_summary = Mock()
        mock_detector_class.return_value = mock_detector

        profiler = DQProfiler(mock_workspace_client)

        options = {
            "enable_llm_pk_detection": True,
            "llm_pk_detection_endpoint": "test-endpoint",
            "llm_pk_validate_duplicates": True,
            "llm_pk_max_retries": 3,
        }

        result = profiler.detect_primary_keys_with_llm("customers", "main", "sales", options, llm=True)

        assert result is not None
        assert result["success"] is True
        assert result["primary_key_columns"] == ["customer_id"]
        assert result["confidence"] == "high"

        # Verify detector was called with correct parameters
        mock_detector_class.assert_called_once_with(
            table_name="customers",
            schema="sales",
            catalog="main",
            endpoint="test-endpoint",
            validate_duplicates=True,
            spark_session=mock_spark,
            max_retries=3,
        )

    def test_profiler_detect_primary_keys_with_llm_not_requested(self, mock_workspace_client):
        """Test that LLM PK detection returns None when not explicitly requested."""
        profiler = DQProfiler(mock_workspace_client)

        # Test with llm=False
        result = profiler.detect_primary_keys_with_llm("customers", "main", "sales", {}, llm=False)
        assert result is None

        # Test with no LLM options
        result = profiler.detect_primary_keys_with_llm("customers", "main", "sales", {})
        assert result is None

        # Test with empty options
        result = profiler.detect_primary_keys_with_llm("customers", "main", "sales", None)
        assert result is None

    def test_profiler_detect_primary_key_disabled(self, mock_workspace_client):
        """Test that LLM PK detection returns None when disabled."""
        profiler = DQProfiler(mock_workspace_client)

        options = {"enable_llm_pk_detection": False}

        result = profiler.detect_primary_keys_with_llm("customers", "main", "sales", options, llm=False)

        assert result is None

    @patch('databricks.labs.dqx.llm.pk_identifier.DatabricksPrimaryKeyDetector')
    def test_profiler_detect_primary_key_failure(self, mock_detector_class, mock_workspace_client, mock_spark):
        """Test primary key detection failure handling."""
        # Setup mock detector to return failure
        mock_detector = Mock()
        mock_detector.detect_primary_key_from_table_name.return_value = {"success": False, "error": "Table not found"}
        mock_detector_class.return_value = mock_detector

        profiler = DQProfiler(mock_workspace_client)

        options = {"enable_llm_pk_detection": True}

        result = profiler.detect_primary_keys_with_llm("nonexistent_table", "main", "sales", options, llm=True)

        assert result is None

    @patch('databricks.labs.dqx.utils.read_input_data')
    @patch('databricks.labs.dqx.llm.pk_identifier.DatabricksPrimaryKeyDetector')
    def test_profile_table_with_pk_detection(
        self, mock_detector_class, mock_read_input, mock_workspace_client, mock_spark
    ):
        """Test profile_table method with PK detection enabled."""
        # Setup mock DataFrame
        mock_df = Mock()
        mock_df.columns = ["customer_id", "name", "email"]
        mock_df.schema.fields = [Mock(name="customer_id"), Mock(name="name"), Mock(name="email")]
        mock_df.select.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_read_input.return_value = mock_df

        # Setup mock detector
        mock_detector = Mock()
        mock_detector.detect_primary_key_from_table_name.return_value = {
            "success": True,
            "primary_key_columns": ["customer_id"],
            "confidence": "high",
            "reasoning": "customer_id is a unique identifier",
            "has_duplicates": False,
            "validation_performed": True,
        }
        mock_detector.print_pk_detection_summary = Mock()
        mock_detector_class.return_value = mock_detector

        # Mock DataFrame operations for profiling
        mock_df.summary.return_value.collect.return_value = []

        profiler = DQProfiler(mock_workspace_client)

        options = {
            "enable_pk_detection": True,
            "pk_generate_uniqueness_checks": True,
            "sample_fraction": 0.1,
            "limit": 100,
        }

        summary_stats, dq_rules = profiler.profile_table("main.sales.customers", options=options)

        # Check that PK detection was added to summary stats
        assert "primary_key_detection" in summary_stats
        pk_info = summary_stats["primary_key_detection"]
        assert pk_info["detected_columns"] == ["customer_id"]
        assert pk_info["confidence"] == "high"
        assert pk_info["has_duplicates"] is False

        # Note: Automatic rule generation has been removed
        # Users must manually create rules based on the detection results

    def test_dq_generator_primary_key_rule(self):
        """Test DQGenerator can generate primary key validation rules."""

        generator = DQGenerator(Mock())

        # Create a primary key profile
        pk_profile = DQProfile(
            name="is_primary_key",
            column="customer_id",
            parameters={
                "columns": ["customer_id"],
                "confidence": "high",
                "reasoning": "customer_id is a unique identifier",
                "nulls_distinct": False,
            },
        )

        rules = generator.generate_dq_rules([pk_profile])

        assert len(rules) == 1
        rule = rules[0]
        assert rule["check"]["function"] == "is_unique"
        assert rule["check"]["arguments"]["columns"] == ["customer_id"]
        assert rule["check"]["arguments"]["nulls_distinct"] is False
        assert rule["name"] == "primary_key_customer_id_validation"
        assert rule["user_metadata"]["detected_primary_key"] is True
        assert rule["user_metadata"]["pk_detection_confidence"] == "high"

    def test_dq_generator_composite_primary_key_rule(self):
        """Test DQGenerator can generate composite primary key validation rules."""

        generator = DQGenerator(Mock())

        # Create a composite primary key profile
        pk_profile = DQProfile(
            name="is_primary_key",
            column="order_id,product_id",
            parameters={
                "columns": ["order_id", "product_id"],
                "confidence": "medium",
                "reasoning": "Combination of order_id and product_id forms composite primary key",
                "nulls_distinct": False,
            },
        )

        rules = generator.generate_dq_rules([pk_profile])

        assert len(rules) == 1
        rule = rules[0]
        assert rule["check"]["function"] == "is_unique"
        assert rule["check"]["arguments"]["columns"] == ["order_id", "product_id"]
        assert rule["name"] == "primary_key_order_id_product_id_validation"

    @patch('databricks.labs.dqx.utils.read_input_data')
    @patch('databricks.labs.dqx.llm.pk_identifier.DatabricksPrimaryKeyDetector')
    def test_profiler_runner_with_pk_detection(
        self,
        mock_detector_class,
        mock_read_input,
        mock_workspace_client,
        mock_spark,
        mock_installation,
        profiler_config_with_llm_pk,
    ):
        """Test ProfilerRunner with PK detection enabled."""
        # Setup mocks
        mock_df = Mock()
        mock_df.columns = ["id", "name"]
        mock_df.schema.fields = [Mock(name="id"), Mock(name="name")]
        mock_df.select.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.summary.return_value.collect.return_value = []
        mock_read_input.return_value = mock_df

        mock_detector = Mock()
        mock_detector.detect_primary_key_from_table_name.return_value = {
            "success": True,
            "primary_key_columns": ["id"],
            "confidence": "high",
            "reasoning": "id is a unique identifier",
        }
        mock_detector.print_pk_detection_summary = Mock()
        mock_detector_class.return_value = mock_detector

        profiler = DQProfiler(mock_workspace_client)
        generator = DQGenerator(mock_workspace_client)
        runner = ProfilerRunner(mock_workspace_client, mock_spark, mock_installation, profiler, generator)

        input_config = InputConfig(location="main.test.users")

        checks, summary_stats = runner.run(input_config, profiler_config_with_llm_pk)

        # Verify PK detection was performed
        assert "primary_key_detection" in summary_stats

        # Note: Automatic rule generation has been removed
        # PK detection results are available in summary_stats for manual rule creation

    def test_profiler_runner_without_pk_detection(
        self, mock_workspace_client, mock_spark, mock_installation, profiler_config_without_llm_pk
    ):
        """Test ProfilerRunner with PK detection disabled."""
        profiler = DQProfiler(mock_workspace_client)
        generator = DQGenerator(mock_workspace_client)
        runner = ProfilerRunner(mock_workspace_client, mock_spark, mock_installation, profiler, generator)

        # Mock DataFrame for regular profiling
        with patch('databricks.labs.dqx.utils.read_input_data') as mock_read_input:
            mock_df = Mock()
            mock_df.columns = ["id", "name"]
            mock_df.schema.fields = [Mock(name="id"), Mock(name="name")]
            mock_df.select.return_value = mock_df
            mock_df.count.return_value = 100
            mock_df.summary.return_value.collect.return_value = []
            mock_read_input.return_value = mock_df

            input_config = InputConfig(location="/path/to/file.parquet")  # Non-table path

            checks, summary_stats = runner.run(input_config, profiler_config_without_llm_pk)

            # Verify PK detection was NOT performed
            assert "primary_key_detection" not in summary_stats

            # Note: No automatic rule generation occurs regardless of PK detection status
