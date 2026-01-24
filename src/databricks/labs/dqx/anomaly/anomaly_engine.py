"""
AnomalyEngine entrypoint for anomaly detection workflows.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.telemetry import telemetry_logger, log_telemetry
from databricks.labs.dqx.anomaly.trainer import (
    _apply_expected_anomaly_rate,
    _perform_auto_discovery,
    _prepare_training_config,
    _process_exclude_columns,
    _train_global,
    _train_segmented,
    _validate_columns,
    _validate_spark_version,
)
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class AnomalyEngine(DQEngineBase):
    """Engine for anomaly detection model lifecycle management.

    This class provides methods for training, managing, and working with anomaly detection models.
    It follows the same architectural pattern as DQProfiler, managing state like SparkSession
    and WorkspaceClient while delegating operations to internal helper functions.

    Args:
        workspace_client: WorkspaceClient instance used to access the Databricks workspace.
        spark: Optional SparkSession to use. If not provided, the active session is used.

    Examples:
        # Initialize engine
        from databricks.sdk import WorkspaceClient
        from databricks.labs.dqx.anomaly import AnomalyEngine

        ws = WorkspaceClient()
        anomaly_engine = AnomalyEngine(ws)

        # Train a model with auto-discovery
        model_name = anomaly_engine.train(df, model_name="my_anomaly_model")

        # Train with specific configuration
        model_name = anomaly_engine.train(
            df=df,
            model_name="regional_model",
            columns=["revenue", "transactions"],
            segment_by=["region"]
        )
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
    ):
        super().__init__(workspace_client)
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

    @telemetry_logger("anomaly", "train")
    def train(
        self,
        df: DataFrame,
        model_name: str,
        columns: list[str] | None = None,
        segment_by: list[str] | None = None,
        registry_table: str | None = None,
        exclude_columns: list[str] | None = None,
        expected_anomaly_rate: float = 0.02,
        row_id_columns: list[str] | None = None,
    ) -> str:
        """
        Train anomaly detection model(s) with intelligent auto-discovery.

        Requires Spark >= 3.4 and the 'anomaly' extras installed:
            pip install 'databricks-labs-dqx[anomaly]'

        Auto-discovery behavior:
        - columns=None, segment_by=None: Auto-discovers both (simplest)
        - columns specified, segment_by=None: Uses columns, no segmentation
        - columns=None, segment_by specified: Auto-discovers columns, uses segments

        Args:
            df: Input DataFrame containing historical "normal" data.
            model_name: Model name (REQUIRED). Provide a descriptive name like 'field_force_anomaly'.
                       Can be simple name ('my_model') or full path ('catalog.schema.my_model').
                       If simple name provided, catalog.schema will be derived from registry_table.
            columns: Columns to use for anomaly detection (auto-discovered if omitted).
            segment_by: Segment columns (auto-discovered if both columns and segment_by omitted).
            registry_table: Optional registry table; auto-derived if not provided.
            exclude_columns: Columns to exclude from training (e.g., IDs, labels, ground truth).
                            Useful with auto-discovery to filter out unwanted columns without
                            specifying all desired columns manually.
            expected_anomaly_rate: Expected fraction of anomalies in your data (default: 0.02 = 2%).
                                  This helps the model calibrate what's "normal" vs "unusual".
                                  Common values: 0.01-0.02 (fraud), 0.03-0.05 (quality issues), 0.10 (exploration).
                                  This sets the model contamination default.
            row_id_columns: Unique identifier columns (optional). These are preserved for
                          validation/feature-importance shuffling but are NOT used for training.

        Important Notes:
            - Avoid ID columns (user_id, order_id, etc.) - use exclude_columns to filter them out.
            - Choose behavioral columns, not identifiers. Good: amount, quantity. Bad: user_id.
            - See documentation for detailed column selection best practices.

        Returns:
            Base model name (e.g., 'catalog.schema.model_name'). For segmented models,
            individual segments are stored with suffixes like '__seg_region=APAC', but
            the base name is returned for simplified API usage.

        Examples:
            # Auto-discovery with default 2% expected anomaly rate (simplest)
            anomaly_engine.train(df, model_name="my_model")

            # Exclude ID fields (recommended)
            anomaly_engine.train(df, model_name="my_model", exclude_columns=["user_id", "order_id"])

            # Adjust expected anomaly rate for specific use cases
            anomaly_engine.train(df, model_name="fraud_detector", expected_anomaly_rate=0.01)  # 1% fraud
            anomaly_engine.train(df, model_name="quality_monitor", expected_anomaly_rate=0.10)  # 10% defects

            # Explicit columns
            anomaly_engine.train(df, model_name="sales_monitor", columns=["revenue", "transactions"])
        """
        _validate_spark_version(self.spark)

        # Track if auto-discovery will be used (for telemetry)
        auto_discovery_used = columns is None

        # Validate model_name is provided
        if not model_name:
            raise InvalidParameterError(
                "model_name is required. Provide a descriptive name like 'field_force_anomaly' or "
                "'sales_rep_monitor'. The full catalog.schema.model path will be constructed automatically."
            )

        # Process exclude_columns
        df_filtered, _exclude_list = _process_exclude_columns(df, columns, exclude_columns)

        # Auto-discovery
        columns, segment_by, discovery_warnings = _perform_auto_discovery(df_filtered, columns, segment_by)

        # Show auto-discovery warnings
        for warning in discovery_warnings:
            logger.warning(warning)

        # Validate columns
        if not columns:
            raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

        params = AnomalyParams()
        validation_warnings = _validate_columns(df, columns, params)

        # Show validation warnings
        for warning in validation_warnings:
            logger.warning(warning)

        # Prepare training configuration
        derived_model_name, derived_registry_table = _prepare_training_config(
            model_name, registry_table, self.spark, columns, segment_by
        )

        # Apply expected_anomaly_rate to params if contamination not explicitly set
        params = _apply_expected_anomaly_rate(params, expected_anomaly_rate)

        # Log telemetry details (utilization metrics, no customer data)
        log_telemetry(self.ws, "anomaly_auto_discovery", str(auto_discovery_used).lower())
        log_telemetry(self.ws, "anomaly_segmented", str(segment_by is not None).lower())
        log_telemetry(self.ws, "anomaly_num_features", str(len(columns)))

        # Execute training
        if segment_by:
            return _train_segmented(
                self.spark,
                df_filtered,
                columns,
                segment_by,
                derived_model_name,
                derived_registry_table,
                params,
                row_id_columns,
            )
        return _train_global(
            self.spark, df_filtered, columns, derived_model_name, derived_registry_table, params, row_id_columns
        )
