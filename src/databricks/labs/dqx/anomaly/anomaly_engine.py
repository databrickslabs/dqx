"""
AnomalyEngine entrypoint for anomaly detection.
"""

from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.service import AnomalyTrainingService
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.telemetry import log_telemetry, telemetry_logger
from databricks.sdk import WorkspaceClient


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
        model_name = anomaly_engine.train(
            df,
            model_name="catalog.schema.my_anomaly_model",
            registry_table="catalog.schema.dqx_anomaly_models",
        )

        # Train with specific configuration
        model_name = anomaly_engine.train(
            df=df,
            model_name="catalog.schema.regional_model",
            registry_table="catalog.schema.dqx_anomaly_models",
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
        registry_table: str,
        columns: list[str] | None = None,
        segment_by: list[str] | None = None,
        params: AnomalyParams | None = None,
        exclude_columns: list[str] | None = None,
        expected_anomaly_rate: float = 0.02,
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
            model_name: Model name (REQUIRED). Must be fully qualified as
                       'catalog.schema.model'.
            registry_table: Registry table (REQUIRED). Must be fully qualified as
                            'catalog.schema.table'.
            columns: Columns to use for anomaly detection (auto-discovered if omitted).
            segment_by: Segment columns (auto-discovered if both columns and segment_by omitted).
            params: Optional anomaly parameters for tuning training behavior.
            exclude_columns: Columns to exclude from training (e.g., IDs, labels, ground truth).
                            Exclusions always take precedence over `columns` if both are provided.
                            Useful with auto-discovery to filter out unwanted columns without
                            specifying all desired columns manually.
            expected_anomaly_rate: Expected fraction of anomalies in your data (default: 0.02 = 2%).
                                   This helps the model calibrate what's "normal" vs "unusual".
                                   Common values: 0.01-0.02 (fraud), 0.03-0.05 (quality issues), 0.10 (exploration).
                                   This sets the model contamination default.
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
            anomaly_engine.train(
                df,
                model_name="catalog.schema.my_model",
                registry_table="catalog.schema.dqx_anomaly_models",
            )

            # Exclude ID fields (recommended)
            anomaly_engine.train(
                df,
                model_name="catalog.schema.my_model",
                registry_table="catalog.schema.dqx_anomaly_models",
                exclude_columns=["user_id", "order_id"],
            )

            # Adjust expected anomaly rate for specific use cases
            anomaly_engine.train(
                df,
                model_name="catalog.schema.fraud_detector",
                registry_table="catalog.schema.dqx_anomaly_models",
                expected_anomaly_rate=0.01,  # 1% fraud
            )
            anomaly_engine.train(
                df,
                model_name="catalog.schema.quality_monitor",
                registry_table="catalog.schema.dqx_anomaly_models",
                expected_anomaly_rate=0.10,  # 10% defects
            )

            # Explicit columns
            anomaly_engine.train(
                df,
                model_name="catalog.schema.sales_monitor",
                registry_table="catalog.schema.dqx_anomaly_models",
                columns=["revenue", "transactions"],
            )
        """
        training_service = AnomalyTrainingService(self.spark)
        context = training_service.build_context(
            df,
            model_name,
            registry_table,
            columns=columns,
            segment_by=segment_by,
            params=params,
            exclude_columns=exclude_columns,
            expected_anomaly_rate=expected_anomaly_rate,
        )

        log_telemetry(self.ws, "anomaly_auto_discovery", str(columns is None).lower())
        log_telemetry(self.ws, "anomaly_segmented", str(context.segment_by is not None).lower())
        log_telemetry(self.ws, "anomaly_num_features", str(len(context.columns)))

        return training_service.train(context)
