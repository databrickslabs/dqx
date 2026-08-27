"""
AnomalyEngine entrypoint for row anomaly detection.
"""

from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.training_service import AnomalyTrainingService
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.telemetry import log_telemetry, telemetry_logger
from databricks.sdk import WorkspaceClient


class AnomalyEngine(DQEngineBase):
    """Engine for row anomaly detection model lifecycle management.

    This class provides methods for training, managing, and working with row anomaly detection models.

    Args:
        workspace_client: WorkspaceClient instance used to access the Databricks workspace.
        spark: Optional SparkSession to use. If not provided, the active session is used.

    Examples:
        # Initialize engine
        from databricks.sdk import WorkspaceClient
        from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine

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
            baseline_by=["region"]
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
        params: AnomalyParams | None = None,
        exclude_columns: list[str] | None = None,
        expected_anomaly_rate: float = 0.02,
        baseline_by: list[str] | None = None,
        profile: str | None = None,
    ) -> str:
        """
            Train a row anomaly detection model with intelligent auto-discovery.

            Requires Spark >= 3.4 and the 'anomaly' extras installed:
                pip install 'databricks-labs-dqx[anomaly]'

            Auto-discovery behavior:
            - columns=None, baseline_by=None: Auto-discovers both the feature columns and a grouping
            - columns specified, baseline_by=None: Uses the columns, still discovers a grouping
            - baseline_by specified: Conditions on that grouping

        Args:
            df: Input DataFrame containing historical "normal" data.
            model_name: Model name (REQUIRED). Must be fully qualified Unity Catalog name as
                       'catalog.schema.model'.
            registry_table: Registry table (REQUIRED). Must be fully qualified Unity Catalog table as
                            'catalog.schema.table'.
            columns: Columns to use for row anomaly detection (auto-discovered if omitted).
            profile: What kind of data this is, which selects the detector. Defaults to
                ``"tabular"`` -- IsolationForest, exactly the behaviour before this option existed.
                ``"timeseries"`` selects a correlation-aware detector suited to multivariate metrics,
                where anomalies are broken correlations rather than extreme single values; measured on
                the SMD benchmark it surfaces 82% of incidents inside a 1%-of-rows alert budget against
                36% for the tabular detector. It needs no timestamp column, and trains a single model
                rather than an ensemble because it is deterministic. There is no automatic option: DQX
                never changes the algorithm on your behalf, because the choice cannot be verified
                without labels. The resolved profile is logged on every run.
            baseline_by: Columns identifying the group a row belongs to, so a metric is judged
                      against its own group's baseline rather than against the whole table. Each
                      numeric metric gains its deviation from that baseline as an extra feature on
                      a single pooled model, so the cost does not grow with the group count. This
                      is what catches a value that is unremarkable across the table but wrong for
                      its own group. Auto-discovered when omitted.
            params: Optional anomaly parameters for tuning training behavior.
            exclude_columns: Columns to exclude from training (e.g., IDs, labels, ground truth).
                            Exclusions always take precedence over `columns` if both are provided.
                            Useful with auto-discovery to filter out unwanted columns without
                            specifying all desired columns manually.
            expected_anomaly_rate: Expected fraction of anomalies in your data (default: 0.02 = 2%).
                                   Used as the default contamination parameter for the Isolation Forest
                                   algorithm, which controls the proportion of training data that the model
                                   treats as outliers when learning the decision boundary. A higher value
                                   makes the model flag more rows as anomalous.
                                   Common values: 0.01-0.02 (fraud), 0.03-0.05 (quality issues), 0.10 (exploration).
                                   Overridden if params.algorithm_config.contamination is set explicitly.
        Important Notes:
            - Avoid ID columns (user_id, order_id, etc.) - use exclude_columns to filter them out.
            - Choose behavioral columns, not identifiers. Good: amount, quantity. Bad: user_id.
            - See documentation for detailed column selection best practices.

        Returns:
            The model name (e.g., 'catalog.schema.model_name').

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

            # Judge each row against its own group's baseline rather than the whole table, on a
            # single model however many groups there are.
            anomaly_engine.train(
                df,
                model_name="catalog.schema.regional_model",
                registry_table="catalog.schema.dqx_anomaly_models",
                columns=["event_count"],
                baseline_by=["country", "product"],
            )
        """
        training_service = AnomalyTrainingService(self.spark)
        context = training_service.build_context(
            df,
            model_name,
            registry_table,
            columns=columns,
            params=params,
            exclude_columns=exclude_columns,
            expected_anomaly_rate=expected_anomaly_rate,
            baseline_by=baseline_by,
            profile=profile,
        )

        log_telemetry(self.ws, "anomaly_num_features", str(len(context.columns)))

        return training_service.train(context)
