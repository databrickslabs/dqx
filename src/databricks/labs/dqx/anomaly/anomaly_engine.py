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
        baseline_over_time: str | None = None,
    ) -> str:
        """
            Train a row anomaly detection model with intelligent auto-discovery.

            Requires Spark >= 3.4 and the 'anomaly' extras installed:
                pip install 'databricks-labs-dqx[anomaly]'

            Auto-discovery behavior:
            - columns=None, baseline_by=None: Auto-discovers both the feature columns and a grouping
            - columns specified, baseline_by=None: Uses the columns and compares against the whole
              table. Naming the columns means you decided what to measure, so no grouping is added
              on your behalf. If the data looks grouped, a warning names the grouping to pass.
            - baseline_by=[]: Compares against the whole table, and suppresses both the grouping
              discovery above and that warning
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
                where anomalies are broken correlations rather than extreme single values. It needs no
                timestamp column, and trains a single model rather than an ensemble because it is
                deterministic. There is no automatic option: DQX never changes the algorithm on your
                behalf, because the choice cannot be verified without labels. The resolved profile is
                logged on every run. Measured detection quality, with the protocol it was measured
                under, is in the row anomaly detection guide -- deliberately not repeated here, because
                a benchmark figure copied into a docstring is how five files came to disagree about it.
            baseline_by: Columns identifying the group a row belongs to, so a metric is judged
                      against its own group's baseline rather than against the whole table. Each
                      numeric metric gains its deviation from that baseline as an extra feature on
                      a single pooled model, so the cost does not grow with the group count. This
                      is what catches a value that is unremarkable across the table but wrong for
                      its own group. Auto-discovered only when *columns* is also omitted; pass
                      ``baseline_by=[]`` to compare against the whole table and suppress both that
                      discovery and the advisory warning.
            baseline_over_time: A timestamp or date column each metric is judged *along*, so a value is
                      compared with what its own history says to expect at that point in time. Each
                      numeric metric gains its deviation from that expected level as an extra feature,
                      on the same single pooled model. This is what catches a value that is ordinary
                      against the whole training range and wrong for where the trend had got to.
                      Independent of *profile*: measured across nine anomaly types it was worth about the
                      same on both detectors. Composes with *baseline_by*, and the expectation is then
                      fitted on the group-relative value, so one model still covers every group.
                      Never auto-discovered: whether a metric's history is worth comparing against is a
                      judgement about the data, so DQX will warn when the training window shows little
                      trend but will not turn this on for you. **Not a forecaster.** It models the level
                      expected at a time, not the next value, and it does not use the previous row.
                      A seasonal term is fitted only where the training window holds enough complete
                      cycles to identify one; skipped periods are logged with the reason. The named
                      column must not also appear in *columns*, since a time axis is not a metric.
            params: Optional anomaly parameters for tuning training behavior.
            exclude_columns: Columns to exclude from training (e.g., IDs, labels, ground truth).
                            Exclusions always take precedence over `columns` if both are provided.
                            Useful with auto-discovery to filter out unwanted columns without
                            specifying all desired columns manually.
            expected_anomaly_rate: Expected fraction of anomalies in your data (default: 0.02 = 2%).
                                   Supplies the default *contamination* for the estimator, which places
                                   scikit-learn's own ``predict`` / ``offset_`` boundary.
                                   **It does not change which rows DQX flags.** Scoring reads
                                   ``score_samples`` and ranks it against the training score quantiles,
                                   so the rows you see are decided by the *threshold* on the check, not
                                   by this. Nor does it mitigate anomalies present in the training
                                   sample: nothing downweights them. Set it if you load the registered
                                   model yourself and call ``predict``; otherwise tune *threshold*.
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
            baseline_over_time=baseline_over_time,
        )

        log_telemetry(self.ws, "anomaly_num_features", str(len(context.columns)))

        return training_service.train(context)
