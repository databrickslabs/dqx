"""
Model registry utilities for row anomaly detection.

Persistence only: schema and AnomalyModelRegistry. Record types live in model_config.
"""

from decimal import Decimal
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    GroupingConfig,
    ModelIdentity,
    TrainingMetadata,
)
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.utils import table_exists


ANOMALY_MODEL_TABLE_SCHEMA = (
    "identity struct<model_name:string, model_uri:string, algorithm:string, mlflow_run_id:string, status:string>, "
    "training struct<columns:array<string>, hyperparameters:map<string,string>, training_rows:bigint, "
    "training_time:timestamp, metrics:map<string,double>, score_quantiles:map<string,double>, "
    "baseline_stats:map<string,map<string,double>>>, "
    "features struct<mode:string, column_types:map<string,string>, feature_metadata:string, "
    "feature_importance:map<string,double>, temporal_config:map<string,string>>, "
    "grouping struct<baseline_by:array<string>, sklearn_version:string, config_hash:string>"
)


class AnomalyModelRegistry:
    """Manage anomaly model metadata in a Delta table."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def convert_decimals(obj: Any) -> Any:
        """Recursively convert Decimal values to float for PyArrow compatibility.

        This is a public utility method that can be used by tests and other code
        to handle Decimal to float conversions for PyArrow compatibility.
        """
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, dict):
            return {k: AnomalyModelRegistry.convert_decimals(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [AnomalyModelRegistry.convert_decimals(item) for item in obj]
        return obj

    @staticmethod
    def build_model_df(spark: SparkSession, record: AnomalyModelRecord) -> DataFrame:
        """Convert a registry record into a DataFrame with nested structure."""
        # Convert composed dataclass to nested dict structure
        record_dict = {
            "identity": {
                "model_name": record.identity.model_name,
                "model_uri": record.identity.model_uri,
                "algorithm": record.identity.algorithm,
                "mlflow_run_id": record.identity.mlflow_run_id,
                "status": record.identity.status,
            },
            "training": {
                "columns": record.training.columns,
                "hyperparameters": record.training.hyperparameters,
                "training_rows": record.training.training_rows,
                "training_time": record.training.training_time,
                "metrics": record.training.metrics,
                "score_quantiles": record.training.score_quantiles,
                "baseline_stats": record.training.baseline_stats,
            },
            "features": {
                "mode": record.features.mode,
                "column_types": record.features.column_types,
                "feature_metadata": record.features.feature_metadata,
                "feature_importance": record.features.feature_importance,
                "temporal_config": record.features.temporal_config,
            },
            "grouping": {
                "baseline_by": record.grouping.baseline_by,
                "sklearn_version": record.grouping.sklearn_version,
                "config_hash": record.grouping.config_hash,
            },
        }

        # Convert Decimals in nested structures (baseline_stats, metrics, etc.)
        record_dict = AnomalyModelRegistry.convert_decimals(record_dict)

        return spark.createDataFrame([record_dict], schema=ANOMALY_MODEL_TABLE_SCHEMA)

    def save_model(self, record: AnomalyModelRecord, table: str) -> None:
        """Archive previous active model with the same name and insert the new record."""
        table_existed = table_exists(self.spark, table)
        if not table_existed:
            self._create_table(table)
        else:
            self._archive_previous(table, record.identity.model_name)

        df = self.build_model_df(self.spark, record)
        # mergeSchema so a registry table created by an earlier DQX reconciles to the new struct
        # shape on the next write instead of failing -- the `grouping` struct replaced `segmentation`
        # this release. Retraining is exactly what the configuration-hash error tells the user to do,
        # and it writes to their existing table, so the remedy has to work.
        save_dataframe_as_table(df, OutputConfig(location=table, mode="append", options={"mergeSchema": "true"}))

    def get_active_model(self, table: str, model_name: str) -> AnomalyModelRecord | None:
        """Fetch the active model for a given name."""
        if not table_exists(self.spark, table):
            return None

        row = (
            self.spark.table(table)
            .filter((F.col("identity.model_name") == model_name) & (F.col("identity.status") == "active"))
            .orderBy(F.col("training.training_time").desc())
            .limit(1)
            .first()
        )
        if not row:
            return None

        # Convert nested Row structure to dataclasses
        values = row.asDict(recursive=True)
        record = AnomalyModelRecord(
            identity=ModelIdentity(**values["identity"]),
            training=TrainingMetadata(**values["training"]),
            features=FeatureEngineering(**values["features"]),
            grouping=GroupingConfig(**values["grouping"]),
        )

        return record

    def _create_table(self, table: str) -> None:
        empty_df = self.spark.createDataFrame([], schema=ANOMALY_MODEL_TABLE_SCHEMA)
        save_dataframe_as_table(empty_df, OutputConfig(location=table, mode="overwrite"))

    def _archive_previous(self, table: str, model_name: str) -> None:
        """Call only when table is known to exist (e.g. from save_model)."""
        df = self.spark.table(table)
        identity_col = F.col("identity")
        match = (F.lower(identity_col.getField("model_name")) == model_name.lower()) & (
            identity_col.getField("status") == "active"
        )
        new_status = F.when(match, F.lit("archived")).otherwise(identity_col.getField("status"))
        new_identity = F.struct(
            identity_col.getField("model_name"),
            identity_col.getField("model_uri"),
            identity_col.getField("algorithm"),
            identity_col.getField("mlflow_run_id"),
            new_status.alias("status"),
        )
        updated = df.withColumn("identity", new_identity)
        save_dataframe_as_table(updated, OutputConfig(location=table, mode="overwrite"))
