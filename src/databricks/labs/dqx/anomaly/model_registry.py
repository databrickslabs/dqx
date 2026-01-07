"""
Model registry utilities for anomaly detection.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException
import pyspark.sql.functions as F

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.io import save_dataframe_as_table


def compute_config_hash(columns: list[str], segment_by: list[str] | None) -> str:
    """Generate stable hash of model configuration.

    Args:
        columns: List of column names used for training
        segment_by: List of columns used for segmentation, or None

    Returns:
        16-character hex string (first 16 chars of SHA256 hash)

    Note:
        This hash uniquely identifies a model configuration based on:
        - Sorted list of columns (order-independent)
        - Sorted list of segment_by columns (order-independent)
        Used for collision detection when same model_name is reused with different configs.
    """
    config = {
        "columns": sorted(columns),
        "segment_by": sorted(segment_by) if segment_by else None,
    }
    config_str = json.dumps(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]


ANOMALY_MODEL_TABLE_SCHEMA = (
    "identity struct<model_name:string, model_uri:string, algorithm:string, mlflow_run_id:string, status:string>, "
    "training struct<columns:array<string>, hyperparameters:map<string,string>, training_rows:bigint, "
    "training_time:timestamp, metrics:map<string,double>, baseline_stats:map<string,map<string,double>>>, "
    "features struct<mode:string, column_types:map<string,string>, feature_metadata:string, "
    "feature_importance:map<string,double>, temporal_config:map<string,string>>, "
    "segmentation struct<segment_by:array<string>, segment_values:map<string,string>, "
    "is_global_model:boolean, sklearn_version:string, config_hash:string>"
)


@dataclass
class ModelIdentity:
    """Core model identification (5 fields)."""

    model_name: str
    model_uri: str
    algorithm: str
    mlflow_run_id: str
    status: str = "active"


@dataclass
class TrainingMetadata:
    """Training configuration and metrics (6 fields)."""

    columns: list[str]
    hyperparameters: dict[str, str]
    training_rows: int
    training_time: datetime
    metrics: dict[str, float] | None = None
    baseline_stats: dict[str, dict[str, float]] | None = None


@dataclass
class FeatureEngineering:
    """Feature engineering metadata (5 fields)."""

    mode: str = "spark"
    column_types: dict[str, str] | None = None
    feature_metadata: str | None = None
    feature_importance: dict[str, float] | None = None
    temporal_config: dict[str, str] | None = None


@dataclass
class SegmentationConfig:
    """Segmentation configuration (5 fields)."""

    segment_by: list[str] | None = None
    segment_values: dict[str, str] | None = None
    is_global_model: bool = True
    sklearn_version: str | None = None
    config_hash: str | None = None


@dataclass
class AnomalyModelRecord:
    """Registry record for a trained anomaly model using composition.

    Composed of 4 focused components, each under the 16-attribute limit:
    - identity: Core model identification (5 fields)
    - training: Training configuration and metrics (6 fields)
    - features: Feature engineering metadata (5 fields)
    - segmentation: Segmentation configuration (5 fields)

    Stored as nested structs in Delta tables (no flattening needed).
    """

    identity: ModelIdentity
    training: TrainingMetadata
    features: FeatureEngineering
    segmentation: SegmentationConfig


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
                "baseline_stats": record.training.baseline_stats,
            },
            "features": {
                "mode": record.features.mode,
                "column_types": record.features.column_types,
                "feature_metadata": record.features.feature_metadata,
                "feature_importance": record.features.feature_importance,
                "temporal_config": record.features.temporal_config,
            },
            "segmentation": {
                "segment_by": record.segmentation.segment_by,
                "segment_values": record.segmentation.segment_values,
                "is_global_model": record.segmentation.is_global_model,
                "sklearn_version": record.segmentation.sklearn_version,
                "config_hash": record.segmentation.config_hash,
            },
        }

        # Convert Decimals in nested structures (baseline_stats, metrics, etc.)
        record_dict = AnomalyModelRegistry.convert_decimals(record_dict)

        return spark.createDataFrame([record_dict], schema=ANOMALY_MODEL_TABLE_SCHEMA)

    def save_model(self, record: AnomalyModelRecord, table: str) -> None:
        """Archive previous active model with the same name and insert the new record."""
        if not self._table_exists(table):
            self._create_table(table)

        self._archive_previous(table, record.identity.model_name)

        df = self.build_model_df(self.spark, record)
        save_dataframe_as_table(df, OutputConfig(location=table, mode="append"))

    def get_active_model(self, table: str, model_name: str) -> AnomalyModelRecord | None:
        """Fetch the active model for a given name."""
        if not self._table_exists(table):
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
            segmentation=SegmentationConfig(**values["segmentation"]),
        )

        return record

    def get_segment_model(
        self, table: str, base_model_name: str, segment_values: dict[str, str]
    ) -> AnomalyModelRecord | None:
        """Fetch model for specific segment combination."""
        if not self._table_exists(table):
            return None

        # Build segment name matching the training logic
        segment_name = "_".join(f"{k}={v}" for k, v in segment_values.items())
        segment_model_name = f"{base_model_name}__seg_{segment_name}"

        return self.get_active_model(table, segment_model_name)

    def get_all_segment_models(self, table: str, base_model_name: str) -> list[AnomalyModelRecord]:
        """Fetch all segment models for a base name."""
        if not self._table_exists(table):
            return []

        # Get all active models that start with base_model_name__seg_
        # Use window function to get only the latest version of each segment
        df = self.spark.table(table).filter(
            (F.col("identity.model_name").startswith(f"{base_model_name}__seg_"))
            & (F.col("identity.status") == "active")
        )

        # Deduplicate by model_name (segment), taking the most recent by training_time
        window = Window.partitionBy("identity.model_name").orderBy(F.col("training.training_time").desc())
        df_deduped = df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

        rows = df_deduped.orderBy(F.col("training.training_time").desc()).collect()

        # Convert nested Row structures to dataclasses
        return [
            AnomalyModelRecord(
                identity=ModelIdentity(**row.asDict(recursive=True)["identity"]),
                training=TrainingMetadata(**row.asDict(recursive=True)["training"]),
                features=FeatureEngineering(**row.asDict(recursive=True)["features"]),
                segmentation=SegmentationConfig(**row.asDict(recursive=True)["segmentation"]),
            )
            for row in rows
        ]

    def _table_exists(self, table: str) -> bool:
        """Check if table exists (Unity Catalog compatible)."""
        try:
            # Try to read table schema - more reliable than catalog.tableExists()
            # and compatible with Unity Catalog
            self.spark.table(table).limit(0).count()
            return True
        except AnalysisException:
            # Table doesn't exist or no permissions
            return False

    def _create_table(self, table: str) -> None:
        empty_df = self.spark.createDataFrame([], schema=ANOMALY_MODEL_TABLE_SCHEMA)
        save_dataframe_as_table(empty_df, OutputConfig(location=table, mode="overwrite"))

    def _archive_previous(self, table: str, model_name: str) -> None:
        if not self._table_exists(table):
            return
        # Use LOWER() for case-insensitive matching since Unity Catalog model names are case-insensitive
        self.spark.sql(
            f"UPDATE {table} SET identity.status = 'archived' "
            f"WHERE LOWER(identity.model_name) = LOWER('{model_name}') AND identity.status = 'active'"
        )
