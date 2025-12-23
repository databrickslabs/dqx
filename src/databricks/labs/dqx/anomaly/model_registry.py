"""
Model registry utilities for anomaly detection.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.io import save_dataframe_as_table

ANOMALY_MODEL_TABLE_SCHEMA = (
    "model_name string, model_uri string, input_table string, "
    "columns array<string>, algorithm string, hyperparameters map<string,string>, "
    "training_rows bigint, training_time timestamp, mlflow_run_id string, "
    "status string, metrics map<string,double>, mode string, "
    "baseline_stats map<string,map<string,double>>, "
    "feature_importance map<string,double>, "
    "temporal_config map<string,string>, "
    "segment_by array<string>, "
    "segment_values map<string,string>, "
    "is_global_model boolean, "
    "column_types map<string,string>, "
    "feature_metadata string, "  # JSON string with feature engineering metadata
    "sklearn_version string"  # scikit-learn version used during training
)


@dataclass
class AnomalyModelRecord:  # pylint: disable=too-many-instance-attributes
    """Registry record for a trained anomaly model.

    Note: This dataclass intentionally has many attributes (21) to comprehensively
    track ML model metadata including training config, metrics, feature engineering,
    and segmentation info. Each field serves a specific purpose for model lifecycle management.
    """

    model_name: str
    model_uri: str
    input_table: str
    columns: list[str]
    algorithm: str
    hyperparameters: dict[str, str]
    training_rows: int
    training_time: datetime
    mlflow_run_id: str
    status: str = "active"
    metrics: dict[str, float] | None = None
    mode: str = "spark"
    baseline_stats: dict[str, dict[str, float]] | None = None
    feature_importance: dict[str, float] | None = None
    temporal_config: dict[str, str] | None = None
    segment_by: list[str] | None = None
    segment_values: dict[str, str] | None = None
    is_global_model: bool = True
    column_types: dict[str, str] | None = None  # Maps column name -> type category
    feature_metadata: str | None = None  # JSON string with feature engineering metadata
    sklearn_version: str | None = None  # scikit-learn version used during training


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
        """Convert a registry record into a DataFrame."""
        # Convert record to dict and handle Decimal values for PyArrow compatibility
        record_dict = record.__dict__.copy()

        # Convert Decimals in nested structures (baseline_stats, metrics, etc.)
        for key, value in record_dict.items():
            record_dict[key] = AnomalyModelRegistry.convert_decimals(value)

        return spark.createDataFrame([record_dict], schema=ANOMALY_MODEL_TABLE_SCHEMA)

    def save_model(self, record: AnomalyModelRecord, table: str) -> None:
        """Archive previous active model with the same name and insert the new record."""
        if not self._table_exists(table):
            self._create_table(table)

        self._archive_previous(table, record.model_name)

        df = self.build_model_df(self.spark, record)
        save_dataframe_as_table(df, OutputConfig(location=table, mode="append"))

    def get_active_model(self, table: str, model_name: str) -> AnomalyModelRecord | None:
        """Fetch the active model for a given name."""
        if not self._table_exists(table):
            return None

        row = (
            self.spark.table(table)
            .filter((F.col("model_name") == model_name) & (F.col("status") == "active"))
            .orderBy(F.col("training_time").desc())
            .limit(1)
            .first()
        )
        if not row:
            return None
        values = row.asDict(recursive=True)
        return AnomalyModelRecord(**values)  # type: ignore[arg-type]

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
            (F.col("model_name").startswith(f"{base_model_name}__seg_")) & (F.col("status") == "active")
        )

        # Deduplicate by model_name (segment), taking the most recent by training_time
        window = Window.partitionBy("model_name").orderBy(F.col("training_time").desc())
        df_deduped = df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

        rows = df_deduped.orderBy(F.col("training_time").desc()).collect()

        return [AnomalyModelRecord(**row.asDict(recursive=True)) for row in rows]  # type: ignore[arg-type]

    def _table_exists(self, table: str) -> bool:
        """Check if table exists (Unity Catalog compatible)."""
        try:
            # Try to read table schema - more reliable than catalog.tableExists()
            # and compatible with Unity Catalog
            self.spark.table(table).limit(0).count()
            return True
        except Exception:  # noqa: BLE001
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
            f"UPDATE {table} SET status = 'archived' "
            f"WHERE LOWER(model_name) = LOWER('{model_name}') AND status = 'active'"
        )
