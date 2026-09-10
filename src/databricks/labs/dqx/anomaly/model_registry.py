"""
Model registry utilities for row anomaly detection.

Persistence only: schema and AnomalyModelRegistry. Record types live in model_config.
"""

import logging
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
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.utils import table_exists


logger = logging.getLogger(__name__)


ANOMALY_MODEL_TABLE_SCHEMA = (
    "identity struct<model_name:string, model_uri:string, algorithm:string, mlflow_run_id:string, status:string>, "
    "training struct<columns:array<string>, hyperparameters:map<string,string>, training_rows:bigint, "
    "training_time:timestamp, metrics:map<string,double>, score_quantiles:map<string,double>, "
    "baseline_stats:map<string,map<string,double>>>, "
    "features struct<mode:string, column_types:map<string,string>, feature_metadata:string, "
    "feature_importance:map<string,double>, temporal_config:map<string,string>>, "
    "grouping struct<baseline_by:array<string>, sklearn_version:string, config_hash:string>"
)


#: The pre-release grouping column. A registry table written before ``segment_by`` was removed carries
#: this instead of ``grouping``. Everything that reads or migrates such a table keys off this name, so
#: there is one place to delete when support for those tables is dropped.
LEGACY_GROUPING_COLUMN = "segmentation"

#: Substrings Unity Catalog and Spark use when a write is refused for want of a grant. Matched rather
#: than parsed: the surrounding message wording is not a stable interface, but these codes are what the
#: platform documents, and mistaking a real failure for a permissions one would send a user to their
#: admin over a bug.
_PERMISSION_DENIED_MARKERS = ("PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS", "does not have permission")


def is_permission_error(exc: Exception) -> bool:
    """Whether *exc* reports a missing grant rather than a genuine failure.

    Public because it encodes a judgement worth testing directly, in both directions: mistaking a real
    failure for a missing grant sends a user to their platform team over a defect in DQX, with nothing in
    the message to suggest otherwise. Reaching into a private name from a test would be the alternative,
    which AGENTS.md rules out.
    """
    message = str(exc).upper()
    return any(marker.upper() in message for marker in _PERMISSION_DENIED_MARKERS)


def normalise_grouping(values: dict[str, Any]) -> dict[str, Any]:
    """Return the ``grouping`` payload for a registry row, whatever schema the row was written in.

    Exists so that reading a pre-release table produces the *useful* error rather than an internal one.
    A model registered before this release cannot be scored at all -- the configuration hash formula
    changed, so the recomputed hash always mismatches and scoring raises with instructions to retrain --
    but without this the read died on ``KeyError: 'grouping'`` first, and the caller saw an internal
    error instead of those instructions.

    Three shapes reach here. A pre-release table has ``segmentation`` and no ``grouping``. A table that an
    earlier build of this release appended to with ``mergeSchema`` has both, with ``grouping`` null on the
    rows written before. A current table has ``grouping`` alone.

    ``segment_values`` and ``is_global_model`` are dropped rather than carried: there is exactly one
    pooled model now, so neither has a meaning to preserve.
    """
    grouping = values.get("grouping")
    if grouping:
        return grouping

    legacy = values.get(LEGACY_GROUPING_COLUMN) or {}
    return {
        "baseline_by": legacy.get("segment_by") or [],
        "sklearn_version": legacy.get("sklearn_version"),
        "config_hash": legacy.get("config_hash"),
    }


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
    def _record_dict(record: AnomalyModelRecord) -> dict[str, Any]:
        """Flatten a record into the nested dict the table schema expects.

        Extracted from build_model_df so the migration writes rows through exactly the same mapping a
        normal save uses. Two copies of this would diverge on the next field added, and the divergence
        would only show up as a migrated row missing a value.
        """
        return {
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

    @staticmethod
    def build_model_df(spark: SparkSession, record: AnomalyModelRecord) -> DataFrame:
        """Convert a registry record into a DataFrame with nested structure."""
        # Convert Decimals in nested structures (baseline_stats, metrics, etc.)
        record_dict = AnomalyModelRegistry.convert_decimals(AnomalyModelRegistry._record_dict(record))

        return spark.createDataFrame([record_dict], schema=ANOMALY_MODEL_TABLE_SCHEMA)

    def save_model(self, record: AnomalyModelRecord, table: str) -> None:
        """Archive previous active model with the same name and insert the new record."""
        table_existed = table_exists(self.spark, table)
        if not table_existed:
            self._create_table(table)
        else:
            # Migrate before anything else touches the table, so archiving and the append below run
            # against the current schema and need no compatibility handling of their own.
            self._migrate_legacy_registry(table)
            self._archive_previous(table, record.identity.model_name)

        df = self.build_model_df(self.spark, record)
        # A plain append, deliberately without mergeSchema. Its only purpose was to reconcile the
        # pre-release `segmentation` column, which _migrate_legacy_registry now does explicitly and
        # completely. Leaving it on would mean a future schema change silently widens a user's table
        # instead of failing and demanding its own migration.
        save_dataframe_as_table(df, OutputConfig(location=table, mode="append"))

    def _migrate_legacy_registry(self, table: str) -> None:
        """Rewrite a pre-release registry table into the current schema, in place.

        The alternative was appending with ``mergeSchema``, which is not a migration: the table keeps both
        ``segmentation`` and ``grouping`` for good, each null on half its rows, and every reader afterwards
        has to know about both. Rewriting costs one pass over a table holding one row per model version,
        and leaves a schema a user can read without knowing this release happened.

        Historical rows are migrated rather than dropped, so the archive of previous versions survives.
        Delta keeps the pre-migration versions in table history if anyone needs to look.

        No-op unless the legacy column is present, so the normal path pays a schema read and nothing else.
        """
        if LEGACY_GROUPING_COLUMN not in self.spark.table(table).columns:
            return

        logger.warning(
            f"Registry table '{table}' uses the pre-release schema. Migrating it to the current one: "
            f"'{LEGACY_GROUPING_COLUMN}' becomes 'grouping', and segment_values and is_global_model are "
            f"dropped because there is one pooled model now. Previous versions remain in Delta history."
        )

        migrated = [
            AnomalyModelRecord(
                identity=ModelIdentity(**values["identity"]),
                training=TrainingMetadata(**values["training"]),
                features=FeatureEngineering(**values["features"]),
                grouping=GroupingConfig(**normalise_grouping(values)),
            )
            for values in (row.asDict(recursive=True) for row in self.spark.table(table).collect())
        ]

        rewritten = (
            self.spark.createDataFrame(
                [self.convert_decimals(self._record_dict(record)) for record in migrated],
                schema=ANOMALY_MODEL_TABLE_SCHEMA,
            )
            if migrated
            else self.spark.createDataFrame([], schema=ANOMALY_MODEL_TABLE_SCHEMA)
        )

        try:
            save_dataframe_as_table(
                rewritten,
                OutputConfig(location=table, mode="overwrite", options={"overwriteSchema": "true"}),
            )
        except Exception as exc:
            if not is_permission_error(exc):
                raise
            raise InvalidParameterError(
                f"Cannot migrate registry table '{table}' from the pre-release schema: this needs MODIFY "
                f"on the table. Ask an owner to grant it, or train into a registry table you own. Note "
                f"that models already in '{table}' cannot be scored either until they are retrained, "
                f"because the configuration hash changed this release."
            ) from exc

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
            grouping=GroupingConfig(**normalise_grouping(values)),
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
