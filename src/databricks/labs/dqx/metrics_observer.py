from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, Observation, SparkSession
import pyspark.sql.functions as F


OBSERVATION_TABLE_SCHEMA = (
    "run_id string, run_name string, input_location string, output_location string, quarantine_location string, "
    "checks_location string, metric_name string, metric_value string, run_time timestamp, error_column_name string, "
    "warning_column_name string, user_metadata map<string, string>"
)


@dataclass(frozen=True)
class DQMetricsObservation:
    """
    Observer metrics class used to persist summary metrics.

    Args:
        run_id: Unique observation id.
        run_name: Name of the observations (default is 'dqx').
        observed_metrics: Dictionary of observed metrics.
        run_time_overwrite: Run time when the data quality summary metrics were observed. If None, current_timestamp() is used.
        error_column_name: Name of the error column when running quality checks.
        warning_column_name: Name of the warning column when running quality checks.
        input_location: (optional) Location where input data is loaded from when running quality checks (fully-qualified table
            name or file path).
        output_location: (optional) Location where output data is persisted when running quality checks (fully-qualified table
            name or file path).
        quarantine_location: (optional) Location where quarantined data is persisted when running quality checks (fully-qualified
            table name or file path).
        checks_location: (optional) Location where checks are loaded from when running quality checks (fully-qualified table name
            or file path).
    """

    run_id: str
    run_name: str
    error_column_name: str
    warning_column_name: str
    run_time_overwrite: datetime | None = None
    observed_metrics: dict[str, Any] | None = None
    input_location: str | None = None
    output_location: str | None = None
    quarantine_location: str | None = None
    checks_location: str | None = None
    user_metadata: dict[str, str] | None = None


@dataclass
class DQMetricsObserver:
    """
    Observation class used to track summary metrics about data quality when validating datasets with DQX

    Args:
        name: Name of the observations which will be displayed in listener metrics (default is 'dqx').
            Also used as run_name field when saving the metrics to a table.
        custom_metrics: Optional list of SQL expressions defining custom, dataset-level quality metrics
    """

    name: str = "dqx"
    custom_metrics: list[str] | None = None
    id_overwrite: str | None = None

    _error_column_name: str = "_errors"
    _warning_column_name: str = "_warnings"

    @cached_property
    def id(self) -> str:
        """
        ID of the observer.

        Returns:
            Unique ID
        """
        return self.id_overwrite or str(uuid4())

    @cached_property
    def metrics(self) -> list[str]:
        """
        Gets the observer metrics as Spark SQL expressions.

        Returns:
            A list of Spark SQL expressions defining the observer metrics (both default and custom).
        """
        default_metrics = [
            "count(1) as input_row_count",
            f"count(case when {self._error_column_name} is not null then 1 end) as error_row_count",
            f"count(case when {self._warning_column_name} is not null then 1 end) as warning_row_count",
            f"count(case when {self._error_column_name} is null and {self._warning_column_name} is null then 1 end) as valid_row_count",
        ]
        if self.custom_metrics:
            default_metrics.extend(self.custom_metrics)
        return default_metrics

    @property
    def observation(self) -> Observation:
        """
        Spark `Observation` which can be attached to a `DataFrame` to track summary metrics. Metrics will be collected
        when the 1st action is triggered on the attached `DataFrame`. Subsequent operations on the attached `DataFrame`
        will not update the observed metrics. See: [PySpark Observation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Observation.html)
        for complete documentation.

        Returns:
            A Spark `Observation` instance
        """
        return Observation()

    def set_column_names(self, error_column_name: str, warning_column_name: str) -> None:
        """
        Sets the default column names (e.g. `_errors` and `_warnings`) for monitoring summary metrics.

        Args:
            error_column_name: Error column name
            warning_column_name: Warning column name
        """
        self._error_column_name = error_column_name
        self._warning_column_name = warning_column_name

    @staticmethod
    def build_metrics_df(spark: SparkSession, observation: DQMetricsObservation) -> DataFrame:
        """
        Builds a Spark `DataFrame` from a `DQMetricsObservation`.

        Args:
            spark: `SparkSession` used to create the `DataFrame`
            observation: `DQMetricsObservation` with summary metrics

        Returns:
            A Spark `DataFrame` with summary metrics
        """

        if not observation.observed_metrics:
            return spark.createDataFrame([], schema=OBSERVATION_TABLE_SCHEMA)

        df = spark.createDataFrame(
            [
                [
                    observation.run_id,
                    observation.run_name,
                    observation.input_location,
                    observation.output_location,
                    observation.quarantine_location,
                    observation.checks_location,
                    metric_key,
                    metric_value,
                    observation.run_time_overwrite,
                    observation.error_column_name,
                    observation.warning_column_name,
                    observation.user_metadata if observation.user_metadata else None,
                ]
                for metric_key, metric_value in observation.observed_metrics.items()
            ],
            schema=OBSERVATION_TABLE_SCHEMA,
        )

        if observation.run_time_overwrite is None:
            df = df.withColumn("run_time", F.current_timestamp())

        return df
