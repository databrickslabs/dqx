from dataclasses import dataclass, field
from functools import cached_property
from uuid import uuid4

from pyspark.sql import Observation


@dataclass
class DQMetricsObserver:
    """
    Observation class used to track summary metrics about data quality when validating datasets with DQX

    Args:
        name: Name of the observations which will be displayed in listener metrics (default is 'dqx').
        custom_metrics: Optional list of SQL expressions defining custom, dataset-level quality metrics
    """

    name: str = "dqx"
    custom_metrics: list[str] | None = None
    _id: str = field(default_factory=str)
    _error_column_name: str = "_errors"
    _warning_column_name: str = "_warnings"

    def __post_init__(self) -> None:
        self._id = str(uuid4())

    @cached_property
    def observation_id(self) -> str:
        """
        Observer ID.

        Returns:
            Unique observation ID
        """
        return self._id

    @cached_property
    def metrics(self) -> list[str]:
        """
        Gets the observer metrics as Spark SQL expressions.

        Returns:
            A list of Spark SQL expressions defining the observer metrics (both default and custom).
        """
        default_metrics = [
            "count(1) as input_count",
            f"count(case when {self._error_column_name} is not null then 1 end) as error_count",
            f"count(case when {self._warning_column_name} is not null then 1 end) as warning_count",
            f"count(case when {self._error_column_name} is null and {self._warning_column_name} is null then 1 end) as valid_count",
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
        return Observation(name=self.name)

    def _set_column_names(self, error_column_name: str, warning_column_name: str) -> None:
        """
        Sets the default column names (e.g. `_errors` and `_warnings`) for monitoring summary metrics.

        Args:
            error_column_name: Error column name
            warning_column_name: Warning column name
        """
        self._error_column_name = error_column_name
        self._warning_column_name = warning_column_name
