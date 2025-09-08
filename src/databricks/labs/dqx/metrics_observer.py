from dataclasses import dataclass, field
from uuid import uuid4

from pyspark.sql import Observation
from databricks.labs.dqx.rule import ColumnArguments, DefaultColumnNames


@dataclass
class DQMetricsObserver:
    """
    Observation class used to track summary metrics about data quality when validating datasets with DQX

    Args:
        name: Name of the observations which will be displayed in listener metrics (default is 'dqx').
        custom_metrics: Optional list of SQL expressions defining custom, dataset-level quality metrics
        result_columns: Optional dictionary of result column names (e.g. 'errors_column' or 'warnings_column') for
        tracking summary metrics (defaults are '_errors' and '_warnings')
    """

    name: str = "dqx"
    custom_metrics: list[str] | None = None
    result_columns: dict[str, str] | None = field(default_factory=dict)
    metrics: list[str] = field(default_factory=list)
    _id: str = field(default_factory=str)

    def __post_init__(self) -> None:
        self._id = str(uuid4())
        self.metrics.extend(self.default_metrics)
        if self.custom_metrics:
            self.metrics.extend(self.custom_metrics)

    @property
    def id(self) -> str:
        """
        Observer ID.

        Returns:
            Unique observation ID
        """
        return self._id

    @property
    def default_metrics(self) -> list[str]:
        """
        Default metrics tracked by the DQObservation.

        Returns:
            A list of Spark SQL expressions as strings
        """
        result_columns = self.result_columns or {}
        errors_column = result_columns.get(ColumnArguments.ERRORS.value, DefaultColumnNames.ERRORS.value)
        warnings_column = result_columns.get(ColumnArguments.WARNINGS.value, DefaultColumnNames.WARNINGS.value)
        return [
            "count(1) as input_count",
            f"count(case when {errors_column} is not null then 1 end) as error_count",
            f"count(case when {warnings_column} is not null then 1 end) as warning_count",
            f"count(case when {errors_column} is null and {warnings_column} is null then 1 end) as valid_count",
        ]

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
