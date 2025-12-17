import logging
import re

from typing import Any
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery, DataStreamWriter

from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.errors import InvalidConfigError

logger = logging.getLogger(__name__)

STORAGE_PATH_PATTERN = re.compile(r"^(/|s3:/|abfss:/|gs:/)")
# catalog.schema.table or schema.table or database.table (backticks allow special chars like hyphens for table)
TABLE_PATTERN = re.compile(r"^(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+\.(?:`[^`]+`|[a-zA-Z0-9_]+)$")


def read_input_data(
    spark: SparkSession,
    input_config: InputConfig,
) -> DataFrame:
    """
    Reads input data from the specified location and format.

    Args:
        spark: SparkSession
        input_config: InputConfig with source location/table name, format, and options

    Returns:
        DataFrame with values read from the input data
    """
    if not input_config.location:
        raise InvalidConfigError("Input location not configured")

    if TABLE_PATTERN.match(input_config.location):
        return _read_table_data(spark, input_config)

    if STORAGE_PATH_PATTERN.match(input_config.location):
        return _read_file_data(spark, input_config)

    raise InvalidConfigError(
        f"Invalid input location. It must be a 2 or 3-level table namespace or storage path, given {input_config.location}"
    )


def _read_file_data(spark: SparkSession, input_config: InputConfig) -> DataFrame:
    """
    Reads input data from files (e.g. JSON). Streaming reads must use auto loader with a 'cloudFiles' format.

    Args:
        spark: SparkSession
        input_config: InputConfig with source location, format, and options

    Returns:
        DataFrame with values read from the file data
    """
    if not input_config.is_streaming:
        return spark.read.options(**input_config.options).load(
            input_config.location, format=input_config.format, schema=input_config.schema
        )

    if input_config.format != "cloudFiles":
        raise InvalidConfigError("Streaming reads from file sources must use 'cloudFiles' format")

    return spark.readStream.options(**input_config.options).load(
        input_config.location, format=input_config.format, schema=input_config.schema
    )


def _read_table_data(spark: SparkSession, input_config: InputConfig) -> DataFrame:
    """
    Reads input data from a table registered in Unity Catalog.

    Args:
        spark: SparkSession
        input_config: InputConfig with source location, format, and options

    Returns:
        DataFrame with values read from the table data
    """
    if not input_config.is_streaming:
        return spark.read.options(**input_config.options).table(input_config.location)
    return spark.readStream.options(**input_config.options).table(input_config.location)


def save_dataframe_as_table(df: DataFrame, output_config: OutputConfig) -> StreamingQuery | None:
    """
    Saves a DataFrame as a table using a Unity Catalog table reference or storage path.

    Supports both batch and streaming writes. For streaming DataFrames, returns a StreamingQuery
    that can be used by the caller to monitor or wait for completion. For batch DataFrames, data is
    written synchronously and None is returned.

    Args:
        df: The DataFrame to save (batch or streaming)
        output_config: Output configuration specifying:
            - location: Table name (e.g., 'catalog.schema.table') or storage path
              (e.g., '/Volumes/...', 's3://...', 'abfss://...', 'gs://...')
            - mode: Write mode ('overwrite', 'append', etc.)
            - format: Data format (default: 'delta')
            - options: Additional Spark write options as dict (e.g., "mergeSchema", "overwriteSchema")
            - trigger: (Streaming only) Trigger configuration dict (e.g., "availableNow", "processingTime")

    Returns:
        StreamingQuery if the DataFrame is streaming, None if the DataFrame is batch

    Raises:
        InvalidConfigError: If the output location format is invalid (must be a 2 or 3-level
            table namespace or a storage path starting with /, s3:/, abfss:/, or gs:/)
    """
    if df.isStreaming:
        stream_writer = (
            df.writeStream.format(output_config.format).outputMode(output_config.mode).options(**output_config.options)
        )

        if output_config.trigger:
            logger.info(f"Setting streaming trigger: {output_config.trigger}")
            trigger: dict[str, Any] = output_config.trigger
            stream_writer = stream_writer.trigger(**trigger)
        else:
            logger.info("Using default streaming trigger")

        return _write_stream(stream_writer, output_config)

    batch_writer = df.write.format(output_config.format).mode(output_config.mode).options(**output_config.options)
    _write_batch(batch_writer, output_config)
    return None


def _write_batch(writer: DataFrameWriter, output_config: OutputConfig) -> None:
    """
    Helper method to save a DataFrame to a Delta table or files using batch APIs.

    Args:
        writer: Spark DataFrameWriter for saving data using Spark batch APIs
        output_config: Output table name or delta path, write mode, and options
    """

    if TABLE_PATTERN.match(output_config.location):
        writer.saveAsTable(output_config.location)
    elif STORAGE_PATH_PATTERN.match(output_config.location):
        writer.save(output_config.location)
    else:
        raise InvalidConfigError(
            f"Invalid output location. It must be a 2 or 3-level table namespace or storage path, given {output_config.location}"
        )


def _write_stream(writer: DataStreamWriter, output_config: OutputConfig) -> StreamingQuery:
    """
    Helper method to save a DataFrame to a Delta table or files using streaming APIs.

    Args:
        writer: Spark DataStreamWriter for saving data using Spark streaming APIs
        output_config: Output table name or delta path, write mode, and options
    """

    if TABLE_PATTERN.match(output_config.location):
        return writer.toTable(output_config.location)

    if STORAGE_PATH_PATTERN.match(output_config.location):
        return writer.start(output_config.location)

    raise InvalidConfigError(
        f"Invalid output location. It must be a 2 or 3-level table namespace or storage path, given {output_config.location}"
    )


def is_one_time_trigger(trigger: dict[str, Any] | None) -> bool:
    """
    Checks if a trigger is a one-time trigger that should wait for completion.

    Args:
        trigger: Trigger configuration dict

    Returns:
        True if the trigger is 'once' or 'availableNow', False otherwise
    """
    if trigger is None:
        return False
    return "once" in trigger or "availableNow" in trigger


def get_reference_dataframes(
    spark: SparkSession, reference_tables: dict[str, InputConfig] | None = None
) -> dict[str, DataFrame] | None:
    """
    Get reference DataFrames from the provided reference tables configuration.

    Args:
        spark: SparkSession
        reference_tables: A dictionary mapping of reference table names to their input configurations.

    Examples:
    ```
    reference_tables = {
        "reference_table_1": InputConfig(location="db.schema.table1", format="delta"),
        "reference_table_2": InputConfig(location="db.schema.table2", format="delta")
    }
    ```

    Returns:
        A dictionary mapping reference table names to their DataFrames.
    """
    if not reference_tables:
        return None

    logger.info("Reading reference tables.")
    return {name: read_input_data(spark, input_config) for name, input_config in reference_tables.items()}
