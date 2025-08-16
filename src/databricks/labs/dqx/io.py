import logging
import re

from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricks.labs.dqx.config import InputConfig, OutputConfig

logger = logging.getLogger(__name__)

STORAGE_PATH_PATTERN = re.compile(r"^(/|s3:/|abfss:/|gs:/)")
# catalog.schema.table or schema.table or database.table
TABLE_PATTERN = re.compile(r"^(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$")


def read_input_data(
    spark: SparkSession,
    input_config: InputConfig,
) -> DataFrame:
    """
    Reads input data from the specified location and format.

    :param spark: SparkSession
    :param input_config: InputConfig with source location/table name, format, and options
    :return: DataFrame with values read from the input data
    """
    if not input_config.location:
        raise ValueError("Input location not configured")

    if TABLE_PATTERN.match(input_config.location):
        return _read_table_data(spark, input_config)

    if STORAGE_PATH_PATTERN.match(input_config.location):
        return _read_file_data(spark, input_config)

    raise ValueError(
        f"Invalid input location. It must be a 2 or 3-level table namespace or storage path, given {input_config.location}"
    )


def _read_file_data(spark: SparkSession, input_config: InputConfig) -> DataFrame:
    """
    Reads input data from files (e.g. JSON). Streaming reads must use auto loader with a 'cloudFiles' format.
    :param spark: SparkSession
    :param input_config: InputConfig with source location, format, and options
    :return: DataFrame with values read from the file data
    """
    if not input_config.is_streaming:
        return spark.read.options(**input_config.options).load(
            input_config.location, format=input_config.format, schema=input_config.schema
        )

    if input_config.format != "cloudFiles":
        raise ValueError("Streaming reads from file sources must use 'cloudFiles' format")

    return spark.readStream.options(**input_config.options).load(
        input_config.location, format=input_config.format, schema=input_config.schema
    )


def _read_table_data(spark: SparkSession, input_config: InputConfig) -> DataFrame:
    """
    Reads input data from a table registered in Unity Catalog.
    :param spark: SparkSession
    :param input_config: InputConfig with source location, format, and options
    :return: DataFrame with values read from the table data
    """
    if not input_config.is_streaming:
        return spark.read.options(**input_config.options).table(input_config.location)
    return spark.readStream.options(**input_config.options).table(input_config.location)


def save_dataframe_as_table(df: DataFrame, output_config: OutputConfig):
    """
    Helper method to save a DataFrame to a Delta table.
    :param df: The DataFrame to save
    :param output_config: Output table name, write mode, and options
    """
    logger.info(f"Saving data to {output_config.location} table")

    if df.isStreaming:
        if not output_config.trigger:
            query = (
                df.writeStream.format(output_config.format)
                .outputMode(output_config.mode)
                .options(**output_config.options)
                .toTable(output_config.location)
            )
        else:
            trigger: dict[str, Any] = output_config.trigger
            query = (
                df.writeStream.format(output_config.format)
                .outputMode(output_config.mode)
                .options(**output_config.options)
                .trigger(**trigger)
                .toTable(output_config.location)
            )
        query.awaitTermination()
    else:
        (
            df.write.format(output_config.format)
            .mode(output_config.mode)
            .options(**output_config.options)
            .saveAsTable(output_config.location)
        )
