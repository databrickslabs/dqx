import logging
import json
import warnings
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, TextIO
from collections.abc import Callable

import yaml
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.checks_validator import ChecksValidator
from databricks.labs.dqx.rule import (
    DQRule,
    DQRowRule,
    DQDatasetRule,
    DQForEachColRule,
    CHECK_FUNC_REGISTRY,
    normalize_bound_args,
)
from databricks.labs.dqx.utils import safe_json_load
from databricks.labs.dqx.errors import InvalidCheckError
from databricks.labs.dqx.checks_formats import FILE_SERIALIZERS, FILE_DESERIALIZERS

CHECKS_TABLE_SCHEMA = (
    "name STRING, criticality STRING, check STRUCT<function STRING, for_each_column ARRAY<STRING>,"
    " arguments MAP<STRING, STRING>>, filter STRING, run_config_name STRING, user_metadata MAP<STRING, STRING>"
)

logger = logging.getLogger(__name__)


class ChecksNormalizer:
    """
    Handles normalization and denormalization of check dictionaries.
    E.g. responsible for converting Decimal values to/from serializable format.
    """

    @staticmethod
    def normalize(checks: list[dict]) -> list[dict]:
        """
        Recursively normalize checks dictionary to make it JSON/YAML serializable.

        Args:
            checks: List of check dictionaries that may contain non-serializable values.

        Returns:
            List of normalized check dictionaries.
        """

        def normalize_value(val: Any) -> Any:
            """Recursively normalize a value."""
            if isinstance(val, dict):
                return {k: normalize_value(v) for k, v in val.items()}
            # normalize_bound_args handles None, primitives, lists, tuples, Decimal, etc.
            return normalize_bound_args(val)

        return [normalize_value(check) for check in checks]

    @staticmethod
    def denormalize_value(val: Any) -> Any:
        """Recursively convert special markers (e.g. Decimal) back to original objects."""
        if isinstance(val, dict):
            # Check if this is a Decimal marker
            if "__decimal__" in val and len(val) == 1:
                return Decimal(val["__decimal__"])
            # Otherwise, recursively process the dict
            return {k: ChecksNormalizer.denormalize_value(v) for k, v in val.items()}
        if isinstance(val, (list, tuple)):
            return type(val)(ChecksNormalizer.denormalize_value(v) for v in val)
        return val

    @staticmethod
    def denormalize(checks: list[dict]) -> list[dict]:
        """
        Recursively convert special markers back to objects after deserialization.
        Converts special markers (e.g., __decimal__ format) back to Decimal objects.

        Args:
            checks: List of check dictionaries that may contain special markers.

        Returns:
            List of check dictionaries with special markers converted to objects.
        """
        return [ChecksNormalizer.denormalize_value(check) for check in checks]


class FileFormatSerializer(ABC):
    """
    Abstract base class for file format serializers.
    """

    @abstractmethod
    def serialize(self, data: list[dict]) -> str:
        """Serialize data to string format."""

    @abstractmethod
    def deserialize(self, file_like: TextIO) -> list[dict]:
        """Deserialize data from file-like object."""


class JsonSerializer(FileFormatSerializer):
    """JSON format serializer implementation."""

    def serialize(self, data: list[dict]) -> str:
        """Serialize data to JSON string."""
        return json.dumps(data)

    def deserialize(self, file_like: TextIO) -> list[dict]:
        """Deserialize data from JSON file."""
        return json.load(file_like) or []


class YamlSerializer(FileFormatSerializer):
    """YAML format serializer implementation."""

    def serialize(self, data: list[dict]) -> str:
        """Serialize data to YAML string."""
        return yaml.safe_dump(data)

    def deserialize(self, file_like: TextIO) -> list[dict]:
        """Deserialize data from YAML file."""
        return yaml.safe_load(file_like) or []


class SerializerFactory:
    """
    Factory for creating appropriate serializers based on file extension.
    """

    _serializers: dict[str, type[FileFormatSerializer]] = {
        ".json": JsonSerializer,
        ".yaml": YamlSerializer,
        ".yml": YamlSerializer,
    }

    @classmethod
    def get_supported_extensions(cls) -> tuple[str, ...]:
        """
        Get tuple of supported file extensions.

        Returns:
            Tuple of supported file extensions (e.g., (".json", ".yaml", ".yml")).
        """
        return tuple(cls._serializers.keys())

    @classmethod
    def create_serializer(cls, extension: str | None = None) -> FileFormatSerializer:
        """
        Create a serializer based on file extension.

        Args:
            extension: File extension (e.g., ".json", ".yaml", ".yml").
                       If None or empty, defaults to YAML.

        Returns:
            Appropriate serializer instance. Defaults to YAML if extension not recognized or not provided.
        """
        if not extension:
            return YamlSerializer()
        ext = extension.lower()
        serializer_class = cls._serializers.get(ext, YamlSerializer)
        return serializer_class()

    @classmethod
    def register_format(cls, extension: str, serializer_class: type[FileFormatSerializer]) -> None:
        """
        Register a new file format serializer.

        Args:
            extension: File extension
            serializer_class: Serializer class implementing FileFormatSerializer interface.
        """
        cls._serializers[extension.lower()] = serializer_class


class ChecksSerializer:
    """
    Handles serialization of DQRule objects to dictionaries and file formats.
    """

    @staticmethod
    def serialize(checks: list[DQRule]) -> list[dict]:
        """
        Converts a list of quality checks defined as *DQRule* objects to a list of quality checks
        defined as Python dictionaries.

        Args:
            checks: List of DQRule instances to convert.

        Returns:
            List of dictionaries representing the DQRule instances.

        Raises:
            InvalidCheckError: If any item in the list is not a DQRule instance.
        """
        dq_rules = []
        for check in checks:
            if not isinstance(check, DQRule):
                raise InvalidCheckError(f"Expected DQRule instance, got {type(check).__name__}")
            dq_rules.append(check.to_dict())
        return dq_rules

    @staticmethod
    def serialize_to_bytes(checks: list[dict], extension: str) -> bytes:
        """
        Serializes a list of checks to bytes in json or yaml (default) format.

        Args:
            checks: List of checks to serialize.
            extension: File extension (e.g., ".json", ".yaml", ".yml").
        Returns:
            Serialized checks as bytes.
        """
        serializer = SerializerFactory.create_serializer(extension)
        normalized_checks = ChecksNormalizer.normalize(checks)
        serialized_str = serializer.serialize(normalized_checks)
        return serialized_str.encode("utf-8")


class ChecksDeserializer:
    """
    Handles deserialization of dictionaries to DQRule objects and from file formats.
    """

    def __init__(self, custom_checks: dict[str, Callable] | None = None):
        """
        Initialize the deserializer.

        Args:
            custom_checks: Dictionary with custom check functions.
        """
        self.custom_checks = custom_checks

    def deserialize(self, checks: list[dict]) -> list[DQRule]:
        """
        Converts a list of quality checks defined as Python dictionaries to a list of `DQRule` objects.

        Args:
            checks: list of dictionaries describing checks. Each check is a dictionary
                consisting of following fields:
                - *check* - Column expression to evaluate. This expression should return string value if it's evaluated to true
                    or *null* if it's evaluated to *false*
                - *name* - name that will be given to a resulting column. Autogenerated if not provided
                - *criticality* (optional) - possible values are *error* (data going only into "bad" dataframe),
                and *warn* (data is going into both dataframes)
                - *filter* (optional) - Expression for filtering data quality checks
                - *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.

        Returns:
            list of data quality check rules

        Raises:
            InvalidCheckError: If any dictionary is invalid or unsupported.
        """
        status = ChecksValidator.validate_checks(checks, self.custom_checks)
        if status.has_errors:
            raise InvalidCheckError(str(status))

        dq_rule_checks: list[DQRule] = []
        for check_def in checks:
            logger.debug(f"Processing check definition: {check_def}")

            check = check_def.get("check", {})
            name = check_def.get("name", None)
            func_name = check.get("function")
            func = resolve_check_function(func_name, self.custom_checks, fail_on_missing=True)
            assert func  # should already be validated

            func_args = check.get("arguments", {})
            for_each_column = check.get("for_each_column")
            column = func_args.get("column")  # should be defined for single-column checks only
            columns = func_args.get("columns")  # should be defined for multi-column checks only
            assert not (column and columns)  # should already be validated
            criticality = check_def.get("criticality", "error")
            filter_str = check_def.get("filter")
            user_metadata = check_def.get("user_metadata")

            # Exclude `column` and `columns` from check_func_kwargs
            # as these are always included in the check function call
            check_func_kwargs = {k: v for k, v in func_args.items() if k not in {"column", "columns"}}

            # treat non-registered function as row-level checks
            if for_each_column:
                dq_rule_checks += DQForEachColRule(
                    columns=for_each_column,
                    name=name,
                    check_func=func,
                    criticality=criticality,
                    filter=filter_str,
                    check_func_kwargs=check_func_kwargs,
                    user_metadata=user_metadata,
                ).get_rules()
            else:
                rule_type = CHECK_FUNC_REGISTRY.get(func_name)
                if rule_type == "dataset":
                    dq_rule_checks.append(
                        DQDatasetRule(
                            column=column,
                            columns=columns,
                            check_func=func,
                            check_func_kwargs=check_func_kwargs,
                            name=name,
                            criticality=criticality,
                            filter=filter_str,
                            user_metadata=user_metadata,
                        )
                    )
                else:  # default to row-level rule
                    dq_rule_checks.append(
                        DQRowRule(
                            column=column,
                            columns=columns,
                            check_func=func,
                            check_func_kwargs=check_func_kwargs,
                            name=name,
                            criticality=criticality,
                            filter=filter_str,
                            user_metadata=user_metadata,
                        )
                    )

        return dq_rule_checks

    @staticmethod
    def deserialize_from_file(extension: str, file_like: TextIO) -> list[dict]:
        """
        Deserialize checks from a file-like object based on file extension.
        Automatically denormalizes special markers back to objects.

        Args:
            extension: File extension (e.g., ".json", ".yaml", ".yml").
            file_like: File-like object to read from.

        Returns:
            List of check dictionaries with special markers converted to objects.
        """
        serializer = SerializerFactory.create_serializer(extension)
        checks = serializer.deserialize(file_like)
        return ChecksNormalizer.denormalize(checks)


class DataFrameConverter:
    """
    Handles conversion between DataFrames and check dictionaries.
    """

    @staticmethod
    def from_dataframe(df: DataFrame, run_config_name: str = "default") -> list[dict]:
        """
        Converts a list of quality checks defined in a DataFrame to a list of quality checks
        defined as Python dictionaries.

        Args:
            df: DataFrame with data quality check rules. Each row should define a check. Rows should
            have the following columns:
                - *name* - Name that will be given to a resulting column. Autogenerated if not provided.
                - *criticality* (optional) - Possible values are *error* (data going only into "bad" dataframe) and *warn* (data is going into both dataframes).
                - *check* - DQX check function used in the check; A *StructType* column defining the data quality check.
                - *filter* - Expression for filtering data quality checks.
                - *run_config_name* (optional) - Run configuration name for storing checks across runs.
                - *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
            run_config_name: Run configuration name for filtering quality rules, e.g. input table or job name (use "default" if not provided).

        Returns:
                List of data quality check specifications as a Python dictionary
        """
        check_rows = df.where(f"run_config_name = '{run_config_name}'").collect()
        collect_limit = 500
        if len(check_rows) > collect_limit:
            warnings.warn(
                f"Collecting large number of rows from DataFrame: {len(check_rows)}",
                category=UserWarning,
                stacklevel=2,
            )

        checks = []
        for row in check_rows:
            check_dict = {
                "name": row.name,
                "criticality": row.criticality,
                "check": {
                    "function": row.check["function"],
                    "arguments": (
                        {k: safe_json_load(v) for k, v in row.check["arguments"].items()}
                        if row.check["arguments"] is not None
                        else {}
                    ),
                },
            }
            if "for_each_column" in row.check and row.check["for_each_column"]:
                check_dict["check"]["for_each_column"] = row.check["for_each_column"]
            if row.filter is not None:
                check_dict["filter"] = row.filter
            if row.user_metadata is not None:
                check_dict["user_metadata"] = row.user_metadata
            # Denormalize special markers back to objects
            checks.append(ChecksNormalizer.denormalize_value(check_dict))
        return checks

    @staticmethod
    def to_dataframe(
        spark: SparkSession,
        checks: list[dict],
        run_config_name: str = "default",
    ) -> DataFrame:
        """
        Converts a list of quality checks defined as Python dictionaries to a DataFrame.

        Args:
            spark: Spark session.
            checks: list of check specifications as Python dictionaries. Each check consists of the following fields:
                - *check* - Column expression to evaluate. This expression should return string value if it's evaluated to
                   true (it will be used as an error/warning message) or *null* if it's evaluated to *false*
                - *name* - Name that will be given to a resulting column. Autogenerated if not provided
                - *criticality* (optional) - Possible values are *error* (data going only into "bad" dataframe) and *warn*
                   (data is going into both dataframes)
                - *filter* (optional) - Expression for filtering data quality checks
                - *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
            run_config_name: Run configuration name for storing quality checks across runs, e.g. input table or job name (use "default" if not provided)

        Returns:
            DataFrame with data quality check rules

        Raises:
            InvalidCheckError: If any check is invalid or unsupported.
        """
        dq_rule_checks: list[DQRule] = deserialize_checks(checks)

        dq_rule_rows = []
        for dq_rule_check in dq_rule_checks:
            arguments = dict(dq_rule_check.check_func_kwargs)

            if dq_rule_check.column is not None:
                arguments["column"] = dq_rule_check.column

            if dq_rule_check.columns is not None:
                arguments["columns"] = dq_rule_check.columns

            json_arguments = {k: json.dumps(normalize_bound_args(v)) for k, v in arguments.items()}
            dq_rule_rows.append(
                [
                    dq_rule_check.name,
                    dq_rule_check.criticality,
                    {"function": dq_rule_check.check_func.__name__, "arguments": json_arguments},
                    dq_rule_check.filter,
                    run_config_name,
                    dq_rule_check.user_metadata,
                ]
            )
        return spark.createDataFrame(dq_rule_rows, CHECKS_TABLE_SCHEMA)


def serialize_checks(checks: list[DQRule]) -> list[dict]:
    """
    Converts a list of quality checks defined as *DQRule* objects to a list of quality checks
    defined as Python dictionaries.

    This is a convenience user-friendly function that wraps ChecksSerializer.serialize.

    Args:
        checks: List of DQRule instances to convert.

    Returns:
        List of dictionaries representing the DQRule instances.

    Raises:
        InvalidCheckError: If any item in the list is not a DQRule instance.
    """
    return ChecksSerializer.serialize(checks)


def deserialize_checks(checks: list[dict], custom_checks: dict[str, Callable] | None = None) -> list[DQRule]:
    """
    Converts a list of quality checks defined as Python dictionaries to a list of DQRule objects.

    This is a convenience user-friendly function that wraps ChecksDeserializer.deserialize.

    Args:
        checks: list of dictionaries describing checks. Each check is a dictionary
            consisting of following fields:
            - *check* - Column expression to evaluate. This expression should return string value if it's evaluated to true
                or *null* if it's evaluated to *false*
            - *name* - name that will be given to a resulting column. Autogenerated if not provided
            - *criticality* (optional) - possible values are *error* (data going only into "bad" dataframe),
            and *warn* (data is going into both dataframes)
            - *filter* (optional) - Expression for filtering data quality checks
            - *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
        custom_checks: Dictionary with custom check functions.

    Returns:
        list of data quality check rules

    Raises:
        InvalidCheckError: If any dictionary is invalid or unsupported.
    """
    deserializer = ChecksDeserializer(custom_checks)
    return deserializer.deserialize(checks)
