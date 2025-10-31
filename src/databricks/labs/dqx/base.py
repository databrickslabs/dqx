import abc
from collections.abc import Callable
from functools import cached_property
from typing import final
from pyspark.sql import DataFrame, Observation
from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.labs.dqx.rule import DQRule
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.__about__ import __version__


class DQEngineBase(abc.ABC):
    def __init__(self, workspace_client: WorkspaceClient):
        self._workspace_client = self._verify_workspace_client(workspace_client)

    @cached_property
    def ws(self) -> WorkspaceClient:
        """Return a verified *WorkspaceClient* configured for DQX.

        Ensures workspace connectivity and sets the product info used for
        telemetry so that requests are attributed to *dqx*.
        """
        return self._workspace_client

    @staticmethod
    @final
    def _verify_workspace_client(ws: WorkspaceClient) -> WorkspaceClient:
        """
        Verify the Databricks WorkspaceClient configuration and connectivity.
        """
        # Using reflection to set right value for _product_info as dqx for telemetry
        product_info = getattr(ws.config, '_product_info')
        if product_info[0] != "dqx":
            setattr(ws.config, '_product_info', ('dqx', __version__))

        # make sure Databricks workspace is accessible
        # use api that works on all workspaces and clusters including group assigned clusters
        ws.clusters.select_spark_version()
        return ws


class DQEngineCoreBase(DQEngineBase):
    @abc.abstractmethod
    def apply_checks(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame | tuple[DataFrame, Observation]:
        """Apply data quality checks to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply to the DataFrame. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality
            summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """

    @abc.abstractmethod
    def apply_checks_and_split(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]:
        """Apply data quality checks to the given DataFrame and split the results into two DataFrames
        ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply to the DataFrame. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows
            with errors or warnings and the corresponding result columns) and an optional Observation which tracks data
            quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """

    @abc.abstractmethod
    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame | tuple[DataFrame, Observation]:
        """
        Apply data quality checks defined as metadata to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of dictionaries describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality
            summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """

    @abc.abstractmethod
    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]:
        """Apply data quality checks defined as metadata to the given DataFrame and split the results into
        two DataFrames ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of dictionaries describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows
            with errors or warnings and the corresponding result columns) and an optional Observation which tracks data
            quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """

    @staticmethod
    @abc.abstractmethod
    def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
        """
        Validate checks defined as metadata to ensure they conform to the expected structure and types.

        This method validates the presence of required keys, the existence and callability of functions,
        and the types of arguments passed to those functions.

        Args:
            checks: List of checks to apply to the DataFrame. Each check should be a dictionary.
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            validate_custom_check_functions: If True, validate custom check functions.

        Returns:
            ChecksValidationStatus indicating the validation result.
        """

    @abc.abstractmethod
    def get_invalid(self, df: DataFrame) -> DataFrame:
        """
        Return records that violate data quality checks (rows with warnings or errors).

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with rows that have errors or warnings and the corresponding result columns.
        """

    @abc.abstractmethod
    def get_valid(self, df: DataFrame) -> DataFrame:
        """
        Return records that do not violate data quality checks (rows with warnings but no errors).

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with warning rows but without the results columns.
        """

    @staticmethod
    @abc.abstractmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        """
        Load DQ rules (checks) from a local JSON or YAML file.

        The returned checks can be used as input to *apply_checks_by_metadata*.

        Args:
            filepath: Path to a file containing checks definitions.

        Returns:
            List of DQ rules (checks).
        """

    @staticmethod
    @abc.abstractmethod
    def save_checks_in_local_file(checks: list[dict], filepath: str):
        """
        Save DQ rules (checks) to a local YAML or JSON file.

        Args:
            checks: List of DQ rules (checks) to save.
            filepath: Path to a file where the checks definitions will be saved.
        """
