"""
Pandas-compatible implementation of the DQX engine.

This module provides a DQX engine implementation that works with Pandas DataFrames
instead of Spark DataFrames, enabling local data quality analysis without
requiring a Spark cluster.
"""

import logging
from typing import Any, List, Dict, Union, Optional, Tuple
import pandas as pd

from databricks.labs.dqx.base import DQEngineCoreBase, DQEngineBase
from databricks.labs.dqx.rule import DQRule, ChecksValidationStatus
from databricks.labs.dqx.df_backend import PandasBackend, get_backend
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class PandasDQEngineCore(DQEngineCoreBase):
    """Pandas-compatible implementation of the DQX engine core."""
    
    def __init__(self, workspace_client: WorkspaceClient, extra_params: Optional[Dict[str, Any]] = None, spark_session=None):
        """Initialize the Pandas DQX engine core.
        
        Args:
            workspace_client: WorkspaceClient instance
            extra_params: Extra parameters for the engine
            spark_session: Optional SparkSession for compatibility
        """
        # Create a mock Spark session to avoid actual Spark initialization
        if spark_session is None:
            # We'll create a simple mock object that satisfies the interface
            import unittest.mock
            spark_session = unittest.mock.MagicMock()
        
        super().__init__(workspace_client, spark_session)
        self._backend = PandasBackend()
        self._extra_params = extra_params or {}
    
    def apply_checks(
        self, 
        df: pd.DataFrame, 
        checks: List[DQRule], 
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None
    ) -> pd.DataFrame:
        """Applies data quality checks to a given Pandas DataFrame.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of checks to apply
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Pandas DataFrame with errors and warning result columns
        """
        if not checks:
            return self._backend.append_empty_checks(df, "dq_errors", "dq_warnings")
        
        # Separate checks by criticality using backend abstraction
        from databricks.labs.dqx.rule import Criticality
        error_checks = self._backend.filter_checks_by_criticality(checks, Criticality.ERROR.value)
        warning_checks = self._backend.filter_checks_by_criticality(checks, Criticality.WARN.value)
        
        # Apply error checks
        result_df = self._backend.create_results_array(df, error_checks, "dq_errors", ref_dfs)
        
        # Apply warning checks
        result_df = self._backend.create_results_array(result_df, warning_checks, "dq_warnings", ref_dfs)
        
        return result_df
    
    def apply_checks_and_split(
        self, 
        df: pd.DataFrame, 
        checks: List[DQRule], 
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Applies data quality checks and splits into "good" and "bad" DataFrames.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of checks to apply
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Tuple of (good_df, bad_df) DataFrames
        """
        if not checks:
            return df, self._backend.limit(self._backend.append_empty_checks(df, "dq_errors", "dq_warnings"), 0)
        
        # Apply checks to get results
        checked_df = self.apply_checks(df, checks, ref_dfs)
        
        # Split using backend abstraction
        good_df = self._backend.get_valid(checked_df, "dq_errors", "dq_warnings")
        bad_df = self._backend.get_invalid(checked_df, "dq_errors", "dq_warnings")
        
        return good_df, bad_df
    
    def apply_checks_by_metadata_and_split(
        self,
        df: pd.DataFrame,
        checks: List[Dict],
        custom_check_functions: Optional[Dict[str, Any]] = None,
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Wrapper for metadata-driven checks with splitting.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of dictionaries describing checks
            custom_check_functions: Custom check functions
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Tuple of (good_df, bad_df) DataFrames
        """
        # Convert metadata checks to DQRule objects using the same logic as Spark engine
        dq_rules = self._build_quality_rules_by_metadata(checks, custom_check_functions)
        return self.apply_checks_and_split(df, dq_rules, ref_dfs)
    
    def apply_checks_by_metadata(
        self,
        df: pd.DataFrame,
        checks: List[Dict],
        custom_check_functions: Optional[Dict[str, Any]] = None,
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> pd.DataFrame:
        """Wrapper for metadata-driven checks.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of dictionaries describing checks
            custom_check_functions: Custom check functions
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Pandas DataFrame with error and warning columns
        """
        # Convert metadata checks to DQRule objects using the same logic as Spark engine
        dq_rules = self._build_quality_rules_by_metadata(checks, custom_check_functions)
        return self.apply_checks(df, dq_rules, ref_dfs)
    
    def validate_checks(self, checks: List[Dict], custom_check_functions: Optional[Dict[str, Any]] = None) -> ChecksValidationStatus:
        """Validate the input dict to ensure they conform to expected structure.
        
        Args:
            checks: List of checks to validate
            custom_check_functions: Custom check functions
            
        Returns:
            Validation status
        """
        # Use the same validation logic as the Spark engine
        from databricks.labs.dqx.engine import DQEngineCore
        return DQEngineCore.validate_checks(checks, custom_check_functions)
    
    def get_invalid(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get records that violate data quality checks (records with warnings and errors).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with error and warning rows and corresponding result columns
        """
        return self._backend.get_invalid(df, "dq_errors", "dq_warnings")
    
    def get_valid(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get records that don't violate data quality checks (records with warnings but no errors).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with warning rows but no result columns
        """
        return self._backend.get_valid(df, "dq_errors", "dq_warnings")
    
    def _build_quality_rules_by_metadata(self, checks: List[Dict], custom_check_functions: Optional[Dict[str, Any]] = None) -> List[DQRule]:
        """Build DQRule objects from metadata check specifications.
        
        Args:
            checks: List of dictionaries describing checks
            custom_check_functions: Custom check functions
            
        Returns:
            List of DQRule objects
        """
        # Use the same logic as the Spark engine
        from databricks.labs.dqx.engine import DQEngineCore
        return DQEngineCore.build_quality_rules_by_metadata(checks, custom_check_functions)
    
    @staticmethod
    def load_checks_from_local_file(filepath: str) -> List[Dict]:
        """Load checks (dq rules) from a file (json or yaml) in the local file system.
        
        Args:
            filepath: Path to a file containing the checks
            
        Returns:
            List of dq rules
        """
        if not filepath:
            raise ValueError("filepath must be provided")
        
        try:
            from databricks.labs.blueprint.installation import Installation
            from databricks.labs.dqx.utils import deserialize_dicts
            from pathlib import Path
            
            checks = Installation.load_local(list[dict[str, str]], Path(filepath))
            return deserialize_dicts(checks)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None
    
    @staticmethod
    def save_checks_in_local_file(checks: List[Dict], filepath: str):
        """Save checks (dq rules) to yaml file in the local file system.
        
        Args:
            checks: List of dq rules to save
            filepath: Path to a file containing the checks
        """
        if not filepath:
            raise ValueError("filepath must be provided")
        
        try:
            import yaml
            with open(filepath, 'w', encoding="utf-8") as file:
                yaml.safe_dump(checks, file)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None


class PandasDQEngine(DQEngineBase):
    """Pandas-compatible implementation of the DQX engine."""
    
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        engine: Optional[PandasDQEngineCore] = None,
        extra_params: Optional[Dict[str, Any]] = None,
        spark_session=None,
    ):
        """Initialize the Pandas DQX engine.
        
        Args:
            workspace_client: WorkspaceClient instance
            engine: Optional engine core instance
            extra_params: Extra parameters for the engine
            spark_session: Optional SparkSession for compatibility
        """
        # Create a mock Spark session to avoid actual Spark initialization
        if spark_session is None:
            # We'll create a simple mock object that satisfies the interface
            import unittest.mock
            spark_session = unittest.mock.MagicMock()
        
        super().__init__(workspace_client, spark_session)
        self._engine = engine or PandasDQEngineCore(workspace_client, extra_params, spark_session)
    
    def apply_checks(
        self, 
        df: pd.DataFrame, 
        checks: List[DQRule], 
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None
    ) -> pd.DataFrame:
        """Applies data quality checks to a given Pandas DataFrame.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of checks to apply
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Pandas DataFrame with errors and warning result columns
        """
        return self._engine.apply_checks(df, checks, ref_dfs)
    
    def apply_checks_and_split(
        self, 
        df: pd.DataFrame, 
        checks: List[DQRule], 
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Applies data quality checks and splits into "good" and "bad" DataFrames.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of checks to apply
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Tuple of (good_df, bad_df) DataFrames
        """
        return self._engine.apply_checks_and_split(df, checks, ref_dfs)
    
    def apply_checks_by_metadata_and_split(
        self,
        df: pd.DataFrame,
        checks: List[Dict],
        custom_check_functions: Optional[Dict[str, Any]] = None,
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Wrapper for metadata-driven checks with splitting.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of dictionaries describing checks
            custom_check_functions: Custom check functions
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Tuple of (good_df, bad_df) DataFrames
        """
        return self._engine.apply_checks_by_metadata_and_split(df, checks, custom_check_functions, ref_dfs)
    
    def apply_checks_by_metadata(
        self,
        df: pd.DataFrame,
        checks: List[Dict],
        custom_check_functions: Optional[Dict[str, Any]] = None,
        ref_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> pd.DataFrame:
        """Wrapper for metadata-driven checks.
        
        Args:
            df: Pandas DataFrame to check
            checks: List of dictionaries describing checks
            custom_check_functions: Custom check functions
            ref_dfs: Reference DataFrames to use in checks, if applicable
            
        Returns:
            Pandas DataFrame with error and warning columns
        """
        return self._engine.apply_checks_by_metadata(df, checks, custom_check_functions, ref_dfs)
    
    def validate_checks(self, checks: List[Dict], custom_check_functions: Optional[Dict[str, Any]] = None) -> ChecksValidationStatus:
        """Validate the input dict to ensure they conform to expected structure.
        
        Args:
            checks: List of checks to validate
            custom_check_functions: Custom check functions
            
        Returns:
            Validation status
        """
        return self._engine.validate_checks(checks, custom_check_functions)
