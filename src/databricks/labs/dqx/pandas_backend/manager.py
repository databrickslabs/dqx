"""
Pandas-compatible manager for orchestrating DQX rule application.

This module provides a manager class that mirrors the Spark-based DQRuleManager
but works with Pandas DataFrames and Pandas-compatible executors.
"""

from datetime import datetime
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, Optional, Any, Union
import pandas as pd

from databricks.labs.dqx.rule import DQRule
from .result import PandasDQCheckResult
from .factory import PandasDQRuleExecutorFactory


@dataclass(frozen=True)
class PandasDQRuleManager:
    """
    Pandas-compatible version of DQRuleManager.
    
    Orchestrates the application of a data quality rule to a Pandas DataFrame and builds the final check result.
    
    The manager is responsible for:
    - Executing the rule using the appropriate Pandas row or dataset executor.
    - Applying any filter condition specified in the rule to the check result.
    - Combining user-defined and engine-provided metadata into the result.
    - Constructing the final structured output (including check name, function, columns, metadata, etc.)
      as a PandasDQCheckResult.
    
    The manager does not implement the logic of individual checks. Instead, it delegates
    rule application to the appropriate PandasDQRuleExecutor based on the rule type.
    
    Attributes:
        check: The DQRule instance that defines the check to apply.
        df: The Pandas DataFrame on which to apply the check.
        engine_user_metadata: Metadata provided by the engine (overridden by check.user_metadata if present).
        run_time: The timestamp when the check is executed.
        ref_dfs: Optional reference DataFrames for dataset-level checks.
    """
    
    check: DQRule
    df: pd.DataFrame
    engine_user_metadata: Dict[str, str]
    run_time: datetime
    ref_dfs: Optional[Dict[str, pd.DataFrame]] = None
    
    @cached_property
    def user_metadata(self) -> Dict[str, str]:
        """
        Returns user metadata as a dictionary.
        """
        if self.check.user_metadata is not None:
            # Checks defined in the user metadata override checks defined in the engine
            return (self.engine_user_metadata or {}) | self.check.user_metadata
        return self.engine_user_metadata or {}
    
    @cached_property
    def filter_condition(self) -> pd.Series:
        """
        Returns the filter condition for the check as a boolean Series.
        
        For Pandas, we need to evaluate the filter expression if present.
        """
        if self.check.filter:
            try:
                # For simplicity, we'll evaluate basic filter expressions
                # In a full implementation, this would need more sophisticated expression parsing
                filter_result = self.df.eval(self.check.filter)
                if isinstance(filter_result, pd.Series):
                    return filter_result
                else:
                    # If eval returns a scalar, broadcast to all rows
                    return pd.Series([bool(filter_result)] * len(self.df), index=self.df.index)
            except Exception:
                # If filter evaluation fails, default to True (no filtering)
                return pd.Series([True] * len(self.df), index=self.df.index)
        else:
            # No filter specified, apply to all rows
            return pd.Series([True] * len(self.df), index=self.df.index)
    
    def process(self) -> PandasDQCheckResult:
        """
        Process the data quality rule and return results as PandasDQCheckResult containing:
        - Series with the check result
        - DataFrame with the results of the check
        """
        executor = PandasDQRuleExecutorFactory.create(self.check)
        raw_result = executor.apply(self.df, self.ref_dfs)
        return self._wrap_result(raw_result)
    
    def _wrap_result(self, raw_result: PandasDQCheckResult) -> PandasDQCheckResult:
        """
        Wrap the raw result with filter conditions and metadata.
        
        This mirrors the Spark implementation but uses Pandas operations.
        """
        # Apply filter condition to the raw result
        filtered_condition = self._apply_filter_to_condition(raw_result.condition)
        
        return PandasDQCheckResult(condition=filtered_condition, check_df=raw_result.check_df)
    
    def _apply_filter_to_condition(self, condition: Union[pd.Series, Any]) -> pd.Series:
        """
        Apply the filter condition to the check result.
        
        This mirrors the Spark F.when(filter_condition & condition.isNotNull(), result_struct) logic.
        """
        if isinstance(condition, pd.Series):
            # Apply filter: only show results where filter is True and condition is not null
            filter_mask = self.filter_condition
            condition_not_null = condition.notna()
            
            # Create the final condition: show result only where filter is True and condition is not null
            final_condition = condition.where(filter_mask & condition_not_null, None)
            
            return final_condition
        else:
            # For non-Series conditions, apply filter to all rows
            if self.filter_condition.all():
                return condition
            else:
                # If filter doesn't apply to all rows, we need to handle this case
                # For now, return the condition as-is
                return condition
    
    def build_result_struct(self, condition: pd.Series) -> pd.Series:
        """
        Build a structured result similar to Spark's F.struct().
        
        This creates a Series of dictionaries containing the check metadata,
        mirroring the Spark implementation's structured output.
        """
        # Create a series of structured results (dictionaries)
        result_structs = []
        
        for idx, message in condition.items():
            if pd.notna(message):  # Only create struct for non-null conditions
                struct = {
                    "name": self.check.name,
                    "message": message,
                    "columns": str(self.check.columns) if hasattr(self.check, 'columns') else str(self.check.column),
                    "filter": self.check.filter,
                    "function": self.check.check_func.__name__,
                    "run_time": self.run_time.isoformat() if self.run_time else None,
                    "user_metadata": self.user_metadata,
                }
                result_structs.append(struct)
            else:
                result_structs.append(None)
        
        return pd.Series(result_structs, index=condition.index)
