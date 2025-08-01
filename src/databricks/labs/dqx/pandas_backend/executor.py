"""
Pandas-compatible executors for DQX rules.

This module provides executor classes that mirror the Spark-based DQRuleExecutor
pattern but work with Pandas DataFrames.
"""

import abc
import inspect
from typing import Dict, Optional, Any
import pandas as pd

from databricks.labs.dqx.rule import DQRule, DQRowRule, DQDatasetRule
from .result import PandasDQCheckResult


class PandasDQRuleExecutor(abc.ABC):
    """
    Abstract base class for executing a data quality rule on a Pandas DataFrame.
    
    Pandas-compatible version of DQRuleExecutor that mirrors the Spark implementation
    but works with Pandas DataFrames instead of Spark DataFrames.
    
    The executor is responsible for:
    - Applying the rule's logic to the provided DataFrame (and optional reference DataFrames).
    - Returning the raw result of the check, without applying filters or attaching metadata.
    
    Executors are specialized for different types of rules:
    - PandasDQRowRuleExecutor: Handles row-level rules.
    - PandasDQDatasetRuleExecutor: Handles dataset-level rules.
    """
    
    def __init__(self, rule: DQRule):
        self.rule = rule
    
    @abc.abstractmethod
    def apply(self, df: pd.DataFrame, ref_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> PandasDQCheckResult:
        """Apply a rule and return results"""
        pass


class PandasDQRowRuleExecutor(PandasDQRuleExecutor):
    """
    Executor for row-level data quality rules using Pandas.
    
    This executor applies a DQRowRule to the provided Pandas DataFrame. Row-level rules generate
    a condition (boolean mask or message series) that evaluates whether each row satisfies
    or violates the check.
    
    Responsibilities:
    - Obtain the condition result by applying the check function.
    - Return a PandasDQCheckResult containing:
        - The condition series (boolean mask or messages).
        - The input DataFrame.
    """
    
    rule: DQRowRule
    
    def __init__(self, rule: DQRowRule):
        super().__init__(rule)
    
    def apply(self, df: pd.DataFrame, ref_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> PandasDQCheckResult:
        """
        Apply the row-level data quality rule to the provided Pandas DataFrame.
        
        The rule produces a condition (boolean mask or message series) that indicates whether 
        each row satisfies or violates the check.
        
        :param df: The input Pandas DataFrame to which the rule is applied.
        :param ref_dfs: Optional dictionary of reference DataFrames (unused for row rules).
        :return: PandasDQCheckResult containing:
             - condition: Boolean mask or message series representing the check condition.
             - check_df: The input DataFrame (used for downstream processing).
        """
        # Get the check function and its arguments
        check_func = self.rule.check_func
        args, kwargs = self.rule.prepare_check_func_args_and_kwargs()
        
        # Apply the check function to get the condition
        # The check function should return a boolean mask or message series
        try:
            if hasattr(check_func, '__call__'):
                # For built-in check functions, we need to handle them specially
                condition = self._apply_check_function(df, check_func, args, kwargs)
            else:
                # For other cases, try to call directly
                condition = check_func(df, *args, **kwargs)
        except Exception as e:
            # If check function fails, create a failure condition for all rows
            condition = pd.Series([f"Check function failed: {str(e)}"] * len(df), index=df.index)
        
        return PandasDQCheckResult(condition=condition, check_df=df)
    
    def _apply_check_function(self, df: pd.DataFrame, check_func, args, kwargs) -> pd.Series:
        """Apply the check function and return a condition series."""
        from databricks.labs.dqx import check_funcs
        
        # Handle built-in check functions
        func_name = check_func.__name__
        
        if func_name == 'is_not_null':
            column = self.rule.column
            if column and column in df.columns:
                # Return boolean mask: True where null (violation), False where not null (pass)
                null_mask = df[column].isna()
                # Convert to message series: message where violation, None where pass
                return pd.Series([f"Column '{column}' value is null" if is_null else None 
                                for is_null in null_mask], index=df.index)
            else:
                # Column doesn't exist - all rows fail
                return pd.Series([f"Column '{column}' not found"] * len(df), index=df.index)
        
        elif func_name == 'is_not_null_and_not_empty':
            column = self.rule.column
            if column and column in df.columns:
                # Check for null or empty
                null_mask = df[column].isna()
                empty_mask = df[column].astype(str) == ''
                violation_mask = null_mask | empty_mask
                # Convert to message series
                return pd.Series([f"Column '{column}' value is null or empty" if is_violation else None 
                                for is_violation in violation_mask], index=df.index)
            else:
                # Column doesn't exist - all rows fail
                return pd.Series([f"Column '{column}' not found"] * len(df), index=df.index)
        
        else:
            # For other check functions, try to call them directly
            try:
                result = check_func(df, *args, **kwargs)
                if isinstance(result, pd.Series):
                    return result
                else:
                    # Convert result to series if needed
                    return pd.Series([result] * len(df), index=df.index)
            except Exception as e:
                return pd.Series([f"Check function '{func_name}' failed: {str(e)}"] * len(df), index=df.index)


class PandasDQDatasetRuleExecutor(PandasDQRuleExecutor):
    """
    Executor for dataset-level data quality rules using Pandas.
    
    This executor applies a DQDatasetRule to the provided Pandas DataFrame 
    (and optional reference DataFrames). Dataset-level rules can produce conditions 
    that involve multiple rows, aggregations, or comparisons across datasets.
    
    Responsibilities:
    - Obtain condition and check function closure containing computation logic.
    - Return a PandasDQCheckResult containing:
     - The condition result.
     - The resulting DataFrame produced by the rule.
    """
    
    rule: DQDatasetRule
    
    def __init__(self, rule: DQDatasetRule):
        super().__init__(rule)
    
    def apply(self, df: pd.DataFrame, ref_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> PandasDQCheckResult:
        """
        Apply the dataset-level data quality rule to the provided Pandas DataFrame.
        
        The rule produces a condition and may transform the DataFrame through a closure function.
        
        :param df: The input Pandas DataFrame to which the rule is applied.
        :param ref_dfs: Optional dictionary of reference DataFrames for dataset-level checks.
        :return: PandasDQCheckResult containing:
             - condition: The condition result from the rule.
             - check_df: The DataFrame produced by the rule (may be transformed).
        """
        condition, closure_func = self.rule.check
        
        closure_func_signature = inspect.signature(closure_func)
        kwargs: Dict[str, Any] = {}
        
        # Inject additional arguments if they are defined in the closure signature
        # Note: We don't have 'spark' in Pandas context, so we skip that
        if "ref_dfs" in closure_func_signature.parameters:
            kwargs["ref_dfs"] = ref_dfs
        
        try:
            check_df: pd.DataFrame = closure_func(df=df, **kwargs)
            return PandasDQCheckResult(condition=condition, check_df=check_df)
        except Exception as e:
            # If closure function fails, return original DataFrame with error condition
            error_condition = pd.Series([f"Dataset rule failed: {str(e)}"] * len(df), index=df.index)
            return PandasDQCheckResult(condition=error_condition, check_df=df)
