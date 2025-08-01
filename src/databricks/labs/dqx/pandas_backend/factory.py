"""
Factory for creating Pandas-compatible DQRuleExecutor instances.

This module provides a factory class that mirrors the Spark-based DQRuleExecutorFactory
but creates Pandas-compatible executors.
"""

from databricks.labs.dqx.rule import DQRule, DQRowRule, DQDatasetRule
from .executor import PandasDQRuleExecutor, PandasDQRowRuleExecutor, PandasDQDatasetRuleExecutor


class PandasDQRuleExecutorFactory:
    """
    Factory for creating the appropriate PandasDQRuleExecutor instance for a given DQRule.
    
    Pandas-compatible version of DQRuleExecutorFactory that mirrors the Spark implementation
    but creates Pandas-compatible executors.
    
    This class encapsulates the logic for selecting the correct executor type
    (row-level or dataset-level) based on the rule instance provided.
    
    Responsibilities:
    - Determine the type of rule (DQRowRule or DQDatasetRule).
    - Return the corresponding Pandas executor (PandasDQRowRuleExecutor or PandasDQDatasetRuleExecutor).
    - Raise an error if the rule type is unsupported.
    """
    
    @staticmethod
    def create(rule: DQRule) -> PandasDQRuleExecutor:
        """
        Create the appropriate PandasDQRuleExecutor for the given rule.
        
        :param rule: The DQRule instance for which to create an executor.
        :return: The appropriate PandasDQRuleExecutor instance.
        :raises ValueError: If the rule type is not supported.
        """
        if isinstance(rule, DQRowRule):
            return PandasDQRowRuleExecutor(rule)
        elif isinstance(rule, DQDatasetRule):
            return PandasDQDatasetRuleExecutor(rule)
        else:
            raise ValueError(f"Unsupported rule type: {type(rule)}")
