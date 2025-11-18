"""
Data Contract Integration for DQX.

This module provides functionality to generate DQX quality rules from data contract
specifications. Currently supports ODCS (Open Data Contract Standard) v3.0.x.

Use DQGenerator.generate_rules_from_contract() as the main entry point for generating
rules from data contracts.

Note: The datacontract package is required. LLM extras are optional and only needed
if you want to use text-based rule generation with llm_engine.
"""

try:
    from databricks.labs.dqx.datacontract.contract_rules_generator import DataContractRulesGenerator
except ImportError as e:
    if "datacontract" in str(e) and "dspy" not in str(e):
        raise ImportError(
            "datacontract extras not installed. Install additional dependencies by running "
            "`pip install databricks-labs-dqx[datacontract]`."
        ) from e
    if "dspy" in str(e) or "DQLLMEngine" in str(e):
        raise ImportError(
            "LLM extras not installed. Install additional dependencies by running "
            "`pip install databricks-labs-dqx[llm]`. "
            "Note: LLM extras are only needed if you want to use text-based rule generation with llm_engine."
        ) from e
    raise

__all__ = [
    "DataContractRulesGenerator",
]
