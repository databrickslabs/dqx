"""
Data Contract Integration for DQX.

This module provides functionality to generate DQX quality rules from data contract
specifications. Currently supports ODCS (Open Data Contract Standard) v3.0.x.

Use DQGenerator.generate_rules_from_contract() as the main entry point for generating
rules from data contracts.
"""

from databricks.labs.dqx.datacontract.contract_rules_generator import DataContractRulesGenerator
from databricks.labs.dqx.datacontract.formats import ODCSContract, ODCSProperty

__all__ = ["DataContractRulesGenerator", "ODCSContract", "ODCSProperty"]
