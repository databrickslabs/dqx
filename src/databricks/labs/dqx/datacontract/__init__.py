"""
Data Contract Integration for DQX.

This module provides functionality to generate DQX quality rules from data contract
specifications. Supports ODCS (Open Data Contract Standard) v3.x contracts (any apiVersion
accepted by the open_data_contract_standard library, e.g. v3.0.0, v3.0.2, v3.1.0).

Use DQGenerator.generate_rules_from_contract() as the main entry point for generating
rules from data contracts.

Note: The datacontract package is required. LLM extras are optional and only needed
if you want to use text-based rule generation with llm_engine.
"""

from databricks.labs.dqx.package_utils import missing_required_packages

required_specs = [
    "datacontract",
]

# Check if required datacontract packages are installed
if missing_required_packages(required_specs):
    raise ImportError(
        "datacontract extras not installed. Install additional dependencies by running "
        "`pip install databricks-labs-dqx[datacontract]`."
    )
