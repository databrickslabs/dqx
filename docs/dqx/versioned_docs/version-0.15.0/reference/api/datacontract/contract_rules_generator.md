---
sidebar_label: contract_rules_generator
title: databricks.labs.dqx.datacontract.contract_rules_generator
---

Data Contract to DQX Rules Generator.

Generates DQX quality rules from ODCS (Open Data Contract Standard) v3.x contracts.

For schema validation we require every property to have physicalType set to a Unity Catalog
data type (e.g. STRING, INT, ARRAY&lt;STRING&gt;, DECIMAL(10,2)). No ODCS→Unity mapping is performed.
See: https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes

## DataContractRulesGenerator Objects

```python
class DataContractRulesGenerator(DQEngineBase)
```

Generator for DQX quality rules from ODCS v3.x data contracts.

Schema validation requires every property to have physicalType set to a Unity Catalog type.
We do not map ODCS types; invalid or missing physicalType raises InvalidPhysicalTypeError.
Supports predefined rules from schema, explicit quality sections, and LLM-based expectations.

#### \_\_init\_\_

```python
def __init__(workspace_client: WorkspaceClient,
             llm_engine: "DQLLMEngine | None" = None,
             custom_check_functions: dict[str, Callable] | None = None)
```

Initialize the DataContractRulesGenerator.

**Arguments**:

- `workspace_client` - Databricks WorkspaceClient instance.
- `llm_engine` - Optional LLM engine for processing text-based quality expectations.
- `custom_check_functions` - Optional dictionary of custom check functions.
  

**Raises**:

- `ImportError` - If LLM dependencies are missing when llm_engine is provided.

#### generate\_rules\_from\_contract

```python
@telemetry_logger("datacontract", "generate_rules_from_contract")
def generate_rules_from_contract(
        contract: DataContract | None = None,
        contract_file: str | None = None,
        contract_format: str = "odcs",
        generate_predefined_rules: bool = True,
        process_text_rules: bool = True,
        generate_schema_validation: bool = True,
        strict_schema_validation: bool = True,
        default_criticality: str = "error") -> list[dict]
```

Generate DQX quality rules from an ODCS v3.x data contract.

Parses an ODCS v3.x contract natively and generates rules based on schema properties,
logicalTypeOptions constraints, explicit quality definitions, and text-based expectations.
When the contract defines a schema and generate_schema_validation is True, one dataset-level
has_valid_schema rule per schema is generated. strict_schema_validation is passed as the
strict argument to has_valid_schema (default True = exact match).

**Arguments**:

- `contract` - Pre-loaded DataContract object from datacontract-cli. Can be created with:
  - DataContract(data_contract_file=path) - from a file path
  - DataContract(data_contract_str=yaml_string) - from a YAML/JSON string
  Either `contract` or `contract_file` must be provided.
- `contract_file` - Path to contract YAML/JSON file (local, volume, or workspace). Either `contract` or `contract_file` must be provided.
- `contract_format` - Contract format specification (default is &quot;odcs&quot;). Only &quot;odcs&quot; is supported.
- `generate_predefined_rules` - Whether to generate rules from schema properties (default True). Set to False to only generate explicit rules.
- `process_text_rules` - Whether to process text-based expectations using LLM (default True). Requires llm_engine to be provided in __init__.
- `generate_schema_validation` - Whether to generate dataset-level has_valid_schema rules from the contract schema (default True).
- `contract`0 - Passed as the strict argument to has_valid_schema (default True = exact columns, order, types; False = permissive).
- `contract`1 - Default criticality level for generated rules (default is &quot;error&quot;).
  

**Returns**:

  A list of dictionaries representing the generated DQX quality rules.
  

**Raises**:

- `contract`2 - If neither or both contract parameters are provided, or format not supported.
  

**Notes**:

  Exactly one of &#x27;contract&#x27; or &#x27;contract_file&#x27; must be provided.

