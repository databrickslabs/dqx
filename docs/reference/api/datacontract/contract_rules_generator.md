# databricks.labs.dqx.datacontract.contract\_rules\_generator

Data Contract to DQX Rules Generator.

Generates DQX quality rules from ODCS (Open Data Contract Standard) v3.x contracts.

For schema validation we require every property to have physicalType set to a Unity Catalog data type (e.g. STRING, INT, ARRAY\<STRING>, DECIMAL(10,2)). No ODCS→Unity mapping is performed. See: <https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes>

## DataContractRulesGenerator Objects[​](#datacontractrulesgenerator-objects "Direct link to DataContractRulesGenerator Objects")

```python
class DataContractRulesGenerator(DQEngineBase)

```

Generator for DQX quality rules from ODCS v3.x data contracts.

Schema validation requires every property to have physicalType set to a Unity Catalog type. We do not map ODCS types; invalid or missing physicalType raises InvalidPhysicalTypeError. Supports predefined rules from schema, explicit quality sections, and LLM-based expectations.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(workspace_client: WorkspaceClient,
             llm_engine: DQLLMEngine | None = None,
             custom_check_functions: dict[str, Callable] | None = None)

```

Initialize the DataContractRulesGenerator.

**Arguments**:

* `workspace_client` - Databricks WorkspaceClient instance.
* `llm_engine` - Optional LLM engine for processing text-based quality expectations.
* `custom_check_functions` - Optional dictionary of custom check functions.

**Raises**:

* `ImportError` - If LLM dependencies are missing when llm\_engine is provided.

### generate\_rules\_from\_contract[​](#generate_rules_from_contract "Direct link to generate_rules_from_contract")

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

Parses an ODCS v3.x contract natively and generates rules based on schema properties, logicalTypeOptions constraints, explicit quality definitions, and text-based expectations. When the contract defines a schema and generate\_schema\_validation is True, one dataset-level has\_valid\_schema rule per schema is generated. strict\_schema\_validation is passed as the strict argument to has\_valid\_schema (default True = exact match).

**Arguments**:

* `contract` - Pre-loaded DataContract object from datacontract-cli. Can be created with:

  <!-- -->

  * DataContract(data\_contract\_file=path) - from a file path
  * DataContract(data\_contract\_str=yaml\_string) - from a YAML/JSON string Either `contract` or `contract_file` must be provided.

* `contract_file` - Path to contract YAML/JSON file (local, volume, or workspace). Either `contract` or `contract_file` must be provided.

* `contract_format` - Contract format specification (default is "odcs"). Only "odcs" is supported.

* `generate_predefined_rules` - Whether to generate rules from schema properties (default True). Set to False to only generate explicit rules.

* `process_text_rules` - Whether to process text-based expectations using LLM (default True). Requires llm\_engine to be provided in **init**.

* `generate_schema_validation` - Whether to generate dataset-level has\_valid\_schema rules from the contract schema (default True).

* `contract`0 - Passed as the strict argument to has\_valid\_schema (default True = exact columns, order, types; False = permissive).

* `contract`1 - Default criticality level for generated rules (default is "error").

**Returns**:

A list of dictionaries representing the generated DQX quality rules.

**Raises**:

* `contract`2 - If neither or both contract parameters are provided, or format not supported.

**Notes**:

Exactly one of 'contract' or 'contract\_file' must be provided.
