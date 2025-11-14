# ODCS JSON Schemas

This directory contains the bundled ODCS (Open Data Contract Standard) JSON Schemas for validation.

## Schema Versions

- **odcs-json-schema-v3.0.0.json** - ODCS v3.0.0 specification
- **odcs-json-schema-v3.0.1.json** - ODCS v3.0.1 specification  
- **odcs-json-schema-v3.0.2.json** - ODCS v3.0.2 specification (latest)

## Automatic Version Detection

The validator automatically detects which schema to use based on the contract's **required** `apiVersion` field:

```python
from databricks.labs.dqx.datacontract import validate_contract

# apiVersion is required - selects appropriate schema
contract = {
    "apiVersion": "v3.0.2",  # Required: must be v3.0.0, v3.0.1, or v3.0.2
    "name": "my_contract",
    "version": "1.0.0",
    ...
}
validate_contract(contract)  # ✅ Uses v3.0.2 schema

# Missing apiVersion raises error
invalid_contract = {"name": "test", "version": "1.0"}
validate_contract(invalid_contract)  # ❌ ODCSValidationError: Missing required field: 'apiVersion'
```

## Validation

Always validates against the full ODCS JSON Schema specification. All required fields per the ODCS spec must be present:

```python
from databricks.labs.dqx.datacontract import validate_contract

# All required ODCS fields must be present
contract = {
    "apiVersion": "v3.0.2",
    "kind": "DataContract", 
    "id": "contract-123",
    "status": "active",
    "name": "my_contract",
    "version": "1.0.0",
    "schema": {"properties": {...}}
}
validate_contract(contract)  # ✅ Validates against full v3.0.2 schema
```

## Schema Source

Official ODCS schemas: https://github.com/bitol-io/open-data-contract-standard/tree/main/schema
