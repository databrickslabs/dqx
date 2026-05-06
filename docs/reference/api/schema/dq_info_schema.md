# databricks.labs.dqx.schema.dq\_info\_schema

Wide schema for \_dq\_info: one struct type with optional fields (anomaly, …).

Check modules register their field on import; build\_dq\_info\_struct builds a struct with the registered fields.

### register\_dq\_info\_field[​](#register_dq_info_field "Direct link to register_dq_info_field")

```python
def register_dq_info_field(name: str, dtype: DataType) -> None

```

Register a field for the wide \_dq\_info struct. Call at module load from check modules.

Duplicate names are ignored (first registration wins) so that multiple imports of the same check module do not add the same field twice.

### dq\_info\_item\_schema[​](#dq_info_item_schema "Direct link to dq_info_item_schema")

```python
def dq_info_item_schema() -> StructType

```

Return the current wide struct schema for one \_dq\_info element.

### build\_dq\_info\_struct[​](#build_dq_info_struct "Direct link to build_dq_info_struct")

```python
def build_dq_info_struct(**kwargs: Column | None) -> Column

```

Build a single struct column for \_dq\_info with one element per registered field.

For each registered (name, dtype): use kwargs\[name] if provided, else F.lit(None).cast(dtype). Result is always the same struct type.
