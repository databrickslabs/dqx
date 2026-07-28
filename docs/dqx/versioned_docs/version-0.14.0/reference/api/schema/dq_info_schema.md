---
sidebar_label: dq_info_schema
title: databricks.labs.dqx.schema.dq_info_schema
---

Wide schema for _dq_info: one struct type with optional fields (anomaly, …).

Check modules register their field on import; build_dq_info_struct builds a struct
with the registered fields.

#### register\_dq\_info\_field

```python
def register_dq_info_field(name: str, dtype: DataType) -> None
```

Register a field for the wide _dq_info struct. Call at module load from check modules.

Duplicate names are ignored (first registration wins) so that multiple imports
of the same check module do not add the same field twice.

#### dq\_info\_item\_schema

```python
def dq_info_item_schema() -> StructType
```

Return the current wide struct schema for one _dq_info element.

#### build\_dq\_info\_struct

```python
def build_dq_info_struct(**kwargs: Column | None) -> Column
```

Build a single struct column for _dq_info with one element per registered field.

For each registered (name, dtype): use kwargs[name] if provided, else F.lit(None).cast(dtype).
Result is always the same struct type.

