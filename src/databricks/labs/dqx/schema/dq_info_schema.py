"""Wide schema for _dq_info: one struct type with optional fields (anomaly, …).

Check modules register their field on import; build_dq_info_struct builds a struct
with the registered fields.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructField, StructType

_DQ_INFO_FIELDS: dict[str, DataType] = {}


def register_dq_info_field(name: str, dtype: DataType) -> None:
    """Register a field for the wide _dq_info struct. Call at module load from check modules.

    Duplicate names are ignored (first registration wins) so that multiple imports
    of the same check module do not add the same field twice.
    """
    if name not in _DQ_INFO_FIELDS:
        _DQ_INFO_FIELDS[name] = dtype


def _get_dq_info_item_schema() -> StructType:
    """Build the wide struct schema from registered fields."""
    return StructType([StructField(name, dtype, True) for name, dtype in _DQ_INFO_FIELDS.items()])


def dq_info_item_schema() -> StructType:
    """Return the current wide struct schema for one _dq_info element."""
    return _get_dq_info_item_schema()


def build_dq_info_struct(**kwargs: Column | None) -> Column:
    """Build a single struct column for _dq_info with one element per registered field.

    For each registered (name, dtype): use kwargs[name] if provided, else F.lit(None).cast(dtype).
    Result is always the same struct type.
    """
    parts = []
    for name, dtype in _DQ_INFO_FIELDS.items():
        col = kwargs.get(name)
        if col is None:
            col = F.lit(None).cast(dtype)
        parts.append(col.alias(name))
    return F.struct(*parts).cast(_get_dq_info_item_schema())
