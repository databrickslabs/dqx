# databricks.labs.dqx.reporting\_columns

## DefaultColumnNames Objects[​](#defaultcolumnnames-objects "Direct link to DefaultColumnNames Objects")

```python
class DefaultColumnNames(Enum)

```

Enum class to represent columns in the dataframe that will be used for error and warning reporting.

## ColumnArguments Objects[​](#columnarguments-objects "Direct link to ColumnArguments Objects")

```python
class ColumnArguments(Enum)

```

Enum class that is used as input parsing for custom column naming.

### merge\_info\_columns[​](#merge_info_columns "Direct link to merge_info_columns")

```python
def merge_info_columns(dest_name: str,
                       df: DataFrame,
                       info_col_names: list[str] | None = None) -> DataFrame

```

Merge dataset-level info columns into a single column as an array of structs.

Each source column must be a struct with the shared wide schema (e.g. fields like `anomaly`). Each such column becomes one element in the output array. Names in info\_col\_names that are not present in the DataFrame are skipped.

The result is `array&lt;struct&lt;...&gt;&gt;`. Element order matches the order of info\_col\_names (e.g. first anomaly check = dest\_name\[0], second = dest\_name\[1]). If dest\_name already exists, it must be an array of structs; new structs are appended via concat.

**Arguments**:

* `dest_name` - Name of the output column (e.g. \_dq\_info).
* `df` - DataFrame that may contain the info columns.
* `info_col_names` - Names of the info columns to merge; None or empty means no merge.

**Returns**:

DataFrame with the merged column (array of structs) and source info columns dropped.
