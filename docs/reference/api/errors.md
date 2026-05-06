# databricks.labs.dqx.errors

## DQXError Objects[​](#dqxerror-objects "Direct link to DQXError Objects")

```python
class DQXError(Exception)

```

Base class for all DQX exceptions.

## ParameterError Objects[​](#parametererror-objects "Direct link to ParameterError Objects")

```python
class ParameterError(DQXError)

```

Base class for parameter-related errors.

## InvalidConfigError Objects[​](#invalidconfigerror-objects "Direct link to InvalidConfigError Objects")

```python
class InvalidConfigError(DQXError)

```

Raised when a configuration error occurs.

## InvalidParameterError Objects[​](#invalidparametererror-objects "Direct link to InvalidParameterError Objects")

```python
class InvalidParameterError(ParameterError)

```

Raised when a parameter is invalid (malformed, wrong type, not supported, ambiguous, or incompatible with other inputs).

## MissingParameterError Objects[​](#missingparametererror-objects "Direct link to MissingParameterError Objects")

```python
class MissingParameterError(ParameterError)

```

Raised when a required parameter is missing, i.e when the user fails to provide a required parameter (None/absent).

## UnsafeSqlQueryError Objects[​](#unsafesqlqueryerror-objects "Direct link to UnsafeSqlQueryError Objects")

```python
class UnsafeSqlQueryError(DQXError)

```

Raised when a SQL query is considered unsafe.

## InvalidCheckError Objects[​](#invalidcheckerror-objects "Direct link to InvalidCheckError Objects")

```python
class InvalidCheckError(DQXError)

```

Raised when a check is invalid or not supported.

## CheckDownloadError Objects[​](#checkdownloaderror-objects "Direct link to CheckDownloadError Objects")

```python
class CheckDownloadError(InvalidCheckError)

```

Raised when a data quality check cannot be downloaded (e.g., from Unity Catalog).

## ODCSContractError Objects[​](#odcscontracterror-objects "Direct link to ODCSContractError Objects")

```python
class ODCSContractError(DQXError)

```

Raised when there is an error related to ODCS data contracts.

## ComputationError Objects[​](#computationerror-objects "Direct link to ComputationError Objects")

```python
class ComputationError(DQXError)

```

Raised when an internal computation fails (e.g. aggregation returned no result, empty data).

## ModelLoadError Objects[​](#modelloaderror-objects "Direct link to ModelLoadError Objects")

```python
class ModelLoadError(DQXError)

```

Raised when a model cannot be loaded (e.g. version mismatch, corrupted artifact).

## InvalidPhysicalTypeError Objects[​](#invalidphysicaltypeerror-objects "Direct link to InvalidPhysicalTypeError Objects")

```python
class InvalidPhysicalTypeError(ODCSContractError)

```

Raised when a schema property is missing physicalType or physicalType is not a valid Unity Catalog data type.

For schema validation we require every property to have physicalType set to a Unity Catalog type (e.g. STRING, INT, ARRAY\<STRING>, DECIMAL(10,2)). See: <https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes>
