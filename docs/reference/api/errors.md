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

## MissingResourceError Objects[​](#missingresourceerror-objects "Direct link to MissingResourceError Objects")

```python
class MissingResourceError(DQXError)

```

Raised when a packaged resource file required by a check is missing or empty.

This indicates a packaging or installation fault (e.g. a data file was dropped from the wheel), not a runtime computation failure.

## InvalidPhysicalTypeError Objects[​](#invalidphysicaltypeerror-objects "Direct link to InvalidPhysicalTypeError Objects")

```python
class InvalidPhysicalTypeError(ODCSContractError)

```

Raised when a schema property is missing physicalType or physicalType is not a valid Unity Catalog data type.

For schema validation we require every property to have physicalType set to a Unity Catalog type (e.g. STRING, INT, ARRAY\<STRING>, DECIMAL(10,2)). See: <https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes>

## TerminalActionError Objects[​](#terminalactionerror-objects "Direct link to TerminalActionError Objects")

```python
class TerminalActionError(DQXError)

```

Base class for errors that abort the current run when a triggered action fails unrecoverably.

## PipelineFailedError Objects[​](#pipelinefailederror-objects "Direct link to PipelineFailedError Objects")

```python
class PipelineFailedError(TerminalActionError)

```

Raised when a triggered DLT or Databricks pipeline action terminates with a failure status.

## InvalidConditionError Objects[​](#invalidconditionerror-objects "Direct link to InvalidConditionError Objects")

```python
class InvalidConditionError(DQXError)

```

Raised when a DQ action condition expression is malformed or cannot be evaluated.

## InvalidActionError Objects[​](#invalidactionerror-objects "Direct link to InvalidActionError Objects")

```python
class InvalidActionError(DQXError)

```

Raised when a DQ action definition is invalid or references an unsupported action type.

## AlertDeliveryError Objects[​](#alertdeliveryerror-objects "Direct link to AlertDeliveryError Objects")

```python
class AlertDeliveryError(DQXError)

```

Raised when an alert notification cannot be delivered to its target (e.g. webhook, email).

## UnsafeWebhookUrlError Objects[​](#unsafewebhookurlerror-objects "Direct link to UnsafeWebhookUrlError Objects")

```python
class UnsafeWebhookUrlError(DQXError)

```

Raised when a webhook URL fails the safety validation check (e.g. non-HTTPS, disallowed host).
