class DQXError(Exception):
    """Base class for all DQX exceptions."""


class ParameterError(DQXError):
    """Base class for parameter-related errors."""


class InvalidConfigError(DQXError):
    """Raised when a configuration error occurs."""


class InvalidParameterError(ParameterError):
    """Raised when a parameter is invalid (malformed, wrong type, not supported, ambiguous, or incompatible with other inputs)."""


class MissingParameterError(ParameterError):
    """Raised when a required parameter is missing, i.e when the user fails to provide a required parameter (None/absent)."""


class UnsafeSqlQueryError(DQXError):
    """Raised when a SQL query is considered unsafe."""


class InvalidCheckError(DQXError):
    """Raised when a check is invalid or not supported."""


class CheckDownloadError(InvalidCheckError):
    """Raised when a data quality check cannot be downloaded (e.g., from Unity Catalog)."""


class ODCSContractError(DQXError):
    """Raised when there is an error related to ODCS data contracts."""


class ComputationError(DQXError):
    """Raised when an internal computation fails (e.g. aggregation returned no result, empty data)."""


class ModelLoadError(DQXError):
    """Raised when a model cannot be loaded (e.g. version mismatch, corrupted artifact)."""


class InvalidPhysicalTypeError(ODCSContractError):
    """Raised when a schema property is missing physicalType or physicalType is not a valid Unity Catalog data type.

    For schema validation we require every property to have physicalType set to a Unity Catalog
    type (e.g. STRING, INT, ARRAY<STRING>, DECIMAL(10,2)). See:
    https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes
    """


class TerminalActionError(DQXError):
    """Base class for errors that abort the current run when a triggered action fails unrecoverably."""


class PipelineFailedError(TerminalActionError):
    """Raised when a triggered DLT or Databricks pipeline action terminates with a failure status."""


class InvalidConditionError(DQXError):
    """Raised when a DQ action condition expression is malformed or cannot be evaluated."""


class InvalidActionError(DQXError):
    """Raised when a DQ action definition is invalid or references an unsupported action type."""


class AlertDeliveryError(DQXError):
    """Raised when an alert notification cannot be delivered to its target (e.g. webhook, email)."""


class UnsafeWebhookUrlError(DQXError):
    """Raised when a webhook URL fails the safety validation check (e.g. non-HTTPS, disallowed host)."""
