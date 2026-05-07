class DQXError(Exception):
    """Base class for all DQX exceptions."""


class ParameterError(DQXError):
    """Base class for parameter-related errors."""


class InvalidConfigError(DQXError):
    """Raised when a configuration error occurs."""


class InvalidParameterError(ParameterError):
    """Raised when a parameter is invalid (malformed, wrong type, not supported, ambiguous, or incompatible with other inputs)."""

    @classmethod
    def require(cls, condition: bool, error: str) -> None:
        """Raise this error type if condition is False."""
        if not condition:
            raise cls(error)


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
