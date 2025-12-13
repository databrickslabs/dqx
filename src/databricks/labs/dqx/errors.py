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
