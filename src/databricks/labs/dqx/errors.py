class DQXError(Exception):
    """Base class for all DQX exceptions."""
    pass

class ParameterError(DQXError):
    """Base class for parameter-related errors."""
    pass

class InvalidConfigError(DQXError):
    """Raised when a configuration error occurs."""
    pass

class InvalidParameterError(ParameterError):
    """Raised when a parameter is invalid (malformed, wrong type, or not supported)."""
    pass

class MissingParameterError(ParameterError):
    """Raised when a required parameter is missing."""
    pass

class UnsafeSqlQueryError(DQXError):
    """Raised when a SQL query is considered unsafe."""
    pass
