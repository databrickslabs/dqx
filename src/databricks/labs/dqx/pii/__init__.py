try:
    from databricks.labs.dqx.pii.check_funcs import contains_pii

    __all__ = ["contains_pii"]

except ImportError:
    import warnings

    warnings.warn(
        "PII detection dependencies not found. Install with: pip install dqx[pii-detection]",
        ImportWarning,
        stacklevel=2,
    )

    def contains_pii(*args, **kwargs):
        raise ImportError(
            "PII detection dependencies not installed. "
            "Install with: pip install 'databricks-labs-dqx[pii-detection]'"
        )

    __all__ = ["contains_pii"]
