try:
    from ._version import version
except ImportError:
    version = "0.0.0.dev"  # Fallback for development/CI when not built

__version__ = version
