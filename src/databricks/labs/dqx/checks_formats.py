"""Convenience format maps derived from *SerializerFactory*.

*FILE_SERIALIZERS* and *FILE_DESERIALIZERS* are kept for backwards-compatibility with
callers that expect plain callables keyed by file extension.  Their content is now
derived from *SerializerFactory* — the single source of truth for supported extensions
and serializer implementations — so that adding a new format requires only one change.
"""

from typing import Any
from collections.abc import Callable

from databricks.labs.dqx.checks_serializer import SerializerFactory

# Build FILE_SERIALIZERS: ext -> (list[dict]) -> str
FILE_SERIALIZERS: dict[str, Callable[[list[dict[Any, Any]]], str]] = {
    ext: SerializerFactory.create_serializer(ext).serialize for ext in SerializerFactory.get_supported_extensions()
}

# Build FILE_DESERIALIZERS: ext -> (file_like_or_str) -> list[dict]
# yaml.safe_load accepts both str and file-like objects; json.load requires a file-like.
# The raw callables from the serializer classes already handle these conventions.
FILE_DESERIALIZERS: dict[str, Callable] = {
    ext: SerializerFactory.create_serializer(ext).deserialize for ext in SerializerFactory.get_supported_extensions()
}
