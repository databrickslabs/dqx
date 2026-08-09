"""Version-agnostic helpers for DQX storage-config objects.

DQX's storage-config classes (e.g. ``TableChecksStorageConfig``) are **pydantic models** in newer
releases and plain **dataclasses** in older ones. The two shapes expose their fields and support
copy-with-overrides differently:

- pydantic: fields in ``type(config).model_fields``; copy via the config's own ``replace()`` method
  (which re-runs validation).
- dataclass: fields via ``dataclasses.fields``; copy via ``dataclasses.replace``.

The runner installs a **pinned, published** DQX release in its job environment, which may be either
shape depending on the pinned version, so it must not assume one API. These helpers are stdlib-only
(no pyspark / DQX import) so they can be unit-tested in the server test environment.
"""

import dataclasses
from typing import Any


def config_has_field(config: Any, field_name: str) -> bool:
    """Return True if the storage *config* declares *field_name*, for pydantic or dataclass configs."""
    model_fields = getattr(type(config), "model_fields", None)
    if model_fields is not None:  # pydantic model
        return field_name in model_fields
    if dataclasses.is_dataclass(config):  # plain dataclass
        return any(f.name == field_name for f in dataclasses.fields(config))
    return False


def config_replace(config: Any, **changes: Any) -> Any:
    """Return a copy of *config* with *changes* applied, for pydantic or dataclass configs.

    Prefer the config's own ``replace()`` method when present (pydantic configs re-run validation);
    otherwise fall back to ``dataclasses.replace``.
    """
    replace = getattr(config, "replace", None)
    if callable(replace):
        return replace(**changes)
    return dataclasses.replace(config, **changes)
