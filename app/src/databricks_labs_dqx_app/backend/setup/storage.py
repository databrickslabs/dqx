"""Prefix-derived Unity Catalog storage locations for DQX Studio."""

import re
from dataclasses import dataclass

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.sql_utils import validate_identifier
from databricks_labs_dqx_app.backend.volume import VolumeLocation

DEFAULT_PREFIX = "dqx_studio"
WHEELS_VOLUME_NAME = "wheels"
_PREFIX_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}")
_MAX_IDENTIFIER_LENGTH = 255


@dataclass(frozen=True)
class StudioStorage:
    """Validated Unity Catalog locations owned by one Studio installation."""

    catalog: str
    schema: str
    tmp_schema: str
    genie_schema: str
    demo_schema: str
    volume: str = WHEELS_VOLUME_NAME

    @property
    def schemas(self) -> tuple[str, str, str, str]:
        """Return the main, temporary, Genie and demo schema names."""
        return (self.schema, self.tmp_schema, self.genie_schema, self.demo_schema)

    @property
    def volume_location(self) -> VolumeLocation:
        """Return the wheels volume inside the main schema."""
        return VolumeLocation(
            catalog=self.catalog,
            schema=self.schema,
            volume=self.volume,
            path=f"/Volumes/{self.catalog}/{self.schema}/{self.volume}",
        )

    def full_name(self, schema: str) -> str:
        """Return the two-part name of *schema* in the Studio catalog."""
        return f"{self.catalog}.{schema}"


def validate_prefix(prefix: str) -> str:
    """Validate a storage prefix.

    Args:
        prefix: Lower-case prefix starting with a letter (letters, digits, underscores).

    Returns:
        The unchanged prefix.

    Raises:
        InvalidParameterError: If the prefix is empty, unsafe, or not lower-case.
    """
    if not _PREFIX_PATTERN.fullmatch(prefix):
        raise InvalidParameterError(
            "The storage prefix must start with a lower-case letter and contain only "
            "lower-case letters, digits and underscores (at most 64 characters)."
        )
    return prefix


def derive_storage(
    catalog: str,
    prefix: str,
    *,
    schema: str = "",
    tmp_schema: str = "",
    genie_schema: str = "",
    demo_schema: str = "",
) -> StudioStorage:
    """Derive and validate Studio storage names from a catalog and prefix.

    Args:
        catalog: Existing Unity Catalog catalog name.
        prefix: Storage prefix; see *validate_prefix*.
        schema: Optional main schema override.
        tmp_schema: Optional temporary schema override.
        genie_schema: Optional Genie schema override.
        demo_schema: Optional demo schema override.

    Returns:
        Validated storage locations.

    Raises:
        InvalidParameterError: If any name is invalid, over-length, or duplicated.
    """
    validate_prefix(prefix)
    storage = StudioStorage(
        catalog=_identifier(catalog.strip(), "catalog"),
        schema=_identifier(schema.strip() or prefix, "main schema"),
        tmp_schema=_identifier(tmp_schema.strip() or f"{prefix}_tmp", "temporary schema"),
        genie_schema=_identifier(genie_schema.strip() or f"{prefix}_genie", "Genie schema"),
        demo_schema=_identifier(demo_schema.strip() or f"{prefix}_demo", "demo schema"),
    )
    if len({name.casefold() for name in storage.schemas}) != len(storage.schemas):
        raise InvalidParameterError("Studio schema names must be distinct.")
    return storage


def _identifier(value: str, label: str) -> str:
    try:
        validate_identifier(value)
    except ValueError:
        raise InvalidParameterError(f"The {label} name is not a valid identifier.") from None
    if "." in value or len(value) > _MAX_IDENTIFIER_LENGTH:
        raise InvalidParameterError(f"The {label} name is not a valid identifier.")
    return value
