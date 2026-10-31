"""Shared validation of bound Unity Catalog volume locations."""

from dataclasses import dataclass

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.sql_utils import validate_fqn


@dataclass(frozen=True)
class VolumeLocation:
    """Validated Unity Catalog volume location and its derived identifiers."""

    catalog: str
    schema: str
    volume: str
    path: str


def parse_volume_path(path: str) -> VolumeLocation:
    """Parse an exact */Volumes/catalog/schema/volume* path.

    Args:
        path: Bound Unity Catalog volume path.

    Returns:
        The validated location and derived catalog/schema identifiers.

    Raises:
        InvalidParameterError: If the path shape or an identifier is invalid.
    """
    parts = path.split("/") if isinstance(path, str) else []
    if len(parts) != 5 or parts[:2] != ["", "Volumes"] or any(not part for part in parts[2:]):
        raise InvalidParameterError("The wheels volume must use /Volumes/<catalog>/<schema>/<volume>.")

    catalog, schema, volume = parts[2:]
    if any(part in {".", ".."} for part in (catalog, schema, volume)):
        raise InvalidParameterError("The wheels volume contains an invalid identifier.")
    try:
        validate_fqn(f"{catalog}.{schema}.{volume}")
    except ValueError:
        raise InvalidParameterError("The wheels volume contains an invalid identifier.") from None
    return VolumeLocation(catalog=catalog, schema=schema, volume=volume, path=path)
