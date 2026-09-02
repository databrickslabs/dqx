"""Normalize installation-bound resources without deployment-origin logic."""

from collections.abc import Mapping
from dataclasses import dataclass

from pydantic import SecretStr

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.sql_utils import validate_fqn, validate_identifier


@dataclass(frozen=True)
class VolumeLocation:
    """Validated Unity Catalog volume location and its derived identifiers."""

    catalog: str
    schema: str
    volume: str
    path: str


@dataclass(frozen=True)
class LakebaseConnection:
    """Normalized endpoint-based or platform-bound Lakebase connection."""

    endpoint: str | None
    host: str | None
    port: int
    database: str
    username: str | None
    password: SecretStr | None
    schema: str


@dataclass(frozen=True)
class ActiveResources:
    """Installation resources that passed setup readiness checks."""

    volume: VolumeLocation
    lakebase: LakebaseConnection
    warehouse_id: str
    job_id: str | None
    tmp_schema: str
    genie_schema: str


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


def resolve_lakebase_connection(
    config: AppConfig,
    environ: Mapping[str, str],
) -> LakebaseConnection | None:
    """Normalize configured endpoint or platform Postgres binding values.

    Endpoint configuration takes precedence when supplied. No deployment mode
    or source metadata participates in resolution.

    Args:
        config: Application configuration.
        environ: Runtime environment values.

    Returns:
        A normalized connection, or *None* when no connection input exists.

    Raises:
        InvalidParameterError: If a supplied connection value is malformed.
    """
    schema = _validated_identifier(config.lakebase_schema_name, "Lakebase schema")
    endpoint = config.lakebase_endpoint.strip()
    if endpoint:
        _reject_control_characters(endpoint, "Lakebase endpoint")
        return LakebaseConnection(
            endpoint=endpoint,
            host=None,
            port=5432,
            database=_required_value(config.lakebase_database_name, "Lakebase database"),
            username=None,
            password=None,
            schema=schema,
        )

    host = environ.get("PGHOST", "").strip()
    if not host:
        return None
    _reject_control_characters(host, "Lakebase host")
    port = _parse_port(environ.get("PGPORT", "5432"))
    database = _required_value(
        environ.get("PGDATABASE", config.lakebase_database_name),
        "Lakebase database",
    )
    username = environ.get("PGUSER", "").strip() or None
    if username is not None:
        _reject_control_characters(username, "Lakebase username")
    raw_password = environ.get("PGPASSWORD")
    return LakebaseConnection(
        endpoint=None,
        host=host,
        port=port,
        database=database,
        username=username,
        password=SecretStr(raw_password) if raw_password else None,
        schema=schema,
    )


def _parse_port(raw_port: str) -> int:
    try:
        port = int(raw_port)
    except (TypeError, ValueError):
        raise InvalidParameterError("Lakebase port must be an integer between 1 and 65535.") from None
    if not 1 <= port <= 65535:
        raise InvalidParameterError("Lakebase port must be an integer between 1 and 65535.")
    return port


def _required_value(value: str, label: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise InvalidParameterError(f"{label} is required.")
    _reject_control_characters(cleaned, label)
    return cleaned


def _validated_identifier(value: str, label: str) -> str:
    cleaned = value.strip()
    try:
        return validate_identifier(cleaned)
    except ValueError:
        raise InvalidParameterError(f"{label} contains an invalid identifier.") from None


def _reject_control_characters(value: str, label: str) -> None:
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise InvalidParameterError(f"{label} contains invalid control characters.")
