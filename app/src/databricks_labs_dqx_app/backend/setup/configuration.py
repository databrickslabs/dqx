"""Resolve Studio storage and audience from deployment config or saved setup choices."""

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.setup.audience import StudioAudience, resolve_audience
from databricks_labs_dqx_app.backend.setup.storage import DEFAULT_PREFIX, StudioStorage, derive_storage

_CATALOG_KEY = "setup_catalog"
_PREFIX_KEY = "setup_prefix"
_AUDIENCE_KEY = "setup_audience_group"
_LOCK_KEY = "setup_storage_locked"


class ConfigurationSource(str, Enum):
    """Where the active Studio configuration came from."""

    DEPLOYMENT = "deployment"
    SAVED = "saved"
    NONE = "none"


@dataclass(frozen=True)
class SetupChoices:
    """Administrator-submitted setup form values."""

    catalog: str
    prefix: str
    audience_group: str


@dataclass(frozen=True)
class ResolvedConfiguration:
    """Validated storage and audience, or the reason none is available."""

    source: ConfigurationSource
    choices: SetupChoices | None
    storage: StudioStorage | None
    audience: StudioAudience | None
    locked: bool
    error: str | None = None


class SetupSettings(Protocol):
    """Key-value persistence used for setup choices."""

    def get_setting(self, key: str) -> str | None: ...

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None: ...


class SetupConfigurationStore:
    """Persist setup choices in the Lakebase application settings table."""

    def __init__(self, settings: SetupSettings) -> None:
        self._settings = settings

    def load(self) -> SetupChoices | None:
        """Return saved choices, or *None* when the form was never submitted."""
        catalog = self._settings.get_setting(_CATALOG_KEY)
        prefix = self._settings.get_setting(_PREFIX_KEY)
        audience = self._settings.get_setting(_AUDIENCE_KEY)
        if not catalog or not prefix or not audience:
            return None
        return SetupChoices(catalog=catalog, prefix=prefix, audience_group=audience)

    def save(self, choices: SetupChoices, *, user_email: str | None) -> None:
        """Persist validated choices."""
        self._settings.save_setting(_CATALOG_KEY, choices.catalog, user_email=user_email)
        self._settings.save_setting(_PREFIX_KEY, choices.prefix, user_email=user_email)
        self._settings.save_setting(_AUDIENCE_KEY, choices.audience_group, user_email=user_email)

    def is_locked(self) -> bool:
        """Whether storage was provisioned and choices are immutable."""
        return self._settings.get_setting(_LOCK_KEY) == "true"

    def lock(self, *, user_email: str | None) -> None:
        """Mark storage as provisioned."""
        self._settings.save_setting(_LOCK_KEY, "true", user_email=user_email)


def validate_choices(choices: SetupChoices, admin_group: str) -> tuple[StudioStorage, StudioAudience]:
    """Validate setup form values; broad audience mode is never allowed.

    Args:
        choices: The setup choices to validate.
        admin_group: The configured Studio administrator group.

    Returns:
        A tuple of the validated *StudioStorage* and *StudioAudience*.

    Raises:
        InvalidParameterError: If any value is invalid.
    """
    storage = derive_storage(choices.catalog, choices.prefix)
    audience = resolve_audience([choices.audience_group], admin_group, allow_broad=False)
    return storage, audience


def resolve_configuration(config: AppConfig, store: SetupConfigurationStore) -> ResolvedConfiguration:
    """Resolve deployment configuration first, then saved choices.

    Args:
        config: Application configuration (deployment environment).
        store: Saved setup choices.

    Returns:
        The resolved configuration; invalid inputs are reported via *error*.
    """
    locked = store.is_locked()
    if config.has_deployment_storage:
        try:
            storage = derive_storage(
                config.catalog,
                config.prefix.strip() or DEFAULT_PREFIX,
                schema=config.schema_name,
                tmp_schema=config.tmp_schema_name,
                genie_schema=config.genie_schema_name,
                demo_schema=config.demo_schema_name,
            )
            audience = resolve_audience(config.user_groups, config.admin_group, allow_broad=True)
        except InvalidParameterError:
            return ResolvedConfiguration(
                ConfigurationSource.DEPLOYMENT,
                None,
                None,
                None,
                locked,
                "deployment_configuration_invalid",
            )
        return ResolvedConfiguration(ConfigurationSource.DEPLOYMENT, None, storage, audience, locked)

    choices = store.load()
    if choices is None:
        return ResolvedConfiguration(ConfigurationSource.NONE, None, None, None, locked)
    try:
        storage, audience = validate_choices(choices, config.admin_group)
    except InvalidParameterError:
        return ResolvedConfiguration(
            ConfigurationSource.SAVED,
            choices,
            None,
            None,
            locked,
            "saved_configuration_invalid",
        )
    return ResolvedConfiguration(ConfigurationSource.SAVED, choices, storage, audience, locked)
