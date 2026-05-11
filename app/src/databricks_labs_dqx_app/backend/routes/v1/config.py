import json
import re
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import get_app_settings_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from pydantic import BaseModel, Field

from databricks_labs_dqx_app.backend.models import (
    ConfigIn,
    ConfigOut,
    RunConfigIn,
    RunConfigOut,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

_TZ_SETTING_KEY = "display_timezone"
_TZ_DEFAULT = "UTC"

# Defaults for the retention sweep — kept in sync with
# ``backend.services.scheduler_service``. Imported lazily inside the
# route to avoid pulling the scheduler module into the import graph
# of routes that have no scheduler dependency.
_RETENTION_DAYS_DEFAULT = 90
_QUARANTINE_RETENTION_DAYS_DEFAULT = 30
_RETENTION_DAYS_MIN = 7
# Generous upper bound — anything past ~3 years is almost certainly a
# typo, and lets the UI render a meaningful slider/input range.
_RETENTION_DAYS_MAX = 3650

_LABEL_DEFS_SETTING_KEY = "label_definitions"
# Keys must be safe for YAML round-tripping and stable as DataFrame columns:
# letters, digits, underscore, leading with a letter.
_LABEL_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


class TimezoneOut(BaseModel):
    timezone: str


class TimezoneIn(BaseModel):
    timezone: str


class LabelDefinition(BaseModel):
    """An admin-managed label definition.

    Defines one label key plus the values rule authors can choose from. When
    ``values`` is empty the label is a boolean tag. When ``allow_custom_values``
    is true authors can type a value not in the list.

    The reserved key ``weight`` plays a special role: its values populate the
    weight selector in the labels editor on rule authoring pages. Weight is
    stored entirely in ``user_metadata`` (no separate native ``weight`` field).
    """

    key: str
    description: str | None = ""
    values: list[str] = Field(default_factory=list)
    allow_custom_values: bool = False


class LabelDefinitionsOut(BaseModel):
    definitions: list[LabelDefinition]


class LabelDefinitionsIn(BaseModel):
    definitions: list[LabelDefinition]


def _notify_scheduler() -> None:
    """Best-effort reload of the background scheduler after config changes."""
    try:
        from databricks_labs_dqx_app.backend._scheduler_registry import notify_scheduler

        notify_scheduler()
    except Exception:
        pass


router = APIRouter()


@router.get(
    "",
    response_model=ConfigOut,
    operation_id="config",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_config(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ConfigOut:
    """Load workspace config from application state (admin only)."""
    try:
        config = svc.get_config()
        logger.info(f"Loaded config with {len(config.run_configs)} run configs")
        return ConfigOut(config=config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load configuration: {e}")


@router.post(
    "",
    response_model=ConfigOut,
    operation_id="saveConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_config(
    body: ConfigIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> ConfigOut:
    """Save workspace config to application state (admin only)."""
    try:
        svc.save_config(body.config, user_email=email)
        _notify_scheduler()
        config = svc.get_config()
        return ConfigOut(config=config)
    except Exception as e:
        logger.error(f"Failed to save config: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save configuration: {e}")


@router.get(
    "/run/{name}",
    response_model=RunConfigOut,
    operation_id="getRunConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_run_config(
    name: str,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RunConfigOut:
    """Get a single run config by name (admin only)."""
    config = svc.get_config()
    for rc in config.run_configs:
        if rc.name == name:
            return RunConfigOut(config=rc)
    raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")


@router.post(
    "/run",
    response_model=RunConfigOut,
    operation_id="saveRunConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_run_config(
    body: RunConfigIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> RunConfigOut:
    """Save a run config — creates or updates by name (admin only)."""
    config = svc.get_config()
    # Replace existing or append
    updated = False
    for i, rc in enumerate(config.run_configs):
        if rc.name == body.config.name:
            config.run_configs[i] = body.config
            updated = True
            break
    if not updated:
        config.run_configs.append(body.config)

    svc.save_config(config, user_email=email)
    _notify_scheduler()
    return RunConfigOut(config=body.config)


@router.delete(
    "/run/{name}",
    response_model=ConfigOut,
    operation_id="deleteRunConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def delete_run_config(
    name: str,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> ConfigOut:
    """Delete a run config by name (admin only)."""
    config = svc.get_config()
    original_count = len(config.run_configs)
    config.run_configs = [rc for rc in config.run_configs if rc.name != name]

    if len(config.run_configs) == original_count:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    svc.save_config(config, user_email=email)
    _notify_scheduler()
    return ConfigOut(config=config)


@router.get(
    "/timezone",
    response_model=TimezoneOut,
    operation_id="getTimezone",
)
def get_timezone(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> TimezoneOut:
    """Get the display timezone (accessible by all authenticated users)."""
    tz = svc.get_setting(_TZ_SETTING_KEY) or _TZ_DEFAULT
    return TimezoneOut(timezone=tz)


@router.put(
    "/timezone",
    response_model=TimezoneOut,
    operation_id="saveTimezone",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_timezone(
    body: TimezoneIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> TimezoneOut:
    """Set the display timezone (admin only)."""
    svc.save_setting(_TZ_SETTING_KEY, body.timezone, user_email=email)
    return TimezoneOut(timezone=body.timezone)


# ---------------------------------------------------------------------------
# Retention — global vs. quarantine-specific DELETE windows surfaced for the
# admin UI. The scheduler reads the same keys directly from
# ``dq_app_settings`` (see ``SchedulerService._resolve_retention_days`` /
# ``_resolve_quarantine_retention_days``); these endpoints are the
# read/write surface and the only place we centralise validation.
# ---------------------------------------------------------------------------


class RetentionSettingsOut(BaseModel):
    """Effective retention settings + the defaults the scheduler falls back to.

    ``retention_days`` / ``quarantine_retention_days`` reflect the
    *current effective values* — the persisted setting if one exists,
    otherwise the compiled-in default. The ``*_default`` and ``*_min``
    fields let the UI render hints and validation without duplicating
    the constants on the frontend.
    """

    retention_days: int
    quarantine_retention_days: int
    retention_days_default: int = _RETENTION_DAYS_DEFAULT
    quarantine_retention_days_default: int = _QUARANTINE_RETENTION_DAYS_DEFAULT
    retention_days_min: int = _RETENTION_DAYS_MIN
    retention_days_max: int = _RETENTION_DAYS_MAX
    retention_days_set: bool
    quarantine_retention_days_set: bool


class RetentionSettingsIn(BaseModel):
    """Update payload — either field omitted means *leave unchanged*."""

    retention_days: int | None = None
    quarantine_retention_days: int | None = None


def _validate_retention_days(value: int, *, field: str) -> int:
    if value < _RETENTION_DAYS_MIN:
        raise HTTPException(
            status_code=400,
            detail=(f"{field} must be at least {_RETENTION_DAYS_MIN} days " "to protect against accidental data loss."),
        )
    if value > _RETENTION_DAYS_MAX:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be at most {_RETENTION_DAYS_MAX} days.",
        )
    return value


@router.get(
    "/retention",
    response_model=RetentionSettingsOut,
    operation_id="getRetentionSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_retention_settings(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RetentionSettingsOut:
    """Return the current retention windows + defaults (admin only)."""
    rd = svc.get_retention_days()
    qd = svc.get_quarantine_retention_days()
    return RetentionSettingsOut(
        retention_days=rd if rd is not None else _RETENTION_DAYS_DEFAULT,
        quarantine_retention_days=qd if qd is not None else _QUARANTINE_RETENTION_DAYS_DEFAULT,
        retention_days_set=rd is not None,
        quarantine_retention_days_set=qd is not None,
    )


@router.put(
    "/retention",
    response_model=RetentionSettingsOut,
    operation_id="saveRetentionSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_retention_settings(
    body: RetentionSettingsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> RetentionSettingsOut:
    """Update one or both retention windows (admin only).

    Either field may be omitted to leave the existing value unchanged.
    Both values are validated against the safety floor and ceiling
    before being persisted.
    """
    if body.retention_days is None and body.quarantine_retention_days is None:
        raise HTTPException(
            status_code=400,
            detail="At least one of retention_days or quarantine_retention_days must be provided.",
        )

    if body.retention_days is not None:
        validated = _validate_retention_days(body.retention_days, field="retention_days")
        svc.save_retention_days(validated, user_email=email)
        logger.info("Saved global retention_days=%d", validated)

    if body.quarantine_retention_days is not None:
        validated_q = _validate_retention_days(body.quarantine_retention_days, field="quarantine_retention_days")
        svc.save_quarantine_retention_days(validated_q, user_email=email)
        logger.info("Saved quarantine_retention_days=%d", validated_q)

    return get_retention_settings(svc)


# ---------------------------------------------------------------------------
# Label definitions — admin-managed catalog of label keys + allowed values.
# Powers the constrained-mode label picker on rule authoring pages, and
# (via the reserved ``weight`` key) the weight selector. Storage is one JSON
# blob in ``dq_app_settings`` keyed by ``label_definitions``.
# ---------------------------------------------------------------------------


def _load_label_definitions(svc: AppSettingsService) -> list[LabelDefinition]:
    raw = svc.get_setting(_LABEL_DEFS_SETTING_KEY)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Failed to parse label_definitions JSON; treating as empty")
        return []
    if not isinstance(data, list):
        logger.warning("label_definitions setting is not a list; treating as empty")
        return []
    out: list[LabelDefinition] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            out.append(LabelDefinition.model_validate(item))
        except Exception as e:  # noqa: BLE001 — best-effort migration of legacy shapes
            logger.warning("Skipping malformed label definition %r: %s", item, e)
    return out


@router.get(
    "/label-definitions",
    response_model=LabelDefinitionsOut,
    operation_id="getLabelDefinitions",
)
def get_label_definitions(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> LabelDefinitionsOut:
    """Return all admin-defined label definitions.

    Available to any authenticated user — the rule authoring UI needs these
    to populate the constrained label picker (including the weight selector).
    """
    return LabelDefinitionsOut(definitions=_load_label_definitions(svc))


@router.put(
    "/label-definitions",
    response_model=LabelDefinitionsOut,
    operation_id="saveLabelDefinitions",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_label_definitions(
    body: LabelDefinitionsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> LabelDefinitionsOut:
    """Replace the full set of label definitions (admin only).

    Validates each key against ``_LABEL_KEY_RE``, rejects duplicates, trims
    descriptions, and dedupes the value list per definition.
    """
    seen_keys: set[str] = set()
    cleaned: list[LabelDefinition] = []
    for d in body.definitions:
        key = (d.key or "").strip()
        if not key:
            raise HTTPException(status_code=400, detail="Label key cannot be blank.")
        if not _LABEL_KEY_RE.match(key):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid label key '{key}'. Keys must start with a letter and "
                    "contain only letters, digits, and underscores."
                ),
            )
        if key in seen_keys:
            raise HTTPException(status_code=400, detail=f"Duplicate label key: '{key}'.")
        seen_keys.add(key)

        cleaned_values: list[str] = []
        seen_values: set[str] = set()
        for v in d.values:
            sv = (v or "").strip()
            if not sv or sv in seen_values:
                continue
            seen_values.add(sv)
            cleaned_values.append(sv)
        cleaned.append(
            LabelDefinition(
                key=key,
                description=(d.description or "").strip(),
                values=cleaned_values,
                allow_custom_values=bool(d.allow_custom_values),
            )
        )

    svc.save_setting(_LABEL_DEFS_SETTING_KEY, json.dumps([d.model_dump() for d in cleaned]), user_email=email)
    logger.info("Saved %d label definition(s)", len(cleaned))
    return LabelDefinitionsOut(definitions=cleaned)


# ----------------------------------------------------------------------
# Custom metrics — global SQL expressions appended to DQMetricsObserver.
# Persisted as a JSON list of strings under ``custom_metrics_v1`` in
# ``dq_app_settings`` (handled by ``AppSettingsService``).
# ----------------------------------------------------------------------

# Each entry must be ``<aggregate_expression> as <alias>``. We validate
# the alias and reject obvious DDL/DML using DQX's denylist; the runner
# re-validates before passing them to the observer (defence in depth).
_CUSTOM_METRIC_RE = re.compile(r"^[A-Za-z0-9_(),.\s'\"\-+*/=<>!|&%:?\[\]]+ as [A-Za-z_][A-Za-z0-9_]*$")


class CustomMetricsOut(BaseModel):
    metrics: list[str]


class CustomMetricsIn(BaseModel):
    metrics: list[str]


def _validate_custom_metric_expr(expr: str) -> str:
    """Reject malformed or unsafe metric SQL. Returns the trimmed expression on success."""
    e = (expr or "").strip()
    if not e:
        raise HTTPException(status_code=400, detail="Custom metric expression cannot be blank.")
    if not _CUSTOM_METRIC_RE.match(e):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid custom metric: {e!r}. "
                "Expected '<aggregate expression> as <alias>' where alias is a valid identifier."
            ),
        )
    try:
        from databricks.labs.dqx.utils import is_sql_query_safe

        if not is_sql_query_safe(e):
            raise HTTPException(
                status_code=400,
                detail=f"Custom metric contains prohibited SQL keywords: {e!r}",
            )
    except ImportError:
        # If the helper isn't importable in this environment we fail open
        # — the runner re-validates, and the SQL only ever runs on the
        # task-runner cluster, never on the app process.
        pass
    return e


@router.get(
    "/custom-metrics",
    response_model=CustomMetricsOut,
    operation_id="getCustomMetrics",
)
def get_custom_metrics(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> CustomMetricsOut:
    """Return the global list of custom metric SQL expressions.

    Available to any authenticated user so the UI can preview which metrics
    will be collected; the actual application of these metrics happens in
    the task runner via ``DQMetricsObserver(custom_metrics=…)``.
    """
    return CustomMetricsOut(metrics=svc.get_custom_metrics())


@router.put(
    "/custom-metrics",
    response_model=CustomMetricsOut,
    operation_id="saveCustomMetrics",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_custom_metrics(
    body: CustomMetricsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> CustomMetricsOut:
    """Replace the global custom-metrics list (admin only).

    Each entry is validated for shape and SQL safety. Duplicates (after
    normalisation) are collapsed.
    """
    seen: set[str] = set()
    cleaned: list[str] = []
    for raw in body.metrics:
        expr = _validate_custom_metric_expr(raw)
        if expr in seen:
            continue
        seen.add(expr)
        cleaned.append(expr)
    saved = svc.save_custom_metrics(cleaned, user_email=email)
    logger.info("Saved %d custom metric expression(s)", len(saved))
    return CustomMetricsOut(metrics=saved)
