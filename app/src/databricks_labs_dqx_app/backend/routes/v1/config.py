import asyncio
import json
import os
import re
import threading
from typing import Annotated

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_conf,
    get_sp_ws,
    get_vector_store_provisioner,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from pydantic import BaseModel, Field, field_validator

from databricks_labs_dqx_app.backend.models import (
    ConfigIn,
    ConfigOut,
    RunConfigIn,
    RunConfigOut,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import (
    DEFAULT_PASS_THRESHOLD_DEFAULT,
    DRAFT_RUN_SAMPLE_LIMIT_DEFAULT,
    AppSettingsService,
)
from databricks_labs_dqx_app.backend.services.vector_store import VectorStoreProvisioner

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
# Per-value badge colors: strict 6-digit hex so the UI can trust the value
# without re-validating (e.g. drop straight into a CSS custom property).
_HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")

# Reserved keys whose value set is fixed and admin-curated rather than
# author-extensible: rule authors can never add a value the admin hasn't
# already defined. Enforced server-side (independent of client payload)
# so an old client — or a stale/tampered request — can't smuggle
# ``allow_custom_values: true`` back onto these two keys.
_NO_CUSTOM_VALUE_BUILTIN_KEYS = frozenset({"dimension", "severity"})


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

    ``value_colors`` optionally maps a subset (or all) of ``values`` to a
    ``#RRGGBB`` hex color for badge rendering; unmapped values fall back to a
    UI default. ``value_descriptions`` optionally maps a subset (or all) of
    ``values`` to a short human-readable explanation, shown as help text next
    to each value in the admin editor and as a tooltip wherever the value is
    picked (e.g. the ``dimension`` key's per-dimension descriptions). Both
    maps are pruned to keys present in ``values`` on save.

    ``value_criticality`` optionally maps a subset (or all) of ``values`` to a
    DQX ``criticality`` (``"warn"`` or ``"error"``). Only meaningful on the
    reserved ``severity`` key today: the materializer reads it to decide which
    criticality a registry rule's effective severity renders as (see
    ``registry_models.resolve_criticality``). Unmapped values fall back to the
    built-in defaults. Pruned to keys present in ``values`` on save, like the
    other per-value maps.

    ``is_builtin`` flags a reserved, pre-seeded key (e.g. the Rules Registry
    ``dimension``/``severity`` tags) — such keys cannot be deleted or renamed
    via :func:`save_label_definitions`, though their values, colors, and
    descriptions may still be edited. The ``dimension``/``severity`` keys
    additionally can never have ``allow_custom_values=True``: their value set
    is fixed and admin-curated, not something rule authors extend inline.
    """

    key: str
    description: str | None = ""
    values: list[str] = Field(default_factory=list)
    allow_custom_values: bool = False
    value_colors: dict[str, str] | None = None
    value_descriptions: dict[str, str] | None = None
    value_criticality: dict[str, str] | None = None
    is_builtin: bool = False

    @field_validator("value_colors")
    @classmethod
    def _validate_value_colors(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is None:
            return None
        for label_value, color in value.items():
            if not _HEX_COLOR_RE.match(color):
                raise ValueError(f"Invalid color {color!r} for value {label_value!r}; expected '#RRGGBB' hex format.")
        return value

    @field_validator("value_criticality")
    @classmethod
    def _validate_value_criticality(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is None:
            return None
        for label_value, criticality in value.items():
            if criticality not in ("warn", "error"):
                raise ValueError(
                    f"Invalid criticality {criticality!r} for value {label_value!r}; expected 'warn' or 'error'."
                )
        return value


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
    operation_id="getConfig",
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
            detail=(f"{field} must be at least {_RETENTION_DAYS_MIN} days to protect against accidental data loss."),
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
# Draft-run sample limit — admin knob capping the rows a DRAFT monitored-
# table run reads. Approved/published runs never sample (they always scan
# the whole table — see ``BindingRunService.run_binding``); this setting
# exists only so exploratory draft runs on large tables stay cheap.
# 0 = unlimited (draft runs also scan the whole table).
# ---------------------------------------------------------------------------

# Generous ceiling — a draft "sample" past ten million rows is almost
# certainly a typo; admins wanting full scans should use 0 (unlimited).
_DRAFT_SAMPLE_LIMIT_MAX = 10_000_000


class DraftRunSampleLimitOut(BaseModel):
    """Effective draft-run sample limit + the default/bounds for the UI."""

    draft_run_sample_limit: int
    draft_run_sample_limit_default: int = DRAFT_RUN_SAMPLE_LIMIT_DEFAULT
    draft_run_sample_limit_max: int = _DRAFT_SAMPLE_LIMIT_MAX
    draft_run_sample_limit_set: bool


class DraftRunSampleLimitIn(BaseModel):
    draft_run_sample_limit: int = Field(
        ge=0,
        le=_DRAFT_SAMPLE_LIMIT_MAX,
        description="Draft runs sample at most this many rows; 0 checks the whole table.",
    )


@router.get(
    "/draft-run-sample-limit",
    response_model=DraftRunSampleLimitOut,
    operation_id="getDraftRunSampleLimit",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_draft_run_sample_limit(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DraftRunSampleLimitOut:
    """Return the current draft-run sample limit + default (admin only)."""
    limit = svc.get_draft_run_sample_limit()
    return DraftRunSampleLimitOut(
        draft_run_sample_limit=limit if limit is not None else DRAFT_RUN_SAMPLE_LIMIT_DEFAULT,
        draft_run_sample_limit_set=limit is not None,
    )


@router.put(
    "/draft-run-sample-limit",
    response_model=DraftRunSampleLimitOut,
    operation_id="saveDraftRunSampleLimit",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_draft_run_sample_limit(
    body: DraftRunSampleLimitIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> DraftRunSampleLimitOut:
    """Update the draft-run sample limit (admin only). 0 = unlimited."""
    svc.save_draft_run_sample_limit(body.draft_run_sample_limit, user_email=email)
    logger.info("Saved draft_run_sample_limit=%d", body.draft_run_sample_limit)
    return get_draft_run_sample_limit(svc)


# ---------------------------------------------------------------------------
# Default pass threshold — org-wide minimum pass rate (%) below which a
# check warns. Resolution order: per-column → per-rule → registry default →
# this admin default (compiled fallback 70).
# ---------------------------------------------------------------------------


class DefaultPassThresholdOut(BaseModel):
    """Effective default pass threshold + the compiled default for the UI."""

    default_pass_threshold: int
    default_pass_threshold_default: int


class DefaultPassThresholdIn(BaseModel):
    default_pass_threshold: int = Field(
        ge=0,
        le=100,
        description="Org-wide default minimum pass rate (%); checks warn when pass rate drops below this.",
    )


@router.get(
    "/default-pass-threshold",
    response_model=DefaultPassThresholdOut,
    operation_id="getDefaultPassThreshold",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_default_pass_threshold(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DefaultPassThresholdOut:
    """Return the current default pass threshold (admin only)."""
    return DefaultPassThresholdOut(
        default_pass_threshold=svc.get_default_pass_threshold(),
        default_pass_threshold_default=DEFAULT_PASS_THRESHOLD_DEFAULT,
    )


@router.put(
    "/default-pass-threshold",
    response_model=DefaultPassThresholdOut,
    operation_id="saveDefaultPassThreshold",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_default_pass_threshold(
    body: DefaultPassThresholdIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> DefaultPassThresholdOut:
    """Update the default pass threshold (admin only)."""
    svc.save_default_pass_threshold(body.default_pass_threshold, user_email=email)
    logger.info("Saved default_pass_threshold=%d", body.default_pass_threshold)
    return get_default_pass_threshold(svc)


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
            definition = LabelDefinition.model_validate(item)
            if definition.key in _NO_CUSTOM_VALUE_BUILTIN_KEYS and definition.allow_custom_values:
                # Defense-in-depth: coerce even rows persisted before this
                # invariant existed (or written directly to the settings
                # table) rather than trusting every historical write path.
                definition = definition.model_copy(update={"allow_custom_values": False})
            out.append(definition)
        except Exception as e:
            # Per-item resilience: settings stored before the v1 label-
            # definition schema may carry legacy keys or extra fields
            # that ``LabelDefinition.model_validate`` rejects. Skipping
            # the malformed entry preserves the rest of the user's
            # configured labels; failing the whole list would 500 the
            # label-definitions endpoint over a single bad row. Logged
            # so the operator can fix the source row at their leisure.
            # ValidationError alone would be too narrow — older payload
            # shapes can trip TypeError / AttributeError inside the
            # validator before Pydantic's own error wrapping fires.
            # See the BLE001 policy block in pyproject.toml.
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

    Reserved keys (``is_builtin=True`` in the currently-persisted catalog —
    e.g. the Rules Registry ``dimension``/``severity`` tags) cannot be
    deleted or renamed: the incoming payload must still contain an entry
    with the same key. Their values, colors, and description may still be
    freely edited. ``is_builtin`` itself is authoritative from the stored
    state, not the client payload — a caller can't strip the flag off a
    reserved key by omitting/flipping it in the request. ``dimension`` and
    ``severity`` additionally always save with ``allow_custom_values=False``
    regardless of what the client sends — their value set is fixed/admin-
    curated, never author-extensible.
    """
    existing_builtin_keys = {d.key for d in _load_label_definitions(svc) if d.is_builtin}

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

        cleaned_colors = {v: c for v, c in (d.value_colors or {}).items() if v in seen_values} or None
        cleaned_descriptions = {
            v: desc.strip()
            for v, desc in (d.value_descriptions or {}).items()
            if v in seen_values and (desc or "").strip()
        } or None
        cleaned_criticality = {v: c for v, c in (d.value_criticality or {}).items() if v in seen_values} or None

        cleaned.append(
            LabelDefinition(
                key=key,
                description=(d.description or "").strip(),
                values=cleaned_values,
                allow_custom_values=False if key in _NO_CUSTOM_VALUE_BUILTIN_KEYS else bool(d.allow_custom_values),
                value_colors=cleaned_colors,
                value_descriptions=cleaned_descriptions,
                value_criticality=cleaned_criticality,
                # Authoritative from the previously-persisted state, never
                # from the client payload — a caller cannot grant or strip
                # ``is_builtin`` protection via the request body.
                is_builtin=key in existing_builtin_keys,
            )
        )

    missing_reserved = existing_builtin_keys - seen_keys
    if missing_reserved:
        raise HTTPException(
            status_code=400,
            detail=(f"Cannot delete or rename reserved label key(s): {', '.join(sorted(missing_reserved))}."),
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


# ----------------------------------------------------------------------
# Workspace host — read from ``DATABRICKS_HOST`` (always set inside a
# Databricks App container) and exposed so the frontend can build deep
# links into the workspace UI (e.g. Unity Catalog explorer, run pages).
# ----------------------------------------------------------------------


def _workspace_host() -> str:
    """Read the workspace host from the env Databricks Apps populates at runtime.

    Returns an empty string if unset (e.g. local dev without DATABRICKS_HOST);
    the UI handles this by showing a config-required message rather than a
    broken iframe.
    """
    host = (os.environ.get("DATABRICKS_HOST") or "").strip()
    if host and not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host.rstrip("/")


class WorkspaceHostOut(BaseModel):
    """Workspace host for building deep links into the Databricks workspace UI."""

    workspace_host: str = Field(
        default="",
        description=(
            "Workspace host (e.g. 'https://e2-...cloud.databricks.com') used to build "
            "links into the workspace UI, such as Unity Catalog explorer pages. "
            "Empty string when unset (local dev)."
        ),
    )
    job_id: str = Field(
        default="",
        description=(
            "Task-runner Databricks job id (``DQX_JOB_ID``). Combined with the host "
            "and a run's ``job_run_id`` the UI builds a deep link to the run page: "
            "``{workspace_host}/jobs/{job_id}/runs/{job_run_id}``. Empty when unset "
            "(local dev / job not configured)."
        ),
    )


@router.get(
    "/workspace-host",
    response_model=WorkspaceHostOut,
    operation_id="getWorkspaceHost",
)
def get_workspace_host(conf: Annotated[AppConfig, Depends(get_conf)]) -> WorkspaceHostOut:
    """Return the workspace host + task-runner job id (accessible by all authenticated users).

    Neither value grants data access on its own — links built from them (e.g.
    Unity Catalog explorer, job-run pages) still enforce the caller's own
    workspace/UC permissions on arrival.
    """
    return WorkspaceHostOut(workspace_host=_workspace_host(), job_id=conf.job_id)


# ----------------------------------------------------------------------
# Run review statuses — admin-managed catalogue of values for the per-run
# review label shown on the Runs detail page and filterable on the Runs
# History page. Stored as a JSON list under ``run_review_statuses_v1``
# (see :meth:`AppSettingsService.get_run_review_statuses`). The catalogue
# always includes exactly one entry marked ``is_default``; the service
# enforces that invariant on save so the Runs detail dropdown is never
# empty and listing endpoints always have something to surface for
# unreviewed runs.
# ----------------------------------------------------------------------

# Conservative value validation: the catalogue values become filter
# chips on the History page and audit-log strings, so we want them
# printable and short. Letters/digits/spaces/hyphens/underscores covers
# "Pending review", "Acknowledged", "False positive", custom domain
# terms, while rejecting newlines, control chars, and quote characters
# that would break our raw-SQL escape path on the history table.
_REVIEW_STATUS_VALUE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _\-/.]{0,79}$")


class RunReviewStatusOption(BaseModel):
    """One catalogue entry. ``is_default`` flags the value auto-surfaced for unreviewed runs."""

    value: str
    description: str = ""
    color: str = "gray"
    is_default: bool = False


class RunReviewStatusesOut(BaseModel):
    statuses: list[RunReviewStatusOption]


class RunReviewStatusesIn(BaseModel):
    statuses: list[RunReviewStatusOption]


def _statuses_to_out(entries: list[dict]) -> RunReviewStatusesOut:
    """Coerce the service's dict-shaped entries into the pydantic out model."""
    return RunReviewStatusesOut(
        statuses=[
            RunReviewStatusOption(
                value=e.get("value") or "",
                description=e.get("description") or "",
                color=e.get("color") or "gray",
                is_default=bool(e.get("is_default")),
            )
            for e in entries
        ]
    )


@router.get(
    "/run-review-statuses",
    response_model=RunReviewStatusesOut,
    operation_id="getRunReviewStatuses",
)
def get_run_review_statuses(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RunReviewStatusesOut:
    """Return the admin-managed list of run review status values.

    Visible to any authenticated user — both the Runs detail dropdown
    and the Runs History filter need this list, and neither is
    admin-gated. The list is seeded on first read so the UI never
    sees an empty dropdown on a fresh deploy.
    """
    return _statuses_to_out(svc.get_run_review_statuses())


@router.put(
    "/run-review-statuses",
    response_model=RunReviewStatusesOut,
    operation_id="saveRunReviewStatuses",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_run_review_statuses(
    body: RunReviewStatusesIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> RunReviewStatusesOut:
    """Replace the full catalogue (admin only).

    Each value is validated against ``_REVIEW_STATUS_VALUE_RE`` (printable
    short strings, no quotes/control chars), descriptions are trimmed,
    duplicates are rejected, and exactly one ``is_default`` is enforced
    by :meth:`AppSettingsService.save_run_review_statuses`.

    NOTE: renaming an existing value does *not* update historical
    references in ``dq_run_review_status`` / ``dq_run_review_status_history``;
    those rows keep the original string so the audit trail stays
    accurate. The UI surfaces orphaned historical values as-is. If a
    workspace operator wants to retire a value cleanly they should
    either keep it in the list (without ``is_default``) until the
    affected runs age out, or do a one-off UPDATE through the SQL
    warehouse.
    """
    cleaned_payload: list[dict] = []
    for option in body.statuses or []:
        value = (option.value or "").strip()
        if not value:
            raise HTTPException(status_code=400, detail="Run review status 'value' cannot be blank.")
        if not _REVIEW_STATUS_VALUE_RE.match(value):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid review status value {value!r}. Use printable ASCII "
                    "(letters, digits, spaces, hyphens, underscores, dots, slashes), "
                    "1–80 characters, starting with a letter or digit."
                ),
            )
        cleaned_payload.append(
            {
                "value": value,
                "description": (option.description or "").strip(),
                "color": (option.color or "gray").strip() or "gray",
                "is_default": bool(option.is_default),
            }
        )

    try:
        saved = svc.save_run_review_statuses(cleaned_payload, user_email=email)
    except ValueError as e:
        # The service's invariants (at-least-one entry, unique values,
        # exactly one default) surface as ValueError. Route surface
        # them as 400 so the UI can show the human-readable message.
        raise HTTPException(status_code=400, detail=str(e))
    logger.info("Saved %d run review status(es)", len(saved))
    return _statuses_to_out(saved)


# ----------------------------------------------------------------------
# AI Gateway settings — Rules Registry Phase 4A. Kill-switch, serving
# endpoint name, and per-user hourly rate limit for AIGateway
# (services/ai_gateway.py). ADMIN only: this is infrastructure config, not
# an authoring-time preference. A full "AI settings card" UI is Phase 4.5
# — these endpoints are the read/write surface it will consume.
# ----------------------------------------------------------------------


class AiSettingsOut(BaseModel):
    """Effective AI Gateway + Vector Search settings.

    ``embedding_endpoint_name``/``vs_endpoint_name``/``vs_index_name``
    (Rules Registry Phase 4B/4C) are auto-derived since Phase 8B — the
    admin UI no longer exposes them as separate inputs. They always
    resolve to a usable value (see ``AppSettingsService.EMBEDDING_ENDPOINT_NAME_DEFAULT``
    and ``_default_vs_endpoint_name``/``_default_vs_index_name``) so the
    rule-mapping suggester's vector store works from the AI enable
    toggle + serving endpoint alone. Still independently settable via
    this API for backwards compatibility/testing.
    """

    ai_enabled: bool
    ai_endpoint_name: str
    ai_endpoint_name_default: str = AppSettingsService.AI_ENDPOINT_NAME_DEFAULT
    ai_rate_limit_per_user_per_hour: int
    ai_rate_limit_default: int = AppSettingsService.AI_RATE_LIMIT_DEFAULT
    embedding_endpoint_name: str = ""
    vs_endpoint_name: str = ""
    vs_index_name: str = ""


class AiSettingsIn(BaseModel):
    """Update payload — omitted fields are left unchanged."""

    ai_enabled: bool | None = None
    ai_endpoint_name: str | None = None
    ai_rate_limit_per_user_per_hour: int | None = None
    embedding_endpoint_name: str | None = None
    vs_endpoint_name: str | None = None
    vs_index_name: str | None = None


def _fire_and_forget_ensure_vector_store(provisioner: VectorStoreProvisioner) -> None:
    """Kick off Vector Search auto-provisioning on a background thread.

    ``VectorStoreProvisioner.ensure_vector_store`` is an async, best-effort,
    never-raising coroutine (see ``services/vector_store.py``) that submits
    the Vector Search endpoint/index creation calls and returns immediately
    — creation itself finishes asynchronously on the Databricks control
    plane. The app startup lifespan can attach it to the running event loop
    via ``asyncio.create_task``, but this route runs synchronously with no
    event loop of its own, so we run the coroutine to completion on a
    dedicated daemon thread via ``asyncio.run`` instead.

    This must never block the admin's "save AI settings" request or
    propagate an error back to the caller: any failure here — including one
    that somehow escapes ``ensure_vector_store``'s own swallow-and-log
    behaviour — is logged and dropped.
    """

    def _run() -> None:
        try:
            asyncio.run(provisioner.ensure_vector_store())
        except Exception:
            logger.warning("Background Vector Search auto-provisioning failed (non-fatal)", exc_info=True)

    threading.Thread(target=_run, name="ensure-vector-store-on-save", daemon=True).start()


def _ai_settings_out(svc: AppSettingsService) -> AiSettingsOut:
    return AiSettingsOut(
        ai_enabled=svc.get_ai_enabled(),
        ai_endpoint_name=svc.get_ai_endpoint_name(),
        ai_rate_limit_per_user_per_hour=svc.get_ai_rate_limit_per_user_per_hour(),
        embedding_endpoint_name=svc.get_embedding_endpoint_name(),
        vs_endpoint_name=svc.get_vs_endpoint_name(),
        vs_index_name=svc.get_vs_index_name(),
    )


@router.get(
    "/ai-settings",
    response_model=AiSettingsOut,
    operation_id="getAiSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_ai_settings(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> AiSettingsOut:
    """Return the current AI Gateway + Vector Search settings (admin only)."""
    return _ai_settings_out(svc)


@router.put(
    "/ai-settings",
    response_model=AiSettingsOut,
    operation_id="saveAiSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_ai_settings(
    body: AiSettingsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    provisioner: Annotated[VectorStoreProvisioner, Depends(get_vector_store_provisioner)],
    email: Annotated[str, Depends(get_user_email)],
) -> AiSettingsOut:
    """Update one or more AI Gateway / Vector Search settings (admin only)."""
    fields = (
        body.ai_enabled,
        body.ai_endpoint_name,
        body.ai_rate_limit_per_user_per_hour,
        body.embedding_endpoint_name,
        body.vs_endpoint_name,
        body.vs_index_name,
    )
    if all(field is None for field in fields):
        raise HTTPException(status_code=400, detail="At least one AI/Vector Search setting must be provided.")

    if body.ai_enabled is not None:
        svc.save_ai_enabled(body.ai_enabled, user_email=email)
    if body.ai_endpoint_name is not None:
        svc.save_ai_endpoint_name(body.ai_endpoint_name, user_email=email)
    if body.ai_rate_limit_per_user_per_hour is not None:
        if body.ai_rate_limit_per_user_per_hour < 0:
            raise HTTPException(status_code=400, detail="ai_rate_limit_per_user_per_hour must be >= 0.")
        svc.save_ai_rate_limit_per_user_per_hour(body.ai_rate_limit_per_user_per_hour, user_email=email)
    if body.embedding_endpoint_name is not None:
        svc.save_embedding_endpoint_name(body.embedding_endpoint_name, user_email=email)
    if body.vs_endpoint_name is not None:
        svc.save_vs_endpoint_name(body.vs_endpoint_name, user_email=email)
    if body.vs_index_name is not None:
        svc.save_vs_index_name(body.vs_index_name, user_email=email)

    logger.info("Saved AI Gateway / Vector Search settings (by=%s)", email)

    # Best-effort, non-blocking Vector Search auto-provisioning: whenever a
    # save leaves AI enabled (whether this call just flipped the switch on
    # or merely updated an endpoint/rate-limit while it was already on),
    # give the endpoint/index creation a kick so admins don't also have to
    # remember to hit the dedicated ``POST /ensure-vector-store`` endpoint.
    # No-op (inside the provisioner) when embedding/VS settings aren't
    # fully configured; never raises; never blocks this response.
    if svc.get_ai_enabled():
        _fire_and_forget_ensure_vector_store(provisioner)

    return _ai_settings_out(svc)


# ----------------------------------------------------------------------
# Serving endpoints — Rules Registry Phase 7F. Backs the AI settings
# dropdown so admins pick ``ai_endpoint_name``/``embedding_endpoint_name``
# from the workspace's actual serving endpoints instead of typing a raw
# string. Read-only, best-effort: any SDK failure (permissions, transient
# outage) degrades to an empty list rather than a 500 so the settings page
# still renders and free-text fallback remains possible.
# ----------------------------------------------------------------------


class ServingEndpointsOut(BaseModel):
    names: list[str]


@router.get(
    "/serving-endpoints",
    response_model=ServingEndpointsOut,
    operation_id="listServingEndpoints",
    dependencies=[require_role(UserRole.ADMIN)],
)
async def list_serving_endpoints(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> ServingEndpointsOut:
    """Return the workspace's serving endpoint names, or ``[]`` on any SDK failure."""
    try:
        endpoints = await asyncio.to_thread(lambda: list(sp_ws.serving_endpoints.list()))
    except DatabricksError:
        logger.warning("Failed to list serving endpoints", exc_info=True)
        return ServingEndpointsOut(names=[])
    names = sorted({endpoint.name for endpoint in endpoints if endpoint.name})
    return ServingEndpointsOut(names=names)


# ----------------------------------------------------------------------
# Rules Registry governance settings (P21-G). Two distinct admin knobs
# that both shape "what happens as registry rules evolve", but at
# different moments — surfaced together here so the UI can present them
# side by side without conflating them:
#
#   * ``auto_upgrade_without_approval`` — governs RE-APPROVAL of an
#     EXISTING following (unpinned) application when its rule is
#     re-published with a materially different rendered check: silently
#     re-approve (True) vs. fall back to ``pending_approval`` for
#     per-table re-review (False, default).
#   * ``default_auto_upgrade`` — governs the PIN CHOSEN AT ATTACH TIME
#     for a brand-new rule application / data-product member that
#     doesn't request an explicit pin: follow latest (True, default) vs.
#     freeze to the current version (False). Existing applications are
#     never affected by a later change to this setting — see
#     ``AppSettingsService.resolve_pinned_version_for_new_attachment``.
#
# Both read at VIEWER+ (stewards should be able to see the effective
# governance policy) and write at ADMIN-only, matching the AI Gateway
# settings pattern above.
# ----------------------------------------------------------------------


class RulesRegistrySettingsOut(BaseModel):
    """Effective Rules Registry governance settings."""

    auto_upgrade_without_approval: bool = Field(
        description="Re-approval behaviour: silently re-approve a following application's "
        "re-rendered check (True, default) vs. send it back to pending_approval (False)."
    )
    default_auto_upgrade: bool = Field(
        description="Attach-time default pin for new applications/members: follow latest "
        "(True, default) vs. pin to the current version (False)."
    )
    tag_auto_apply: bool = Field(
        description="Tag-mapping apply behaviour: eagerly auto-attach tag-mapped rules "
        "across monitored tables (True) vs. only surface them as suggestions (False, default)."
    )
    default_pass_threshold: int = Field(
        description="Org-wide default minimum pass rate (%) below which a check warns. "
        "Overridable per rule and per column. Clamped to [0, 100]."
    )
    pass_threshold_enabled: bool = Field(
        description="Master switch for the pass-threshold feature. When False, all threshold "
        "UI is hidden and breach evaluation is disabled server-side. Default True."
    )


class RulesRegistrySettingsIn(BaseModel):
    """Update payload — omitted fields are left unchanged."""

    auto_upgrade_without_approval: bool | None = None
    default_auto_upgrade: bool | None = None
    tag_auto_apply: bool | None = None
    default_pass_threshold: int | None = Field(default=None, ge=0, le=100)
    pass_threshold_enabled: bool | None = None


def _rules_registry_settings_out(svc: AppSettingsService) -> RulesRegistrySettingsOut:
    return RulesRegistrySettingsOut(
        auto_upgrade_without_approval=svc.get_auto_upgrade_without_approval(),
        default_auto_upgrade=svc.get_default_auto_upgrade(),
        tag_auto_apply=svc.get_tag_auto_apply(),
        default_pass_threshold=svc.get_default_pass_threshold(),
        pass_threshold_enabled=svc.get_pass_threshold_enabled(),
    )


@router.get(
    "/rules-registry-settings",
    response_model=RulesRegistrySettingsOut,
    operation_id="getRulesRegistrySettings",
)
def get_rules_registry_settings(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RulesRegistrySettingsOut:
    """Return the current Rules Registry governance settings.

    Available to any authenticated user — stewards benefit from seeing
    the effective governance policy even though only admins can change it.
    """
    return _rules_registry_settings_out(svc)


@router.put(
    "/rules-registry-settings",
    response_model=RulesRegistrySettingsOut,
    operation_id="saveRulesRegistrySettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_rules_registry_settings(
    body: RulesRegistrySettingsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> RulesRegistrySettingsOut:
    """Update one or more Rules Registry governance settings (admin only)."""
    if (
        body.auto_upgrade_without_approval is None
        and body.default_auto_upgrade is None
        and body.tag_auto_apply is None
        and body.default_pass_threshold is None
        and body.pass_threshold_enabled is None
    ):
        raise HTTPException(
            status_code=400,
            detail="At least one of auto_upgrade_without_approval, default_auto_upgrade, "
            "tag_auto_apply, default_pass_threshold, or pass_threshold_enabled must be provided.",
        )
    if body.auto_upgrade_without_approval is not None:
        svc.save_auto_upgrade_without_approval(body.auto_upgrade_without_approval, user_email=email)
    if body.default_auto_upgrade is not None:
        svc.save_default_auto_upgrade(body.default_auto_upgrade, user_email=email)
    if body.tag_auto_apply is not None:
        svc.save_tag_auto_apply(body.tag_auto_apply, user_email=email)
    if body.default_pass_threshold is not None:
        svc.save_default_pass_threshold(body.default_pass_threshold, user_email=email)
    if body.pass_threshold_enabled is not None:
        svc.save_pass_threshold_enabled(body.pass_threshold_enabled, user_email=email)
    logger.info("Saved Rules Registry governance settings (by=%s)", email)
    return _rules_registry_settings_out(svc)


# ----------------------------------------------------------------------
# Approvals mode (issue #94) — the app-wide submit→approve gate. A 3-value
# enum string: ``enabled`` (default), ``auto_bypass``, ``disabled`` (see
# ``backend.common.approvals.ApprovalMode``). Read at VIEWER+ (every submit/
# approve surface needs to know the effective mode to render the right button)
# and written ADMIN-only, matching the other governance settings above.
# ----------------------------------------------------------------------


class ApprovalsModeOut(BaseModel):
    """Effective approvals-workflow mode."""

    mode: str = Field(
        description="One of 'enabled' (authors submit, approvers approve), "
        "'auto_bypass' (submit auto-approves when the caller could approve it "
        "themselves), or 'disabled' (every submit auto-approves)."
    )


class ApprovalsModeIn(BaseModel):
    """Update payload for the approvals mode."""

    mode: str


@router.get(
    "/approvals-mode",
    response_model=ApprovalsModeOut,
    operation_id="getApprovalsMode",
)
def get_approvals_mode(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ApprovalsModeOut:
    """Return the current approvals mode (defaults to ``enabled`` when unset).

    Available to any authenticated user — every submit/approve surface reads it
    to decide whether to show "Submit for review" vs "Save & publish".
    """
    return ApprovalsModeOut(mode=svc.get_approvals_mode())


@router.put(
    "/approvals-mode",
    response_model=ApprovalsModeOut,
    operation_id="saveApprovalsMode",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_approvals_mode(
    body: ApprovalsModeIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> ApprovalsModeOut:
    """Update the approvals mode (admin only). 400 on an unrecognised value."""
    try:
        saved = svc.save_approvals_mode(body.mode, user_email=email)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    logger.info("Saved approvals mode = %s (by=%s)", saved, email)
    return ApprovalsModeOut(mode=saved)


# ----------------------------------------------------------------------
# Global Results tab gating (issue B2-20) — an admin toggle that enables
# the app-wide, all-tables Results surface (hidden by default). Read at
# VIEWER+ so every authenticated user's sidebar can decide whether to show
# the global Results nav item (and the homepage overall-score "?" icon),
# written ADMIN-only, matching the other governance settings above.
# ----------------------------------------------------------------------


class GlobalResultsSettingsOut(BaseModel):
    """Effective global-Results-tab gating settings."""

    global_results_enabled: bool = Field(
        description="Whether the app-wide, all-tables Results surface (nav item + homepage "
        "overall-score explainer) is enabled. Defaults to False (hidden)."
    )
    rules_results_tab_enabled: bool = Field(
        default=False,
        description="Whether the per-rule Results tab is shown inside the Rules Registry rule "
        "dialog. Distinct from global_results_enabled. Defaults to False (hidden).",
    )


class GlobalResultsSettingsIn(BaseModel):
    """Update payload for the global-Results-tab gating settings.

    Both fields are optional so a caller can flip just one toggle without
    having to echo the other's current value back.
    """

    global_results_enabled: bool | None = None
    rules_results_tab_enabled: bool | None = None


@router.get(
    "/global-results-settings",
    response_model=GlobalResultsSettingsOut,
    operation_id="getGlobalResultsSettings",
)
def get_global_results_settings(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> GlobalResultsSettingsOut:
    """Return whether the global Results tab is enabled (defaults to False when unset).

    Available to any authenticated user — the sidebar and homepage both read
    it to decide whether to surface the global Results nav item and the
    overall-score "?" explainer, and the rule dialog reads it to decide
    whether to surface the per-rule Results tab.
    """
    return GlobalResultsSettingsOut(
        global_results_enabled=svc.get_global_results_enabled(),
        rules_results_tab_enabled=svc.get_rules_results_tab_enabled(),
    )


@router.put(
    "/global-results-settings",
    response_model=GlobalResultsSettingsOut,
    operation_id="saveGlobalResultsSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_global_results_settings(
    body: GlobalResultsSettingsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> GlobalResultsSettingsOut:
    """Enable or disable the global Results tab and/or the per-rule Results tab (admin only).

    Each toggle is updated only when its field is present in the body, so a
    caller can flip one without echoing the other's current value.
    """
    if body.global_results_enabled is not None:
        saved_global = svc.save_global_results_enabled(body.global_results_enabled, user_email=email)
        logger.info("Saved global_results_enabled = %s (by=%s)", saved_global, email)
    else:
        saved_global = svc.get_global_results_enabled()
    if body.rules_results_tab_enabled is not None:
        saved_rules = svc.save_rules_results_tab_enabled(body.rules_results_tab_enabled, user_email=email)
        logger.info("Saved rules_results_tab_enabled = %s (by=%s)", saved_rules, email)
    else:
        saved_rules = svc.get_rules_results_tab_enabled()
    return GlobalResultsSettingsOut(
        global_results_enabled=saved_global,
        rules_results_tab_enabled=saved_rules,
    )


# ----------------------------------------------------------------------
# Require-draft-run-before-submit (issue B2-12) — a governance gate that, when
# on, refuses to submit a monitored table / table space (or a per-table
# applied rule) for review — and the approvals-mode auto-approve shortcut —
# until a draft run has been recorded for the target table(s). Read at VIEWER+
# so every submit surface can decide whether to disable its Submit button;
# written ADMIN-only, matching the other governance settings above.
# ----------------------------------------------------------------------


class RequireDraftRunSettingsOut(BaseModel):
    """Effective require-draft-run-before-submit gating setting."""

    require_draft_run_before_submit: bool = Field(
        description="Whether a draft run must exist for the target table(s) before a monitored "
        "table / table space / per-table rule can be submitted (or auto-approved) for review. "
        "Defaults to False (no draft-run requirement). Registry rules and cross-table SQL checks "
        "are table-agnostic and are never gated."
    )


class RequireDraftRunSettingsIn(BaseModel):
    """Update payload for the require-draft-run-before-submit gating setting."""

    require_draft_run_before_submit: bool


@router.get(
    "/require-draft-run",
    response_model=RequireDraftRunSettingsOut,
    operation_id="getRequireDraftRunSettings",
)
def get_require_draft_run_settings(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RequireDraftRunSettingsOut:
    """Return whether a draft run is required before submit (defaults to False when unset).

    Available to any authenticated user — the RR/MT/TS submit surfaces read it
    to decide whether to disable Submit until a draft run exists.
    """
    return RequireDraftRunSettingsOut(
        require_draft_run_before_submit=svc.get_require_draft_run_before_submit()
    )


@router.put(
    "/require-draft-run",
    response_model=RequireDraftRunSettingsOut,
    operation_id="saveRequireDraftRunSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_require_draft_run_settings(
    body: RequireDraftRunSettingsIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> RequireDraftRunSettingsOut:
    """Enable or disable the require-draft-run-before-submit gate (admin only)."""
    saved = svc.save_require_draft_run_before_submit(body.require_draft_run_before_submit, user_email=email)
    logger.info("Saved require_draft_run_before_submit = %s (by=%s)", saved, email)
    return RequireDraftRunSettingsOut(require_draft_run_before_submit=saved)


# ----------------------------------------------------------------------
# Vector Search auto-provisioning trigger — Rules Registry Phase 7F. A
# dedicated endpoint (rather than folding this into ``save_ai_settings``)
# so provisioning can be retried independently of a settings save, and so
# the settings route's synchronous, dependency-light signature stays
# unchanged for its existing tests. Always returns 204 — provisioning is
# async on the Databricks side and ``ensure_vector_store`` never raises;
# admins check actual readiness via the rule-mapping suggester's
# ``available``/``reason`` fields, not this call's response.
# ----------------------------------------------------------------------


@router.post(
    "/ensure-vector-store",
    operation_id="ensureVectorStore",
    status_code=204,
    dependencies=[require_role(UserRole.ADMIN)],
)
async def ensure_vector_store_route(
    provisioner: Annotated[VectorStoreProvisioner, Depends(get_vector_store_provisioner)],
) -> None:
    """Best-effort kick off Vector Search endpoint/index creation (admin-triggered).

    No-op when embedding/Vector Search settings aren't fully configured.
    Never raises — see :meth:`VectorStoreProvisioner.ensure_vector_store`.
    """
    await provisioner.ensure_vector_store()
