"""Unit tests for the settings-backed database-reset status store.

Mirrors ``test_demo_status.py``: the reset now runs on a background daemon
thread and the UI polls this store, so it must degrade gracefully (idle default
on a missing/corrupt blob) and treat a stale ``running`` status as dead so an
interrupted reset self-heals rather than wedging future resets with a 409.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.reset_status import (
    RESET_STATUS_KEY,
    ResetStatus,
    ResetStatusStore,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ago_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def test_get_returns_idle_when_unset():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = None
    store = ResetStatusStore(settings)
    assert store.get().state == "idle"
    assert store.is_running() is False


def test_set_then_get_round_trips_with_counts():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = ResetStatusStore(settings)
    store.set(
        ResetStatus(
            state="succeeded",
            message="all clear",
            started_at=_now_iso(),
            updated_at=_now_iso(),
            cleared_count=12,
            failed_count=1,
        ),
        user_email="admin@example.com",
    )
    got = store.get()
    assert got.state == "succeeded"
    assert got.cleared_count == 12
    assert got.failed_count == 1
    assert stored[RESET_STATUS_KEY]  # JSON persisted under the key


def test_corrupt_blob_degrades_to_idle():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = "not-json{{"
    assert ResetStatusStore(settings).get().state == "idle"


def test_is_running_true_when_recently_updated():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = ResetStatusStore(settings)
    store.set(ResetStatus("running", "clearing", _now_iso(), _ago_iso(5)))
    assert store.is_running() is True


def test_is_running_false_when_running_status_is_stale():
    # A `running` status not advanced within the stale window means the reset
    # thread was killed by an app restart without writing a terminal status —
    # treat it as dead so it can't wedge future resets.
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = ResetStatusStore(settings)
    store.set(ResetStatus("running", "clearing", _ago_iso(60 * 60), _ago_iso(60 * 60)))
    assert store.is_running() is False


def test_get_downgrades_stale_running_to_terminal():
    # The status endpoint returns whatever get() yields, so the stale-running
    # heal must live on the read path — not only inside is_running. A running
    # status left behind by an app restart is reported as a terminal 'failed' so
    # the Danger Zone spinner clears instead of wedging forever.
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = ResetStatusStore(settings)
    store.set(ResetStatus("running", "clearing", _ago_iso(60 * 60), _ago_iso(60 * 60)))
    got = store.get()
    assert got.state == "failed"
    assert got.message  # a human-readable explanation, not empty


def test_get_returns_running_unchanged_when_recent():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = ResetStatusStore(settings)
    store.set(ResetStatus("running", "clearing", _now_iso(), _ago_iso(3)))
    assert store.get().state == "running"


def test_is_running_false_when_updated_at_unparseable():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = ResetStatusStore(settings)
    store.set(ResetStatus("running", "clearing", "", "not-a-timestamp"))
    assert store.is_running() is False
