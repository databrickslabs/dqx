from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.demo.status import DemoStatus, DemoStatusStore, DEMO_STATUS_KEY


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ago_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def test_get_returns_idle_when_unset():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = None
    store = DemoStatusStore(settings)
    assert store.get().state == "idle"
    assert store.is_running() is False


def test_set_then_get_round_trips():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = DemoStatusStore(settings)
    store.set(DemoStatus(state="running", phase="datagen", message="building tables",
                         started_at=_now_iso(), updated_at=_now_iso()),
              user_email="admin@example.com")
    got = store.get()
    assert got.state == "running" and got.phase == "datagen"
    assert store.is_running() is True
    assert stored[DEMO_STATUS_KEY]  # JSON persisted under the key


def test_corrupt_blob_degrades_to_idle():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = "not-json{{"
    assert DemoStatusStore(settings).get().state == "idle"


def test_is_running_true_when_recently_updated():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = DemoStatusStore(settings)
    store.set(DemoStatus("running", "trend", "building", _now_iso(), _ago_iso(30)))
    assert store.is_running() is True


def test_is_running_false_when_running_status_is_stale():
    # A `running` status not advanced for >2h means the seed thread was killed
    # by an app restart without writing a terminal status — treat it as dead so
    # it can't wedge future deploys.
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = DemoStatusStore(settings)
    store.set(DemoStatus("running", "trend", "building", _ago_iso(3 * 60 * 60), _ago_iso(3 * 60 * 60)))
    assert store.is_running() is False


def test_is_running_false_when_updated_at_unparseable():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = DemoStatusStore(settings)
    store.set(DemoStatus("running", "trend", "building", "", "not-a-timestamp"))
    assert store.is_running() is False
