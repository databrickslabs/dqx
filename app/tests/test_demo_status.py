from unittest.mock import create_autospec
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.demo.status import DemoStatus, DemoStatusStore, DEMO_STATUS_KEY


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
                         started_at="2026-07-14 10:00:00", updated_at="2026-07-14 10:00:05"),
              user_email="admin@example.com")
    got = store.get()
    assert got.state == "running" and got.phase == "datagen"
    assert store.is_running() is True
    assert stored[DEMO_STATUS_KEY]  # JSON persisted under the key


def test_corrupt_blob_degrades_to_idle():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = "not-json{{"
    assert DemoStatusStore(settings).get().state == "idle"
