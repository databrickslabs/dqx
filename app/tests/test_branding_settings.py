"""Tests for branding persistence on AppSettingsService."""

import json

import pytest

from databricks_labs_dqx_app.backend.common.branding import BrandingValidationError, default_branding
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

PNG = b"\x89PNG\r\n\x1a\n" + b"\x01" * 16


@pytest.fixture
def store(sql_executor_mock) -> dict[str, str]:
    data: dict[str, str] = {}

    def _upsert(_table, *, key_cols, value_cols, **_kwargs):
        data[key_cols["setting_key"]] = value_cols["setting_value"]

    def _query(sql):
        for key, value in data.items():
            if f"'{key}'" in sql:
                return [(value,)]
        return []

    def _execute(sql, **_kwargs):
        for key in list(data):
            if sql.startswith("DELETE") and f"'{key}'" in sql:
                del data[key]

    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.upsert.side_effect = _upsert
    sql_executor_mock.query.side_effect = _query
    sql_executor_mock.execute.side_effect = _execute
    return data


@pytest.fixture
def svc(sql_executor_mock, store) -> AppSettingsService:
    return AppSettingsService(sql=sql_executor_mock)


class TestBrandingSettings:
    def test_unset_returns_default(self, svc):
        assert svc.get_branding() == default_branding()

    def test_company_name_round_trip(self, svc):
        out = svc.save_branding_company_name("  Acme ", user_email="a@x")
        assert out["company_name"] == "Acme"
        assert svc.get_branding()["company_name"] == "Acme"

    def test_empty_company_name_clears(self, svc):
        svc.save_branding_company_name("Acme")
        svc.save_branding_company_name("")
        assert svc.get_branding()["company_name"] is None

    def test_theme_round_trip_keeps_other_fields(self, svc):
        svc.save_branding_company_name("Acme")
        svc.save_branding_theme("aubergine", {"header": "#3f0e40"}, False, {})
        branding = svc.get_branding()
        assert branding["company_name"] == "Acme"
        assert branding["preset"] == "aubergine"
        assert branding["light"] == {"colors": {"header": "#3F0E40"}}
        assert branding["dark"] == {"customised": False, "colors": {}}

    def test_theme_rejects_bad_colour(self, svc):
        with pytest.raises(BrandingValidationError):
            svc.save_branding_theme(None, {"header": "red"}, False, {})

    def test_logo_mode_round_trip(self, svc):
        svc.save_branding_logo_mode("separate")
        assert svc.get_branding()["logo_mode"] == "separate"

    def test_corrupt_stored_value_is_tolerated(self, svc, store):
        store["branding_v1"] = json.dumps({"light": {"colors": {"header": "nope", "text": "#000000"}}})
        assert svc.get_branding()["light"] == {"colors": {"text": "#000000"}}

    def test_logo_round_trip_and_hashes(self, svc):
        assert svc.get_branding_logo_hashes() == {"light": None, "dark": None}
        saved = svc.save_branding_logo("light", "image/png", PNG)
        logo = svc.get_branding_logo("light")
        assert logo is not None and logo.data == PNG and logo.mime == "image/png"
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}

    def test_delete_logo(self, svc):
        svc.save_branding_logo("dark", "image/png", PNG)
        svc.delete_branding_logo("dark")
        assert svc.get_branding_logo("dark") is None
        assert svc.get_branding_logo_hashes() == {"light": None, "dark": None}

    def test_logo_hashes_read_from_branding_value_only(self, svc, sql_executor_mock):
        saved = svc.save_branding_logo("light", "image/png", PNG)
        sql_executor_mock.query.reset_mock()
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}
        assert sql_executor_mock.query.call_count == 1

    def test_logo_hash_survives_other_saves(self, svc):
        saved = svc.save_branding_logo("light", "image/png", PNG)
        svc.save_branding_company_name("Acme")
        svc.save_branding_theme("nord", {}, False, {})
        svc.save_branding_logo_mode("separate")
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}

    def test_unknown_slot_rejected(self, svc):
        with pytest.raises(BrandingValidationError):
            svc.get_branding_logo("sepia")

    def test_reset_clears_everything(self, svc):
        svc.save_branding_company_name("Acme")
        svc.save_branding_logo("light", "image/png", PNG)
        svc.reset_branding()
        assert svc.get_branding() == default_branding()
        assert svc.get_branding_logo("light") is None
        assert svc.get_branding_logo_hashes() == {"light": None, "dark": None}


class TestCustomPresets:
    def test_edited_theme_is_kept_as_custom_preset(self, svc):
        out = svc.save_branding_theme(None, {"brand": "#112233"}, False, {})
        assert out["preset"] == "custom-1"
        assert svc.get_branding()["custom_presets"] == [
            {"id": "custom-1", "light": {"colors": {"brand": "#112233"}}, "dark": {"customised": False, "colors": {}}}
        ]

    def test_same_colours_reuse_the_custom_preset(self, svc):
        svc.save_branding_theme(None, {"brand": "#112233"}, False, {})
        svc.save_branding_theme("nord", {"brand": "#5E81AC"}, False, {})
        out = svc.save_branding_theme(None, {"brand": "#112233"}, False, {})
        assert out["preset"] == "custom-1"
        assert len(svc.get_branding()["custom_presets"]) == 1

    def test_numbers_increase_and_are_not_reused(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        svc.save_branding_theme(None, {"brand": "#222222"}, False, {})
        svc.delete_branding_custom_preset("custom-1")
        out = svc.save_branding_theme(None, {"brand": "#333333"}, False, {})
        assert out["preset"] == "custom-3"

    def test_default_and_built_in_presets_are_not_copied(self, svc):
        svc.save_branding_theme(None, {}, False, {})
        svc.save_branding_theme("nord", {"brand": "#5E81AC"}, False, {})
        assert svc.get_branding()["custom_presets"] == []

    def test_selecting_a_custom_preset(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        out = svc.save_branding_theme("custom-1", {"brand": "#111111"}, False, {})
        assert out["preset"] == "custom-1"

    def test_unknown_custom_preset_rejected(self, svc):
        with pytest.raises(BrandingValidationError):
            svc.save_branding_theme("custom-9", {"brand": "#111111"}, False, {})

    def test_delete_keeps_current_colours(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        out = svc.delete_branding_custom_preset("custom-1")
        assert out["preset"] is None
        assert out["light"] == {"colors": {"brand": "#111111"}}
        assert out["custom_presets"] == []

    def test_delete_unknown_rejected(self, svc):
        with pytest.raises(BrandingValidationError):
            svc.delete_branding_custom_preset("custom-1")

    def test_limit(self, svc):
        for i in range(20):
            svc.save_branding_theme(None, {"brand": f"#0000{i:02X}"}, False, {})
        with pytest.raises(BrandingValidationError):
            svc.save_branding_theme(None, {"brand": "#FFFFFF"}, False, {})

    def test_reset_keeps_custom_presets(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        svc.reset_branding()
        branding = svc.get_branding()
        assert branding["preset"] is None and branding["light"] == {"colors": {}}
        assert [c["id"] for c in branding["custom_presets"]] == ["custom-1"]

    def test_corrupt_custom_presets_dropped(self, svc, store):
        store["branding_v1"] = json.dumps(
            {"preset": "custom-2", "custom_presets": [{"id": "evil"}, "x", {"id": "custom-2", "light": {"colors": {"brand": "red"}}}]}
        )
        branding = svc.get_branding()
        assert branding["preset"] == "custom-2"
        assert branding["custom_presets"] == [
            {"id": "custom-2", "light": {"colors": {}}, "dark": {"customised": False, "colors": {}}}
        ]
