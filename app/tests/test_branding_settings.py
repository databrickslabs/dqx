"""Tests for branding persistence on AppSettingsService."""

import json

import pytest

from databricks_labs_dqx_app.backend.common.branding import BrandingValidationError, default_branding
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

PNG = b"\x89PNG\r\n\x1a\n" + b"\x01" * 16
PNG2 = b"\x89PNG\r\n\x1a\n" + b"\x02" * 16


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
        svc.save_branding_logo("light", "image/png", PNG)
        svc.delete_branding_logo("light")
        assert svc.get_branding_logo("light") is None
        assert svc.get_branding_logo_hashes() == {"light": None, "dark": None}

    def test_first_logo_is_shared_whichever_slot(self, svc):
        saved = svc.save_branding_logo("dark", "image/png", PNG)
        branding = svc.get_branding()
        assert branding["logo_mode"] == "shared"
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}

    def test_later_light_upload_keeps_shared_logo_for_dark(self, svc):
        first = svc.save_branding_logo("light", "image/png", PNG)
        second = svc.save_branding_logo("light", "image/png", PNG2)
        assert svc.get_branding()["logo_mode"] == "separate"
        assert svc.get_branding_logo_hashes() == {"light": second.hash, "dark": first.hash}

    def test_later_dark_upload_only_changes_dark(self, svc):
        first = svc.save_branding_logo("light", "image/png", PNG)
        second = svc.save_branding_logo("dark", "image/png", PNG2)
        assert svc.get_branding()["logo_mode"] == "separate"
        assert svc.get_branding_logo_hashes() == {"light": first.hash, "dark": second.hash}

    def test_removing_all_logos_makes_next_upload_shared(self, svc):
        svc.save_branding_logo("light", "image/png", PNG)
        svc.save_branding_logo("dark", "image/png", PNG2)
        svc.delete_branding_logo("light")
        svc.delete_branding_logo("dark")
        assert svc.get_branding()["logo_mode"] == "shared"
        saved = svc.save_branding_logo("dark", "image/png", PNG)
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}

    def test_logo_hashes_read_from_branding_value_only(self, svc, sql_executor_mock):
        saved = svc.save_branding_logo("light", "image/png", PNG)
        sql_executor_mock.query.reset_mock()
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}
        assert sql_executor_mock.query.call_count == 1

    def test_logo_hash_survives_other_saves(self, svc):
        saved = svc.save_branding_logo("light", "image/png", PNG)
        svc.save_branding_company_name("Acme")
        svc.save_branding_theme("nord", {}, False, {})
        assert svc.get_branding_logo_hashes() == {"light": saved.hash, "dark": None}

    def test_unknown_slot_rejected(self, svc):
        with pytest.raises(BrandingValidationError):
            svc.get_branding_logo("sepia")

class TestCustomPresets:
    def test_edited_theme_is_kept_as_custom_preset(self, svc):
        out = svc.save_branding_theme(None, {"brand": "#112233"}, False, {})
        assert out["preset"] == "custom-1"
        assert svc.get_branding()["custom_presets"] == [
            {
                "id": "custom-1",
                "name": None,
                "light": {"colors": {"brand": "#112233"}},
                "dark": {"customised": False, "colors": {}},
            }
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

    def test_saving_a_built_in_preset_updates_its_edit(self, svc):
        svc.save_branding_theme("solarized", {"brand": "#111111"}, False, {})
        out = svc.save_branding_theme("solarized", {"brand": "#222222"}, True, {"text": "#EEEEEE"})
        assert out["preset"] == "solarized"
        assert out["custom_presets"] == []
        assert out["edited_presets"] == [
            {
                "id": "solarized",
                "light": {"colors": {"brand": "#222222"}},
                "dark": {"customised": True, "colors": {"text": "#EEEEEE"}},
            }
        ]
        svc.save_branding_theme("nord", {"brand": "#5E81AC"}, False, {})
        assert [e["id"] for e in svc.get_branding()["edited_presets"]] == ["solarized", "nord"]

    def test_invalid_stored_preset_edits_dropped(self, svc, store):
        store["branding_v1"] = json.dumps({"edited_presets": [{"id": "neon"}, {"id": "nord", "light": {"colors": {"brand": "#5E81AC"}}}]})
        assert svc.get_branding()["edited_presets"] == [
            {"id": "nord", "light": {"colors": {"brand": "#5E81AC"}}, "dark": {"customised": False, "colors": {}}}
        ]

    def test_selecting_a_custom_preset(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        out = svc.save_branding_theme("custom-1", {"brand": "#111111"}, False, {})
        assert out["preset"] == "custom-1"

    def test_saving_a_custom_preset_updates_it(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        out = svc.save_branding_theme("custom-1", {"brand": "#222222"}, True, {"text": "#EEEEEE"})
        assert out["preset"] == "custom-1"
        assert out["custom_presets"] == [
            {
                "id": "custom-1",
                "name": None,
                "light": {"colors": {"brand": "#222222"}},
                "dark": {"customised": True, "colors": {"text": "#EEEEEE"}},
            }
        ]

    def test_rename(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        out = svc.rename_branding_custom_preset("custom-1", "  Acme   night ")
        assert out["custom_presets"][0]["name"] == "Acme night"
        assert svc.get_branding()["custom_presets"][0]["name"] == "Acme night"
        assert svc.rename_branding_custom_preset("custom-1", "")["custom_presets"][0]["name"] is None

    def test_rename_rejects_long_names_and_unknown_presets(self, svc):
        svc.save_branding_theme(None, {"brand": "#111111"}, False, {})
        with pytest.raises(BrandingValidationError):
            svc.rename_branding_custom_preset("custom-1", "x" * 41)
        with pytest.raises(LookupError):
            svc.rename_branding_custom_preset("custom-9", "Night")

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
        with pytest.raises(LookupError):
            svc.delete_branding_custom_preset("custom-1")

    def test_limit(self, svc):
        for i in range(20):
            svc.save_branding_theme(None, {"brand": f"#0000{i:02X}"}, False, {})
        with pytest.raises(BrandingValidationError):
            svc.save_branding_theme(None, {"brand": "#FFFFFF"}, False, {})

    def test_corrupt_custom_presets_dropped(self, svc, store):
        store["branding_v1"] = json.dumps(
            {"preset": "custom-2", "custom_presets": [{"id": "evil"}, "x", {"id": "custom-2", "light": {"colors": {"brand": "red"}}}]}
        )
        branding = svc.get_branding()
        assert branding["preset"] == "custom-2"
        assert branding["custom_presets"] == [
            {"id": "custom-2", "name": None, "light": {"colors": {}}, "dark": {"customised": False, "colors": {}}}
        ]
