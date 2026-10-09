"""Tests for the pure branding validation helpers."""

import base64
import json

import pytest

from databricks_labs_dqx_app.backend.common.branding import (
    COLOR_GROUPS,
    MAX_LOGO_BYTES,
    PRESET_IDS,
    BrandingValidationError,
    decode_logo,
    default_branding,
    logo_hash,
    normalize_colors,
    normalize_hex,
    parse_stored_branding,
    sanitize_company_name,
    validate_logo_mode,
    validate_preset,
)

PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
JPEG_BYTES = b"\xff\xd8\xff\xe0" + b"\x00" * 32
WEBP_BYTES = b"RIFF\x24\x00\x00\x00WEBPVP8 " + b"\x00" * 32
SVG_BYTES = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


class TestConstants:
    def test_groups_and_presets_are_exact(self):
        assert COLOR_GROUPS == ("header", "page_background", "text", "brand", "sidebar")
        assert PRESET_IDS[0] == "dqx-default"
        assert PRESET_IDS == (
            "dqx-default",
            "databricks",
            "aubergine",
            "ocean",
            "dimmed",
            "high-contrast",
            "nord",
            "solarized",
            "dracula",
        )


class TestNormalizeHex:
    @pytest.mark.parametrize("raw,expected", [("#ff3621", "#FF3621"), ("#ABCDEF", "#ABCDEF")])
    def test_valid(self, raw, expected):
        assert normalize_hex(raw) == expected

    @pytest.mark.parametrize(
        "raw", ["ff3621", "#fff", "#GGGGGG", "red", "#FF3621;}", "#FF362100", None, 123, " #FF3621"]
    )
    def test_invalid(self, raw):
        with pytest.raises(BrandingValidationError):
            normalize_hex(raw)


class TestNormalizeColors:
    def test_valid_subset(self):
        assert normalize_colors({"header": "#3f0e40"}) == {"header": "#3F0E40"}

    def test_unknown_group_rejected(self):
        with pytest.raises(BrandingValidationError):
            normalize_colors({"border": "#000000"})

    def test_non_dict_rejected(self):
        with pytest.raises(BrandingValidationError):
            normalize_colors(["#000000"])


class TestCompanyName:
    def test_trims_and_strips_control_chars(self):
        assert sanitize_company_name("  Acme\u0000 Corp\n ") == "Acme Corp"

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_empty_is_none(self, raw):
        assert sanitize_company_name(raw) is None

    def test_too_long_rejected(self):
        with pytest.raises(BrandingValidationError):
            sanitize_company_name("x" * 61)

    def test_exactly_sixty_ok(self):
        assert sanitize_company_name("x" * 60) == "x" * 60


class TestPresetAndLogoMode:
    def test_known_preset(self):
        assert validate_preset("aubergine") == "aubergine"

    def test_none_preset(self):
        assert validate_preset(None) is None

    def test_unknown_preset(self):
        with pytest.raises(BrandingValidationError):
            validate_preset("neon")

    @pytest.mark.parametrize("mode", ["shared", "separate"])
    def test_logo_modes(self, mode):
        assert validate_logo_mode(mode) == mode

    def test_bad_logo_mode(self):
        with pytest.raises(BrandingValidationError):
            validate_logo_mode("both")


class TestDecodeLogo:
    @pytest.mark.parametrize(
        "raw,mime", [(PNG_BYTES, "image/png"), (JPEG_BYTES, "image/jpeg"), (WEBP_BYTES, "image/webp")]
    )
    def test_supported_types(self, raw, mime):
        assert decode_logo(mime, _b64(raw)) == (mime, raw)

    def test_svg_rejected_even_if_labelled_png(self):
        with pytest.raises(BrandingValidationError):
            decode_logo("image/png", _b64(SVG_BYTES))

    def test_svg_content_type_rejected(self):
        with pytest.raises(BrandingValidationError):
            decode_logo("image/svg+xml", _b64(SVG_BYTES))

    def test_mismatched_label_rejected(self):
        with pytest.raises(BrandingValidationError):
            decode_logo("image/jpeg", _b64(PNG_BYTES))

    def test_too_large_rejected(self):
        with pytest.raises(BrandingValidationError):
            decode_logo("image/png", _b64(PNG_BYTES + b"\x00" * MAX_LOGO_BYTES))

    def test_bad_base64_rejected(self):
        with pytest.raises(BrandingValidationError):
            decode_logo("image/png", "not base64 !!!")

    def test_logo_hash_is_stable_16_hex(self):
        assert logo_hash(PNG_BYTES) == logo_hash(PNG_BYTES)
        assert len(logo_hash(PNG_BYTES)) == 16


class TestParseStoredBranding:
    def test_none_gives_default(self):
        assert parse_stored_branding(None) == default_branding()

    def test_default_shape(self):
        assert default_branding() == {
            "version": 1,
            "company_name": None,
            "preset": None,
            "logo_mode": "shared",
            "light": {"colors": {}},
            "dark": {"customised": False, "colors": {}},
        }

    def test_corrupt_json_gives_default(self):
        assert parse_stored_branding("{not json") == default_branding()

    def test_drops_invalid_fields_keeps_valid(self):
        raw = json.dumps(
            {
                "company_name": "Acme",
                "preset": "neon",
                "logo_mode": 7,
                "light": {"colors": {"header": "#123456", "border": "#000000", "text": "red"}},
                "dark": {"customised": "yes", "colors": {"brand": "#ABCDEF"}},
            }
        )
        parsed = parse_stored_branding(raw)
        assert parsed["company_name"] == "Acme"
        assert parsed["preset"] is None
        assert parsed["logo_mode"] == "shared"
        assert parsed["light"] == {"colors": {"header": "#123456"}}
        assert parsed["dark"] == {"customised": False, "colors": {"brand": "#ABCDEF"}}


def test_decode_logo_rejects_huge_base64_before_decoding():
    with pytest.raises(BrandingValidationError, match="256 KB"):
        decode_logo("image/png", "A" * (MAX_LOGO_BYTES * 2))
