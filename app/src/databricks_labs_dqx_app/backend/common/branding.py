"""Pure validation and parsing for DQX Studio custom styling (company name, logo, theme).

Everything here is side-effect free so it can be unit-tested without a database and reused by
both the settings service (when reading stored values) and the routes (when validating input).
"""

import base64
import binascii
import hashlib
import json
import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

COLOR_GROUPS: tuple[str, ...] = ("header", "page_background", "text", "brand", "sidebar")
PRESET_IDS: tuple[str, ...] = (
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
LOGO_SLOTS: tuple[str, ...] = ("light", "dark")
LOGO_MODES: tuple[str, ...] = ("shared", "separate")
MAX_LOGO_BYTES = 262144
MAX_COMPANY_NAME_LENGTH = 60
MAX_LOGO_BASE64_LENGTH = (MAX_LOGO_BYTES * 4) // 3 + 4

_HEX_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")
_LOGO_HASH_RE = re.compile(r"^[0-9a-f]{16}$")
_SUPPORTED_MIME = ("image/png", "image/jpeg", "image/webp")


class BrandingValidationError(ValueError):
    """Raised when a branding value fails validation."""


def normalize_hex(value: object) -> str:
    """Return *value* as an uppercase #RRGGBB colour.

    Args:
        value: Candidate colour.

    Returns:
        The normalised colour.

    Raises:
        BrandingValidationError: If *value* is not a strict #RRGGBB string.
    """
    if not isinstance(value, str) or not _HEX_RE.fullmatch(value):
        raise BrandingValidationError("Colours must be hex values like #1A2B3C.")
    return value.upper()


def normalize_colors(value: object) -> dict[str, str]:
    """Validate a colour-group map.

    Args:
        value: Mapping of colour group to hex colour.

    Returns:
        The validated mapping with normalised colours.

    Raises:
        BrandingValidationError: On a non-mapping, an unknown group or an invalid colour.
    """
    if not isinstance(value, dict):
        raise BrandingValidationError("Colours must be an object of group to hex colour.")
    colors: dict[str, str] = {}
    for group, color in value.items():
        if group not in COLOR_GROUPS:
            raise BrandingValidationError(f"Unknown colour group: {str(group)[:40]}")
        colors[group] = normalize_hex(color)
    return colors


def sanitize_company_name(value: object) -> str | None:
    """Trim, strip control characters and length-check a company name.

    Args:
        value: Candidate company name.

    Returns:
        The cleaned name, or None when empty.

    Raises:
        BrandingValidationError: If the value is not a string or exceeds the length limit.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise BrandingValidationError("Company name must be text.")
    cleaned = "".join(ch for ch in value if unicodedata.category(ch)[0] != "C").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        return None
    if len(cleaned) > MAX_COMPANY_NAME_LENGTH:
        raise BrandingValidationError(f"Company name must be {MAX_COMPANY_NAME_LENGTH} characters or fewer.")
    return cleaned


def validate_preset(value: object) -> str | None:
    """Validate a preset id (None allowed)."""
    if value is None:
        return None
    if value not in PRESET_IDS:
        raise BrandingValidationError("Unknown preset.")
    return str(value)


def validate_logo_mode(value: object) -> str:
    """Validate the logo mode."""
    if value not in LOGO_MODES:
        raise BrandingValidationError("Logo mode must be 'shared' or 'separate'.")
    return str(value)


def _sniff_mime(raw: bytes) -> str | None:
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if raw.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    return None


def decode_logo(content_type: str, data_base64: str) -> tuple[str, bytes]:
    """Decode and validate an uploaded logo.

    The type is detected from the file's leading bytes and must match the declared
    *content_type*; SVG and any other format are rejected.

    Args:
        content_type: Declared MIME type.
        data_base64: Base64-encoded image bytes.

    Returns:
        The detected MIME type and the raw bytes.

    Raises:
        BrandingValidationError: On invalid base64, an unsupported or mismatched type,
            or an oversized file.
    """
    if content_type not in _SUPPORTED_MIME:
        raise BrandingValidationError("Logos must be PNG, JPEG or WebP images.")
    if len(data_base64) > MAX_LOGO_BASE64_LENGTH:
        raise BrandingValidationError("Logos must be 256 KB or smaller.")
    try:
        raw = base64.b64decode(data_base64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise BrandingValidationError("The logo could not be read.") from e
    if len(raw) > MAX_LOGO_BYTES:
        raise BrandingValidationError("Logos must be 256 KB or smaller.")
    detected = _sniff_mime(raw)
    if detected is None or detected != content_type:
        raise BrandingValidationError("Logos must be PNG, JPEG or WebP images.")
    return detected, raw


def logo_hash(raw: bytes) -> str:
    """Return a short, stable content hash used to version logo URLs."""
    return hashlib.sha256(raw).hexdigest()[:16]


def default_branding() -> dict[str, object]:
    """Return the DQX Default branding value (no customisation)."""
    return {
        "version": 1,
        "company_name": None,
        "preset": None,
        "logo_mode": "shared",
        "light": {"colors": {}},
        "dark": {"customised": False, "colors": {}},
        "logos": {slot: None for slot in LOGO_SLOTS},
    }


def _safe_colors(value: object, mode: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        logger.warning(f"Dropped invalid stored {mode} colours")
        return {}
    colors: dict[str, str] = {}
    dropped = 0
    for group, color in value.items():
        if group in COLOR_GROUPS and isinstance(color, str) and _HEX_RE.fullmatch(color):
            colors[group] = color.upper()
        else:
            dropped += 1
    if dropped:
        logger.warning(f"Dropped {dropped} invalid stored {mode} colour(s)")
    return colors


def _safe_logo_hashes(value: object) -> dict[str, str | None]:
    hashes: dict[str, str | None] = {slot: None for slot in LOGO_SLOTS}
    if not isinstance(value, dict):
        return hashes
    for slot in LOGO_SLOTS:
        digest = value.get(slot)
        if isinstance(digest, str) and _LOGO_HASH_RE.fullmatch(digest):
            hashes[slot] = digest
        elif digest is not None:
            logger.warning(f"Dropped an invalid stored {slot} logo hash")
    return hashes


def parse_stored_branding(raw: str | None) -> dict[str, object]:
    """Parse a stored branding value, dropping anything invalid.

    Never raises: a missing or corrupt value yields DQX Default, and individual invalid
    fields fall back to their defaults.

    Args:
        raw: The stored JSON string, or None.

    Returns:
        A branding value with the same shape as default_branding().
    """
    result = default_branding()
    if not raw:
        return result
    try:
        data = json.loads(raw)
    except ValueError:
        logger.warning("Stored branding is not valid JSON; using DQX Default")
        return result
    if not isinstance(data, dict):
        logger.warning("Stored branding is not an object; using DQX Default")
        return result
    try:
        result["company_name"] = sanitize_company_name(data.get("company_name"))
    except BrandingValidationError:
        logger.warning("Dropped an invalid stored company name")
    preset = data.get("preset")
    if preset is not None and preset not in PRESET_IDS:
        logger.warning("Dropped an invalid stored preset")
        preset = None
    result["preset"] = preset
    mode = data.get("logo_mode")
    if mode is not None and mode not in LOGO_MODES:
        logger.warning("Dropped an invalid stored logo mode")
    result["logo_mode"] = mode if mode in LOGO_MODES else "shared"
    light = data.get("light")
    result["light"] = {"colors": _safe_colors(light.get("colors") if isinstance(light, dict) else None, "light")}
    dark = data.get("dark")
    dark_dict = dark if isinstance(dark, dict) else {}
    customised = dark_dict.get("customised")
    result["dark"] = {
        "customised": customised if isinstance(customised, bool) else False,
        "colors": _safe_colors(dark_dict.get("colors"), "dark"),
    }
    result["logos"] = _safe_logo_hashes(data.get("logos"))
    return result
