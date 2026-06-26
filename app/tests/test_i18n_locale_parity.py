"""CI guard for i18n locale integrity. Runs under ``make app-test`` (pytest),
so no JS toolchain is needed.

Asserts, with ``en.json`` as the source of truth, that every locale:
  1. has the same key set (a key only in en falls back to English silently);
  2. has the same ``{{placeholder}}`` set per key (a dropped/extra placeholder
     makes i18next log ``missingInterpolation`` and renders inconsistently);
and that no locale uses a hard-coded ``{{*Plural}}`` placeholder (English
grammar baked into the translation layer — use native ``_one``/``_other``
keys with ``{{count}}`` instead). See ui/CLAUDE.md "Internationalization".

Note: strict key-set equality assumes every locale uses the same plural
categories (``_one``/``_other``), which holds for en/es/it/pt-BR. A future
locale needing extra CLDR forms (e.g. Polish ``_few``/``_many``) would
require comparing plural base-groups rather than exact keys.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

LOCALES_DIR = (
    Path(__file__).resolve().parent.parent / "src" / "databricks_labs_dqx_app" / "ui" / "lib" / "i18n" / "locales"
)
SOURCE_LOCALE = "en"
# i18next placeholder forms: {{name}}, {{ name }}, {{- name}} (unescaped),
# {{name, formatter}}. We only need the variable name.
_PLACEHOLDER_RE = re.compile(r"\{\{-?\s*(\w+)")


def _flatten(obj: object, prefix: str = "") -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            out.update(_flatten(value, f"{prefix}{key}."))
    elif isinstance(obj, str):
        out[prefix.rstrip(".")] = obj
    return out


def _load(locale: str) -> dict[str, str]:
    return _flatten(json.loads((LOCALES_DIR / f"{locale}.json").read_text("utf-8")))


def _placeholders(value: str) -> set[str]:
    return set(_PLACEHOLDER_RE.findall(value))


def _all_locales() -> list[str]:
    return sorted(p.stem for p in LOCALES_DIR.glob("*.json"))


def _target_locales() -> list[str]:
    return [locale for locale in _all_locales() if locale != SOURCE_LOCALE]


@pytest.fixture(scope="module")
def source() -> dict[str, str]:
    return _load(SOURCE_LOCALE)


@pytest.mark.parametrize("locale", _target_locales())
def test_locale_key_set_matches_english(locale: str, source: dict[str, str]) -> None:
    target = _load(locale)
    missing = sorted(set(source) - set(target))
    extra = sorted(set(target) - set(source))
    assert (
        not missing and not extra
    ), f"{locale}.json key set diverges from {SOURCE_LOCALE}.json — missing={missing} extra={extra}"


@pytest.mark.parametrize("locale", _target_locales())
def test_locale_placeholders_match_english(locale: str, source: dict[str, str]) -> None:
    target = _load(locale)
    mismatches = [
        f"{key}: en={sorted(_placeholders(en_val))} {locale}={sorted(_placeholders(target[key]))}"
        for key, en_val in source.items()
        if key in target and _placeholders(en_val) != _placeholders(target[key])
    ]
    assert not mismatches, f"{locale}.json placeholder sets diverge:\n" + "\n".join(mismatches)


@pytest.mark.parametrize("locale", _all_locales())
def test_no_hardcoded_plural_placeholders(locale: str) -> None:
    offenders = [
        f"{key}: {{{{{ph}}}}}"
        for key, value in _load(locale).items()
        for ph in _placeholders(value)
        if ph.endswith("Plural")
    ]
    assert not offenders, (
        f"{locale}.json uses hard-coded plural placeholders "
        f"(use native _one/_other + {{{{count}}}} instead):\n" + "\n".join(offenders)
    )
