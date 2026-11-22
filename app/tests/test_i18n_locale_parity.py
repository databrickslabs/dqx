"""CI guard for i18n locale integrity. Runs under ``make app-test`` (pytest),
so no JS toolchain is needed.

Asserts, with ``en.json`` as the source of truth, that every locale:
  1. has the same key set (a key only in en falls back to English silently);
  2. has the same ``{{placeholder}}`` set per key (a dropped/extra placeholder
     makes i18next log ``missingInterpolation`` and renders inconsistently);
and that no locale uses a hard-coded ``{{*Plural}}`` placeholder (English
grammar baked into the translation layer — use native ``_one``/``_other``
keys with ``{{count}}`` instead). See app/AGENTS.md "Internationalization".

Plural keys are compared per group: every English ``_one``/``_other`` pair must
appear in each locale with that locale's CLDR plural categories (``_PLURAL_FORMS``;
e.g. Polish also needs ``_few``/``_many``, or i18next falls back to English for
those counts).
"""

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
# CLDR plural categories a locale needs for each English _one/_other group.
_DEFAULT_PLURAL_FORMS = ("one", "other")
_PLURAL_FORMS = {"pl": ("one", "few", "many", "other")}
_PLURAL_SUFFIX_RE = re.compile(r"_(zero|one|two|few|many|other)$")


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


def _plural_bases(keys: dict[str, str]) -> set[str]:
    return {key.removesuffix("_other") for key in keys if key.endswith("_other")}


def _expected_keys(source: dict[str, str], locale: str) -> set[str]:
    """English keys, with each plural group expanded to *locale*'s plural forms."""
    bases = _plural_bases(source)
    forms = _PLURAL_FORMS.get(locale, _DEFAULT_PLURAL_FORMS)
    plain = {key for key in source if _PLURAL_SUFFIX_RE.sub("", key) not in bases}
    return plain | {f"{base}_{form}" for base in bases for form in forms}


def _english_counterpart(key: str, source: dict[str, str]) -> str | None:
    """The English string a locale key translates; extra plural forms map to English _other."""
    if key in source:
        return source[key]
    if _PLURAL_SUFFIX_RE.search(key):
        return source.get(_PLURAL_SUFFIX_RE.sub("", key) + "_other")
    return None


@pytest.fixture(scope="module")
def source() -> dict[str, str]:
    return _load(SOURCE_LOCALE)


@pytest.mark.parametrize("locale", _target_locales())
def test_locale_key_set_matches_english(locale: str, source: dict[str, str]) -> None:
    target = _load(locale)
    expected = _expected_keys(source, locale)
    missing = sorted(expected - set(target))
    extra = sorted(set(target) - expected)
    assert (
        not missing and not extra
    ), f"{locale}.json key set diverges from {SOURCE_LOCALE}.json — missing={missing} extra={extra}"


@pytest.mark.parametrize("locale", _target_locales())
def test_locale_placeholders_match_english(locale: str, source: dict[str, str]) -> None:
    target = _load(locale)
    counterparts = {key: _english_counterpart(key, source) for key in target}
    mismatches = [
        f"{key}: en={sorted(_placeholders(en_val))} {locale}={sorted(_placeholders(value))}"
        for key, value in target.items()
        if (en_val := counterparts[key]) is not None and _placeholders(en_val) != _placeholders(value)
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
