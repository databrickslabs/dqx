import pycountry

from tests.unit.iso_code_freshness import assert_packaged_codes_match

# pycountry 26.2.16 lags the SIL ISO 639-3 registry (https://iso639-3.sil.org/, checked 2026-08-01) by
# these 10 codes: 7 valid codes SIL has assigned that pycountry doesn't yet carry, and 3 codes SIL has
# since retired that pycountry still carries. The packaged resource file was reconciled against SIL
# directly rather than against pycountry's stale snapshot; this allow-list keeps the freshness test
# honest about that one known divergence instead of silently passing or being loosened to a subset
# check. Remove an entry here once pycountry's own data catches up to SIL for it.
_SIL_ADDED_NOT_YET_IN_PYCOUNTRY = frozenset({"dyl", "lfb", "olb", "osd", "scz", "tvg", "zhk"})
_SIL_RETIRED_STILL_IN_PYCOUNTRY = frozenset({"yol", "mrd", "shl"})


def test_iso_639_codes_match_pycountry():
    """Guards against the packaged ISO 639 resource files drifting from the upstream standard data.

    Runs as part of the unit suite, which is also executed nightly (.github/workflows/nightly.yml),
    so a pycountry upgrade whose language data no longer matches
    src/databricks/labs/dqx/resources/iso_639_{1_alpha_2,3_alpha_3}.txt is caught automatically
    rather than silently validating against a stale code list.

    Exact equality (not a subset) so drift is caught in both directions: codes ISO *adds* that are
    missing from the packaged list (the check would falsely reject valid new codes) and codes the
    packaged list contains that pycountry no longer does (stale/invalid codes), modulo the one known
    pycountry-vs-SIL divergence recorded above. The packaged lists are reconciled against the official
    ISO 639-1/639-3 standards before committing; if a future intentional divergence from pycountry is
    introduced, record it as an explicit allow-list here rather than loosening the assertion.

    Language attributes are read with getattr(..., None) and the None values filtered out, so a
    future pycountry release that renames an attribute or omits it for some entries surfaces as a
    readable code-set mismatch here rather than an AttributeError raised during collection.
    """
    expected = {
        "iso_639_1_alpha_2.txt": frozenset(
            code for language in pycountry.languages if (code := getattr(language, "alpha_2", None)) is not None
        ),
        "iso_639_3_alpha_3.txt": (
            frozenset(
                code for language in pycountry.languages if (code := getattr(language, "alpha_3", None)) is not None
            )
            | _SIL_ADDED_NOT_YET_IN_PYCOUNTRY
        )
        - _SIL_RETIRED_STILL_IN_PYCOUNTRY,
    }

    assert_packaged_codes_match(expected)
