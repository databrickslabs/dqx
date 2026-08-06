import pycountry

from tests.unit.iso_code_freshness import assert_packaged_codes_match


def test_iso_3166_2_codes_match_pycountry():
    """Guards against the packaged ISO 3166-2 resource file drifting from the upstream standard data.

    Runs as part of the unit suite, which is also executed nightly (.github/workflows/nightly.yml),
    so a pycountry upgrade whose subdivision data no longer matches
    src/databricks/labs/dqx/resources/iso_3166_2.txt is caught automatically rather than silently
    validating against a stale code list.

    Exact equality (not a subset) so drift is caught in both directions: codes ISO *adds* that are
    missing from the packaged list (the check would falsely reject valid new codes) and codes the
    packaged list contains that pycountry no longer does (stale/invalid codes). The packaged list is
    reconciled against the official ISO 3166-2 standard before committing; if a future intentional
    divergence from pycountry is introduced, record it as an explicit allow-list here rather than
    loosening the assertion.

    Subdivision attributes are read with getattr(..., None) and the None values filtered out, so a
    future pycountry release that renames an attribute or omits it for some entries surfaces as a
    readable code-set mismatch here rather than an AttributeError raised during collection.
    """
    expected = {
        "iso_3166_2.txt": frozenset(
            code for subdivision in pycountry.subdivisions if (code := getattr(subdivision, "code", None)) is not None
        ),
    }

    assert_packaged_codes_match(expected)
