import pycountry

from databricks.labs.dqx.check_funcs import load_iso_codes


def test_iso_4217_codes_match_pycountry():
    """Guards against the packaged ISO 4217 resource files drifting from the upstream standard data.

    Runs as part of the unit suite, which is also executed nightly (.github/workflows/nightly.yml),
    so a pycountry upgrade whose currency data no longer matches
    src/databricks/labs/dqx/resources/iso_4217_{alphabetic,numeric}.txt is caught automatically
    rather than silently validating against a stale code list.

    Exact equality (not a subset) so drift is caught in both directions: codes ISO *adds* that are
    missing from the packaged list (the check would falsely reject valid new codes) and codes the
    packaged list contains that pycountry no longer does (stale/invalid codes). The packaged lists are
    reconciled against the official ISO 4217 list before committing; if a future intentional
    divergence from pycountry is introduced, record it as an explicit allow-list here rather than
    loosening the assertion. The test reads via the production loader (load_iso_codes) so it covers
    what actually runs rather than reimplementing the reader.

    Currency attributes are read with getattr(..., None) and the None values filtered out, so a
    future pycountry release that renames an attribute or omits it for some entries surfaces as a
    readable code-set mismatch here rather than an AttributeError raised during collection.
    """
    expected = {
        "iso_4217_alphabetic.txt": frozenset(
            code for currency in pycountry.currencies if (code := getattr(currency, "alpha_3", None)) is not None
        ),
        "iso_4217_numeric.txt": frozenset(
            code for currency in pycountry.currencies if (code := getattr(currency, "numeric", None)) is not None
        ),
    }

    for resource_name, expected_codes in expected.items():
        packaged = load_iso_codes(resource_name)
        missing = expected_codes - packaged
        stale = packaged - expected_codes
        assert packaged == expected_codes, (
            f"Packaged codes in '{resource_name}' do not match pycountry. "
            f"Missing (in pycountry, not packaged): {sorted(missing)}. "
            f"Stale (packaged, not in pycountry): {sorted(stale)}."
        )
