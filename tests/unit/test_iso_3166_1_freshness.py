import pycountry

from databricks.labs.dqx.check_funcs import load_iso_codes


def test_iso_3166_1_codes_are_subset_of_pycountry():
    """Guards against the packaged ISO 3166-1 resource files drifting from the upstream standard data.

    Runs as part of the unit suite, which is also executed nightly (.github/workflows/nightly.yml),
    so a pycountry upgrade whose country data no longer contains a code packaged under
    src/databricks/labs/dqx/resources/iso_3166_1_{alpha_2,alpha_3,numeric}.txt is caught
    automatically rather than silently validating against a stale code list.

    The assertion is a subset (packaged ⊆ pycountry), not exact equality: pycountry is only a
    convenience source and the committed lists are reconciled against the official ISO 3166-1 list,
    which may intentionally exclude transitional/exceptional reservations that pycountry includes.
    A subset check catches stale/invalid packaged codes without red-building when the curated list is
    deliberately narrower than pycountry. It reads via the production loader (load_iso_codes) so the
    test covers what actually runs, rather than reimplementing the reader.
    """
    expected = {
        "iso_3166_1_alpha_2.txt": frozenset(country.alpha_2 for country in pycountry.countries),
        "iso_3166_1_alpha_3.txt": frozenset(country.alpha_3 for country in pycountry.countries),
        "iso_3166_1_numeric.txt": frozenset(country.numeric for country in pycountry.countries),
    }

    for resource_name, expected_codes in expected.items():
        stale = load_iso_codes(resource_name) - expected_codes
        assert not stale, f"Packaged codes in '{resource_name}' not present in pycountry (stale?): {sorted(stale)}"
