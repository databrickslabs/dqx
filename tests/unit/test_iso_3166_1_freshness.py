from importlib.resources import files

import pycountry


def _read_packaged_codes(resource_name: str) -> frozenset[str]:
    return frozenset((files("databricks.labs.dqx.resources") / resource_name).read_text(encoding="utf-8").split())


def test_iso_3166_1_codes_match_pycountry():
    """Guards against the packaged ISO 3166-1 resource files drifting from the upstream standard data.

    Runs as part of the unit suite, which is also executed nightly (.github/workflows/nightly.yml),
    so a pycountry upgrade whose country data no longer matches
    src/databricks/labs/dqx/resources/iso_3166_1_{alpha_2,alpha_3,numeric}.txt is caught automatically
    rather than silently validating against a stale code list.
    """
    expected_alpha_2 = frozenset(country.alpha_2 for country in pycountry.countries)
    expected_alpha_3 = frozenset(country.alpha_3 for country in pycountry.countries)
    expected_numeric = frozenset(country.numeric for country in pycountry.countries)

    assert _read_packaged_codes("iso_3166_1_alpha_2.txt") == expected_alpha_2
    assert _read_packaged_codes("iso_3166_1_alpha_3.txt") == expected_alpha_3
    assert _read_packaged_codes("iso_3166_1_numeric.txt") == expected_numeric
