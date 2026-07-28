from importlib.resources import files

import pycountry


def _read_packaged_codes(resource_name: str) -> frozenset[str]:
    return frozenset((files("databricks.labs.dqx.resources") / resource_name).read_text(encoding="utf-8").split())


def test_iso_4217_codes_match_pycountry():
    """Guards against the packaged ISO 4217 resource files drifting from the upstream standard data.

    Fails when pycountry is upgraded to a release whose currency data no longer matches
    src/databricks/labs/dqx/resources/iso_4217_{alphabetic,numeric}.txt, so a maintainer knows to
    regenerate those files (see the regeneration steps in check_funcs.py) rather than silently
    validating against a stale code list.
    """
    expected_alphabetic = frozenset(currency.alpha_3 for currency in pycountry.currencies)
    expected_numeric = frozenset(currency.numeric for currency in pycountry.currencies)

    assert _read_packaged_codes("iso_4217_alphabetic.txt") == expected_alphabetic
    assert _read_packaged_codes("iso_4217_numeric.txt") == expected_numeric
