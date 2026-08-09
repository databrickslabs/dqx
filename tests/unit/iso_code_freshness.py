from databricks.labs.dqx.check_funcs import load_iso_codes


def assert_packaged_codes_match(expected: dict[str, frozenset[str]]) -> None:
    """Assert that each packaged ISO resource file exactly matches the expected code set.

    Shared by the ISO 3166-1, ISO 3166-2, ISO 4217, and ISO 639 freshness tests. Reads via the production loader
    (*load_iso_codes*) so it covers what actually runs rather than reimplementing the reader, and
    uses exact equality so drift is caught in both directions: codes the standard *adds* that are
    missing from the packaged list, and stale codes the packaged list still contains.

    Args:
        expected: mapping of packaged resource file name to the expected set of codes.
    """
    for resource_name, expected_codes in expected.items():
        packaged = load_iso_codes(resource_name)
        missing = expected_codes - packaged
        stale = packaged - expected_codes
        assert packaged == expected_codes, (
            f"Packaged codes in '{resource_name}' do not match the expected code set. "
            f"Missing (expected, not packaged): {sorted(missing)}. "
            f"Stale (packaged, not expected): {sorted(stale)}."
        )
