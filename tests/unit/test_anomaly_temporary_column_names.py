"""Working aliases preserve source fields and remain distinct across allocations."""

from databricks.labs.dqx.anomaly.column_names import names_resolve_same, reserve_temporary_column


def test_transient_allocations_reserve_names_across_case_variants_and_successive_calls() -> None:
    source_names = ["__DQX_LOOKUP", "__dqx_lookup_0", "__DQX_LOOKUP_1", "untouched"]
    occupied = set(source_names)

    first = reserve_temporary_column("__dqx_lookup", occupied)
    second = reserve_temporary_column("__DQX_LOOKUP", occupied)
    unrelated = reserve_temporary_column("__dqx_other", occupied)

    assert first.startswith("__dqx_lookup_")
    assert second.startswith("__DQX_LOOKUP_")
    assert first.casefold() != second.casefold()
    assert not {first.casefold(), second.casefold()} & {name.casefold() for name in source_names}
    assert unrelated == "__dqx_other"
    assert occupied == {*source_names, first, second, unrelated}


def test_transient_allocation_detects_spark_unicode_case_equivalents() -> None:
    """Spark's case-insensitive resolver treats dotted I as the ASCII identifier letter."""
    source_name = "__dqx_baselİne_key"
    occupied = {source_name}

    allocated = reserve_temporary_column("__dqx_baseline_key", occupied)

    assert allocated.startswith("__dqx_baseline_key_")
    assert not names_resolve_same(allocated, source_name)
    assert occupied == {source_name, allocated}
