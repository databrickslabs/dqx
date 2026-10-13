"""Tests for the shared FQN helpers in ``metrics_utils``."""

import pytest

from databricks_labs_dqx_app.backend.metrics_utils import catalog_of


@pytest.mark.parametrize(
    ("fqn", "expected"),
    [
        ("main.sales.orders", "main"),
        ("`main`.sales.orders", "main"),
        ("`my.catalog`.schema.table", "my.catalog"),
        ("`we``ird`.s.t", "we`ird"),
        ("nodots", "nodots"),
        ("", ""),
    ],
)
def test_catalog_of(fqn, expected):
    assert catalog_of(fqn) == expected
