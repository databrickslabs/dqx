"""Tests for the demo saved-views template loader."""

import pytest

from databricks_labs_dqx_app.backend.demo.saved_views import demo_saved_views
from databricks_labs_dqx_app.backend.saved_views_models import SavedViewPage


def test_bundled_template_builds_four_valid_views():
    specs = demo_saved_views("acme", "acme_demo")

    assert [(s.page, s.name) for s in specs] == [
        (SavedViewPage.MONITORED_TABLES, "Demo Tables Only"),
        (SavedViewPage.GLOBAL_RESULTS, "Demo Tables Only"),
        (SavedViewPage.REGISTRY_RULES, "Validity Rules by Severity"),
        (SavedViewPage.COLLECTIONS, "Collection Health"),
    ]
    assert specs[2].payload.values == {"groupBy": "severity", "dimension": "Validity"}


def test_tokens_are_replaced_only_in_values_and_in_a_single_pass():
    template = (
        '{"views": [{"page": "collections", "name": "N", "payload": {"values": {"{catalog}": "{catalog}.{schema}"}}}]}'
    )

    [spec] = demo_saved_views("cat_{schema}", "sch", template)

    assert spec.payload.values == {"{catalog}": "cat_{schema}.sch"}


@pytest.mark.parametrize(
    "template",
    [
        "not json",
        '{"views": {}}',
        '{"views": [{"page": "nope", "name": "N", "payload": {}}]}',
        '{"views": [{"page": "collections", "name": "", "payload": {}}]}',
        '{"views": [{"page": "collections", "name": "N", "payload": {"v": 2}}]}',
    ],
)
def test_malformed_template_raises_value_error(template: str):
    with pytest.raises(ValueError):
        demo_saved_views("acme", "acme_demo", template)
