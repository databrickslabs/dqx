from databricks_labs_dqx_app.backend.marketplace.models import (
    MarketplacePack,
    MarketplaceRule,
    VALID_DIMENSIONS,
    VALID_SEVERITIES,
)


def test_marketplace_rule_defaults():
    r = MarketplaceRule(
        name="Must not be null",
        description="Value is present.",
        dimension="Completeness",
        severity="High",
        check={"function": "is_not_null", "arguments": {"column": "{{column}}"}},
    )
    assert r.industries == []
    assert r.regions == []
    assert r.criticality == "error"


def test_marketplace_pack_parses_nested_rules():
    pack = MarketplacePack(
        id="standard-checks",
        title="Standard checks",
        icon="SquareCheck",
        description="Reusable baseline checks.",
        rules=[
            {
                "name": "Must not be null",
                "description": "Value is present.",
                "dimension": "Completeness",
                "severity": "High",
                "check": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            }
        ],
    )
    assert pack.rules[0].name == "Must not be null"


def test_dimension_and_severity_vocabularies():
    assert "Validity" in VALID_DIMENSIONS
    assert "Critical" in VALID_SEVERITIES
    assert len(VALID_DIMENSIONS) == 6
    assert len(VALID_SEVERITIES) == 4
