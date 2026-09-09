import logging

from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.marketplace.models import (
    MarketplacePack,
    MarketplaceRule,
    VALID_DIMENSIONS,
    VALID_SEVERITIES,
)


def _real_validator(checks):
    from databricks.labs.dqx.engine import DQEngine

    return DQEngine.validate_checks(checks)


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


def test_slugify():
    assert loader.slugify("Valid credit card (Luhn)") == "valid-credit-card-luhn"


def test_normalize_check_shape():
    rule = MarketplaceRule(
        name="Must not be null",
        description="Value is present.",
        dimension="Completeness",
        severity="High",
        check={"function": "is_not_null", "arguments": {"column": "{{column}}"}},
    )
    out = loader.normalize_check(rule)
    assert out["criticality"] == "error"
    assert out["check"] == {"function": "is_not_null", "arguments": {"column": "{{column}}"}}
    assert out["user_metadata"] == {
        "name": "Must not be null",
        "description": "Value is present.",
        "dimension": "Completeness",
        "severity": "High",
    }


def test_load_packs_returns_sorted_nonempty():
    loader.clear_cache()
    packs = loader.load_packs(_real_validator)
    assert packs, "expected bundled packs to load"
    titles = [p.title for p in packs]
    assert titles == sorted(titles), "packs must be sorted A-Z by title"
    for p in packs:
        assert p.rules, f"{p.id}: empty pack"
        for r in p.rules:
            assert r.rule_key.startswith(f"{p.id}:")


def test_load_packs_skips_malformed_pack(monkeypatch, tmp_path, caplog):
    loader.clear_cache()
    # Point the loader at a temp dir with one good and one broken pack.
    good = tmp_path / "good.yaml"
    good.write_text(
        "id: good\ntitle: Good\nicon: SquareCheck\ndescription: ok\n"
        "rules:\n"
        "  - name: Must not be null\n"
        "    description: Value is present.\n"
        "    dimension: Completeness\n"
        "    severity: High\n"
        "    check:\n"
        "      function: is_not_null\n"
        "      arguments:\n"
        "        column: '{{column}}'\n"
    )
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "id: bad\ntitle: Bad\nicon: X\ndescription: broken\n"
        "rules:\n"
        "  - name: Broken\n"
        "    description: Bad function.\n"
        "    dimension: Validity\n"
        "    severity: Low\n"
        "    check:\n"
        "      function: not_a_real_function\n"
        "      arguments: {}\n"
    )
    monkeypatch.setattr(loader, "PACKS_DIR", tmp_path)
    with caplog.at_level(logging.WARNING):
        packs = loader.load_packs(_real_validator)
    ids = {p.id for p in packs}
    assert "good" in ids
    assert "bad" not in ids
    assert any("bad" in rec.message for rec in caplog.records)
    loader.clear_cache()
