from databricks.labs.dqx.engine import DQEngine

from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.marketplace.models import VALID_DIMENSIONS, VALID_SEVERITIES


def _validator(checks):
    return DQEngine.validate_checks(checks)


def _all_rules():
    loader.clear_cache()
    packs = loader.load_packs(_validator)
    return packs, [r for p in packs for r in p.rules]


def test_seven_packs_present():
    packs, _ = _all_rules()
    ids = {p.id for p in packs}
    assert ids == {
        "pricing-and-money",
        "contacts-and-people",
        "addresses-and-geo",
        "dates-and-freshness",
        "standard-checks",
        "codes-and-classifications",
        "transactions-and-amounts",
    }


def test_total_rule_count_in_expected_range():
    _, rules = _all_rules()
    assert 55 <= len(rules) <= 62, f"expected ~59 rules, got {len(rules)}"


def test_every_rule_validates_and_has_valid_vocab():
    _, rules = _all_rules()
    for r in rules:
        status = DQEngine.validate_checks([r.check])
        assert not status.has_errors, f"{r.rule_key}: {status.to_string()}"
        assert r.dimension in VALID_DIMENSIONS, f"{r.rule_key}: bad dimension {r.dimension}"
        assert r.severity in VALID_SEVERITIES, f"{r.rule_key}: bad severity {r.severity}"


def test_rule_keys_unique():
    _, rules = _all_rules()
    keys = [r.rule_key for r in rules]
    assert len(keys) == len(set(keys)), "duplicate rule_key"


def test_name_and_description_conventions():
    _, rules = _all_rules()
    for r in rules:
        assert r.name and len(r.name) <= 80, f"{r.rule_key}: name empty or >80"
        assert r.name.split()[0].lower() not in {"a", "an", "the"}, f"{r.rule_key}: article"
        assert r.description.endswith("."), f"{r.rule_key}: description not one sentence"
        assert r.description.count(".") == 1, f"{r.rule_key}: >1 sentence"


def test_tag_values_are_from_taxonomy():
    _, rules = _all_rules()
    ok_ind = {"banking", "retail", "healthcare", "telco", "insurance", "logistics"}
    ok_reg = {"global", "us", "uk", "eu", "canada", "australia"}
    for r in rules:
        assert set(r.industries) <= ok_ind, f"{r.rule_key}: bad industry {r.industries}"
        assert set(r.regions) <= ok_reg, f"{r.rule_key}: bad region {r.regions}"
