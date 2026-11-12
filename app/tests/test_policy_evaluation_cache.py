"""Tests for the in-process rule policy evaluation caches."""

from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.policy_models import RulePolicy
from databricks_labs_dqx_app.backend.registry_models import MonitoredTable, RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.services.monitored_table_registration import register_table
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.policy_evaluation_cache import (
    POLICY_GENERATION,
    OpenClaim,
    OpenEvaluationDebounce,
    PolicyCatalogCache,
    PolicyGeneration,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _rule(rule_id: str) -> RegistryRule:
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=1,
        definition=RuleDefinition.model_validate({"body": {"function": "is_not_null"}, "slots": []}),
    )


def test_debounce_claims_once_until_finished_then_skips_within_the_window() -> None:
    clock = _Clock()
    debounce = OpenEvaluationDebounce(PolicyGeneration(), ttl_seconds=300, clock=clock)

    claim, key = debounce.claim("u", "b1")
    assert claim is OpenClaim.RUN
    assert debounce.claim("u", "b1")[0] is OpenClaim.PENDING
    debounce.finish(key)
    assert debounce.claim("u", "b1")[0] is OpenClaim.SKIP
    clock.now = 301
    assert debounce.claim("u", "b1")[0] is OpenClaim.RUN


def test_debounce_key_includes_the_generation() -> None:
    generation = PolicyGeneration()
    debounce = OpenEvaluationDebounce(generation, clock=_Clock())
    _, key = debounce.claim("u", "b1")
    debounce.finish(key)

    generation.bump()
    assert debounce.claim("u", "b1")[0] is OpenClaim.RUN


def test_a_stuck_claim_is_retried_after_the_running_timeout() -> None:
    clock = _Clock()
    debounce = OpenEvaluationDebounce(PolicyGeneration(), running_timeout_seconds=120, clock=clock)
    debounce.claim("u", "b1")
    clock.now = 121
    assert debounce.claim("u", "b1")[0] is OpenClaim.RUN


def test_debounce_memory_is_bounded() -> None:
    debounce = OpenEvaluationDebounce(PolicyGeneration(), max_entries=3, clock=_Clock())
    keys = [debounce.claim("u", f"b{i}")[1] for i in range(5)]
    for key in keys:
        debounce.finish(key)

    assert debounce.claim("u", "b0")[0] is OpenClaim.RUN  # the oldest entries were evicted
    assert debounce.claim("u", "b4")[0] is OpenClaim.SKIP


def test_catalog_cache_serves_one_generation_and_reloads_after_a_bump_or_ttl() -> None:
    generation = PolicyGeneration()
    clock = _Clock()
    cache = PolicyCatalogCache(generation, ttl_seconds=60, clock=clock)
    loads: list[str] = []
    policy = RulePolicy(name="n", policy_id="p1", rule_id="r1")

    def load() -> list[RulePolicy]:
        loads.append("policies")
        return [policy]

    assert cache.policies(load) == [policy]
    assert cache.policies(load) == [policy]
    assert loads == ["policies"]
    generation.bump()
    cache.policies(load)
    clock.now = 61
    cache.policies(load)
    assert loads == ["policies"] * 3


def test_catalog_cache_reads_only_rules_it_has_not_seen() -> None:
    cache = PolicyCatalogCache(PolicyGeneration(), clock=_Clock())
    requested: list[list[str]] = []

    def load(ids: list[str]) -> dict[str, RegistryRule]:
        requested.append(list(ids))
        return {i: _rule(i) for i in ids if i != "gone"}

    assert set(cache.rules(["r1", "gone"], load)) == {"r1"}
    assert set(cache.rules(["r1", "r2", "gone"], load)) == {"r1", "r2"}
    assert requested == [["r1", "gone"], ["r2"]]


def test_registering_a_table_bumps_the_policy_generation() -> None:
    monitored = create_autospec(MonitoredTableService, instance=True)
    monitored.register.return_value = MonitoredTable(binding_id="b1", table_fqn="c.s.t", created_by="a@x")
    discovery = create_autospec(DiscoveryService, instance=True)
    discovery.get_table_owner.return_value = "owner@x"
    before = POLICY_GENERATION.current()

    register_table("c.s.t", "a@x", monitored_tables=monitored, discovery=discovery)

    assert POLICY_GENERATION.current() > before


def test_a_slow_load_from_an_older_generation_does_not_replace_the_current_entry() -> None:
    generation = PolicyGeneration()
    cache = PolicyCatalogCache(generation, clock=_Clock())
    old = RulePolicy(name="old", policy_id="p-old", rule_id="r1")
    new = RulePolicy(name="new", policy_id="p-new", rule_id="r1")
    loads: list[str] = []

    def slow_load() -> list[RulePolicy]:
        generation.bump()  # a policy is saved while this load runs
        assert cache.policies(lambda: loads.append("current") or [new]) == [new]
        return [old]

    assert cache.policies(slow_load) == [old]
    assert cache.policies(lambda: loads.append("again") or [new]) == [new]
    assert loads == ["current"]  # the current generation's entry survived the slow store


def test_releasing_a_claim_retries_after_a_short_back_off() -> None:
    clock = _Clock()
    debounce = OpenEvaluationDebounce(PolicyGeneration(), failure_backoff_seconds=30, clock=clock)
    _, key = debounce.claim("u", "b1")
    debounce.release(key)
    clock.now = 10
    assert debounce.claim("u", "b1")[0] is OpenClaim.SKIP
    clock.now = 31
    assert debounce.claim("u", "b1")[0] is OpenClaim.RUN
