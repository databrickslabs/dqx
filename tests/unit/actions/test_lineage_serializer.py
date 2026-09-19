"""Unit tests for *CollectLineageAction* metadata round-trip.

Covers the wire-format contract required for the action to persist through YAML / JSON like every
other registered action, plus the *LineageSearchConfig* invariants.
"""

import pytest

from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.lineage import (
    CollectLineageAction,
    LineageActionConfig,
    LineageSearchConfig,
)
from databricks.labs.dqx.actions.serializer import ActionSerializer
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.errors import InvalidActionError


def _make_dq() -> DQAction:
    return DQAction(
        action=CollectLineageAction(output_config=OutputConfig(location="cat.sch.lin", mode="append")),
    )


def test_action_round_trip_via_model_dump() -> None:
    """CollectLineageAction round-trips through model_dump / DQAction(**dumped)."""
    original = _make_dq()
    dumped = original.model_dump(mode="json")
    restored = DQAction(**dumped)

    assert isinstance(restored.action, CollectLineageAction)
    assert restored.action.output_config.location == "cat.sch.lin"
    assert restored.action.output_config.mode == "append"


def test_metadata_dict_resolves_to_action() -> None:
    """A dict with type='collect_lineage' + nested output_config resolves via the registry."""
    metadata: dict = {
        "action": {
            "type": "collect_lineage",
            "output_config": {"location": "cat.sch.lin", "mode": "overwrite"},
            "config": {
                "upstream": {"enabled": True, "depth": 2, "lookback_days": 7, "max_nodes": 100},
                "downstream": {"enabled": False},
                "columns": {"enabled": True},
            },
        },
    }
    dq_action = ActionSerializer.from_dict(metadata)
    assert isinstance(dq_action.action, CollectLineageAction)
    action = dq_action.action
    assert action.output_config.location == "cat.sch.lin"
    assert action.output_config.mode == "overwrite"
    assert action.config.upstream.depth == 2
    assert action.config.downstream.enabled is False
    assert action.config.columns.enabled is True


def test_missing_output_config_raises_at_construction() -> None:
    """A metadata payload without output_config raises InvalidActionError."""
    metadata: dict = {"action": {"type": "collect_lineage"}}
    with pytest.raises(InvalidActionError):
        ActionSerializer.from_dict(metadata)


@pytest.mark.parametrize(
    "field,bad_value,message",
    [
        ("depth", -1, "depth"),
        ("lookback_days", 0, "lookback_days"),
        ("max_nodes", 0, "max_nodes"),
    ],
)
def test_lineage_search_config_field_validators(field: str, bad_value: int, message: str) -> None:
    """LineageSearchConfig rejects depth < 0, lookback_days < 1, max_nodes < 1."""
    kwargs: dict = {"depth": 1, "lookback_days": 1, "max_nodes": 1, field: bad_value}
    with pytest.raises(Exception) as exc:
        LineageSearchConfig(**kwargs)
    assert message in str(exc.value)


def test_lineage_action_config_defaults_are_isolated() -> None:
    """Each LineageActionConfig field defaults to a fresh instance (default_factory)."""
    cfg = LineageActionConfig()
    assert isinstance(cfg.upstream, LineageSearchConfig)
    assert isinstance(cfg.downstream, LineageSearchConfig)
    assert isinstance(cfg.columns, LineageSearchConfig)
    other = LineageActionConfig()
    assert cfg.upstream is not other.upstream
