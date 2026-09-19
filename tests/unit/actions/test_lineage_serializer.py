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
    """A dict with type='collect_lineage' + nested output_config resolves via the registry.

    *downstream=None* / *column_upstream=None* exercises the "disable a direction by setting
    its sub-config to null" contract that replaces the old per-config *enabled* flag; the
    top-level *failures_source* is also parsed off the metadata dict.
    """
    metadata: dict = {
        "action": {
            "type": "collect_lineage",
            "output_config": {"location": "cat.sch.lin", "mode": "overwrite"},
            "config": {
                "upstream": {"depth": 2, "lookback_days": 7, "max_nodes": 100},
                "downstream": None,
                "column_upstream": None,
                "column_downstream": {"depth": 1},
                "failures_source": "quarantine",
            },
        },
    }
    dq_action = ActionSerializer.from_dict(metadata)
    assert isinstance(dq_action.action, CollectLineageAction)
    action = dq_action.action
    assert action.output_config.location == "cat.sch.lin"
    assert action.output_config.mode == "overwrite"
    assert action.config.upstream is not None
    assert action.config.upstream.depth == 2
    assert action.config.upstream.max_nodes == 100
    assert action.config.downstream is None
    assert action.config.column_upstream is None
    assert isinstance(action.config.column_downstream, LineageSearchConfig)
    assert action.config.column_downstream.depth == 1
    assert action.config.failures_source == "quarantine"


def test_metadata_dict_disables_direction_via_null() -> None:
    """A null sub-config in metadata round-trips through the serializer as *None* on the model."""
    metadata: dict = {
        "action": {
            "type": "collect_lineage",
            "output_config": {"location": "cat.sch.lin"},
            "config": {
                "upstream": None,
                "downstream": None,
                "column_upstream": None,
                "column_downstream": None,
            },
        },
    }
    dq_action = ActionSerializer.from_dict(metadata)
    assert isinstance(dq_action.action, CollectLineageAction)
    assert dq_action.action.config.upstream is None
    assert dq_action.action.config.downstream is None
    assert dq_action.action.config.column_upstream is None
    assert dq_action.action.config.column_downstream is None
    # failures_source defaults to "both" when omitted.
    assert dq_action.action.config.failures_source == "both"


def test_missing_output_config_raises_at_construction() -> None:
    """A metadata payload without output_config raises InvalidActionError."""
    metadata: dict = {"action": {"type": "collect_lineage"}}
    with pytest.raises(InvalidActionError):
        ActionSerializer.from_dict(metadata)


@pytest.mark.parametrize(
    "field,bad_value,message",
    [
        ("depth", 0, "depth"),
        ("lookback_days", 0, "lookback_days"),
        ("max_nodes", 0, "max_nodes"),
    ],
)
def test_lineage_search_config_field_validators(field: str, bad_value: int, message: str) -> None:
    """LineageSearchConfig rejects sub-minimum depth, lookback_days, and max_nodes (each must be >= 1)."""
    kwargs: dict = {"depth": 1, "lookback_days": 1, "max_nodes": 1, field: bad_value}
    with pytest.raises(Exception) as exc:
        LineageSearchConfig(**kwargs)
    assert message in str(exc.value)


def test_lineage_action_config_defaults_are_isolated() -> None:
    """Each LineageActionConfig sub-config defaults to a fresh instance (default_factory);
    *failures_source* defaults to *"both"* and the error / warning column names default to
    DQX's built-in *_errors* / *_warnings*.
    """
    cfg = LineageActionConfig()
    assert isinstance(cfg.upstream, LineageSearchConfig)
    assert isinstance(cfg.downstream, LineageSearchConfig)
    assert isinstance(cfg.column_upstream, LineageSearchConfig)
    assert isinstance(cfg.column_downstream, LineageSearchConfig)
    assert cfg.failures_source == "both"
    assert cfg.errors_column == "_errors"
    assert cfg.warnings_column == "_warnings"
    other = LineageActionConfig()
    assert cfg.upstream is not other.upstream
    assert cfg.column_upstream is not other.column_upstream


def test_metadata_dict_overrides_errors_and_warnings_columns() -> None:
    """Custom *errors_column* / *warnings_column* names round-trip through the serializer."""
    metadata: dict = {
        "action": {
            "type": "collect_lineage",
            "output_config": {"location": "cat.sch.lin"},
            "config": {
                "errors_column": "dq_errors",
                "warnings_column": "dq_warnings",
            },
        },
    }
    dq_action = ActionSerializer.from_dict(metadata)
    assert isinstance(dq_action.action, CollectLineageAction)
    assert dq_action.action.config.errors_column == "dq_errors"
    assert dq_action.action.config.warnings_column == "dq_warnings"
