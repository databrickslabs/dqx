"""Live fixture admission requires CLI consent, never a loaded local profile."""

from pathlib import Path
from unittest.mock import create_autospec

import pytest

from tests import conftest


@pytest.mark.parametrize("opted_in", [False, True])
def test_live_collection_requires_cli_opt_in(monkeypatch: pytest.MonkeyPatch, opted_in: bool) -> None:
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "locally-loaded-profile")
    config = create_autospec(pytest.Config, instance=True)
    config.getoption.return_value = opted_in
    live_item = create_autospec(pytest.Item, instance=True)
    live_item.path = Path(__file__).parent / "integration" / "test_setup_resources.py"
    unit_item = create_autospec(pytest.Item, instance=True)
    unit_item.path = Path(__file__)

    conftest.pytest_collection_modifyitems(config, [live_item, unit_item])

    if opted_in:
        live_item.add_marker.assert_not_called()
    else:
        marker = live_item.add_marker.call_args.args[0]
        assert marker.name == "skip"
    unit_item.add_marker.assert_not_called()
    config.getoption.assert_called_once_with("studio_integration")
