import pytest
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig


DEFAULT_RUN_CONFIG_NAME = "default"
DEFAULT_RUN_CONFIG = RunConfig(
    name=DEFAULT_RUN_CONFIG_NAME,
)

CONFIG = WorkspaceConfig(
    run_configs=[
        DEFAULT_RUN_CONFIG,
        RunConfig(
            name="another_run_config",
        ),
    ]
)


def test_get_run_config_with_name():
    assert CONFIG.get_run_config(DEFAULT_RUN_CONFIG_NAME) == DEFAULT_RUN_CONFIG


def test_get_run_config_when_name_not_provided():
    assert CONFIG.get_run_config(None) == DEFAULT_RUN_CONFIG


def test_get_run_config_when_name_not_found():
    assert CONFIG.get_run_config("not_found") == DEFAULT_RUN_CONFIG


def test_get_run_config_when_no_run_configs():
    config = WorkspaceConfig(run_configs=[])
    with pytest.raises(ValueError, match="No run configurations available"):
        config.get_run_config(None)
