import pytest
from databricks.labs.dqx.runtime import main


def test_runtime_raises_key_error():
    with pytest.raises(KeyError, match=r'Workflow "invalid_workflow" not found.'):
        main("--workflow=invalid_workflow", "--config=config_path")


def test_runtime_no_config():
    with pytest.raises(KeyError, match='no --config specified'):
        main("--workflow=invalid_workflow")


def test_runtime_missing_config():
    with pytest.raises(FileNotFoundError, match='config_path'):
        main("--workflow=profiler", "--config=config_path")
