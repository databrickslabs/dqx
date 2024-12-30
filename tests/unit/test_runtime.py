import pytest
from databricks.labs.dqx.runtime import Workflows


def test_trigger_raises_key_error():
    workflows = Workflows.all()
    invalid_workflow_name = "--workflow=invalid_workflow"
    with pytest.raises(KeyError, match=r'Workflow "invalid_workflow" not found.'):
        workflows.trigger(invalid_workflow_name, "--config=config_path")
