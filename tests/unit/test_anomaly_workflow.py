"""Workflow contracts through injected I/O and training boundaries."""

from dataclasses import replace
from unittest.mock import Mock

import pytest

from databricks.labs.dqx.anomaly.anomaly_workflow import AnomalyTrainerWorkflow
from databricks.labs.dqx.config import AnomalyConfig, InputConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.errors import InvalidConfigError


@pytest.mark.parametrize(
    "anomaly_config,input_config,message",
    [
        (None, InputConfig(location="catalog.schema.input"), "anomaly_config is required"),
        (AnomalyConfig(), None, "input_config is required"),
        (AnomalyConfig(), InputConfig(location="catalog.schema.input"), "model_name is required"),
        (
            AnomalyConfig(model_name="catalog.schema.model"),
            InputConfig(location="catalog.schema.input"),
            "registry_table is required",
        ),
        (
            AnomalyConfig(model_name="catalog.schema.model", registry_table=""),
            InputConfig(location="catalog.schema.input"),
            "registry_table is required",
        ),
    ],
)
def test_invalid_workflow_configuration_fails_before_io(
    anomaly_workflow_context: WorkflowContext,
    anomaly_workflow_dependencies: tuple[Mock, Mock],
    anomaly_config: AnomalyConfig | None,
    input_config: InputConfig | None,
    message: str,
) -> None:
    reader, engine_factory = anomaly_workflow_dependencies
    anomaly_workflow_context.run_config = replace(
        anomaly_workflow_context.run_config, anomaly_config=anomaly_config, input_config=input_config
    )
    workflow = AnomalyTrainerWorkflow(input_reader=reader, engine_factory=engine_factory)
    with pytest.raises(InvalidConfigError, match=message):
        workflow.train_model(anomaly_workflow_context)
    reader.assert_not_called()
    engine_factory.assert_not_called()


@pytest.mark.parametrize(
    "profile,baseline_by,baseline_over_time",
    [(None, None, None), ("correlation", ["machine"], "reading_ts"), ("distribution", [], None)],
)
def test_workflow_preserves_training_configuration(
    anomaly_workflow_context: WorkflowContext,
    anomaly_workflow_dependencies: tuple[Mock, Mock],
    profile: str | None,
    baseline_by: list[str] | None,
    baseline_over_time: str | None,
) -> None:
    reader, engine_factory = anomaly_workflow_dependencies
    config = AnomalyConfig(
        model_name="catalog.schema.model",
        registry_table="catalog.schema.registry",
        columns=["amount"],
        profile=profile,
        baseline_by=baseline_by,
        baseline_over_time=baseline_over_time,
    )
    anomaly_workflow_context.run_config = replace(anomaly_workflow_context.run_config, anomaly_config=config)
    workflow = AnomalyTrainerWorkflow(input_reader=reader, engine_factory=engine_factory)
    workflow.train_model(anomaly_workflow_context)
    reader.assert_called_once_with(anomaly_workflow_context.spark, anomaly_workflow_context.run_config.input_config)
    engine_factory.assert_called_once_with(anomaly_workflow_context.workspace_client, anomaly_workflow_context.spark)
    engine_factory.return_value.train.assert_called_once_with(
        df=reader.return_value,
        columns=["amount"],
        baseline_by=baseline_by,
        profile=profile,
        baseline_over_time=baseline_over_time,
        model_name="catalog.schema.model",
        registry_table="catalog.schema.registry",
    )
    assert anomaly_workflow_context.run_config.anomaly_config == config


def test_failed_input_read_does_not_start_training(
    anomaly_workflow_context: WorkflowContext, anomaly_workflow_dependencies: tuple[Mock, Mock]
) -> None:
    reader, engine_factory = anomaly_workflow_dependencies
    reader.side_effect = InvalidConfigError("Input is unavailable")
    workflow = AnomalyTrainerWorkflow(input_reader=reader, engine_factory=engine_factory)
    with pytest.raises(InvalidConfigError, match="Input is unavailable"):
        workflow.train_model(anomaly_workflow_context)
    engine_factory.assert_not_called()
