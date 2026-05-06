# databricks.labs.dqx.anomaly.anomaly\_workflow

Workflow to train anomaly detection models on Databricks.

Requires the 'anomaly' extras: pip install databricks-labs-dqx\[anomaly]

## AnomalyTrainerWorkflow Objects[​](#anomalytrainerworkflow-objects "Direct link to AnomalyTrainerWorkflow Objects")

```python
class AnomalyTrainerWorkflow(Workflow)

```

Workflow wrapper for periodic anomaly model training.

### train\_model[​](#train_model "Direct link to train_model")

```python
@workflow_task
def train_model(ctx: WorkflowContext) -> None

```

Train anomaly detection model for the configured run.
