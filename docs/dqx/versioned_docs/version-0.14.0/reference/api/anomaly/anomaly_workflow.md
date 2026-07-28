---
sidebar_label: anomaly_workflow
title: databricks.labs.dqx.anomaly.anomaly_workflow
---

Workflow to train anomaly detection models on Databricks.

Requires the &#x27;anomaly&#x27; extras: pip install databricks-labs-dqx[anomaly]

## AnomalyTrainerWorkflow Objects

```python
class AnomalyTrainerWorkflow(Workflow)
```

Workflow wrapper for periodic anomaly model training.

#### train\_model

```python
@workflow_task
def train_model(ctx: WorkflowContext) -> None
```

Train anomaly detection model for the configured run.

