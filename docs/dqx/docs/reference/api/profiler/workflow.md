---
sidebar_label: workflow
title: databricks.labs.dqx.profiler.workflow
---

## ProfilerWorkflow Objects

```python
class ProfilerWorkflow(Workflow)
```

#### profile

```python
@workflow_task
def profile(ctx: RuntimeContext)
```

Profile the input data and save the generated checks and profile summary stats.

**Arguments**:

- `ctx`: Runtime context.

