---
sidebar_label: profiler_workflow
title: databricks.labs.dqx.profiler.profiler_workflow
---

## ProfilerWorkflow Objects

```python
class ProfilerWorkflow(Workflow)
```

#### profile

```python
@workflow_task
def profile(ctx: WorkflowContext)
```

Profile input data and save the generated checks and profile summary stats.

Logic: Profile based on the provided run config name and location patterns as follows:
* If location patterns are provided, only tables matching the patterns will be profiled,
and the provided run config name will be used as a template for all fields except location.
Additionally, exclude patterns can be specified to skip profiling specific tables.
Output and quarantine tables are excluded by default based on output_table_suffix and quarantine_table_suffix
job parameters to avoid profiling them.
* If no location patterns are provided, but a run config name is given, only that run config will be profiled.
* If neither location patterns nor a run config name are provided, all run configs will be profiled.

**Arguments**:

- `ctx` - Runtime context.
  

**Raises**:

- `InvalidConfigError` - If no input data source is configured during installation.

