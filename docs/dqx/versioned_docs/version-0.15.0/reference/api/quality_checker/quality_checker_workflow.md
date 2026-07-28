---
sidebar_label: quality_checker_workflow
title: databricks.labs.dqx.quality_checker.quality_checker_workflow
---

## DataQualityWorkflow Objects

```python
class DataQualityWorkflow(Workflow)
```

#### apply\_checks

```python
@workflow_task
def apply_checks(ctx: WorkflowContext)
```

Apply data quality checks to the input data and save the results.

Logic:
* If location patterns are provided, only tables matching the patterns will be used,
and the provided run config name will be used as a template for all fields except location.
Additionally, exclude patterns can be specified to skip specific tables.
Output and quarantine tables are excluded by default based on output_table_suffix and quarantine_table_suffix
job parameters to avoid re-applying checks on them.
* If no location patterns are provided, but a run config name is given, only that run config will be used.
* If neither location patterns nor a run config name are provided, all run configs will be used.

**Arguments**:

- `ctx` - Runtime context.

