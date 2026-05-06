# databricks.labs.dqx.quality\_checker.quality\_checker\_workflow

## DataQualityWorkflow Objects[​](#dataqualityworkflow-objects "Direct link to DataQualityWorkflow Objects")

```python
class DataQualityWorkflow(Workflow)

```

### apply\_checks[​](#apply_checks "Direct link to apply_checks")

```python
@workflow_task
def apply_checks(ctx: WorkflowContext)

```

Apply data quality checks to the input data and save the results.

Logic:

* If location patterns are provided, only tables matching the patterns will be used, and the provided run config name will be used as a template for all fields except location. Additionally, exclude patterns can be specified to skip specific tables. Output and quarantine tables are excluded by default based on output\_table\_suffix and quarantine\_table\_suffix job parameters to avoid re-applying checks on them.
* If no location patterns are provided, but a run config name is given, only that run config will be used.
* If neither location patterns nor a run config name are provided, all run configs will be used.

**Arguments**:

* `ctx` - Runtime context.
