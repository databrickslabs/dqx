# databricks.labs.dqx.quality\_checker.e2e\_workflow

## EndToEndWorkflow Objects[​](#endtoendworkflow-objects "Direct link to EndToEndWorkflow Objects")

```python
class EndToEndWorkflow(Workflow)

```

Unified workflow that orchestrates individual jobs such as profiler and quality checker. Run Job tasks execute referenced jobs with their own settings.

### prepare[​](#prepare "Direct link to prepare")

```python
@workflow_task
def prepare(ctx: WorkflowContext)

```

Initialize end-to-end workflow and emit a log record for traceability.

**Arguments**:

* `ctx` *WorkflowContext* - Runtime context.

### run\_profiler[​](#run_profiler "Direct link to run_profiler")

```python
@workflow_task(depends_on=[prepare], run_job_name="profiler")
def run_profiler(ctx: WorkflowContext)

```

Run the profiler to generate checks and summary statistics.

**Arguments**:

* `ctx` - Runtime context.

### run\_quality\_checker[​](#run_quality_checker "Direct link to run_quality_checker")

```python
@workflow_task(depends_on=[run_profiler], run_job_name="quality-checker")
def run_quality_checker(ctx: WorkflowContext)

```

Run the quality checker after the profiler has generated checks.

**Arguments**:

* `ctx` - Runtime context.

### finalize[​](#finalize "Direct link to finalize")

```python
@workflow_task(depends_on=[run_quality_checker])
def finalize(ctx: WorkflowContext)

```

Finalize end-to-end workflow and emit a log record for traceability.

**Arguments**:

* `ctx` *WorkflowContext* - Runtime context.
