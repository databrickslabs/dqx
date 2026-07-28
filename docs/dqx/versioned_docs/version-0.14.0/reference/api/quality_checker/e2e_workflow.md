---
sidebar_label: e2e_workflow
title: databricks.labs.dqx.quality_checker.e2e_workflow
---

## EndToEndWorkflow Objects

```python
class EndToEndWorkflow(Workflow)
```

Unified workflow that orchestrates individual jobs such as profiler and quality checker.
Run Job tasks execute referenced jobs with their own settings.

#### prepare

```python
@workflow_task
def prepare(ctx: WorkflowContext)
```

Initialize end-to-end workflow and emit a log record for traceability.

**Arguments**:

- `ctx` _WorkflowContext_ - Runtime context.

#### run\_profiler

```python
@workflow_task(depends_on=[prepare], run_job_name="profiler")
def run_profiler(ctx: WorkflowContext)
```

Run the profiler to generate checks and summary statistics.

**Arguments**:

- `ctx` - Runtime context.

#### run\_quality\_checker

```python
@workflow_task(depends_on=[run_profiler], run_job_name="quality-checker")
def run_quality_checker(ctx: WorkflowContext)
```

Run the quality checker after the profiler has generated checks.

**Arguments**:

- `ctx` - Runtime context.

#### finalize

```python
@workflow_task(depends_on=[run_quality_checker])
def finalize(ctx: WorkflowContext)
```

Finalize end-to-end workflow and emit a log record for traceability.

**Arguments**:

- `ctx` _WorkflowContext_ - Runtime context.

