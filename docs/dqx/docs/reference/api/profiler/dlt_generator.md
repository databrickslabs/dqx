---
sidebar_label: dlt_generator
title: databricks.labs.dqx.profiler.dlt_generator
---

## DQDltGenerator Objects

```python
class DQDltGenerator(DQEngineBase)
```

#### generate\_dlt\_rules

```python
def generate_dlt_rules(rules: list[DQProfile],
                       action: str | None = None,
                       language: str = "SQL") -> list[str] | str | dict
```

Generates Lakeflow Pipelines (formerly Delta Live Table (DLT)) rules in the specified language.

**Arguments**:

- `rules`: A list of data quality profiles to generate rules for.
- `action`: The action to take on rule violation (e.g., &quot;drop&quot;, &quot;fail&quot;).
- `language`: The language to generate the rules in (&quot;SQL&quot;, &quot;Python&quot; or &quot;Python_Dict&quot;).

**Raises**:

- `ValueError`: If the specified language is not supported.

**Returns**:

A list of strings representing the Lakeflow Pipelines rules in SQL, a string representing
the Lakeflow Pipelines rules in Python, or dictionary with expressions.

