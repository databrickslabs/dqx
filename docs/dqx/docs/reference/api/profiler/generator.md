---
sidebar_label: generator
title: databricks.labs.dqx.profiler.generator
---

## DQGenerator Objects

```python
class DQGenerator(DQEngineBase)
```

#### generate\_dq\_rules

```python
def generate_dq_rules(profiles: list[DQProfile] | None = None,
                      level: str = "error") -> list[dict]
```

Generates a list of data quality rules based on the provided dq profiles.

**Arguments**:

- `profiles`: A list of data quality profiles to generate rules for.
- `level`: The criticality level of the rules (default is &quot;error&quot;).

**Returns**:

A list of dictionaries representing the data quality rules.

#### dq\_generate\_is\_in

```python
@staticmethod
def dq_generate_is_in(column: str, level: str = "error", **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is in a specified list.

**Arguments**:

- `column`: The name of the column to check.
- `level`: The criticality level of the rule (default is &quot;error&quot;).
- `params`: Additional parameters, including the list of values to check against.

**Returns**:

A dictionary representing the data quality rule.

#### dq\_generate\_min\_max

```python
@staticmethod
def dq_generate_min_max(column: str, level: str = "error", **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is within a specified range.

**Arguments**:

- `column`: The name of the column to check.
- `level`: The criticality level of the rule (default is &quot;error&quot;).
- `params`: Additional parameters, including the minimum and maximum values.

**Returns**:

A dictionary representing the data quality rule, or None if no limits are provided.

#### dq\_generate\_is\_not\_null

```python
@staticmethod
def dq_generate_is_not_null(column: str, level: str = "error", **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is not null.

**Arguments**:

- `column`: The name of the column to check.
- `level`: The criticality level of the rule (default is &quot;error&quot;).
- `params`: Additional parameters.

**Returns**:

A dictionary representing the data quality rule.

#### dq\_generate\_is\_not\_null\_or\_empty

```python
@staticmethod
def dq_generate_is_not_null_or_empty(column: str,
                                     level: str = "error",
                                     **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is not null or empty.

**Arguments**:

- `column`: The name of the column to check.
- `level`: The criticality level of the rule (default is &quot;error&quot;).
- `params`: Additional parameters, including whether to trim strings.

**Returns**:

A dictionary representing the data quality rule.

