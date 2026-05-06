# databricks.labs.dqx.profiler.dlt\_generator

## DQDltGenerator Objects[​](#dqdltgenerator-objects "Direct link to DQDltGenerator Objects")

```python
class DQDltGenerator(DQEngineBase)

```

### generate\_dlt\_rules[​](#generate_dlt_rules "Direct link to generate_dlt_rules")

```python
@telemetry_logger("generator", "generate_dlt_rules")
def generate_dlt_rules(rules: list[DQProfile],
                       action: str | None = None,
                       language: str = "SQL") -> list[str] | str | dict

```

Generates Lakeflow Pipelines (formerly Delta Live Table (DLT)) rules in the specified language.

**Arguments**:

* `rules` - A list of data quality profiles to generate rules for.
* `action` - The action to take on rule violation (e.g., "drop", "fail").
* `language` - The language to generate the rules in ("SQL", "Python" or "Python\_Dict").

**Returns**:

A list of strings representing the Lakeflow Pipelines rules in SQL, a string representing the Lakeflow Pipelines rules in Python, or dictionary with expressions.

**Raises**:

* `InvalidParameterError` - If the specified language is not supported.
