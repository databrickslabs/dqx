# databricks.labs.dqx.profiler.profile

## DQProfile Objects[​](#dqprofile-objects "Direct link to DQProfile Objects")

```python
@dataclass(frozen=True)
class DQProfile()

```

Data quality profile class representing a data quality rule candidate.

## DQProfileBuilder Objects[​](#dqprofilebuilder-objects "Direct link to DQProfileBuilder Objects")

```python
@dataclass(frozen=True)
class DQProfileBuilder()

```

Data quality profile builder class: a named builder that may produce a DQProfile for a column.

**Attributes**:

* `name` - Profile type identifier (e.g. "null\_or\_empty", "is\_in", "min\_max"). Used to look up the builder in the registry and in generated rule metadata.

* `builder` - Callable that inspects column data and options and returns a DQProfile when the column matches the profile criteria, otherwise None. Signature:

  (df, column\_name, column\_type, profiler\_metrics, profiler\_options) -> DQProfile | None

  * df: DataFrame for this column (non-null rows only; strings trimmed when profiler\_options\["trim\_strings"] is True). Used for distinct/min/max etc.
  * column\_name: Name of the column being profiled.
  * column\_type: Spark DataType of the column (e.g. StringType(), LongType()).
  * profiler\_metrics: Column-level statistics from the profiler (e.g. count, count\_null, empty\_count, count\_non\_null). Same key set as summary\_stats\[column\_name].
  * profiler\_options: Profiler options for this run (e.g. max\_null\_ratio, max\_empty\_ratio, max\_in\_count, trim\_strings, filter).
