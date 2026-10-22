# databricks.labs.dqx.datacontract.library_metrics

ODCS *type: library* quality metrics mapped onto DQX rules.

ODCS defines five engine-agnostic quality metrics (rowCount, nullValues, missingValues, invalidValues, duplicateValues), each paired with one of eight threshold fields (mustBe, mustNotBe, mustBeGreaterThan, ...) and an optional unit (rows or percent). This module turns one such entry into DQX rule dicts.

The mapping is split into three parts so that each concern exists exactly once:

* *Threshold* owns the eight threshold fields: validation, the matching exact-fit *is_aggr\_*\* function (if any), and the SQL comparison that flags a violation.
* A *measure* owns how a metric's value is computed: as *is_aggr\_*\* arguments where possible, and as a scalar SQL expression for the *sql_query* fallback.
* A *LibraryMetric* handler per metric owns its arguments, its "bad row" predicate and the row-level core checks used for *mustBe: 0*. Both renderings of a predicate are built from the same inputs through the same literal escaper, so they cannot drift apart.

The five-metric surface is intentionally closed; see the Data Contract guide.

### sql_identifier[​](#sql_identifier "Direct link to sql_identifier")

```python
def sql_identifier(column_path: str) -> str

```

Back-quote each segment of a (possibly dotted) column path for interpolation into generated SQL.

Every segment is quoted, plain names included: *is_sql_query_safe* ignores back-quoted identifiers, so a property named after a blocked keyword (e.g. *delete*) is not mistaken for a SQL statement. Quoting uses *quote_column_name*, which doubles embedded backticks, so this never raises.

### sql_literal[​](#sql_literal "Direct link to sql_literal")

```python
def sql_literal(value: str | Number | bool) -> str

```

Render a contract-supplied scalar as a SQL literal.

Numbers stay unquoted so numeric columns compare numerically. Booleans become TRUE/FALSE (checked before numbers, since *bool* is a subclass of *int*). Strings are single-quoted and escaped with *escape_sql_string_literal*, which escapes backslashes and single quotes as *\&#x27;* (Spark does not honour a doubled *''*).

### in_list_literal[​](#in_list_literal "Direct link to in_list_literal")

```python
def in_list_literal(value: str | Number | bool) -> str | Number | bool

```

Render a value for the *allowed*/*forbidden* argument of *is_in_list*/*is_not_in_list*.

Those checks parse a bare string as a column expression, so strings go through *sql_literal*, the same escaper used for the aggregate SQL. Numbers and booleans pass through unchanged.

### pattern_problem[​](#pattern_problem "Direct link to pattern_problem")

```python
def pattern_problem(pattern: str) -> str | None

```

Return why a contract-supplied regex is rejected, or None if it can be used.

Spark evaluates the pattern as a Java regular expression. A single scan checks three things:

* ReDoS: a repeated group (one-or-more, zero-or-more or a range quantifier) that itself contains a variable quantifier or an alternation, at any nesting depth, plus a 200-character cap. Such patterns can backtrack catastrophically on adversarial input.
* Python-only syntax that Java rejects: *(?P...)* named groups, inline comment groups, conditional groups and range quantifiers with no lower bound.
* Syntax errors: Java-only constructs (Unicode classes, possessive quantifiers, atomic groups, mid-pattern inline flags) are rewritten to Python equivalents and the result is compiled with *re*, so validation does not depend on the Python version.

## Threshold Objects[​](#threshold-objects "Direct link to Threshold Objects")

```python
@dataclass(frozen=True)
class Threshold()

```

One ODCS threshold field and its validated value.

Range bounds are inclusive on both ends, as in SQL *BETWEEN*. The ODCS spec is inconsistent here: its operator table writes *mustBeBetween* as a closed interval, while a note calls it equivalent to *mustBeGreaterThan* plus *mustBeLessThan*.

### aggregate_function[​](#aggregate_function "Direct link to aggregate_function")

```python
@property
def aggregate_function() -> str | None

```

The exact-fit *is_aggr\_*\* function for this threshold, or None when sql_query is needed.

### violation_sql[​](#violation_sql "Direct link to violation_sql")

```python
def violation_sql(measure_sql: str) -> str

```

SQL boolean that is TRUE when *measure_sql* violates this threshold.

### resolve_thresholds[​](#resolve_thresholds "Direct link to resolve_thresholds")

```python
def resolve_thresholds(entry: DataQuality, location: str,
                       metric: str) -> list[Threshold]

```

Resolve every threshold field set on *entry*, one *Threshold* per field.

An entry may set several fields to express a range (e.g. *mustBeGreaterOrEqualTo* plus *mustBeLessOrEqualTo*); each is enforced by its own rule. An invalid field is warned about and skipped without dropping the valid ones. Returns an empty list (with a warning) when no field is set.

## Measure Objects[​](#measure-objects "Direct link to Measure Objects")

```python
class Measure(ABC)

```

How a metric's value is computed, rendered for *is_aggr\_*\* or for a *sql_query* fallback.

### aggregate_arguments[​](#aggregate_arguments "Direct link to aggregate_arguments")

```python
@abstractmethod
def aggregate_arguments(unit: str) -> dict[str, object] | None

```

*is_aggr\_*\* arguments (without *limit*) computing the value, or None if not expressible.

### condition_query[​](#condition_query "Direct link to condition_query")

```python
@abstractmethod
def condition_query(unit: str, threshold: Threshold) -> str

```

A single-row *sql_query* returning a *condition* column that is TRUE on violation.

## RowCountMeasure Objects[​](#rowcountmeasure-objects "Direct link to RowCountMeasure Objects")

```python
@dataclass(frozen=True)
class RowCountMeasure(Measure)

```

Total number of rows in the (optionally filtered) dataset.

## BadRowMeasure Objects[​](#badrowmeasure-objects "Direct link to BadRowMeasure Objects")

```python
@dataclass(frozen=True)
class BadRowMeasure(Measure)

```

Number (unit: rows) or percentage (unit: percent) of rows matching a "bad row" SQL predicate.

The predicate is folded into the aggregated column as a CASE WHEN indicator rather than passed as a row filter, so the denominator of a percentage is always the full row count and no star column is involved. Percentages are COALESCEd to 0 in the fallback query, since AVG over zero rows is NULL and a NULL condition would fail the whole run.

## DuplicateValuesMeasure Objects[​](#duplicatevaluesmeasure-objects "Direct link to DuplicateValuesMeasure Objects")

```python
@dataclass(frozen=True)
class DuplicateValuesMeasure(Measure)

```

Number of distinct values (or key combinations) that occur more than once.

For \[A, A, A, B, B, C] this is 2 (A and B recur), not 5 (the rows in those groups); unit: percent divides that count by the total row count. The ODCS spec text ("Counts duplicate values in a column") does not settle this; DQX follows the interpretation used by other ODCS-compliant tooling. Rows with a NULL in any key column are ignored, matching *is_unique*'s default.

The count needs a GROUP BY, which cannot be nested inside an *is_aggr\_*\* column, so every threshold uses sql_query. The query is a scalar subquery with no outer FROM: selecting from the input view would return one row per input row and fail sql_query's single-row requirement.

## MetricPlan Objects[​](#metricplan-objects "Direct link to MetricPlan Objects")

```python
@dataclass(frozen=True)
class MetricPlan()

```

What a metric handler resolved an entry to.

**Attributes**:

* `measure` - How the metric's value is computed for non-zero thresholds.
* `zero_checks` - Row-level core checks used instead of *measure* for *mustBe: 0*, as (name suffix, check dict) pairs. Empty when the metric has no row-level form.
* `metadata` - Extra *user_metadata* entries (e.g. the composite key columns).

## MetricTarget Objects[​](#metrictarget-objects "Direct link to MetricTarget Objects")

```python
@dataclass(frozen=True)
class MetricTarget()

```

Where a library entry sits in the contract.

### subject[​](#subject "Direct link to subject")

```python
@property
def subject() -> str

```

Rule name prefix: the property for property-level entries, otherwise the schema.

## LibraryMetric Objects[​](#librarymetric-objects "Direct link to LibraryMetric Objects")

```python
class LibraryMetric(ABC)

```

One ODCS library metric: its arguments, predicate and row-level form.

To support another metric, subclass this and register it in *LIBRARY_METRICS*; threshold and unit handling, naming and metadata are shared.

### plan[​](#plan "Direct link to plan")

```python
@abstractmethod
def plan(entry: DataQuality, target: MetricTarget) -> MetricPlan | None

```

Resolve *entry* for *target*, warning and returning None if its arguments are unusable.

## MissingValuesMetric Objects[​](#missingvaluesmetric-objects "Direct link to MissingValuesMetric Objects")

```python
class MissingValuesMetric(LibraryMetric)

```

NULL plus any contract-supplied sentinel values (arguments.missingValues, e.g. \["", "N/A"]).

NULL is always counted, whether or not the list contains a null entry, on every threshold.

## InvalidValuesMetric Objects[​](#invalidvaluesmetric-objects "Direct link to InvalidValuesMetric Objects")

```python
class InvalidValuesMetric(LibraryMetric)

```

Values outside an allowlist (arguments.validValues) and/or not matching arguments.pattern.

With both present a row is invalid if it fails either, counted once, against one threshold. NULLs are not invalid, matching *is_in_list*/*regex_match*.

## DuplicateValuesMetric Objects[​](#duplicatevaluesmetric-objects "Direct link to DuplicateValuesMetric Objects")

```python
class DuplicateValuesMetric(LibraryMetric)

```

Recurring values of a property, or of a composite key (schema-level, arguments.properties).

### is_library_entry[​](#is_library_entry "Direct link to is_library_entry")

```python
def is_library_entry(entry: DataQuality) -> bool

```

Whether a quality entry is an ODCS library metric.

ODCS allows *type* to be omitted when a metric is given, and every library example in the spec omits it. The deprecated *rule* key (ODCS 3.0.x) is accepted as a synonym for *metric*.

## LibraryMetricRulesBuilder Objects[​](#librarymetricrulesbuilder-objects "Direct link to LibraryMetricRulesBuilder Objects")

```python
class LibraryMetricRulesBuilder()

```

Builds DQX rule dicts from the ODCS library metric entries of one schema.

**Arguments**:

* `rule_factory` - Assembles each final rule from its check, name, source entry, property name and metric metadata. The contract generator supplies its own rule assembly here, so library rules get the same provenance, owner, row filter and *customProperties* handling as explicit rules.

### build_for_schema[​](#build_for_schema "Direct link to build_for_schema")

```python
def build_for_schema(schema_obj: SchemaObject,
                     schema_name: str,
                     taken_names: set[str] | None = None) -> list[dict]

```

Build rules for every library entry on the schema and its properties.

**Arguments**:

* `schema_obj` - The ODCS schema object.
* `schema_name` - The schema name, used in rule names and log messages.
* `taken_names` - Names of rules already generated for the contract. Library rule names are suffixed so they collide neither with these nor with each other; the set is updated with the names used.
