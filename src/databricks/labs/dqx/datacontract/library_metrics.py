"""ODCS *type: library* quality metrics mapped onto DQX rules.

ODCS defines five engine-agnostic quality metrics (rowCount, nullValues, missingValues,
invalidValues, duplicateValues), each paired with one of eight threshold fields (mustBe,
mustNotBe, mustBeGreaterThan, ...) and an optional unit (rows or percent). This module turns one
such entry into DQX rule dicts.

The mapping is split into three parts so that each concern exists exactly once:

- *Threshold* owns the eight threshold fields: validation, the matching exact-fit *is_aggr_**
  function (if any), and the SQL comparison that flags a violation.
- A *measure* owns how a metric's value is computed: as *is_aggr_** arguments where possible, and
  as a scalar SQL expression for the *sql_query* fallback.
- A *LibraryMetric* handler per metric owns its arguments, its "bad row" predicate and the
  row-level core checks used for *mustBe: 0*. Both renderings of a predicate are built from the
  same inputs through the same literal escaper, so they cannot drift apart.

The five-metric surface is intentionally closed; see the Data Contract guide.
"""

import logging
import math
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from open_data_contract_standard.model import DataQuality, SchemaObject  # type: ignore

from databricks.labs.dqx.utils import is_sql_query_safe, sanitize_for_logging

logger = logging.getLogger(__name__)

# Threshold fields in resolution order: the first one set on an entry is used.
THRESHOLD_FIELDS: tuple[str, ...] = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterOrEqualTo",
    "mustBeLessOrEqualTo",
    "mustBeGreaterThan",
    "mustBeLessThan",
    "mustBeBetween",
    "mustNotBeBetween",
)

_RANGE_THRESHOLD_FIELDS = frozenset({"mustBeBetween", "mustNotBeBetween"})

# Exact-fit dataset-level aggregate checks. Strict inequalities and both range forms have no DQX
# aggregate equivalent and fall back to sql_query.
_AGGREGATE_FUNCTIONS: dict[str, str] = {
    "mustBe": "is_aggr_equal",
    "mustNotBe": "is_aggr_not_equal",
    "mustBeGreaterOrEqualTo": "is_aggr_not_less_than",
    "mustBeLessOrEqualTo": "is_aggr_not_greater_than",
}

# SQL comparison that is TRUE when the measured value violates a scalar threshold.
_VIOLATION_OPERATORS: dict[str, str] = {
    "mustBe": "<>",
    "mustNotBe": "=",
    "mustBeGreaterOrEqualTo": "<",
    "mustBeLessOrEqualTo": ">",
    "mustBeGreaterThan": "<=",
    "mustBeLessThan": ">=",
}

_UNITS: tuple[str, ...] = ("rows", "percent")
_SAFE_SQL_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_MAX_PATTERN_LENGTH = 200
_NESTED_QUANTIFIER_PATTERN = re.compile(r"\([^()]*[+*]\)[+*{]")
_ALTERNATION_QUANTIFIER_PATTERN = re.compile(r"\([^()]*\|[^()]*\)[+*{]")
_INPUT_VIEW = "{{ input_view }}"

Number = int | float


# ---------------------------------------------------------------------------------------------
# SQL rendering helpers (the single literal escaper and identifier quoter for library metrics)
# ---------------------------------------------------------------------------------------------


def sql_identifier(column_path: str) -> str:
    """Quote a (possibly dotted) column path for interpolation into generated SQL.

    Each segment that is not a bare identifier is backtick-quoted, with embedded backticks doubled.
    Every input is quoted rather than rejected, so this never raises.
    """
    return ".".join(
        segment if _SAFE_SQL_IDENTIFIER_PATTERN.match(segment) else f"`{segment.replace('`', '``')}`"
        for segment in column_path.split(".")
    )


def sql_literal(value: str | Number | bool) -> str:
    """Render a contract-supplied scalar as a SQL literal.

    Numbers stay unquoted so numeric columns compare numerically. Booleans become TRUE/FALSE (checked
    before numbers, since *bool* is a subclass of *int*). Strings are single-quoted with backslashes
    doubled first and then single quotes: with Spark's default
    *spark.sql.parser.escapedStringLiterals=false* an unescaped backslash would otherwise be consumed
    as an escape character.
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def in_list_literal(value: str | Number | bool) -> str | Number | bool:
    """Render a value for the *allowed*/*forbidden* argument of *is_in_list*/*is_not_in_list*.

    Those checks parse a bare string as a column expression, so strings go through *sql_literal*,
    the same escaper used for the aggregate SQL. Numbers and booleans pass through unchanged.
    """
    return sql_literal(value) if isinstance(value, str) else value


def _is_scalar(value: object) -> bool:
    return isinstance(value, (str, int, float, bool))


def _as_number(value: object) -> Number | None:
    """Normalize a threshold value to a finite number, or None if it is not one.

    *mustBe*/*mustNotBe* are untyped on the ODCS model, so a boolean, list or arbitrary string can
    reach this point. Booleans are rejected (*False == 0* in Python), numeric strings such as "0"
    are accepted.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return None
        value = int(parsed) if parsed.is_integer() else parsed
    if isinstance(value, (int, float)) and math.isfinite(value):
        return value
    return None


def _is_safe_pattern(pattern: str) -> bool:
    """Best-effort ReDoS guard for a contract-supplied regex (length cap, nested/alternation quantifiers)."""
    if len(pattern) > _MAX_PATTERN_LENGTH:
        return False
    if _NESTED_QUANTIFIER_PATTERN.search(pattern) or _ALTERNATION_QUANTIFIER_PATTERN.search(pattern):
        return False
    try:
        re.compile(pattern)
    except re.error:
        return False
    return True


# ---------------------------------------------------------------------------------------------
# Threshold
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Threshold:
    """One ODCS threshold field and its validated value.

    Range bounds are inclusive on both ends. The ODCS spec text does not settle this; DQX follows
    the interpretation used by other ODCS-compliant tooling.
    """

    field: str
    value: Number | None = None
    bounds: tuple[Number, Number] | None = None

    @property
    def is_zero_must_be(self) -> bool:
        return self.field == "mustBe" and self.value == 0

    @property
    def aggregate_function(self) -> str | None:
        """The exact-fit *is_aggr_** function for this threshold, or None when sql_query is needed."""
        return _AGGREGATE_FUNCTIONS.get(self.field)

    def violation_sql(self, measure_sql: str) -> str:
        """SQL boolean that is TRUE when *measure_sql* violates this threshold."""
        if self.bounds is not None:
            low, high = self.bounds
            within = f"{measure_sql} >= {low} AND {measure_sql} <= {high}"
            return f"({within})" if self.field == "mustNotBeBetween" else f"NOT ({within})"
        return f"{measure_sql} {_VIOLATION_OPERATORS[self.field]} {self.value}"


def resolve_threshold(entry: DataQuality, location: str, metric: str) -> Threshold | None:
    """Resolve the first threshold field set on *entry*, warning and returning None if it is missing or invalid."""
    for field_name in THRESHOLD_FIELDS:
        raw = getattr(entry, field_name)
        if raw is None:
            continue
        if field_name in _RANGE_THRESHOLD_FIELDS:
            bounds = _as_bounds(raw)
            if bounds is None:
                _warn_skip(
                    metric,
                    location,
                    f"'{field_name}' must be a list of two numbers, smallest first (got "
                    f"{sanitize_for_logging(repr(raw))})",
                )
                return None
            return Threshold(field_name, bounds=bounds)
        value = _as_number(raw)
        if value is None:
            _warn_skip(metric, location, f"'{field_name}' must be a number (got {sanitize_for_logging(repr(raw))})")
            return None
        return Threshold(field_name, value=value)

    _warn_skip(
        metric,
        location,
        f"has no recognized threshold field set ({', '.join(THRESHOLD_FIELDS)})",
    )
    return None


def _as_bounds(raw: object) -> tuple[Number, Number] | None:
    if not isinstance(raw, (list, tuple)) or len(raw) != 2:
        return None
    low, high = _as_number(raw[0]), _as_number(raw[1])
    if low is None or high is None or low > high:
        return None
    return low, high


# ---------------------------------------------------------------------------------------------
# Measures: how a metric's value is computed
# ---------------------------------------------------------------------------------------------


class Measure(ABC):
    """How a metric's value is computed, rendered for *is_aggr_** or for a *sql_query* fallback."""

    @abstractmethod
    def aggregate_arguments(self, unit: str) -> dict[str, Any] | None:
        """*is_aggr_** arguments (without *limit*) computing the value, or None if not expressible."""

    @abstractmethod
    def condition_query(self, unit: str, threshold: Threshold) -> str:
        """A single-row *sql_query* returning a *condition* column that is TRUE on violation."""


@dataclass(frozen=True)
class RowCountMeasure(Measure):
    """Total number of rows in the (optionally filtered) dataset."""

    def aggregate_arguments(self, unit: str) -> dict[str, Any] | None:
        return {"column": "*", "aggr_type": "count"}

    def condition_query(self, unit: str, threshold: Threshold) -> str:
        return f"SELECT {threshold.violation_sql('COUNT(*)')} AS condition FROM {_INPUT_VIEW}"


@dataclass(frozen=True)
class BadRowMeasure(Measure):
    """Number (unit: rows) or percentage (unit: percent) of rows matching a "bad row" SQL predicate.

    The predicate is folded into the aggregated column as a CASE WHEN indicator rather than passed as
    a row filter, so the denominator of a percentage is always the full row count and no star column
    is involved. Percentages are COALESCEd to 0 in the fallback query, since AVG over zero rows is
    NULL and a NULL condition would fail the whole run.
    """

    predicate_sql: str

    def aggregate_arguments(self, unit: str) -> dict[str, Any] | None:
        if unit == "percent":
            return {"column": f"CASE WHEN {self.predicate_sql} THEN 100.0 ELSE 0.0 END", "aggr_type": "avg"}
        return {"column": f"CASE WHEN {self.predicate_sql} THEN 1 END", "aggr_type": "count"}

    def condition_query(self, unit: str, threshold: Threshold) -> str:
        if unit == "percent":
            measure = f"COALESCE(AVG(CASE WHEN {self.predicate_sql} THEN 100.0 ELSE 0.0 END), 0.0)"
        else:
            measure = f"COUNT(CASE WHEN {self.predicate_sql} THEN 1 END)"
        return f"SELECT {threshold.violation_sql(measure)} AS condition FROM {_INPUT_VIEW}"


@dataclass(frozen=True)
class DuplicateValuesMeasure(Measure):
    """Number of distinct values (or key combinations) that occur more than once.

    For [A, A, A, B, B, C] this is 2 (A and B recur), not 5 (the rows in those groups); unit: percent
    divides that count by the total row count. The ODCS spec text ("Counts duplicate values in a
    column") does not settle this; DQX follows the interpretation used by other ODCS-compliant
    tooling. Rows with a NULL in any key column are ignored, matching *is_unique*'s default.

    The count needs a GROUP BY, which cannot be nested inside an *is_aggr_** column, so every
    threshold uses sql_query. The query is a scalar subquery with no outer FROM: selecting from the
    input view would return one row per input row and fail sql_query's single-row requirement.
    """

    key_columns: tuple[str, ...]

    def aggregate_arguments(self, unit: str) -> dict[str, Any] | None:
        return None

    def condition_query(self, unit: str, threshold: Threshold) -> str:
        quoted = [sql_identifier(column) for column in self.key_columns]
        not_null = " AND ".join(f"{column} IS NOT NULL" for column in quoted)
        duplicate_count = (
            f"(SELECT COUNT(*) FROM (SELECT 1 FROM {_INPUT_VIEW} WHERE {not_null} "
            f"GROUP BY {', '.join(quoted)} HAVING COUNT(*) > 1) AS dqx_dup_groups)"
        )
        if unit == "percent":
            measure = f"COALESCE(100.0 * {duplicate_count} / NULLIF((SELECT COUNT(*) FROM {_INPUT_VIEW}), 0), 0.0)"
        else:
            measure = duplicate_count
        return f"SELECT {threshold.violation_sql(measure)} AS condition"


# ---------------------------------------------------------------------------------------------
# Metric handlers
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class MetricPlan:
    """What a metric handler resolved an entry to.

    Attributes:
        measure: How the metric's value is computed for non-zero thresholds.
        zero_checks: Row-level core checks used instead of *measure* for *mustBe: 0*, as
            (name suffix, check dict) pairs. Empty when the metric has no row-level form.
        metadata: Extra *user_metadata* entries (e.g. the composite key columns).
    """

    measure: Measure
    zero_checks: tuple[tuple[str, dict[str, Any]], ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetricTarget:
    """Where a library entry sits in the contract."""

    schema_name: str
    property_name: str | None

    @property
    def subject(self) -> str:
        """Rule name prefix: the property for property-level entries, otherwise the schema."""
        return self.property_name or self.schema_name

    @property
    def location(self) -> str:
        schema = f"schema '{sanitize_for_logging(self.schema_name)}'"
        if self.property_name:
            return f"property '{sanitize_for_logging(self.property_name)}' in {schema}"
        return schema


class LibraryMetric(ABC):
    """One ODCS library metric: its arguments, predicate and row-level form.

    To support another metric, subclass this and register it in *LIBRARY_METRICS*; threshold and unit
    handling, naming and metadata are shared.
    """

    name: str
    default_dimension: str
    units: tuple[str, ...] = _UNITS
    allows_schema_level: bool = False
    allows_property_level: bool = True

    @abstractmethod
    def plan(self, entry: DataQuality, target: MetricTarget) -> MetricPlan | None:
        """Resolve *entry* for *target*, warning and returning None if its arguments are unusable."""


class RowCountMetric(LibraryMetric):
    name = "rowCount"
    default_dimension = "completeness"
    units = ("rows",)
    allows_schema_level = True
    allows_property_level = False

    def plan(self, entry: DataQuality, target: MetricTarget) -> MetricPlan | None:
        return MetricPlan(RowCountMeasure())


class NullValuesMetric(LibraryMetric):
    name = "nullValues"
    default_dimension = "completeness"

    def plan(self, entry: DataQuality, target: MetricTarget) -> MetricPlan | None:
        column = target.property_name or ""
        return MetricPlan(
            BadRowMeasure(f"{sql_identifier(column)} IS NULL"),
            zero_checks=(("", {"function": "is_not_null", "arguments": {"column": column}}),),
        )


class MissingValuesMetric(LibraryMetric):
    """NULL plus any contract-supplied sentinel values (arguments.missingValues, e.g. ["", "N/A"]).

    NULL is always counted, whether or not the list contains a null entry, on every threshold.
    """

    name = "missingValues"
    default_dimension = "completeness"

    def plan(self, entry: DataQuality, target: MetricTarget) -> MetricPlan | None:
        sentinels = _read_scalar_list(entry.arguments, "missingValues", target, self.name, allow_null=True)
        if sentinels is None:
            return None
        column = target.property_name or ""
        non_null = [value for value in sentinels if value is not None]

        quoted = sql_identifier(column)
        predicate = f"{quoted} IS NULL"
        zero_checks: list[tuple[str, dict[str, Any]]] = [
            ("null", {"function": "is_not_null", "arguments": {"column": column}})
        ]
        if non_null:
            predicate += f" OR {quoted} IN ({', '.join(sql_literal(value) for value in non_null)})"
            # is_not_in_list cannot catch a real NULL (NULL IN (...) is NULL), hence the separate
            # is_not_null check above; DQX unions both rules' results per row.
            zero_checks.append(
                (
                    "sentinel",
                    {
                        "function": "is_not_in_list",
                        "arguments": {
                            "column": column,
                            "forbidden": [in_list_literal(value) for value in non_null],
                            "case_sensitive": True,
                        },
                    },
                )
            )
        return MetricPlan(BadRowMeasure(predicate), zero_checks=tuple(zero_checks))


class InvalidValuesMetric(LibraryMetric):
    """Values outside an allowlist (arguments.validValues) and/or not matching arguments.pattern.

    With both present a row is invalid if it fails either, counted once, against one threshold.
    NULLs are not invalid, matching *is_in_list*/*regex_match*.
    """

    name = "invalidValues"
    default_dimension = "conformity"

    def plan(self, entry: DataQuality, target: MetricTarget) -> MetricPlan | None:
        arguments = entry.arguments if isinstance(entry.arguments, dict) else {}
        valid_values = None
        if "validValues" in arguments:
            valid_values = _read_scalar_list(arguments, "validValues", target, self.name, allow_null=False)
        pattern = self._read_pattern(arguments, target)
        if valid_values is None and pattern is None:
            _warn_skip(
                self.name,
                target.location,
                "has neither a usable 'arguments.validValues' list nor a usable 'arguments.pattern' string",
            )
            return None

        column = target.property_name or ""
        quoted = sql_identifier(column)
        clauses: list[str] = []
        zero_checks: list[tuple[str, dict[str, Any]]] = []
        if valid_values is not None:
            clauses.append(f"{quoted} NOT IN ({', '.join(sql_literal(value) for value in valid_values)})")
            zero_checks.append(
                (
                    "allowed",
                    {
                        "function": "is_in_list",
                        "arguments": {
                            "column": column,
                            "allowed": [in_list_literal(value) for value in valid_values],
                            "case_sensitive": True,
                        },
                    },
                )
            )
        if pattern is not None:
            clauses.append(f"NOT ({quoted} RLIKE {sql_literal(pattern)})")
            zero_checks.append(
                ("pattern", {"function": "regex_match", "arguments": {"column": column, "regex": pattern}})
            )
        return MetricPlan(BadRowMeasure(" OR ".join(clauses)), zero_checks=tuple(zero_checks))

    def _read_pattern(self, arguments: dict[str, Any], target: MetricTarget) -> str | None:
        if "pattern" not in arguments:
            return None
        pattern = arguments["pattern"]
        if not isinstance(pattern, str) or not pattern:
            logger.warning(
                f"'arguments.pattern' for type: library {self.name} entry on {target.location} must be a "
                "non-empty string; ignoring it."
            )
            return None
        if not _is_safe_pattern(pattern):
            logger.warning(
                f"'arguments.pattern' on {target.location} failed the ReDoS safety guard (200-character cap, "
                "nested-quantifier check, alternation-quantifier check, or re.compile); ignoring it."
            )
            return None
        return pattern


class DuplicateValuesMetric(LibraryMetric):
    """Recurring values of a property, or of a composite key (schema-level, arguments.properties)."""

    name = "duplicateValues"
    default_dimension = "uniqueness"
    allows_schema_level = True

    def plan(self, entry: DataQuality, target: MetricTarget) -> MetricPlan | None:
        if target.property_name is not None:
            key_columns = [target.property_name]
            metadata: dict[str, Any] = {}
        else:
            properties = entry.arguments.get("properties") if isinstance(entry.arguments, dict) else None
            if (
                not isinstance(properties, list)
                or not properties
                or not all(isinstance(p, str) and p for p in properties)
            ):
                _warn_skip(
                    self.name,
                    target.location,
                    "'arguments.properties' must be a non-empty list of property name strings",
                )
                return None
            key_columns = properties
            metadata = {"fields": key_columns}
        return MetricPlan(
            DuplicateValuesMeasure(tuple(key_columns)),
            zero_checks=(("", {"function": "is_unique", "arguments": {"columns": key_columns}}),),
            metadata=metadata,
        )


LIBRARY_METRICS: dict[str, LibraryMetric] = {
    metric.name: metric
    for metric in (
        RowCountMetric(),
        NullValuesMetric(),
        MissingValuesMetric(),
        InvalidValuesMetric(),
        DuplicateValuesMetric(),
    )
}


def _read_scalar_list(
    arguments: dict[str, Any] | None, key: str, target: MetricTarget, metric: str, *, allow_null: bool
) -> list | None:
    """Read a non-empty list of scalars from *arguments[key]*, warning and returning None otherwise."""
    value = arguments.get(key) if isinstance(arguments, dict) else None
    valid = (
        isinstance(value, list)
        and len(value) > 0
        and all(_is_scalar(item) or (allow_null and item is None) for item in value)
    )
    if not valid:
        _warn_skip(metric, target.location, f"'arguments.{key}' must be a non-empty list of scalar values")
        return None
    return value


def _warn_skip(metric: str, location: str, reason: str) -> None:
    logger.warning(f"type: library {metric} entry on {location} {reason}; skipping this quality check.")


# ---------------------------------------------------------------------------------------------
# Rule building
# ---------------------------------------------------------------------------------------------


def is_library_entry(entry: DataQuality) -> bool:
    """Whether a quality entry is an ODCS library metric.

    ODCS allows *type* to be omitted when a metric is given, and every library example in the spec
    omits it. The deprecated *rule* key (ODCS 3.0.x) is accepted as a synonym for *metric*.
    """
    if entry.type == "library":
        return True
    return entry.type is None and (entry.metric is not None or entry.rule is not None)


class LibraryMetricRulesBuilder:
    """Builds DQX rule dicts from the ODCS library metric entries of one schema.

    Args:
        provenance: Contract provenance merged into every rule's *user_metadata* (contract id and
            version, ODCS version, schema name).
        criticality: Criticality given to every generated rule.
        row_filter_resolver: Returns the row filter an entry declares (e.g. via a *row_filter*
            custom property), or None. The filter scopes the check to matching rows.
    """

    def __init__(
        self,
        provenance: dict[str, str],
        criticality: str,
        row_filter_resolver: Callable[[DataQuality], str | None],
    ):
        self._provenance = provenance
        self._criticality = criticality
        self._row_filter_resolver = row_filter_resolver

    def build_for_schema(self, schema_obj: SchemaObject, schema_name: str) -> list[dict]:
        """Build rules for every library entry on the schema and its properties."""
        rules: list[dict] = []
        for prop in schema_obj.properties or []:
            for entry in prop.quality or []:
                rules.extend(self._build_safely(entry, MetricTarget(schema_name, prop.name)))
        for entry in schema_obj.quality or []:
            rules.extend(self._build_safely(entry, MetricTarget(schema_name, None)))
        return _with_unique_names(rules)

    def _build_safely(self, entry: DataQuality, target: MetricTarget) -> list[dict]:
        if not is_library_entry(entry):
            return []
        try:
            return self._build(entry, target)
        except (AttributeError, KeyError, TypeError, ValueError) as e:
            logger.warning(
                f"Skipping malformed type: library quality entry on {target.location}: "
                f"{sanitize_for_logging(str(e))}"
            )
            return []

    def _build(self, entry: DataQuality, target: MetricTarget) -> list[dict]:
        metric = self._resolve_metric(entry, target)
        if metric is None:
            return []
        if target.property_name is None and not metric.allows_schema_level:
            _warn_skip(metric.name, target.location, f"has no property; {metric.name} is a property-level metric")
            return []
        if target.property_name is not None and not metric.allows_property_level:
            _warn_skip(metric.name, target.location, f"is on a property; {metric.name} is a schema-level metric")
            return []

        threshold = resolve_threshold(entry, target.location, metric.name)
        if threshold is None:
            return []
        plan = metric.plan(entry, target)
        if plan is None:
            return []

        base_name = f"{target.subject}_{metric.name}_{threshold.field}"
        metadata = self._metadata(entry, metric, target, threshold, plan)

        # mustBe: 0 uses the row-level core checks, which pinpoint the offending rows. It is
        # unit-independent (zero rows and zero percent are the same fact), so unit is not validated.
        if threshold.is_zero_must_be and plan.zero_checks:
            return [
                self._rule(check, f"{base_name}_{suffix}" if suffix else base_name, metadata, entry)
                for suffix, check in plan.zero_checks
            ]

        unit = entry.unit or "rows"
        if unit not in metric.units:
            _warn_skip(
                metric.name,
                target.location,
                f"has unrecognized unit '{sanitize_for_logging(unit)}'; expected {' or '.join(metric.units)}",
            )
            return []

        check = self._threshold_check(plan.measure, unit, threshold)
        if check is None:
            _warn_skip(metric.name, target.location, "produced a SQL query that failed the SQL safety check")
            return []
        return [self._rule(check, base_name, metadata, entry)]

    @staticmethod
    def _resolve_metric(entry: DataQuality, target: MetricTarget) -> LibraryMetric | None:
        name = entry.metric or entry.rule
        supported = ", ".join(LIBRARY_METRICS)
        if not name:
            logger.warning(
                f"Missing 'metric' on type: library quality entry on {target.location}; skipping this quality "
                f"check. Supported metrics: {supported}."
            )
            return None
        metric = LIBRARY_METRICS.get(name)
        if metric is None:
            logger.warning(
                f"Unrecognized library metric '{sanitize_for_logging(name)}' on {target.location}; skipping "
                f"this quality check. Supported metrics: {supported}."
            )
        return metric

    @staticmethod
    def _threshold_check(measure: Measure, unit: str, threshold: Threshold) -> dict[str, Any] | None:
        """Render a non-zero threshold: an exact-fit is_aggr_* check where possible, else sql_query."""
        function = threshold.aggregate_function
        aggregate_arguments = measure.aggregate_arguments(unit)
        if function is not None and aggregate_arguments is not None:
            return {"function": function, "arguments": {**aggregate_arguments, "limit": threshold.value}}

        query = measure.condition_query(unit, threshold)
        # Validate at generation time so a query the engine would reject at apply time is never emitted.
        if not is_sql_query_safe(query):
            return None
        return {"function": "sql_query", "arguments": {"query": query, "condition_column": "condition"}}

    def _metadata(
        self,
        entry: DataQuality,
        metric: LibraryMetric,
        target: MetricTarget,
        threshold: Threshold,
        plan: MetricPlan,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            **self._provenance,
            "rule_type": "metric",
            "metric": metric.name,
            "threshold_field": threshold.field,
            "unit": entry.unit or "rows",
            "dimension": entry.dimension or metric.default_dimension,
        }
        if target.property_name is not None:
            metadata["field"] = target.property_name
        metadata.update(plan.metadata)
        # ODCS defines no severity vocabulary, so it is recorded verbatim and never mapped to criticality.
        if entry.severity:
            metadata["severity"] = entry.severity
        return metadata

    def _rule(self, check: dict[str, Any], name: str, metadata: dict[str, Any], entry: DataQuality) -> dict:
        rule: dict[str, Any] = {
            "check": check,
            "name": name,
            "criticality": self._criticality,
            "user_metadata": dict(metadata),
        }
        row_filter = self._row_filter_resolver(entry)
        if row_filter:
            rule["filter"] = row_filter
        return rule


def _with_unique_names(rules: list[dict]) -> list[dict]:
    """Suffix repeated rule names (_2, _3, ...) so two entries with the same metric and threshold field stay distinct."""
    seen: dict[str, int] = {}
    for rule in rules:
        name = rule["name"]
        seen[name] = seen.get(name, 0) + 1
        if seen[name] > 1:
            rule["name"] = f"{name}_{seen[name]}"
    return rules
