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

import json
import logging
import math
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field

from open_data_contract_standard.model import DataQuality, SchemaObject  # type: ignore

from databricks.labs.dqx.utils import (
    escape_sql_string_literal,
    is_sql_query_safe,
    quote_column_name,
    sanitize_for_logging,
)

logger = logging.getLogger(__name__)

# Recognized threshold fields, in rule order: each one set on an entry produces its own rule.
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
_MAX_PATTERN_LENGTH = 200
_MAX_SQL_INTEGER = 10**38
_QUANTIFIER_PATTERN = re.compile(r"[*+?]|\{(\d+)(?:,(\d*))?\}")
_INLINE_FLAGS_PATTERN = re.compile(r"\(\?[idmsuxU-]+([:)])")
# Group syntax Python's re accepts but Java's regex engine (used by Spark) rejects.
_PYTHON_ONLY_GROUP_PREFIXES: tuple[str, ...] = ("(?P", "(?#", "(?(")
# Java group prefixes rewritten to a Python form with the same syntax rules (lookbehinds become plain
# groups because Python, unlike Java, requires them to be fixed-width).
_JAVA_GROUP_PREFIX_REWRITES: tuple[tuple[str, str], ...] = (
    ("(?>", "(?:"),
    ("(?<=", "(?:"),
    ("(?<!", "(?:"),
    ("(?<", "(?P<"),
)
# Java escapes with no Python equivalent; any single-character atom stands in for the syntax check.
_JAVA_ONLY_ESCAPE_LETTERS = frozenset("hHRXGe")
_PATTERN_TOO_LONG = f"is longer than {_MAX_PATTERN_LENGTH} characters"
_PATTERN_REDOS_RISK = (
    "repeats a group that contains a variable quantifier or alternation (catastrophic backtracking risk)"
)
_PATTERN_PYTHON_ONLY = "uses Python-only regex syntax; Spark evaluates patterns as Java regular expressions"
_PATTERN_INVALID = "is not a valid regular expression"
_INPUT_VIEW = "{{ input_view }}"

Number = int | float


# ---------------------------------------------------------------------------------------------
# SQL rendering helpers (the single literal escaper and identifier quoter for library metrics)
# ---------------------------------------------------------------------------------------------


def sql_identifier(column_path: str) -> str:
    """Back-quote each segment of a (possibly dotted) column path for interpolation into generated SQL.

    Every segment is quoted, plain names included: *is_sql_query_safe* ignores back-quoted identifiers,
    so a property named after a blocked keyword (e.g. *delete*) is not mistaken for a SQL statement.
    Quoting uses *quote_column_name*, which doubles embedded backticks, so this never raises.
    """
    return ".".join(quote_column_name(segment) for segment in column_path.split("."))


def sql_literal(value: str | Number | bool) -> str:
    """Render a contract-supplied scalar as a SQL literal.

    Numbers stay unquoted so numeric columns compare numerically. Booleans become TRUE/FALSE (checked
    before numbers, since *bool* is a subclass of *int*). Strings are single-quoted and escaped with
    *escape_sql_string_literal*, which escapes backslashes and single quotes as *\\'* (Spark does not
    honour a doubled *''*).
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{escape_sql_string_literal(str(value))}'"


def in_list_literal(value: str | Number | bool) -> str | Number | bool:
    """Render a value for the *allowed*/*forbidden* argument of *is_in_list*/*is_not_in_list*.

    Those checks parse a bare string as a column expression, so strings go through *sql_literal*,
    the same escaper used for the aggregate SQL. Numbers and booleans pass through unchanged.
    """
    return sql_literal(value) if isinstance(value, str) else value


def _is_sql_number(value: int | float) -> bool:
    """Whether a number has a Spark SQL literal form: NaN and infinity have none, and an integer must fit
    *DECIMAL(38)*. Checked without converting to float, which raises *OverflowError* for huge integers."""
    if isinstance(value, float):
        return math.isfinite(value)
    return abs(value) < _MAX_SQL_INTEGER


def _is_scalar(value: object) -> bool:
    """Whether *value* can be rendered as a SQL literal."""
    if isinstance(value, (str, bool)):
        return True
    return isinstance(value, (int, float)) and _is_sql_number(value)


def _as_number(value: object) -> Number | None:
    """Normalize a threshold value to a finite number, or None if it is not one.

    *mustBe*/*mustNotBe* are untyped on the ODCS model, so a boolean, list or arbitrary string can
    reach this point. Booleans are rejected (*False == 0* in Python), numeric strings such as "0"
    are accepted.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = _parse_number(value)
    if isinstance(value, (int, float)) and _is_sql_number(value):
        return value
    return None


def _parse_number(text: str) -> Number | None:
    """Parse a numeric string, as an exact integer where possible (a float round-trip would change
    large integers, e.g. "12345678901234567890")."""
    try:
        return int(text)
    except ValueError:
        pass
    try:
        parsed = float(text)
    except ValueError:
        return None
    return int(parsed) if parsed.is_integer() and abs(parsed) < 2**53 else parsed


def pattern_problem(pattern: str) -> str | None:
    """Return why a contract-supplied regex is rejected, or None if it can be used.

    Spark evaluates the pattern as a Java regular expression. A single scan checks three things:

    - ReDoS: a repeated group (one-or-more, zero-or-more or a range quantifier) that itself contains a
      variable quantifier or an alternation, at any nesting depth, plus a 200-character cap. Such
      patterns can backtrack catastrophically on adversarial input.
    - Python-only syntax that Java rejects: *(?P...)* named groups, inline comment groups,
      conditional groups and range quantifiers with no lower bound.
    - Syntax errors: Java-only constructs (Unicode classes, possessive quantifiers, atomic groups,
      mid-pattern inline flags) are rewritten to Python equivalents and the result is compiled with
      *re*, so validation does not depend on the Python version.
    """
    if len(pattern) > _MAX_PATTERN_LENGTH:
        return _PATTERN_TOO_LONG
    return _PatternScan(pattern).problem()


class _PatternScan:
    """Single left-to-right scan of a Java regex for :func:*pattern_problem*."""

    def __init__(self, pattern: str):
        self._pattern = pattern
        self._pos = 0
        self._python: list[str] = []
        # One flag per open group (the outermost entry is the whole pattern): whether the group
        # contains a variable quantifier or an alternation.
        self._groups: list[bool] = [False]

    def problem(self) -> str | None:
        while self._pos < len(self._pattern):
            is_atom, varies_inside, problem = self._atom()
            if problem is None and is_atom:
                problem = self._quantifier(varies_inside)
            if problem is not None:
                return problem
        if len(self._groups) != 1:
            return _PATTERN_INVALID
        try:
            re.compile("".join(self._python))
        except re.error:
            return _PATTERN_INVALID
        return None

    def _emit(self, text: str, length: int) -> None:
        self._python.append(text)
        self._pos += length

    def _atom(self) -> tuple[bool, bool, str | None]:
        """Consume one token; return (is a quantifiable atom, a closed group that varies inside, problem)."""
        char = self._pattern[self._pos]
        if char == "\\":
            self._escape()
            return True, False, None
        if char == "[":
            return self._char_class()
        if char == "(":
            return False, False, self._open_group()
        if char == ")":
            if len(self._groups) == 1:
                return False, False, _PATTERN_INVALID
            varies_inside = self._groups.pop()
            self._groups[-1] = self._groups[-1] or varies_inside
            self._emit(")", 1)
            return True, varies_inside, None
        if char == "|":
            self._groups[-1] = True
            self._emit("|", 1)
            return False, False, None
        if char == "{":
            # Java rejects a brace that does not form a quantifier.
            return False, False, _PATTERN_INVALID
        self._emit(char, 1)
        return True, False, None

    def _escape(self) -> None:
        pattern, pos = self._pattern, self._pos
        letter = pattern[pos + 1 : pos + 2]
        if self._delimited_escape(letter):
            return
        if letter in {"p", "P", "c"} and pos + 2 < len(pattern) and pattern[pos + 2] != "{":
            self._emit(".", 3)  # \pL, \cM
        elif letter == "Q":
            close = pattern.find("\\E", pos + 2)
            literal_end = len(pattern) if close == -1 else close
            consumed = (literal_end if close == -1 else literal_end + 2) - pos
            self._emit(re.escape(pattern[pos + 2 : literal_end]), consumed)
        elif letter == "z":
            self._emit("\\Z", 2)
        elif letter in _JAVA_ONLY_ESCAPE_LETTERS:
            self._emit(".", 2)
        else:
            # Passed through; an unknown or incomplete escape fails re.compile.
            self._emit(pattern[pos : pos + 2], 2 if letter else 1)

    def _delimited_escape(self, letter: str) -> bool:
        """Consume a Java *\\p{L}*, *\\x{1F600}* or *\\k<name>* escape as a stand-in atom, if present."""
        pattern, pos = self._pattern, self._pos
        opener = pattern[pos + 2 : pos + 3]
        if letter not in {"p", "P", "x", "k"} or opener not in {"{", "<"}:
            return False
        close = pattern.find("}" if opener == "{" else ">", pos + 3)
        if close == -1:
            return False
        self._emit(".", close + 1 - pos)
        return True

    def _char_class(self) -> tuple[bool, bool, str | None]:
        self._emit("[", 1)
        if self._pattern.startswith("^", self._pos):
            self._emit("^", 1)
        if self._pattern.startswith("]", self._pos):
            self._emit("\\]", 1)
        while self._pos < len(self._pattern):
            char = self._pattern[self._pos]
            if char == "]":
                self._emit("]", 1)
                return True, False, None
            if char == "\\":
                self._escape()
            else:
                self._emit("\\[" if char == "[" else char, 1)
        return False, False, _PATTERN_INVALID

    def _open_group(self) -> str | None:
        if self._pattern.startswith(_PYTHON_ONLY_GROUP_PREFIXES, self._pos):
            return _PATTERN_PYTHON_ONLY
        flags = _INLINE_FLAGS_PATTERN.match(self._pattern, self._pos)
        if flags is not None and flags.group(1) == ")":
            # Java allows inline flags anywhere; Python 3.11+ only at the start. They do not affect syntax.
            self._pos = flags.end()
            return None
        if flags is not None:
            self._emit("(?:", flags.end() - self._pos)
        else:
            for java_prefix, python_prefix in _JAVA_GROUP_PREFIX_REWRITES:
                if self._pattern.startswith(java_prefix, self._pos):
                    self._emit(python_prefix, len(java_prefix))
                    break
            else:
                self._emit("(", 1)
        self._groups.append(False)
        return None

    def _quantifier(self, varies_inside: bool) -> str | None:
        if self._pattern.startswith("{,", self._pos):
            return _PATTERN_PYTHON_ONLY
        match = _QUANTIFIER_PATTERN.match(self._pattern, self._pos)
        if match is None:
            return None
        token, minimum, maximum = match.group(0), match.group(1), match.group(2)
        if minimum is None:
            repeats, varies = token != "?", True
        elif maximum is None:
            repeats, varies = int(minimum) > 1, False  # exact count {m}
        else:
            repeats = maximum == "" or int(maximum) > 1
            varies = maximum == "" or int(maximum) != int(minimum)
        if repeats and varies_inside:
            return _PATTERN_REDOS_RISK
        if varies:
            self._groups[-1] = True
        self._emit(token, match.end() - self._pos)
        if self._pattern.startswith("?", self._pos):
            self._emit("?", 1)  # lazy
        elif self._pattern.startswith("+", self._pos):
            self._pos += 1  # possessive: same syntax, but only Python 3.11+ accepts it
        return None


# ---------------------------------------------------------------------------------------------
# Threshold
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Threshold:
    """One ODCS threshold field and its validated value.

    Range bounds are inclusive on both ends, as in SQL *BETWEEN*. The ODCS spec is inconsistent here:
    its operator table writes *mustBeBetween* as a closed interval, while a note calls it equivalent
    to *mustBeGreaterThan* plus *mustBeLessThan*.
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


def resolve_thresholds(entry: DataQuality, location: str, metric: str) -> list[Threshold]:
    """Resolve every threshold field set on *entry*, one *Threshold* per field.

    An entry may set several fields to express a range (e.g. *mustBeGreaterOrEqualTo* plus
    *mustBeLessOrEqualTo*); each is enforced by its own rule. An invalid field is warned about and
    skipped without dropping the valid ones. Returns an empty list (with a warning) when no field is set.
    """
    thresholds: list[Threshold] = []
    has_field = False
    for field_name in THRESHOLD_FIELDS:
        raw = getattr(entry, field_name)
        if raw is None:
            continue
        has_field = True
        if field_name in _RANGE_THRESHOLD_FIELDS:
            bounds = _as_bounds(raw)
            if bounds is None:
                _warn_skip(
                    metric,
                    location,
                    f"'{field_name}' must be a list of two numbers, smallest first (got "
                    f"{sanitize_for_logging(repr(raw))})",
                )
                continue
            thresholds.append(Threshold(field_name, bounds=bounds))
            continue
        value = _as_number(raw)
        if value is None:
            _warn_skip(metric, location, f"'{field_name}' must be a number (got {sanitize_for_logging(repr(raw))})")
            continue
        thresholds.append(Threshold(field_name, value=value))

    if not has_field:
        _warn_skip(
            metric,
            location,
            f"has no recognized threshold field set ({', '.join(THRESHOLD_FIELDS)})",
        )
    return thresholds


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
    def aggregate_arguments(self, unit: str) -> dict[str, object] | None:
        """*is_aggr_** arguments (without *limit*) computing the value, or None if not expressible."""

    @abstractmethod
    def condition_query(self, unit: str, threshold: Threshold) -> str:
        """A single-row *sql_query* returning a *condition* column that is TRUE on violation."""


@dataclass(frozen=True)
class RowCountMeasure(Measure):
    """Total number of rows in the (optionally filtered) dataset."""

    def aggregate_arguments(self, unit: str) -> dict[str, object] | None:
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

    def aggregate_arguments(self, unit: str) -> dict[str, object] | None:
        # Parenthesised so *normalize_column_expr* passes the indicator through as a SQL expression
        # instead of back-quoting it as a column name (operator-free SQL such as "x IS NULL" is
        # otherwise indistinguishable from a name containing spaces).
        if unit == "percent":
            return {"column": f"(CASE WHEN {self.predicate_sql} THEN 100.0 ELSE 0.0 END)", "aggr_type": "avg"}
        return {"column": f"(CASE WHEN {self.predicate_sql} THEN 1 END)", "aggr_type": "count"}

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

    def aggregate_arguments(self, unit: str) -> dict[str, object] | None:
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
    zero_checks: tuple[tuple[str, dict[str, object]], ...] = ()
    metadata: dict[str, str] = field(default_factory=dict)


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
        zero_checks: list[tuple[str, dict[str, object]]] = [
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
        zero_checks: list[tuple[str, dict[str, object]]] = []
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

    def _read_pattern(self, arguments: dict[str, object], target: MetricTarget) -> str | None:
        if "pattern" not in arguments:
            return None
        pattern = arguments["pattern"]
        if not isinstance(pattern, str) or not pattern:
            logger.warning(
                f"'arguments.pattern' for type: library {self.name} entry on {target.location} must be a "
                "non-empty string; ignoring it."
            )
            return None
        problem = pattern_problem(pattern)
        if problem is not None:
            logger.warning(f"'arguments.pattern' on {target.location} {problem}; ignoring it.")
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
            metadata: dict[str, str] = {}
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
            key_columns = list(properties)
            # user_metadata is a string map, so the key list is JSON-encoded (as _quality_metadata does).
            metadata = {"fields": json.dumps(key_columns)}
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
    arguments: dict[str, object] | None, key: str, target: MetricTarget, metric: str, *, allow_null: bool
) -> list | None:
    """Read a non-empty list of scalars from *arguments[key]*, warning and returning None otherwise."""
    value = arguments.get(key) if isinstance(arguments, dict) else None
    if not (
        isinstance(value, list)
        and len(value) > 0
        and all(_is_scalar(item) or (allow_null and item is None) for item in value)
    ):
        _warn_skip(
            metric,
            target.location,
            f"'arguments.{key}' must be a non-empty list of scalar values (finite numbers, strings or booleans)",
        )
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


# Assembles the final rule dict: (check, name, entry, property name, metric metadata) -> rule.
LibraryRuleFactory = Callable[[dict[str, object], str, DataQuality, str | None, dict[str, str]], dict]


class LibraryMetricRulesBuilder:
    """Builds DQX rule dicts from the ODCS library metric entries of one schema.

    Args:
        rule_factory: Assembles each final rule from its check, name, source entry, property name and
            metric metadata. The contract generator supplies its own rule assembly here, so library
            rules get the same provenance, owner, row filter and *customProperties* handling as
            explicit rules.
    """

    def __init__(self, rule_factory: LibraryRuleFactory):
        self._rule_factory = rule_factory

    def build_for_schema(
        self, schema_obj: SchemaObject, schema_name: str, taken_names: set[str] | None = None
    ) -> list[dict]:
        """Build rules for every library entry on the schema and its properties.

        Args:
            schema_obj: The ODCS schema object.
            schema_name: The schema name, used in rule names and log messages.
            taken_names: Names of rules already generated for the contract. Library rule names are
                suffixed so they collide neither with these nor with each other; the set is updated
                with the names used.
        """
        rules: list[dict] = []
        for prop in schema_obj.properties or []:
            for entry in prop.quality or []:
                rules.extend(self._build_safely(entry, MetricTarget(schema_name, prop.name)))
        for entry in schema_obj.quality or []:
            rules.extend(self._build_safely(entry, MetricTarget(schema_name, None)))
        return _with_unique_names(rules, set() if taken_names is None else taken_names)

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

        thresholds = resolve_thresholds(entry, target.location, metric.name)
        if not thresholds:
            return []
        plan = metric.plan(entry, target)
        if plan is None:
            return []

        # mustBe: 0 uses the row-level core checks, which pinpoint the offending rows. It is
        # unit-independent (zero rows and zero percent are the same fact), so only the other
        # thresholds need a recognized unit.
        unit = entry.unit or "rows"
        if unit not in metric.units:
            logger.warning(
                f"type: library {metric.name} entry on {target.location} has unrecognized unit "
                f"'{sanitize_for_logging(unit)}'; expected {' or '.join(metric.units)}; skipping its thresholds "
                f"other than mustBe: 0."
            )
            thresholds = [t for t in thresholds if self._uses_zero_checks(t, plan)]
            # Zero rows and zero percent are the same fact; record the default rather than the bad value.
            unit = "rows"

        rules: list[dict] = []
        for threshold in thresholds:
            rules.extend(self._build_for_threshold(entry, metric, target, threshold, plan, unit))
        return rules

    @staticmethod
    def _uses_zero_checks(threshold: Threshold, plan: MetricPlan) -> bool:
        return threshold.is_zero_must_be and bool(plan.zero_checks)

    def _build_for_threshold(
        self,
        entry: DataQuality,
        metric: LibraryMetric,
        target: MetricTarget,
        threshold: Threshold,
        plan: MetricPlan,
        unit: str,
    ) -> list[dict]:
        base_name = f"{target.subject}_{metric.name}_{threshold.field}"
        metadata = self._metadata(entry, metric, threshold, plan, unit)

        if self._uses_zero_checks(threshold, plan):
            return [
                self._rule_factory(
                    check, f"{base_name}_{suffix}" if suffix else base_name, entry, target.property_name, metadata
                )
                for suffix, check in plan.zero_checks
            ]

        check = self._threshold_check(plan.measure, unit, threshold)
        if check is None:
            _warn_skip(metric.name, target.location, "produced a SQL query that failed the SQL safety check")
            return []
        return [self._rule_factory(check, base_name, entry, target.property_name, metadata)]

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
    def _threshold_check(measure: Measure, unit: str, threshold: Threshold) -> dict[str, object] | None:
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

    @staticmethod
    def _metadata(
        entry: DataQuality, metric: LibraryMetric, threshold: Threshold, plan: MetricPlan, unit: str
    ) -> dict[str, str]:
        """Metric-specific *user_metadata*; provenance, *field* and *customProperties* come from the rule factory."""
        metadata: dict[str, str] = {
            "rule_type": "metric",
            "metric": metric.name,
            "threshold_field": threshold.field,
            "unit": unit,
            "dimension": (entry.dimension or "").strip() or metric.default_dimension,
        }
        metadata.update(plan.metadata)
        # ODCS defines no severity vocabulary, so it is recorded verbatim and never mapped to criticality.
        if entry.severity:
            metadata["severity"] = entry.severity
        return metadata


def _with_unique_names(rules: list[dict], taken_names: set[str]) -> list[dict]:
    """Suffix rule names (_2, _3, ...) that are already taken, recording every name used in *taken_names*."""
    for rule in rules:
        name = rule["name"]
        suffix = 1
        while rule["name"] in taken_names:
            suffix += 1
            rule["name"] = f"{name}_{suffix}"
        taken_names.add(rule["name"])
    return rules
