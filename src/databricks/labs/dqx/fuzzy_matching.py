"""
Fuzzy matching of source rows to reference rows, shared by reference-based dataset checks.

Two matcher families are supported:

* *levenshtein*: string edit distance, portable across all supported runtimes. Every distinct source key is
  compared with every reference row, so the cost grows with the product of the two row counts.
* *vector_cosine* and *vector_l2*: distances between user-supplied *array<float>* embeddings, computed with the
  Databricks SQL vector functions and, for dataset checks, the *NEAREST BY* join. Requires Databricks Runtime
  *VECTOR_MATCH_MIN_DBR_VERSION* or above; checks declare it with *requires_dbr_version* and *uses_vector_matching*.
"""

import math
import uuid
import operator as py_operator

from collections.abc import Sequence
from dataclasses import dataclass, fields
from enum import Enum
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.window import Window

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.utils import normalize_column_expr, quote_column_name

VECTOR_MATCH_MIN_DBR_VERSION = "18.1"
NEAREST_BY_CANDIDATES = 10

FuzzyMatchOptions = dict[str, str | float | bool | None]


class FuzzyMatchMethod(Enum):
    """
    Fuzzy matching methods, keyed by the *method* name used in check arguments.

    Each member carries the SQL it needs. Vector methods set *ranking* to the *NEAREST BY* ranking keyword
    (*SIMILARITY* or *DISTANCE*) and *sql_function* to the Databricks SQL function that compares two embeddings.
    *levenshtein* sets both to None. Row and dataset checks compute distances with the same *sql_function*, so a
    new vector method needs only a new member.
    """

    ranking: str | None
    sql_function: str | None

    LEVENSHTEIN = "levenshtein"
    VECTOR_COSINE = ("vector_cosine", "SIMILARITY", "vector_cosine_similarity")
    VECTOR_L2 = ("vector_l2", "DISTANCE", "vector_l2_distance")

    def __new__(cls, value: str, ranking: str | None = None, sql_function: str | None = None) -> "FuzzyMatchMethod":
        member = object.__new__(cls)
        member._value_ = value
        member.ranking = ranking
        member.sql_function = sql_function
        return member

    @property
    def uses_nearest_by_search(self) -> bool:
        """Whether the method uses a *NEAREST BY* vector search."""
        return self.ranking is not None

    @property
    def supports_min_similarity(self) -> bool:
        """Whether the method accepts a *min_similarity* threshold."""
        return self.ranking != "DISTANCE"

    def distance(self, left: Column, right: Column) -> Column:
        """
        Builds the distance between two values.

        Args:
            left: First value: a string for *levenshtein*, an *array<float>* embedding otherwise.
            right: Second value, of the same type as *left*.

        Returns:
            The distance as a double: the edit distance for *levenshtein*, *sql_function* for distance methods,
            or *1 - sql_function* for similarity methods. NULL when either value is NULL or a similarity is
            undefined, such as for a zero vector.
        """
        if self.sql_function is None:
            return F.levenshtein(left, right).cast("double")
        comparison = F.call_function(self.sql_function, left, right).cast("double")
        return F.lit(1.0) - comparison if self.ranking == "SIMILARITY" else comparison


@dataclass(frozen=True)
class FuzzyMatcher:
    """
    Fuzzy matching configuration. Exactly one threshold must be set. A reference row is a match only if it is
    within the configured threshold.

    Args:
        method: The *FuzzyMatchMethod*; *levenshtein* by default.
        max_distance: Maximum distance of a match. For *levenshtein* it is the total edit distance summed over
            the key columns; for *vector_cosine* it is *1 - cosine similarity*; for *vector_l2* it is the L2
            distance.
        min_similarity: Minimum similarity of a match, between 0 and 1. For *levenshtein* it is
            *1 - distance / max(length)*; for *vector_cosine* it is the cosine similarity. Not supported for
            *vector_l2*.
        case_sensitive: Whether string keys are compared case-sensitively. Applies to *levenshtein* only.
        trim_strings: Whether leading and trailing whitespace is removed from string keys before comparison.
            Applies to *levenshtein* only.

    Raises:
        InvalidParameterError: If the configuration is invalid.
    """

    method: FuzzyMatchMethod = FuzzyMatchMethod.LEVENSHTEIN
    max_distance: float | None = None
    min_similarity: float | None = None
    case_sensitive: bool = True
    trim_strings: bool = False

    def __post_init__(self) -> None:
        if any(value is not None and not math.isfinite(value) for value in (self.max_distance, self.min_similarity)):
            raise InvalidParameterError("'max_distance' and 'min_similarity' must be finite numbers.")
        if (self.max_distance is None) == (self.min_similarity is None):
            raise InvalidParameterError("Fuzzy matching requires exactly one of 'max_distance' or 'min_similarity'.")
        if self.max_distance is not None and self.max_distance < 0:
            raise InvalidParameterError("'max_distance' must be non-negative.")
        if self.min_similarity is not None and not 0 <= self.min_similarity <= 1:
            raise InvalidParameterError("'min_similarity' must be between 0 and 1.")
        if self.min_similarity is not None and not self.method.supports_min_similarity:
            raise InvalidParameterError(
                f"'min_similarity' is not supported for '{self.method.value}'; use 'max_distance'."
            )
        if self.is_vector and (not self.case_sensitive or self.trim_strings):
            raise InvalidParameterError("'case_sensitive' and 'trim_strings' apply to 'levenshtein' matching only.")

    @property
    def is_vector(self) -> bool:
        """Whether the matcher uses vector search."""
        return self.method.uses_nearest_by_search

    def normalize(self, value: Column) -> Column:
        """
        Builds the value compared by the matcher.

        Args:
            value: Column or value expression to compare.

        Returns:
            *value* unchanged for vector methods. For *levenshtein*, *value* cast to string, trimmed when
            *trim_strings* is set and lowercased when *case_sensitive* is not set.
        """
        if self.is_vector:
            return value
        value = value.cast("string")
        value = F.trim(value) if self.trim_strings else value
        return value if self.case_sensitive else F.lower(value)

    def within_threshold(self, distance: Column, max_length: Column | None = None) -> Column:
        """
        Builds the condition that a distance is within the configured threshold.

        Args:
            distance: Distance from *FuzzyMatchMethod.distance*, or a sum of such distances.
            max_length: Length of the longer *levenshtein* value, or a sum of such lengths. Required for a
                *levenshtein* *min_similarity* threshold, which is *1 - distance / max_length*.

        Returns:
            True when *distance* is within the threshold, False when it is not, NULL when *distance* is NULL.
        """
        if self.max_distance is not None:
            return distance <= F.lit(self.max_distance)
        if max_length is not None and not self.is_vector:
            distance = distance / F.greatest(max_length, F.lit(1))
        return F.lit(1.0) - distance >= F.lit(self.min_similarity)


def uses_vector_matching(arguments: dict[str, object]) -> bool:
    """
    Returns whether check arguments select a vector fuzzy matching method.

    Used as the *when* condition of *requires_dbr_version* for checks that take a *fuzzy_match* argument.

    Args:
        arguments: Check function arguments by parameter name.

    Returns:
        True when *fuzzy_match* names a vector method.
    """
    options = arguments.get("fuzzy_matching_options")
    vector_methods = {method.value for method in FuzzyMatchMethod if method.uses_nearest_by_search}
    return isinstance(options, dict) and options.get("method") in vector_methods


def build_fuzzy_matcher(fuzzy_match_options: FuzzyMatchOptions | None, num_columns: int) -> FuzzyMatcher | None:
    """
    Validates the *fuzzy_match* argument of a check and builds a *FuzzyMatcher*.

    Args:
        fuzzy_match_options: Fuzzy matching options keyed by *FuzzyMatcher* field name, or None for exact matching.
        num_columns: Number of key columns.

    Returns:
        A *FuzzyMatcher* with the configured options, or *None* for exact matching.

    Raises:
        InvalidParameterError: If the options are invalid.
    """
    if fuzzy_match_options is None:
        return None
    if num_columns < 1:
        raise InvalidParameterError("Fuzzy matching requires at least one key column.")
    if not isinstance(fuzzy_match_options, dict):
        raise InvalidParameterError("'fuzzy_match' must be a dictionary of matcher options.")
    unknown_options = sorted(set(fuzzy_match_options) - {field.name for field in fields(FuzzyMatcher)})
    if unknown_options:
        raise InvalidParameterError(f"Unknown 'fuzzy_match' options: {', '.join(unknown_options)}.")
    matcher = FuzzyMatcher(
        method=_method_option(fuzzy_match_options),
        max_distance=_number_option(fuzzy_match_options, "max_distance"),
        min_similarity=_number_option(fuzzy_match_options, "min_similarity"),
        case_sensitive=_flag_option(fuzzy_match_options, "case_sensitive", True),
        trim_strings=_flag_option(fuzzy_match_options, "trim_strings", False),
    )
    if matcher.is_vector and num_columns != 1:
        raise InvalidParameterError(f"'{matcher.method.value}' matching requires exactly one embedding column.")
    return matcher


def build_nearest_by_query(key_column: str, ref_column: str, method: FuzzyMatchMethod) -> str:
    """
    Builds a *NEAREST BY* query that returns the *NEAREST_BY_CANDIDATES* nearest reference vectors per source key.

    The query reads the *keys* and *reference* DataFrame arguments of *SparkSession.sql*, so no session view is
    created. Identifiers are back-quoted with *quote_column_name*, and braces in them are doubled because
    *SparkSession.sql* formats the query with its DataFrame arguments.

    Args:
        key_column: Embedding column of the source keys.
        ref_column: Embedding column of the reference vectors.
        method: A vector *FuzzyMatchMethod*.

    Returns:
        The *NEAREST BY* SQL query used for fuzzy matching.

    Raises:
        InvalidParameterError: If *method* is not a vector method.
    """
    if not method.uses_nearest_by_search:
        raise InvalidParameterError(f"'{method.value}' is not a vector match method.")
    comparison = f"{method.sql_function}(k.{_sql_identifier(key_column)}, r.{_sql_identifier(ref_column)})"
    return (
        "SELECT k.*, r.* FROM {keys} AS k "
        f"INNER JOIN {{reference}} AS r EXACT NEAREST {NEAREST_BY_CANDIDATES} BY {method.ranking} {comparison}"
    )


def match_best_reference_rows(
    df: DataFrame,
    ref_df: DataFrame,
    columns: Sequence[str | Column],
    ref_columns: Sequence[str | Column],
    matcher: FuzzyMatcher,
    distance_column: str,
    spark: SparkSession,
    broadcast_ref: bool = False,
) -> DataFrame:
    """
    Left-joins every source row to its best-matching reference row.

    Each source row appears exactly once. The best match has the lowest distance (or the highest similarity
    when *min_similarity* is set); *levenshtein* ties are broken by the reference column values. Vector matching
    takes the *NEAREST_BY_CANDIDATES* nearest distinct reference vectors and applies the same tie-break, so it is
    deterministic unless more vectors than that are exactly equidistant. Rows with a NULL key, or with no reference
    row within the threshold, have NULL reference columns and distance.

    Args:
        df: Source DataFrame. Must be a batch DataFrame.
        ref_df: Reference DataFrame. Must be a batch DataFrame.
        columns: Source key columns (names or expressions).
        ref_columns: Reference key columns, matched to *columns* by position.
        matcher: The matcher configuration.
        distance_column: Name of the output distance column.
        spark: The active SparkSession, used for vector matching.
        broadcast_ref: Whether to add a broadcast hint to the reference rows.

    Returns:
        The joined DataFrame. Source columns are qualified by the *df* alias; reference columns and
        *distance_column* by the *ref_df* alias.

    Raises:
        InvalidParameterError: If either DataFrame is streaming, or the key columns are empty or of unequal length.

    Note:
        *levenshtein* compares every distinct source key with every reference row. Keep the reference small or
        deduplicated, and set *broadcast_ref* when it fits in executor memory.
    """
    if df.isStreaming or ref_df.isStreaming:
        raise InvalidParameterError(
            "Fuzzy matching requires bounded batch DataFrames and does not support streaming inputs. "
            "Run this check inside 'foreachBatch' to fuzzy match inputs for each micro-batch."
        )
    if not columns or len(columns) != len(ref_columns):
        raise InvalidParameterError("'columns' and 'ref_columns' must be non-empty and have the same length.")
    unique_id = uuid.uuid4().hex
    key_columns = [f"__fuzzy_key_{index}_{unique_id}" for index in range(len(columns))]
    ref_key_columns = [f"__fuzzy_ref_key_{index}_{unique_id}" for index in range(len(ref_columns))]
    source = df.withColumns(
        {name: matcher.normalize(_column_expr(column)) for name, column in zip(key_columns, columns)}
    )
    reference = ref_df.withColumns(
        {name: matcher.normalize(_column_expr(column)) for name, column in zip(ref_key_columns, ref_columns)}
    ).dropna(subset=ref_key_columns)
    if broadcast_ref:
        reference = F.broadcast(reference)
    keys = source.select(*key_columns).dropna().distinct()

    if matcher.is_vector:
        best = _nearest_by_vector(keys, reference, key_columns[0], ref_key_columns[0], matcher, distance_column, spark)
    else:
        best = _nearest_by_levenshtein(keys, reference, key_columns, ref_key_columns, matcher, distance_column)

    return source.alias("df").join(best.alias("ref_df"), on=key_columns, how="left").drop(*key_columns)


def _column_expr(column: str | Column) -> Column:
    """
    Resolves a column name or expression.

    Args:
        column: Column name, SQL expression string or *Column*.

    Returns:
        The *Column*; strings are parsed with *normalize_column_expr*.
    """
    return F.expr(normalize_column_expr(column)) if isinstance(column, str) else column


def _nearest_by_levenshtein(
    keys: DataFrame,
    reference: DataFrame,
    key_columns: list[str],
    ref_key_columns: list[str],
    matcher: FuzzyMatcher,
    distance_column: str,
) -> DataFrame:
    """
    Finds the best *levenshtein* match within the threshold for every distinct source key.

    Every key is compared with every reference row, so the cost grows with the product of both row counts.
    Composite keys sum the edit distances. With *max_distance*, a length-difference filter discards pairs that
    cannot be within the threshold before the edit distance is computed.

    Args:
        keys: Distinct, non-null source keys.
        reference: Reference rows with normalized keys.
        key_columns: Normalized source key columns in *keys*.
        ref_key_columns: Normalized reference key columns in *reference*, matched to *key_columns* by position.
        matcher: The matcher configuration.
        distance_column: Output column holding the edit distance.

    Returns:
        One row per matched key, with the key columns, the reference columns and *distance_column*.
    """
    pairs = list(zip(key_columns, ref_key_columns))
    distance = reduce(py_operator.add, [matcher.method.distance(F.col(key), F.col(ref)) for key, ref in pairs])
    max_length = reduce(py_operator.add, [F.greatest(F.length(F.col(key)), F.length(F.col(ref))) for key, ref in pairs])
    within_threshold = matcher.within_threshold(distance, max_length)

    if matcher.max_distance is not None:
        length_gap = reduce(py_operator.add, [F.abs(F.length(F.col(key)) - F.length(F.col(ref))) for key, ref in pairs])
        within_threshold = (length_gap <= F.lit(matcher.max_distance)) & within_threshold
        score = distance
    else:
        score = distance / F.greatest(max_length, F.lit(1))

    candidates = keys.join(reference, on=within_threshold, how="inner")
    return _keep_best(candidates, key_columns, score, reference.columns).select(
        *key_columns, *_columns(reference.columns), distance.alias(distance_column)
    )


def _nearest_by_vector(
    keys: DataFrame,
    reference: DataFrame,
    key_column: str,
    ref_key_column: str,
    matcher: FuzzyMatcher,
    distance_column: str,
    spark: SparkSession,
) -> DataFrame:
    """
    Finds the best vector match within the threshold for every distinct source key.

    The *NEAREST BY* join returns the *NEAREST_BY_CANDIDATES* nearest distinct reference vectors per key. Ties
    are broken by the vector value and then by the reference row values.

    Args:
        keys: Distinct, non-null source embeddings.
        reference: Reference rows with a non-null embedding key.
        key_column: Source embedding column in *keys*.
        ref_key_column: Reference embedding column in *reference*.
        matcher: The matcher configuration.
        distance_column: Output column holding the distance.
        spark: The active SparkSession.

    Returns:
        One row per matched key, with the key column, the reference columns and *distance_column*.
    """
    query = build_nearest_by_query(key_column, ref_key_column, matcher.method)
    vectors = reference.select(ref_key_column).distinct()
    distance = matcher.method.distance(F.col(key_column), F.col(ref_key_column))
    nearest = (
        spark.sql(query, keys=keys, reference=vectors)
        .withColumn(distance_column, distance)
        .where(matcher.within_threshold(F.col(distance_column)))
    )
    best_vectors = _keep_best(nearest, [key_column], F.col(distance_column), [ref_key_column])
    pairs = best_vectors.join(reference, on=ref_key_column, how="inner")
    return _keep_best(pairs, [key_column], F.col(distance_column), reference.columns).select(
        key_column, *_columns(reference.columns), distance_column
    )


def _keep_best(pairs: DataFrame, key_columns: list[str], score: Column, tie_columns: list[str]) -> DataFrame:
    """
    Keeps the lowest-scoring pair per key, breaking ties by the string value of *tie_columns*.

    Args:
        pairs: Candidate pairs of source keys and reference rows.
        key_columns: Columns that identify a source key.
        score: Ranking score; lower is better.
        tie_columns: Columns compared in order, nulls first, when scores are equal.

    Returns:
        *pairs* with exactly one row per key.
    """
    rank_column = f"__fuzzy_rank_{uuid.uuid4().hex}"
    tie_break = [column.cast("string").asc_nulls_first() for column in _columns(tie_columns)]
    ranking = Window.partitionBy(*key_columns).orderBy(score.asc(), *tie_break)
    return pairs.withColumn(rank_column, F.row_number().over(ranking)).where(F.col(rank_column) == 1).drop(rank_column)


def _columns(names: list[str]) -> list[Column]:
    """
    Builds column references for top-level column names, including names with dots or backticks.

    Args:
        names: Top-level column names.

    Returns:
        A *Column* for each name, back-quoted with *quote_column_name*.
    """
    return [F.col(quote_column_name(name)) for name in names]


def _method_option(options: FuzzyMatchOptions) -> FuzzyMatchMethod:
    """
    Parses the *method* fuzzy matching option.

    Args:
        options: Fuzzy matching options.

    Returns:
        The *FuzzyMatchMethod*, *levenshtein* when *method* is not set.

    Raises:
        InvalidParameterError: If *method* is not a string or not a known method.
    """
    value = options.get("method", FuzzyMatchMethod.LEVENSHTEIN.value)
    if not isinstance(value, str):
        raise InvalidParameterError("'fuzzy_match' option 'method' must be a string.")
    try:
        return FuzzyMatchMethod(value)
    except ValueError as e:
        methods = ", ".join(method.value for method in FuzzyMatchMethod)
        raise InvalidParameterError(f"Invalid fuzzy match method: '{value}'. Use one of: {methods}.") from e


def _number_option(options: FuzzyMatchOptions, name: str) -> float | None:
    """
    Parses a numeric fuzzy matching option.

    Args:
        options: Fuzzy matching options.
        name: Option name.

    Returns:
        The value as a float, or None when the option is not set.

    Raises:
        InvalidParameterError: If the value is not a number.
    """
    value = options.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidParameterError(f"'fuzzy_match' option '{name}' must be a number.")
    return float(value)


def _flag_option(options: FuzzyMatchOptions, name: str, default: bool) -> bool:
    """
    Parses a boolean fuzzy matching option.

    Args:
        options: Fuzzy matching options.
        name: Option name.
        default: Value used when the option is not set.

    Returns:
        The option value.

    Raises:
        InvalidParameterError: If the value is not a boolean.
    """
    value = options.get(name, default)
    if not isinstance(value, bool):
        raise InvalidParameterError(f"'fuzzy_match' option '{name}' must be a boolean.")
    return value


def _sql_identifier(name: str) -> str:
    """
    Quotes a column name for a query passed to *SparkSession.sql* with DataFrame arguments.

    Args:
        name: Column name.

    Returns:
        The back-quoted name, with braces doubled so the query formatter keeps them.
    """
    return quote_column_name(name).replace("{", "{{").replace("}", "}}")
