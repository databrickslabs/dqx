"""Array paths for row-level checks on array elements.

A row-level check whose *column* contains "[*]" outside quotes (for example "users[*].events[*].event_date")
is applied to every element the path reaches. The row fails when any element fails, and the result holds one
message with the number of failing values, the number of distinct failing values and up to 10 of them.

The path is parsed with a simple left-to-right scan (no backtracking regular expressions) and the Spark
expression is built with the Column API only, so the path is never embedded in SQL text.
"""

from collections.abc import Callable
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import ArrayType, DataType, StructType

from databricks.labs.dqx.errors import InvalidCheckError
from databricks.labs.dqx.utils import quote_column_name

__all__ = [
    "ARRAY_WILDCARD",
    "MAX_ARRAY_DEPTH",
    "MAX_FAILING_VALUES_SHOWN",
    "ArrayPath",
    "has_array_wildcard",
    "is_array_path_supported",
]

ARRAY_WILDCARD = "[*]"
MAX_ARRAY_DEPTH = 20
MAX_FAILING_VALUES_SHOWN = 10

# Built-in checks that cannot run inside Spark's transform / filter: they use Python or pandas UDFs,
# build SQL text from the column name, or look the column up by name.
_UNSUPPORTED_CHECK_FUNCS = frozenset(
    {
        ("databricks.labs.dqx.check_funcs", "is_valid_ipv6_address"),
        ("databricks.labs.dqx.check_funcs", "is_ipv6_address_in_cidr"),
        ("databricks.labs.dqx.check_funcs", "is_valid_json"),
        ("databricks.labs.dqx.check_funcs", "has_json_keys"),
        ("databricks.labs.dqx.check_funcs", "has_valid_json_schema"),
        ("databricks.labs.dqx.pii.pii_detection_funcs", "does_not_contain_pii"),
    }
)
_UNSUPPORTED_CHECK_MODULES = frozenset({"databricks.labs.dqx.geo.check_funcs"})

_QUOTE_CHARS = "`'\""
_BACK_QUOTE = "`"


def has_array_wildcard(column: str) -> bool:
    """Returns True if *column* contains "[*]" outside back-quoted names and quoted SQL strings.

    The string is scanned left to right. Quoted sections are skipped following Spark's rules: a doubled quote
    is an escaped quote, and inside single- or double-quoted strings a backslash escapes the next character.
    An unclosed quote runs to the end of the string.

    Args:
        column: Column reference provided as a string.

    Returns:
        True if the column should be treated as an array path, otherwise False.
    """
    position = 0
    while position < len(column):
        if column[position] in _QUOTE_CHARS:
            position = _skip_quoted_section(column, position)
        elif column.startswith(ARRAY_WILDCARD, position):
            return True
        else:
            position += 1
    return False


def is_array_path_supported(check_func: Callable) -> bool:
    """Returns True if the check function can be applied to array elements.

    Known built-in checks that cannot run inside Spark's higher-order functions (geo, PII, IPv6 and JSON checks)
    are matched on module and function name, so a custom check that shares a name with one of them is allowed.

    Args:
        check_func: The check function.

    Returns:
        False for known built-in checks that cannot run on array elements, otherwise True.
    """
    module = getattr(check_func, "__module__", "")
    name = getattr(check_func, "__name__", "")
    return module not in _UNSUPPORTED_CHECK_MODULES and (module, name) not in _UNSUPPORTED_CHECK_FUNCS


@dataclass(frozen=True)
class ArrayPath:
    """A parsed array path such as "users[*].events[*].payload.event_date".

    Attributes:
        path: The original path string.
        column: Name of the top-level column the path starts from.
        steps: Steps following the top-level column: None means step into every element of an array ("[*]"),
            and a string means read that struct field.
    """

    path: str
    column: str
    steps: tuple[str | None, ...]

    @classmethod
    def parse(cls, path: str) -> "ArrayPath":
        """Parses and validates an array path.

        A path is a list of segments joined by *.*. Each segment is a name followed by zero or more "[*]".
        A name is either back-quoted (a doubled back-quote is an escaped back-quote) or bare (any characters
        other than *.*, *[* and *]*). An unclosed back-quote runs to the end of the string as part of the name.
        Names are not validated here; names that do not resolve are skipped when the check is applied.

        Args:
            path: The path string.

        Returns:
            The parsed path.

        Raises:
            InvalidCheckError: If the path is malformed or has more than 20 array levels.
        """
        scanner = _PathScanner(path)
        column, column_depth = scanner.read_segment()
        steps: list[str | None] = [None] * column_depth
        while scanner.read_separator():
            name, depth = scanner.read_segment()
            steps += [name, *[None] * depth]

        total_depth = steps.count(None)
        if total_depth > MAX_ARRAY_DEPTH:
            raise _invalid_path(path, f"{total_depth} array levels exceed the maximum of {MAX_ARRAY_DEPTH}")
        return cls(path=path, column=column, steps=tuple(steps))

    def matches_schema(self, schema: StructType, case_sensitive: bool) -> bool:
        """Returns True if the path can be followed through the given DataFrame schema.

        Every "[*]" must reach an array and every field step must reach a struct. Maps are not traversed.
        A missing or ambiguous field, "[*]" on a non-array, a field step on a non-struct, or a left-out "[*]"
        on an array of structs means the path does not match.

        Args:
            schema: The DataFrame schema.
            case_sensitive: Whether field names are matched case-sensitively (*spark.sql.caseSensitive*).

        Returns:
            True if the path matches the schema, otherwise False.
        """
        current: DataType | None = _find_field_type(schema, self.column, case_sensitive)
        for step in self.steps:
            current = _follow_step(current, step, case_sensitive)
        return current is not None

    def leaf_values(self) -> Column:
        """Builds an expression collecting every value the path reaches into one flat array.

        Null inner arrays are dropped before flattening, because *flatten* returns null for the whole array
        if any inner array is null. Null or empty arrays therefore produce no values.

        Returns:
            Expression evaluating to an array of the leaf values.
        """
        return _collect_leaf_values(F.col(quote_column_name(self.column)), self.steps)

    def build_condition(self, element_check: Callable[[Column], Column], function_name: str, alias: str) -> Column:
        """Builds the row-level check condition for the path.

        The check is applied to every leaf value. The condition holds a message when at least one value fails
        (the check returns a non-null message), and null otherwise. The failing values are computed once and
        bound to a lambda variable, so the check appears once in the condition.

        Args:
            element_check: Function applying the check to one element and returning the check's message column.
            function_name: Name of the check function, used in the message.
            alias: Alias of the resulting condition.

        Returns:
            The check condition.
        """

        def message_if_any_failed(failing_values: Column) -> Column:
            return F.when(F.size(failing_values) > 0, self._build_message(failing_values, function_name))

        failing = F.filter(self.leaf_values(), lambda value: element_check(value).isNotNull())
        # Wrapping *failing* in a one-element array binds it to a single lambda variable, so the filter (and the
        # check inside it) is evaluated once rather than once per reference in the message.
        return F.element_at(F.transform(F.array(failing), message_if_any_failed), 1).alias(alias)

    def _build_message(self, failing_values: Column, function_name: str) -> Column:
        """Builds the message from the failing values: their count, distinct count and the first distinct ones."""

        def message_from_distinct_values(distinct_values: Column) -> Column:
            shown_values = F.slice(distinct_values, 1, MAX_FAILING_VALUES_SHOWN)
            return F.concat(
                F.lit(f"Array elements in Column '{self.path}' failed check '{function_name}'. "),
                F.size(failing_values).cast("string"),
                F.lit(" failing values, "),
                F.size(distinct_values).cast("string"),
                F.lit(" distinct (showing first "),
                F.least(F.size(distinct_values), F.lit(MAX_FAILING_VALUES_SHOWN)).cast("string"),
                F.lit("): "),
                F.array_join(F.transform(shown_values, _format_value), ", "),
            )

        # Bind the distinct values to a single lambda variable so they are computed once for both the count and
        # the values shown.
        return F.element_at(F.transform(F.array(F.array_distinct(failing_values)), message_from_distinct_values), 1)


def _format_value(value: Column) -> Column:
    """Formats a failing value for the message: quoted string form, or null without quotes."""
    return F.when(value.isNull(), F.lit("null")).otherwise(F.concat(F.lit("'"), value.cast("string"), F.lit("'")))


def _skip_quoted_section(text: str, start: int) -> int:
    """Returns the position just after the quoted section starting at *start*."""
    quote = text[start]
    position = start + 1
    while position < len(text):
        char = text[position]
        if char == "\\" and quote != _BACK_QUOTE:
            position += 2  # backslash escapes the next character inside SQL strings
        elif char != quote:
            position += 1
        elif text.startswith(quote * 2, position):
            position += 2  # doubled quote is an escaped quote
        else:
            return position + 1
    return len(text)


class _PathScanner:
    """Left-to-right scanner reading the segments of an array path."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._position = 0

    def read_segment(self) -> tuple[str, int]:
        """Reads a name and the "[*]" that follow it, returning the name and the number of "[*]"."""
        name = self._read_name()
        return name, self._read_wildcards()

    def read_separator(self) -> bool:
        """Consumes the "." before the next segment; returns False at the end of the path."""
        if self._at_end():
            return False
        char = self._current()
        if char == "]":
            raise _invalid_path(self._path, "unexpected ']'")
        if char != ".":
            raise _invalid_path(self._path, "expected '.' or '[*]' after a name or '[*]'")
        self._position += 1
        return True

    def _read_name(self) -> str:
        if self._at_end() or self._current() == ".":
            raise _invalid_path(self._path, "empty segment")
        char = self._current()
        if char == "[":
            reason = "path must start with a name" if self._position == 0 else "'[*]' must follow a name"
            raise _invalid_path(self._path, reason)
        if char == "]":
            raise _invalid_path(self._path, "unexpected ']'")
        if char == _BACK_QUOTE:
            return self._read_back_quoted_name()
        return self._read_bare_name()

    def _read_bare_name(self) -> str:
        start = self._position
        while not self._at_end() and self._current() not in ".[]":
            self._position += 1
        return self._path[start : self._position]

    def _read_back_quoted_name(self) -> str:
        """Reads a back-quoted name; an unclosed back-quote keeps the rest of the string, back-quote included."""
        start = self._position
        chars: list[str] = []
        self._position += 1
        while not self._at_end():
            if self._path.startswith(_BACK_QUOTE * 2, self._position):
                chars.append(_BACK_QUOTE)  # doubled back-quote is an escaped back-quote
                self._position += 2
            elif self._current() == _BACK_QUOTE:
                self._position += 1
                return "".join(chars)
            else:
                chars.append(self._current())
                self._position += 1
        return self._path[start:]

    def _read_wildcards(self) -> int:
        count = 0
        while not self._at_end() and self._current() == "[":
            if not self._path.startswith(ARRAY_WILDCARD, self._position):
                raise _invalid_path(self._path, self._bracket_error_reason())
            count += 1
            self._position += len(ARRAY_WILDCARD)
        return count

    def _bracket_error_reason(self) -> str:
        if "]" in self._path[self._position :]:
            return "unsupported brackets, only [*] is allowed"
        return "unclosed '['"

    def _at_end(self) -> bool:
        return self._position >= len(self._path)

    def _current(self) -> str:
        return self._path[self._position]


def _find_field_type(struct: StructType, name: str, case_sensitive: bool) -> DataType | None:
    """Returns the type of the single field matching *name*, or None if there is no unique match."""
    if case_sensitive:
        matches = [field for field in struct.fields if field.name == name]
    else:
        matches = [field for field in struct.fields if field.name.lower() == name.lower()]
    return matches[0].dataType if len(matches) == 1 else None


def _follow_step(current: DataType | None, step: str | None, case_sensitive: bool) -> DataType | None:
    """Returns the type reached by one path step, or None if the step cannot be followed."""
    if step is None:
        return current.elementType if isinstance(current, ArrayType) else None
    return _find_field_type(current, step, case_sensitive) if isinstance(current, StructType) else None


def _collect_leaf_values(expr: Column, steps: tuple[str | None, ...]) -> Column:
    """Applies *steps* to *expr*; the result is the leaf value, or an array of leaf values if a "[*]" is left."""
    fields, remaining = _split_at_first_wildcard(steps)
    for field in fields:
        expr = expr.getField(field)
    if not remaining:  # no "[*]" left, or the last step is "[*]" and *expr* already is the array of leaves
        return expr

    elements = F.transform(expr, lambda element: _collect_leaf_values(element, remaining))
    if None not in remaining:
        return elements
    # Drop null inner arrays first: flatten returns null for the whole array if any inner array is null.
    return F.flatten(F.filter(elements, lambda inner: inner.isNotNull()))


def _split_at_first_wildcard(steps: tuple[str | None, ...]) -> tuple[tuple[str | None, ...], tuple[str | None, ...]]:
    """Splits *steps* into the field steps before the first "[*]" and the steps after it."""
    if None not in steps:
        return steps, ()
    wildcard_index = steps.index(None)
    return steps[:wildcard_index], steps[wildcard_index + 1 :]


def _invalid_path(path: str, reason: str) -> InvalidCheckError:
    return InvalidCheckError(f"Invalid array path '{path}': {reason}.")
