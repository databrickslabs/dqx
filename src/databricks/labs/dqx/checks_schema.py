"""JSON schema for declarative (YAML/JSON) quality check definitions.

The schema is built from the pydantic *CheckSpec* model plus one entry per built-in check function, derived
from each function's signature and docstring, so IDEs can autocomplete function names and arguments and flag
mistakes while checks are written. A generated copy is committed at *schemas/checks.schema.json*.
"""

import inspect
import re
import types
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, get_args, get_origin

from pydantic.json_schema import JsonSchemaValue, models_json_schema

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.checks_validator import CheckSpec
from databricks.labs.dqx.geo import check_funcs as geo_check_funcs
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY, Criticality

__all__ = ["build_checks_json_schema"]

JSON_SCHEMA_DRAFT = "http://json-schema.org/draft-07/schema#"
# Parameters that *for_each_column* fills in, so they are not required when it is set.
_FOR_EACH_COLUMN_PARAMS = ("column", "columns")
# Google-style docstring section headers that end the *Args* section.
_DOCSTRING_SECTION_RE = re.compile(r"^(Args|Returns|Raises|Yields|Examples?|Notes?|Warnings?):\s*$")
_DOCSTRING_ARG_RE = re.compile(r"^(\*{0,2}\w+)(?:\s*\([^)]*\))?:\s*(.*)$")
# Docstrings mark names in *italics* (and a few phrases in **bold**); editors show schema descriptions as
# plain text, so the markers are removed. Simple names first, then spans such as *count(*)* or *"*"*,
# whose inner asterisk is part of the text, then phrases with spaces. A spaced * (multiplication) is kept.
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_NAME_RE = re.compile(r"(?<![\w*])\*([\w.\-]+)\*(?![\w*])")
_ITALIC_SPAN_RE = re.compile(r"(?<![\w*])\*(\S+)\*(?=[\s.,;:)\]'\"/]|$)")
_ITALIC_PHRASE_RE = re.compile(r"(?:(?<=\s)|(?<=^))\*(\S[^*]*?\S)\*(?![\w*])")
# Metadata values for these types are written as strings in YAML/JSON (column names, SQL expressions, ISO
# dates, DDL schemas, WKT geometries).
_STRING_TYPES = (str, bytes, date, datetime)
_NUMBER_TYPES = (float, Decimal)
# User-facing descriptions shown by editors. The pydantic models' own docstrings describe validator internals.
_MODEL_DESCRIPTIONS = {
    "CheckSpec": "A data quality check.",
    "CheckBlock": "The check function to apply and its arguments.",
}
_FIELD_DESCRIPTIONS = {
    "CheckSpec": {
        "check": "The check function to apply and its arguments.",
        "criticality": "'error' sends failing rows only to the invalid (quarantine) output; 'warn' sends them to both "
        "outputs. Defaults to 'error'.",
        "name": "Name of the check, added to the reported errors and warnings. Generated when not given.",
        "filter": "Spark SQL expression selecting the rows the check applies to.",
        "user_metadata": "Key-value pairs added to the reported errors and warnings.",
        "message_expr": "Spark SQL expression for a custom message reported when the check fails.",
    },
    "CheckBlock": {
        "arguments": "Keyword arguments passed to the check function.",
        "for_each_column": "Columns or column expressions to apply the same check to, one check per column.",
    },
}


def build_checks_json_schema() -> JsonSchemaValue:
    """
    Build the JSON schema (draft-07) for a list of declarative quality checks.

    Every built-in check function (from *check_funcs* and the geo checks) gets its own entry: the function
    name is offered for autocompletion with its description, and its *arguments* are validated against the
    function signature (names, required arguments and basic types). Function names that are not built in,
    such as custom check functions, are still accepted with free-form *arguments*.

    Returns:
        The JSON schema as a dictionary.
    """
    _, definitions = models_json_schema([(CheckSpec, "validation")], ref_template="#/definitions/{model}")
    definitions = definitions.get("$defs", {})
    for model, description in _MODEL_DESCRIPTIONS.items():
        definitions[model]["description"] = description
        for field, field_description in _FIELD_DESCRIPTIONS[model].items():
            definitions[model]["properties"][field]["description"] = field_description
    check_spec = definitions["CheckSpec"]
    check_spec["properties"]["criticality"]["enum"] = [criticality.value for criticality in Criticality]

    functions = _built_in_check_functions()
    check_block = definitions["CheckBlock"]
    check_block["properties"]["function"] = {
        "description": "Name of the check function.",
        "anyOf": [
            *({"const": name, "description": _summary(func)} for name, func in functions.items()),
            {"type": "string"},
        ],
    }
    check_block["allOf"] = [_function_rule(name, func) for name, func in functions.items()]

    return {
        "$schema": JSON_SCHEMA_DRAFT,
        "title": "DQX quality checks",
        "description": "A list of DQX quality checks defined declaratively in YAML or JSON.",
        "type": "array",
        "items": {"$ref": "#/definitions/CheckSpec"},
        "definitions": definitions,
    }


def _built_in_check_functions() -> dict[str, Callable]:
    """Registered check functions from the core and geo check modules, sorted by name.

    Anomaly and PII checks live in optional modules that are not always installed, so they are left out to keep
    the committed schema the same in every environment; like custom functions, they are still accepted by name.
    """
    functions = {}
    for name in sorted(CHECK_FUNC_REGISTRY):
        func = getattr(check_funcs, name, None) or getattr(geo_check_funcs, name, None)
        if func is not None:
            functions[name] = func
    return functions


def _function_rule(name: str, func: Callable) -> JsonSchemaValue:
    """The schema applied to a check block whose *function* is *name*."""
    descriptions = _argument_descriptions(func)
    properties: JsonSchemaValue = {}
    required: list[str] = []
    for param in inspect.signature(func).parameters.values():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        schema = _annotation_schema(param.annotation)
        if param.name in descriptions:
            schema = {**schema, "description": descriptions[param.name]}
        properties[param.name] = schema
        if param.default is inspect.Parameter.empty:
            required.append(param.name)

    column_params = [param for param in required if param in _FOR_EACH_COLUMN_PARAMS]
    other_required = [param for param in required if param not in _FOR_EACH_COLUMN_PARAMS]
    arguments: JsonSchemaValue = {"type": "object", "properties": properties, "additionalProperties": False}
    if other_required:
        arguments["required"] = other_required

    then: JsonSchemaValue = {"description": _summary(func), "properties": {"arguments": arguments}}
    if column_params:
        # The column is given either in *arguments* or by *for_each_column*.
        then["anyOf"] = [
            {"required": ["for_each_column"]},
            {"required": ["arguments"], "properties": {"arguments": {"required": column_params}}},
        ]
    if other_required:
        then["required"] = ["arguments"]
    return {"if": {"properties": {"function": {"const": name}}, "required": ["function"]}, "then": then}


def _annotation_schema(annotation: object) -> JsonSchemaValue:
    """
    Map a check function parameter annotation to the JSON schema of the value written in YAML/JSON.

    Spark *Column* and other types without a JSON form are written as strings; anything unknown is left
    unconstrained.
    """
    if annotation is inspect.Parameter.empty:
        return {}
    origin = get_origin(annotation)
    if origin is types.UnionType:
        options: list[JsonSchemaValue] = []
        for arg in get_args(annotation):
            schema = _annotation_schema(arg)
            if not schema:
                return {}
            if schema not in options:
                options.append(schema)
        return options[0] if len(options) == 1 else {"anyOf": options}
    if origin is Literal:
        return {"enum": list(get_args(annotation))}
    if origin is list or annotation is list:
        args = get_args(annotation)
        items = _annotation_schema(args[0]) if args else {}
        return {"type": "array", "items": items} if items else {"type": "array"}
    if origin is dict or annotation is dict:
        return {"type": "object"}
    return _scalar_schema(annotation)


def _scalar_schema(annotation: object) -> JsonSchemaValue:
    if annotation is type(None):
        return {"type": "null"}
    if annotation is bool:
        return {"type": "boolean"}
    if annotation is int:
        return {"type": "integer"}
    if isinstance(annotation, type) and issubclass(annotation, _NUMBER_TYPES):
        return {"type": "number"}
    if isinstance(annotation, type) and (
        issubclass(annotation, _STRING_TYPES) or annotation.__name__ in {"Column", "StructType"}
    ):
        return {"type": "string"}
    return {}


def _summary(func: Callable) -> str:
    """The first paragraph of a function's docstring, on one line."""
    doc = inspect.getdoc(func) or ""
    return _plain_text(" ".join(doc.split("\n\n", 1)[0].split()))


def _plain_text(text: str) -> str:
    """Remove docstring emphasis markers so editors show the description as plain text."""
    text = _BOLD_RE.sub(r"\1", text)
    text = _ITALIC_NAME_RE.sub(r"\1", text)
    text = _ITALIC_SPAN_RE.sub(r"\1", text)
    return _ITALIC_PHRASE_RE.sub(r"\1", text)


def _argument_descriptions(func: Callable) -> dict[str, str]:
    """Parameter descriptions from the *Args* section of a Google-style docstring."""
    descriptions: dict[str, str] = {}
    in_args = False
    current: str | None = None
    for line in (inspect.getdoc(func) or "").splitlines():
        stripped = line.strip()
        if _DOCSTRING_SECTION_RE.match(stripped):
            in_args = stripped == "Args:"
            current = None
            continue
        if not in_args or not stripped:
            continue
        match = _DOCSTRING_ARG_RE.match(stripped)
        if match and len(line) - len(line.lstrip()) <= 4:
            current = match.group(1).lstrip("*")
            descriptions[current] = match.group(2)
        elif current:
            descriptions[current] = f"{descriptions[current]} {stripped}".strip()
    return {name: _plain_text(description) for name, description in descriptions.items()}
