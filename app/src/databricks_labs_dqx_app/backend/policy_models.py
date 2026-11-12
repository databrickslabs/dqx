"""Domain models for rule policies.

A rule policy describes which tables (and which columns on them) a registry
rule should be auto-applied to. Scope narrows by catalog, schema or table
name; the table condition and per-slot column conditions narrow further by
tags or by a small condition builder.
"""

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from databricks_labs_dqx_app.backend.sql_utils import validate_fqn, validate_identifier

ConditionObject = Literal["catalog", "schema", "table", "column"]
Predicate = Literal["has_tag", "not_has_tag", "has_name", "name_like", "has_owner"]
Combinator = Literal["AND", "OR"]
ALL = "*"

_MAX_TAG_LEN = 255


def _has_control_chars(value: str) -> bool:
    return any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value)


class ConditionRow(BaseModel):
    """One row of a condition builder: *object* *predicate* *value*."""

    combinator: Combinator | None = None
    object: ConditionObject
    predicate: Predicate
    value: str = Field(min_length=1, max_length=512)

    @field_validator("value")
    @classmethod
    def _value_has_no_control_chars(cls, value: str) -> str:
        if _has_control_chars(value):
            raise ValueError("Condition value must not contain control characters.")
        return value

    @model_validator(mode="after")
    def _column_has_no_owner(self) -> "ConditionRow":
        if self.object == "column" and self.predicate == "has_owner":
            raise ValueError("Columns do not have owners; has_owner is not valid for column rows.")
        return self


class ConditionExpr(BaseModel):
    """An ordered list of condition rows joined by their combinators."""

    rows: list[ConditionRow] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _validate_combinators(self) -> "ConditionExpr":
        for index, row in enumerate(self.rows):
            if index == 0 and row.combinator is not None:
                raise ValueError("The first condition row must not have a combinator.")
            if index > 0 and row.combinator is None:
                raise ValueError("Every condition row after the first needs a combinator.")
        return self


def _validate_tags(tags: list[str]) -> list[str]:
    for tag in tags:
        key = tag.split("=", 1)[0]
        if not key or len(key) > _MAX_TAG_LEN or _has_control_chars(tag):
            raise ValueError("Tags must be 'key' or 'key=value' with a non-empty key of at most 255 characters.")
    return tags


class TableCondition(BaseModel):
    """Table-level condition: none, a list of tags, or a condition builder."""

    kind: Literal["none", "tags", "builder"] = "none"
    tags: list[str] = Field(default_factory=list)
    expr: ConditionExpr | None = None

    @field_validator("tags")
    @classmethod
    def _tags_valid(cls, tags: list[str]) -> list[str]:
        return _validate_tags(tags)

    @model_validator(mode="after")
    def _validate_kind(self) -> "TableCondition":
        if self.kind == "tags" and not self.tags:
            raise ValueError("A tags condition needs at least one tag.")
        if self.kind == "builder":
            if self.expr is None:
                raise ValueError("A builder condition needs an expression.")
            if any(row.object == "column" for row in self.expr.rows):
                raise ValueError("Table conditions cannot use column rows.")
        return self


class ColumnCondition(BaseModel):
    """Column-level condition for one rule slot: tags or a condition builder."""

    kind: Literal["tags", "builder"] = "tags"
    tags: list[str] = Field(default_factory=list)
    expr: ConditionExpr | None = None

    @field_validator("tags")
    @classmethod
    def _tags_valid(cls, tags: list[str]) -> list[str]:
        return _validate_tags(tags)

    @model_validator(mode="after")
    def _validate_kind(self) -> "ColumnCondition":
        if self.kind == "tags" and not self.tags:
            raise ValueError("A tags condition needs at least one tag.")
        if self.kind == "builder":
            if self.expr is None:
                raise ValueError("A builder condition needs an expression.")
            if any(row.object != "column" for row in self.expr.rows):
                raise ValueError("Column conditions must use column rows only.")
        return self


class PolicyScope(BaseModel):
    """Where a policy applies: catalogs, schemas (cat.sch) and tables (cat.sch.tbl)."""

    catalogs: list[str] | Literal["*"] = ALL
    schemas: list[str] | Literal["*"] = ALL
    tables: list[str] | Literal["*"] = ALL

    @field_validator("catalogs", "schemas", "tables")
    @classmethod
    def _non_empty_list(cls, value: list[str] | str) -> list[str] | str:
        if isinstance(value, list) and not value:
            raise ValueError("Scope lists must not be empty; use '*' for everything.")
        return value

    @field_validator("catalogs")
    @classmethod
    def _catalogs_valid(cls, value: list[str] | str) -> list[str] | str:
        if isinstance(value, list):
            for name in value:
                validate_identifier(name)
        return value

    @field_validator("schemas")
    @classmethod
    def _schemas_valid(cls, value: list[str] | str) -> list[str] | str:
        if isinstance(value, list):
            for schema in value:
                parts = schema.split(".")
                if len(parts) != 2:
                    raise ValueError(f"Invalid schema '{schema}'. Expected catalog.schema.")
                for part in parts:
                    validate_identifier(part)
        return value

    @field_validator("tables")
    @classmethod
    def _tables_valid(cls, value: list[str] | str) -> list[str] | str:
        if isinstance(value, list):
            for table in value:
                validate_fqn(table)
        return value


class RulePolicyIn(BaseModel):
    """Request body for creating or updating a rule policy."""

    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    scope: PolicyScope = Field(default_factory=PolicyScope)
    table_condition: TableCondition = Field(default_factory=TableCondition)
    column_conditions: dict[str, ColumnCondition] = Field(default_factory=dict)
    register_tables: bool = False

    @field_validator("name", mode="before")
    @classmethod
    def _strip_name(cls, value: object) -> object:
        # Strip before the length checks so a whitespace-only name is rejected.
        return value.strip() if isinstance(value, str) else value


class RulePolicy(RulePolicyIn):
    """A stored rule policy."""

    policy_id: str
    rule_id: str
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None
