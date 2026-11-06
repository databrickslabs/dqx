"""Pydantic models and enums for Saved Views (named column/filter presets)."""

from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints, field_validator, model_validator

MAX_KEYS = 64
MAX_STRING_LENGTH = 512
MAX_LIST_ITEMS = 200
MAX_PAYLOAD_JSON_CHARS = 16_384


class SavedViewPage(str, Enum):
    """Pages that support saved views (allowlist for the page key)."""

    MONITORED_TABLES = "monitored_tables"
    REGISTRY_RULES = "registry_rules"
    COLLECTIONS = "collections"
    SCHEDULES = "schedules"
    GLOBAL_RESULTS = "global_results"


class SavedViewScope(str, Enum):
    """Who a view is visible to."""

    ORG = "org"
    USER = "user"


class SavedViewLayout(BaseModel):
    """Order and visibility of columns or filters."""

    order: list[Annotated[str, StringConstraints(max_length=MAX_STRING_LENGTH)]] = Field(
        default_factory=list, max_length=MAX_KEYS
    )
    visibility: dict[str, bool] = Field(default_factory=dict)

    @field_validator("visibility")
    @classmethod
    def _check_visibility(cls, value: dict[str, bool]) -> dict[str, bool]:
        if len(value) > MAX_KEYS:
            raise ValueError(f"visibility may hold at most {MAX_KEYS} keys")
        if any(len(key) > MAX_STRING_LENGTH for key in value):
            raise ValueError(f"visibility keys may be at most {MAX_STRING_LENGTH} characters")
        return value


class SavedViewPayload(BaseModel):
    """Versioned view state: column layout, filter layout and filter values."""

    v: Literal[1] = 1
    columns: SavedViewLayout | None = None
    filters: SavedViewLayout = Field(default_factory=SavedViewLayout)
    values: dict[str, str | list[str] | None] = Field(default_factory=dict)

    @field_validator("values")
    @classmethod
    def _check_values(cls, value: dict[str, str | list[str] | None]) -> dict[str, str | list[str] | None]:
        if len(value) > MAX_KEYS:
            raise ValueError(f"values may hold at most {MAX_KEYS} keys")
        for key, item in value.items():
            if len(key) > MAX_STRING_LENGTH:
                raise ValueError(f"value keys may be at most {MAX_STRING_LENGTH} characters")
            if isinstance(item, str):
                if len(item) > MAX_STRING_LENGTH:
                    raise ValueError(f"string values may be at most {MAX_STRING_LENGTH} characters")
            elif isinstance(item, list):
                if len(item) > MAX_LIST_ITEMS:
                    raise ValueError(f"list values may hold at most {MAX_LIST_ITEMS} items")
                if any(len(entry) > MAX_STRING_LENGTH for entry in item):
                    raise ValueError(f"list items may be at most {MAX_STRING_LENGTH} characters")
        return value

    @model_validator(mode="after")
    def _check_size(self) -> "SavedViewPayload":
        if len(self.model_dump_json()) > MAX_PAYLOAD_JSON_CHARS:
            raise ValueError(f"serialised payload may be at most {MAX_PAYLOAD_JSON_CHARS} characters")
        return self


class SavedViewOut(BaseModel):
    id: str
    page: SavedViewPage
    name: str
    scope: SavedViewScope
    payload: SavedViewPayload | None
    is_builtin: bool
    is_org_default: bool
    is_my_default: bool
    starred: bool
    can_edit: bool
    can_set_org_default: bool


class SavedViewListOut(BaseModel):
    views: list[SavedViewOut]
    effective_default_id: str


class SavedViewCreateIn(BaseModel):
    page: SavedViewPage
    name: str
    scope: SavedViewScope = SavedViewScope.USER
    payload: SavedViewPayload
    make_my_default: bool = False
    make_org_default: bool = False


class SavedViewUpdateIn(BaseModel):
    payload: SavedViewPayload


class SavedViewRenameIn(BaseModel):
    name: str


class SavedViewMyDefaultIn(BaseModel):
    page: SavedViewPage
    view_id: str | None


class SavedViewOrgDefaultIn(BaseModel):
    page: SavedViewPage
    view_id: str
