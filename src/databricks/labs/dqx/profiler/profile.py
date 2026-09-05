from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from databricks.labs.dqx.profiler.semantic import DQProfileContext


@dataclass(frozen=True)
class DQProfile:
    """Data quality profile class representing a data quality rule candidate.

    Attributes:
        name: Profile name (e.g. *is_not_null_or_empty*, *min_max*).
        column: Column the profile applies to.
        description: Optional human-readable description.
        parameters: Optional parameters that specialise the rule.
        filter: Optional filter expression scoping the rule.
        semantic_type: Optional detected semantic type name (e.g. *enum*, *key*,
            *measurement*, *text*) — records *why* the profile was generated
            when semantic profiling is enabled. Defaults to *None*, so
            pre-existing YAML/JSON round-trips are unaffected.
    """

    name: str
    column: str
    description: str | None = None
    parameters: dict[str, Any] | None = None
    filter: str | None = None
    semantic_type: str | None = None


# Legacy 5-argument callback shape used by pre-semantic profile builders. Kept for backward
# compatibility with user-authored builders registered via @register_profile_builder without
# kind="context". Prefer *ContextualProfileBuilder* for new code.
ProfileBuilder = Callable[
    [DataFrame, str, DataType, dict[str, Any], dict[str, Any]],
    DQProfile | None,
]

# Preferred single-argument callback shape. Receives *DQProfileContext* so it can inspect
# *ctx.semantic_type* and gate its output on the detected type — giving the builder access
# to the detected semantic type and other profiling metadata.
ContextualProfileBuilder = Callable[[DQProfileContext], DQProfile | None]


class DQProfileBuilder(BaseModel):
    """Named builder that may produce a *DQProfile* for a column.

    Exactly one of *builder* or *contextual_builder* must be provided.

    Attributes:
        name: Profile type identifier (e.g. *min_max*). Used to look up the
            builder in the registry and in generated rule metadata.
        builder: Legacy 5-argument callback. Left in place for backward
            compatibility so existing user-authored builders keep working
            when wrapped in *DQProfileBuilder* or registered via
            *@register_profile_builder* without *kind="context"*. Does not
            receive semantic-type information — prefer *contextual_builder*
            for new code.
        contextual_builder: Preferred single-argument callback receiving a
            *DQProfileContext*. Sees the detected semantic type via
            *ctx.semantic_type* and can gate its output accordingly.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    builder: ProfileBuilder | None = None
    contextual_builder: ContextualProfileBuilder | None = None

    @model_validator(mode="after")
    def _exactly_one_callback(self) -> "DQProfileBuilder":
        if (self.builder is None) == (self.contextual_builder is None):
            raise ValueError("DQProfileBuilder requires exactly one of `builder` or `contextual_builder`.")
        return self
