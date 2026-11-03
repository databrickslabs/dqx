from enum import Enum
from typing import NamedTuple


class SkipReason(Enum):
    """Reasons a quality entry does not become a rule, logged when the entry is skipped."""

    LIBRARY_DISABLED = "process_library_rules is False"
    TEXT_NO_LLM_ENGINE = (
        "text rules need an LLM engine; install the LLM extras with pip install 'databricks-labs-dqx[llm]'"
    )
    TEXT_NO_DESCRIPTION = "text rules need a description"
    SQL_UNSUPPORTED = "type: sql rules are not supported yet"
    CUSTOM_ENGINE_UNSUPPORTED = "engine '{engine}' is not supported; only engine: dqx is supported"
    CUSTOM_NO_CHECK = "engine: dqx rules need an implementation with a 'check'"
    TYPE_UNSUPPORTED = "this quality type is not supported; use library, custom (engine: dqx) or text"


class FormatCheck(NamedTuple):
    """Check generated for a logicalTypeOptions format."""

    function: str
    format_argument: str
    rule_name_suffix: str
