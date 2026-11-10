from enum import Enum

from open_data_contract_standard.model import DataQuality  # type: ignore

from databricks.labs.dqx.utils import sanitize_for_logging


class SkipReason(Enum):
    """Reasons a quality entry does not become a rule, logged when the entry is skipped."""

    TEXT_NO_LLM_ENGINE = (
        "text rules need an LLM engine; install the LLM extras with pip install 'databricks-labs-dqx[llm]'"
    )
    TEXT_NO_DESCRIPTION = "text rules need a description"
    SQL_UNSUPPORTED = "type: sql rules are not supported yet"
    CUSTOM_NO_ENGINE = "type: custom needs engine: dqx"
    CUSTOM_ENGINE_UNSUPPORTED = "engine '{engine}' is not supported; only engine: dqx is supported"
    CUSTOM_NO_CHECK = "engine: dqx rules need an implementation with a 'check'"
    TYPE_MISSING = "quality entries need a type (library, custom or text) or a metric"
    TYPE_UNSUPPORTED = "this quality type is not supported; use library, custom (engine: dqx) or text"
    NESTED_PROPERTY_UNSUPPORTED = "quality entries on nested properties are not supported yet"

    def message(self, quality_rule: DataQuality) -> str:
        """Return the reason as logged for a quality entry.

        Args:
            quality_rule: The skipped quality entry.

        Returns:
            The reason text, naming the entry's engine when the reason refers to it.
        """
        if self is SkipReason.CUSTOM_ENGINE_UNSUPPORTED:
            return self.value.format(engine=sanitize_for_logging(str(quality_rule.engine)))
        return self.value
