import logging
import json
from collections.abc import Callable

import dspy  # type: ignore
from databricks.labs.dqx.llm.llm_core import DQLLMCore
from databricks.labs.dqx.llm.llm_utils import get_required_check_functions_info


logger = logging.getLogger(__name__)


class DQLLMEngine:
    def __init__(
        self,
        model: str,
        api_key: str = "",
        api_base: str = "",
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        self.available_check_functions = json.dumps(get_required_check_functions_info(custom_check_functions))
        self.core = DQLLMCore(model, api_key, api_base, custom_check_functions)
        logger.info(f"LLM compiler initialized with model: {model}")

    def get_business_rules_with_llm(
        self, user_input: str, schema_info: str = ""
    ) -> dspy.primitives.prediction.Prediction:
        """
        Get DQX rules (checks) based on natural language request with optional schema information
        using pre-initialized LLM compiler.

        If schema_info is empty (default), it will automatically infer the schema from the user_input
        before generating rules.

        Args:
            user_input: Natural language description of data quality requirements.
            schema_info: Optional JSON string containing table schema with column names and types.
                        If empty (default), triggers schema inference. If provided, uses that schema directly.

        Returns:
            A Prediction object containing:
                - quality_rules: The generated DQ rules
                - reasoning: Explanation of the rules (includes schema inference info if schema was inferred)
                - guessed_schema_json: The inferred schema (if schema was inferred)
                - assumptions_bullets: Assumptions made about schema (if schema was inferred)
                - schema_info: The final schema used (if schema was inferred)
        """
        return self.core.compiler(
            schema_info=schema_info,
            business_description=user_input,
            available_functions=self.available_check_functions,
        )
