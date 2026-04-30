from __future__ import annotations

import json
import logging
import re
from importlib.resources import files
from pathlib import Path
from typing import Any, ClassVar

import yaml
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks  # type: ignore[import-untyped]
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage

from databricks.labs.dqx.llm.llm_utils import get_required_check_functions_definitions

from databricks_labs_dqx_app.backend.config import conf

logger = logging.getLogger(__name__)

_SYSTEM_TEMPLATE = """\
You are a data quality rule generator. Given a table schema and a business description, \
generate DQX data quality rules using the available check functions.

Return ONLY a JSON object with two fields:
  - "quality_rules": a valid JSON array of rule objects
  - "reasoning": a short explanation of why these rules were chosen

Rule format:
  {{"criticality": "error"|"warn", "check": {{"function": "<name>", "arguments": {{...}}}}, "filter": "<optional SQL>"}}

Guidelines:
- Use double quotes for all JSON keys and string values.
- For string literal values inside check arguments wrap them in single quotes.
- In SQL filter expressions use single quotes for string literals and capitalise SQL keywords.
- Use "filter" only when the rule applies to a subset of rows.

Available check functions:
{available_functions}"""


class AiRulesService:
    """Generates DQX rules using ChatDatabricks with the OBO WorkspaceClient.

    The few-shot prompt and available-functions list are built once (ClassVar) and
    reused across requests. Only the schema lookup and LLM call are per-request.
    """

    _few_shot_messages: ClassVar[list[BaseMessage] | None] = None
    _available_functions: ClassVar[str | None] = None

    def __init__(self, obo_ws: WorkspaceClient, sp_ws: WorkspaceClient) -> None:
        self._obo_ws = obo_ws  # user identity — UC table access
        self._sp_ws = sp_ws  # service principal — Foundation Model serving scope

    # ------------------------------------------------------------------
    # Class-level prompt construction (once per process)
    # ------------------------------------------------------------------

    @classmethod
    def _get_available_functions(cls) -> str:
        if cls._available_functions is None:
            cls._available_functions = json.dumps(get_required_check_functions_definitions())
        return cls._available_functions

    @classmethod
    def _get_few_shot_messages(cls) -> list[BaseMessage]:
        if cls._few_shot_messages is None:
            cls._few_shot_messages = cls._build_few_shot_messages()
        return cls._few_shot_messages

    @classmethod
    def _build_few_shot_messages(cls) -> list[BaseMessage]:
        resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))
        examples: list[dict[str, Any]] = yaml.safe_load(resource.read_text(encoding="utf-8"))
        messages: list[BaseMessage] = []
        for ex in examples:
            human = HumanMessage(
                content=f"schema_info: {json.dumps(ex['schema_info'])}\n"
                f"business_description: {ex['business_description']}"
            )
            ai = AIMessage(
                content=json.dumps(
                    {"quality_rules": json.loads(ex["quality_rules"]), "reasoning": ex["reasoning"]},
                    indent=None,
                )
            )
            messages.extend([human, ai])
        return messages

    # ------------------------------------------------------------------
    # Per-request helpers
    # ------------------------------------------------------------------

    def _get_schema_info(self, table_fqn: str) -> str:
        table_info = self._obo_ws.tables.get(table_fqn)
        columns = [{"name": col.name or "", "type": col.type_text or ""} for col in (table_info.columns or [])]
        return json.dumps({"columns": columns})

    def _parse_response(self, content: str) -> list[dict[str, Any]]:
        for text in self._extract_json_candidates(content):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict) and "quality_rules" in parsed:
                    return parsed["quality_rules"]
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                continue
        logger.warning("Could not parse LLM response as JSON: %.500s", content)
        return []

    @staticmethod
    def _extract_json_candidates(content: str) -> list[str]:
        """Return candidate JSON strings from an LLM response, most specific first."""
        candidates = [content]
        code_block = re.search(r"```(?:json)?\s*\n?(.*?)```", content, re.DOTALL)
        if code_block:
            candidates.append(code_block.group(1).strip())
        brace_match = re.search(r"\{.*\}", content, re.DOTALL)
        if brace_match:
            candidates.append(brace_match.group(0))
        return candidates

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, user_input: str, table_fqn: str | None = None) -> list[dict[str, Any]]:
        """Generate DQX quality rules from natural language.

        Args:
            user_input: Natural language description of data quality requirements.
            table_fqn: Optional fully-qualified table name for schema context.

        Returns:
            List of DQX rule dicts.
        """
        system = SystemMessage(content=_SYSTEM_TEMPLATE.format(available_functions=self._get_available_functions()))
        schema_info = self._get_schema_info(table_fqn) if table_fqn else ""
        human = HumanMessage(content=f"schema_info: {schema_info}\nbusiness_description: {user_input}")
        messages: list[BaseMessage] = [system, *self._get_few_shot_messages(), human]

        llm = ChatDatabricks(endpoint=conf.llm_endpoint, workspace_client=self._sp_ws)
        response = llm.invoke(messages)
        return self._parse_response(str(response.content))
