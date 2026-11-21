"""End-to-end integration tests for *DQGenerator.generate_dq_rules_ai_assisted*.

Exercises the public signature and the plumbing from the generator into the LLM engine:
*user_input*, *summary_stats*, and *uc_metadata* are three independent context inputs,
combined in the prompt when supplied. Location resolution (reading schemas from UC vs.
Spark, enrichment via *UnityCatalogMetadataConfig*) lives in *test_collect_uc_metadata.py*.
"""

from types import SimpleNamespace

import pytest

from databricks.labs.dqx.errors import MissingParameterError
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.unity_catalog_metadata import UnityCatalogMetadata


def _stub_llm_engine(captured: dict) -> SimpleNamespace:
    """LLM engine double that records every kwarg the generator forwarded to the engine."""

    def detect_business_rules_with_llm(**kwargs) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(quality_rules="[]", reasoning="test")

    return SimpleNamespace(detect_business_rules_with_llm=detect_business_rules_with_llm)


def test_generate_forwards_uc_metadata_schema_json_to_engine(mock_workspace_client):
    captured: dict = {}
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine(captured)

    uc_metadata = UnityCatalogMetadata(
        table="main.default.users",
        columns=[{"name": "id", "type": "string"}],
    )
    generator.generate_dq_rules_ai_assisted(
        user_input="id must be unique",
        uc_metadata=uc_metadata,
    )

    assert captured["schema_info"] == '{"table": "main.default.users", "columns": [{"name": "id", "type": "string"}]}'
    assert captured["user_input"] == "id must be unique"
    assert captured["summary_stats"] is None


def test_generate_forwards_all_three_context_inputs_when_present(mock_workspace_client):
    """user_input, summary_stats, and uc_metadata are combined — not mutually exclusive."""
    captured: dict = {}
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine(captured)

    uc_metadata = UnityCatalogMetadata(columns=[])
    stats = {"profile": {"metrics": {"count": 100}}}
    generator.generate_dq_rules_ai_assisted(
        user_input="generate rules",
        summary_stats=stats,
        uc_metadata=uc_metadata,
    )

    assert captured["user_input"] == "generate rules"
    assert captured["summary_stats"] == stats
    assert captured["schema_info"] == '{"columns": []}'


def test_generate_without_uc_metadata_passes_empty_schema_info(mock_workspace_client):
    """With *uc_metadata=None* the engine receives schema_info=\"\" (LLM-side inference)."""
    captured: dict = {}
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine(captured)

    generator.generate_dq_rules_ai_assisted(user_input="age must be positive")

    assert captured["schema_info"] == ""


def test_generate_rejects_uc_metadata_only_calls(mock_workspace_client):
    """*uc_metadata* on its own is context, not intent — raise instead of silently returning []."""
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine({})

    with pytest.raises(MissingParameterError, match="summary statistics or user input"):
        generator.generate_dq_rules_ai_assisted(uc_metadata=UnityCatalogMetadata(columns=[]))


def test_generate_rejects_fully_empty_calls(mock_workspace_client):
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine({})

    with pytest.raises(MissingParameterError, match="summary statistics or user input"):
        generator.generate_dq_rules_ai_assisted()
