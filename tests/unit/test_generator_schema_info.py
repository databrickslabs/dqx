import json
from types import SimpleNamespace

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableInfo

from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.profiler.generator import DQGenerator

UC_COLUMNS = {"columns": [{"name": "user_id", "type": "string"}, {"name": "age", "type": "int"}]}


@pytest.fixture
def uc_workspace_client(mock_workspace_client):
    """Workspace client returning a two-column Unity Catalog table."""
    mock_workspace_client.tables.get.return_value = TableInfo(
        columns=[
            ColumnInfo(name="user_id", type_text="string"),
            ColumnInfo(name="age", type_text="int"),
        ]
    )
    return mock_workspace_client


def _stub_llm_engine(captured: dict) -> SimpleNamespace:
    """LLM engine double that records into *captured* the schema context it was given."""

    def detect_business_rules_with_llm(**kwargs) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(quality_rules="[]", reasoning="test")

    return SimpleNamespace(detect_business_rules_with_llm=detect_business_rules_with_llm)


def test_generate_for_uc_table_reads_schema_without_spark(uc_workspace_client):
    """Generating rules for a Unity Catalog table must not require a Spark session."""
    captured = {}
    generator = DQGenerator(workspace_client=uc_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine(captured)

    generator.generate_dq_rules_ai_assisted(
        user_input="age must be positive", input_config=InputConfig(location="main.default.users")
    )

    assert json.loads(captured["schema_info"]) == UC_COLUMNS
    uc_workspace_client.tables.get.assert_called_once_with("main.default.users")


def test_generate_for_backticked_uc_table_reads_schema_without_spark(uc_workspace_client):
    """Special-char UC names are still UC tables, so they must take the workspace client path."""
    captured = {}
    generator = DQGenerator(workspace_client=uc_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine(captured)

    generator.generate_dq_rules_ai_assisted(
        user_input="age must be positive", input_config=InputConfig(location="`my-catalog`.`my-schema`.users")
    )

    assert json.loads(captured["schema_info"]) == UC_COLUMNS
    uc_workspace_client.tables.get.assert_called_once_with("my-catalog.my-schema.users")


@pytest.mark.parametrize("location", ["not a location", "users", "main..users"])
def test_generate_rejects_unsupported_input_location(mock_workspace_client, location):
    """An unclassifiable location fails with a clear error instead of falling through to Spark."""
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine({})

    with pytest.raises(InvalidConfigError, match="Invalid input location"):
        generator.generate_dq_rules_ai_assisted(
            user_input="age must be positive", input_config=InputConfig(location=location)
        )

    assert not mock_workspace_client.tables.get.called


def test_generate_for_storage_path_reads_schema_with_spark(mock_workspace_client, mock_spark):
    """A storage path cannot be resolved through Unity Catalog, so Spark reads it."""
    captured = {}
    mock_spark.read.options.return_value.load.return_value.schema.fields = []
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
    generator.llm_engine = _stub_llm_engine(captured)

    generator.generate_dq_rules_ai_assisted(
        user_input="age must be positive",
        input_config=InputConfig(location="s3://bucket/data", format="parquet"),
    )

    assert json.loads(captured["schema_info"]) == {"columns": []}
    assert not mock_workspace_client.tables.get.called


def test_generate_for_two_level_table_name_reads_schema_with_spark(mock_workspace_client, mock_spark):
    """Two-level names resolve against the session catalog, which only Spark can do."""
    captured = {}
    mock_spark.read.options.return_value.table.return_value.schema.fields = []
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
    generator.llm_engine = _stub_llm_engine(captured)

    generator.generate_dq_rules_ai_assisted(
        user_input="age must be positive", input_config=InputConfig(location="default.users")
    )

    assert json.loads(captured["schema_info"]) == {"columns": []}
    assert not mock_workspace_client.tables.get.called


def test_generate_without_input_config_sends_no_schema(mock_workspace_client):
    """With no input config the LLM infers the schema, so neither backend is consulted."""
    captured = {}
    generator = DQGenerator(workspace_client=mock_workspace_client, spark=None)
    generator.llm_engine = _stub_llm_engine(captured)

    generator.generate_dq_rules_ai_assisted(user_input="age must be positive")

    assert captured["schema_info"] == ""
    assert not mock_workspace_client.tables.get.called
