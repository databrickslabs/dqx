import pandas as pd

from databricks.labs.dqx.table_manager import (
    ColumnDefinitionExtractor,
    ColumnStatisticsFormatter,
    PropertyMetadataFormatter,
    TableDefinitionBuilder,
    TableManager,
)


class _StubRepository:
    def __init__(self, describe_df: pd.DataFrame, props_df: pd.DataFrame, stats_df: pd.DataFrame, columns: list[str]):
        self._describe_df = describe_df
        self._props_df = props_df
        self._stats_df = stats_df
        self._columns = columns
        self.executed_queries: list[str] = []

    def get_table_columns(self, _table: str) -> pd.DataFrame:
        return self._describe_df

    def get_existing_primary_key(self, _table: str) -> str | None:
        return "PRIMARY KEY (id)"

    def get_table_properties(self, _table: str) -> pd.DataFrame:
        return self._props_df

    def get_table_foreign_keys(self, _table: str) -> dict[str, dict[str, object]]:
        return {}

    def get_column_statistics(self, _table: str) -> pd.DataFrame:
        return self._stats_df

    def get_table_column_names(self, _table: str) -> list[str]:
        return list(self._columns)

    def execute_query(self, query: str):
        self.executed_queries.append(query)
        return query


def test_column_definition_extractor_and_builder():
    describe_df = pd.DataFrame(
        [
            {"col_name": "id", "data_type": "bigint", "comment": "not null"},
            {"col_name": "name", "data_type": "string", "comment": ""},
            {"col_name": "# Partition Information", "data_type": "", "comment": ""},
            {"col_name": "", "data_type": "", "comment": ""},
        ]
    )
    columns = ColumnDefinitionExtractor.extract_columns(describe_df)
    assert columns == ["    id bigint NOT NULL", "    name string"]

    definition = TableDefinitionBuilder().add_columns(columns).add_primary_key("PRIMARY KEY (id)").build()
    assert "PRIMARY KEY (id)" in definition


def test_metadata_formatters():
    props_df = pd.DataFrame(
        [
            {"key": "numRows", "value": "100"},
            {"key": "randomKey", "value": "ignore"},
            {"key": "totalSize", "value": "2048"},
        ]
    )
    stats_df = pd.DataFrame(
        [
            {"col_name": "amount", "data_type": "double"},
            {"col_name": "name", "data_type": "string"},
            {"col_name": "created_at", "data_type": "timestamp"},
            {"col_name": "# partition", "data_type": ""},
        ]
    )

    prop_lines = PropertyMetadataFormatter().format(props_df)
    assert prop_lines == ["numRows: 100", "totalSize: 2048"]

    stats_lines = ColumnStatisticsFormatter().format(stats_df)
    assert stats_lines[0] == "Column type distribution:"
    assert "Numeric columns (1)" in stats_lines[1]
    assert "String columns (1)" in stats_lines[2]


def test_table_manager_definition_and_metadata():
    describe_df = pd.DataFrame([{"col_name": "id", "data_type": "bigint", "comment": ""}])
    props_df = pd.DataFrame([{"key": "numRows", "value": "10"}])
    stats_df = pd.DataFrame([{"col_name": "id", "data_type": "bigint"}])
    repo = _StubRepository(describe_df, props_df, stats_df, columns=["id"])

    manager = TableManager(repository=repo)
    definition = manager.get_table_definition("catalog.schema.table")
    assert "id bigint" in definition

    metadata = manager.get_table_metadata_info("catalog.schema.table")
    assert metadata.startswith("Metadata information:")

    assert manager.get_table_column_names("catalog.schema.table") == ["id"]

    assert manager.run_sql("SELECT 1") == "SELECT 1"


def test_table_manager_metadata_handles_exceptions():
    describe_df = pd.DataFrame([{"col_name": "id", "data_type": "bigint", "comment": ""}])
    props_df = pd.DataFrame([])
    stats_df = pd.DataFrame([])
    repo = _StubRepository(describe_df, props_df, stats_df, columns=["id"])

    def _raise(*_args, **_kwargs):
        raise ValueError("boom")

    repo.get_table_properties = _raise  # type: ignore[assignment]
    repo.get_column_statistics = _raise  # type: ignore[assignment]

    manager = TableManager(repository=repo)
    assert manager.get_table_metadata_info("catalog.schema.table") == "Limited metadata available"
