import logging
from abc import ABC, abstractmethod
from typing import Protocol
from pandas import DataFrame  # type: ignore
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class TableDataProvider(Protocol):
    """
    Protocol defining the interface for table data access operations.
    """

    def get_table_columns(self, table: str) -> DataFrame:
        """
        Retrieve table column definitions.

        Args:
            table: Fully qualified table name.

        Returns:
            DataFrame with columns: col_name, data_type, comment.
        """

    def get_existing_primary_key(self, table: str) -> str | None:
        """
        Retrieve existing primary key constraint from table properties.

        Args:
            table: Fully qualified table name.

        Returns:
            Primary key constraint string if exists, None otherwise.
        """

    def get_table_properties(self, table: str) -> DataFrame:
        """
        Retrieve table properties/metadata.

        Args:
            table: Fully qualified table name.

        Returns:
            DataFrame with columns: key, value containing table properties.
        """

    def get_column_statistics(self, table: str) -> DataFrame:
        """
        Retrieve column-level statistics and metadata.

        Args:
            table: Fully qualified table name.

        Returns:
            DataFrame with columns: col_name, data_type, and other stats.
        """

    def get_table_column_names(self, table: str) -> list[str]:
        """
        Get list of column names for a table.

        Args:
            table: Fully qualified table name.

        Returns:
            List of column names.
        """

    def execute_query(self, query: str) -> DataFrame:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string.

        Returns:
            DataFrame containing query results.

        Raises:
            ValueError: If query execution fails.
        """


class SparkTableDataProvider:
    """
    Spark implementation of the TableDataProvider protocol.

    This class encapsulates all Spark SQL operations for table metadata retrieval,
    providing a clean interface for accessing table data and structure.

    Attributes:
        spark: SparkSession instance for executing SQL queries.
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        """
        Initialize the Spark data provider.

        Args:
            spark: SparkSession instance. If None, gets or creates a session.
        """
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

    def get_table_columns(self, table: str) -> DataFrame:
        """
        Retrieve table column definitions from DESCRIBE TABLE EXTENDED.

        Args:
            table: Fully qualified table name.

        Returns:
            Pandas DataFrame with columns: col_name, data_type, comment.

        Raises:
            ValueError: If table is not found.
            TypeError: If there's a type error in processing.
        """
        describe_query = f"DESCRIBE TABLE EXTENDED {table}"
        describe_result = self.spark.sql(describe_query)
        return describe_result.toPandas()

    def get_existing_primary_key(self, table: str) -> str | None:
        """
        Retrieve existing primary key from table properties.

        Args:
            table: Fully qualified table name.

        Returns:
            Primary key constraint string if exists, None otherwise.
        """
        try:
            pk_query = f"SHOW TBLPROPERTIES {table}"
            pk_result = self.spark.sql(pk_query)
            pk_df = pk_result.toPandas()

            for _, row in pk_df.iterrows():
                if 'primary' in str(row.get('key', '')).lower():
                    return row.get('value', '')
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            pass
        return None

    def get_table_properties(self, table: str) -> DataFrame:
        """
        Retrieve table properties using SHOW TBLPROPERTIES.

        Args:
            table: Fully qualified table name.

        Returns:
            Pandas DataFrame with columns: key, value.
        """
        stats_query = f"SHOW TBLPROPERTIES {table}"
        stats_result = self.spark.sql(stats_query)
        return stats_result.toPandas()

    def get_column_statistics(self, table: str) -> DataFrame:
        """
        Retrieve column statistics from DESCRIBE TABLE EXTENDED.

        Args:
            table: Fully qualified table name.

        Returns:
            Pandas DataFrame with column information.
        """
        col_stats_query = f"DESCRIBE TABLE EXTENDED {table}"
        col_result = self.spark.sql(col_stats_query)
        return col_result.toPandas()

    def get_table_column_names(self, table: str) -> list[str]:
        """
        Get list of column names for a table.

        Args:
            table: Fully qualified table name.

        Returns:
            List of column names.
        """
        df = self.spark.table(table)
        return df.columns

    def execute_query(self, query: str) -> DataFrame:
        """
        Execute a SQL query and return Spark DataFrame.

        Note: Returns Spark DataFrame, not Pandas DataFrame, for compatibility
        with existing code that calls toPandas() on the result.

        Args:
            query: SQL query string.

        Returns:
            Spark DataFrame containing query results.

        Raises:
            Exception: If query execution fails.
        """
        return self.spark.sql(query)


class TableDefinitionBuilder:
    """
    Builder for constructing table definition strings.

    This class uses the Builder pattern to construct complex table definition
    strings step by step, separating the construction logic from representation.
    """

    def __init__(self) -> None:
        """Initialize the builder with empty state."""
        self._columns: list[str] = []
        self._primary_key: str | None = None

    def add_columns(self, columns: list[str]) -> "TableDefinitionBuilder":
        """
        Add column definitions to the table.

        Args:
            columns: List of column definition strings (e.g., "id bigint").

        Returns:
            Self for method chaining.
        """
        self._columns = columns
        return self

    def add_primary_key(self, primary_key: str | None) -> "TableDefinitionBuilder":
        """
        Add primary key constraint information.

        Args:
            primary_key: Primary key constraint string, or None if no PK exists.

        Returns:
            Self for method chaining.
        """
        self._primary_key = primary_key
        return self

    def build(self) -> str:
        """
        Build and return the final table definition string.

        Returns:
            Formatted table definition string.
        """
        table_definition = "{\n" + ",\n".join(self._columns) + "\n}"
        if self._primary_key:
            table_definition += f"\n-- Existing Primary Key: {self._primary_key}"
        return table_definition


class MetadataFormatter(ABC):
    """
    Abstract base class for metadata formatting strategies.

    This uses the Strategy pattern to allow different formatting
    approaches for various types of metadata.
    """

    @abstractmethod
    def format(self, data: DataFrame) -> list[str]:
        """
        Format metadata from a DataFrame into string lines.

        Args:
            data: DataFrame containing metadata to format.

        Returns:
            List of formatted string lines.
        """
        raise NotImplementedError


class PropertyMetadataFormatter(MetadataFormatter):
    """
    Formatter for table property metadata.

    Extracts and formats useful properties like row counts, data sizes,
    and constraint information.
    """

    def format(self, data: DataFrame) -> list[str]:
        """
        Extract useful properties from table properties DataFrame.

        Args:
            data: DataFrame with columns: key, value.

        Returns:
            List of formatted property strings.
        """
        metadata_info = []
        for _, row in data.iterrows():
            key = row.get('key', '')
            value = row.get('value', '')
            if any(keyword in key.lower() for keyword in ('numrows', 'rawdatasize', 'totalsize', 'primary', 'unique')):
                metadata_info.append(f"{key}: {value}")
        return metadata_info


class ColumnStatisticsFormatter(MetadataFormatter):
    """
    Formatter for column statistics and type distribution.

    Categorizes columns by data type and formats distribution information.
    """

    def format(self, data: DataFrame) -> list[str]:
        """
        Format column type distribution from column statistics.

        Args:
            data: DataFrame with columns: col_name, data_type.

        Returns:
            List of formatted column distribution strings.
        """
        numeric_cols, string_cols, date_cols, timestamp_cols = self._categorize_columns(data)
        return self._format_distribution(numeric_cols, string_cols, date_cols, timestamp_cols)

    def _categorize_columns(self, col_df: DataFrame) -> tuple[list[str], list[str], list[str], list[str]]:
        """
        Categorize columns by their data types.

        Args:
            col_df: DataFrame with column information.

        Returns:
            Tuple of (numeric_cols, string_cols, date_cols, timestamp_cols).
        """
        numeric_cols = []
        string_cols = []
        date_cols = []
        timestamp_cols = []

        for _, row in col_df.iterrows():
            col_name = row.get('col_name', '')
            data_type = str(row.get('data_type', '')).lower()

            # Stop at delimiter rows
            if col_name.startswith('#') or col_name.strip() == '':
                break

            if any(t in data_type for t in ('int', 'long', 'bigint', 'decimal', 'double', 'float')):
                numeric_cols.append(col_name)
            elif any(t in data_type for t in ('string', 'varchar', 'char')):
                string_cols.append(col_name)
            elif 'date' in data_type:
                date_cols.append(col_name)
            elif 'timestamp' in data_type:
                timestamp_cols.append(col_name)

        return numeric_cols, string_cols, date_cols, timestamp_cols

    def _format_distribution(
        self, numeric_cols: list[str], string_cols: list[str], date_cols: list[str], timestamp_cols: list[str]
    ) -> list[str]:
        """
        Format column type distribution information.

        Args:
            numeric_cols: List of numeric column names.
            string_cols: List of string column names.
            date_cols: List of date column names.
            timestamp_cols: List of timestamp column names.

        Returns:
            List of formatted distribution strings.
        """
        metadata_info = [
            "Column type distribution:",
            f"  Numeric columns ({len(numeric_cols)}): {', '.join(numeric_cols[:5])}",
            f"  String columns ({len(string_cols)}): {', '.join(string_cols[:5])}",
            f"  Date columns ({len(date_cols)}): {', '.join(date_cols)}",
            f"  Timestamp columns ({len(timestamp_cols)}): {', '.join(timestamp_cols)}",
        ]
        return metadata_info


class ColumnDefinitionExtractor:
    """
    Extracts and formats column definitions from DESCRIBE TABLE results.

    This class handles the parsing of DESCRIBE TABLE output and converts
    it into formatted column definition strings.
    """

    @staticmethod
    def extract_columns(describe_df: DataFrame) -> list[str]:
        """
        Extract column definitions from DESCRIBE TABLE DataFrame.

        Args:
            describe_df: DataFrame from DESCRIBE TABLE EXTENDED query.

        Returns:
            List of formatted column definition strings.
        """
        definition_lines = []
        in_column_section = True

        for _, row in describe_df.iterrows():
            col_name = row['col_name']
            data_type = row['data_type']
            comment = row['comment'] if 'comment' in row else ''

            # Stop at delimiter or empty rows
            if col_name.startswith('#') or col_name.strip() == '':
                in_column_section = False
                continue

            if in_column_section and not col_name.startswith('#'):
                nullable = " NOT NULL" if "not null" in str(comment).lower() else ""
                definition_lines.append(f"    {col_name} {data_type}{nullable}")

        return definition_lines


class TableManager:
    """
    Facade for table operations providing schema retrieval and metadata checking.

    This class acts as a simplified interface (Facade pattern) that coordinates
    between the data repository and formatters. It delegates actual operations
    to specialized components while maintaining backward compatibility with the
    existing API.

    Attributes:
        repository: Data provider for table operations (defaults to SparkTableDataProvider)
        property_formatter: Formatter for table property metadata
        stats_formatter: Formatter for column statistics and distribution
    """

    def __init__(self, spark: SparkSession | None = None, repository=None) -> None:
        """
        Initialize TableManager with optional dependency injection.

        Args:
            spark: SparkSession instance. Used if repository is not provided.
            repository: Optional TableDataProvider implementation. If None,
                       creates SparkTableDataProvider with the provided spark session.
        """
        if repository is None:
            self.repository = SparkTableDataProvider(spark)
        else:
            self.repository = repository

        self.property_formatter = PropertyMetadataFormatter()
        self.stats_formatter = ColumnStatisticsFormatter()

    def get_table_definition(self, table: str) -> str:
        """
        Retrieve table definition using repository and formatters.

        This method coordinates between the repository for data access and
        the builder/extractor for formatting the result.

        Args:
            table: Fully qualified table name.

        Returns:
            Formatted table definition string with columns and primary key.
        """
        logger.info(f"🔍 Retrieving schema for table: {table}")

        # Retrieve data from repository
        describe_df = self.repository.get_table_columns(table)
        existing_pk = self.repository.get_existing_primary_key(table)

        # Extract and format using dedicated components
        definition_lines = ColumnDefinitionExtractor.extract_columns(describe_df)
        table_definition = TableDefinitionBuilder().add_columns(definition_lines).add_primary_key(existing_pk).build()

        logger.info("✅ Table definition retrieved successfully")
        return table_definition

    def get_table_metadata_info(self, table: str) -> str:
        """
        Get additional metadata information to help with primary key detection.

        This method coordinates multiple formatters to build comprehensive
        metadata information from the repository.

        Args:
            table: Fully qualified table name.

        Returns:
            Formatted metadata information string.
        """
        try:
            metadata_info = []

            # Get and format table properties
            try:
                properties_df = self.repository.get_table_properties(table)
                metadata_info.extend(self.property_formatter.format(properties_df))
            except (ValueError, RuntimeError, KeyError):
                # Silently continue if table properties are not accessible
                pass

            # Get and format column statistics
            try:
                stats_df = self.repository.get_column_statistics(table)
                metadata_info.extend(self.stats_formatter.format(stats_df))
            except (ValueError, RuntimeError, KeyError):
                # Silently continue if column statistics are not accessible
                pass

            return (
                "Metadata information:\n" + "\n".join(metadata_info) if metadata_info else "Limited metadata available"
            )
        except Exception as e:
            logger.warning(f"Unexpected error retrieving metadata: {e}")
            return f"Could not retrieve metadata due to unexpected error: {e}"

    def get_table_column_names(self, table: str) -> list[str]:
        """
        Get table column names.

        Args:
            table: Fully qualified table name.

        Returns:
            List of column names.
        """
        return self.repository.get_table_column_names(table)

    def run_sql(self, query: str):
        """
        Run a SQL query and return the result DataFrame.

        Args:
            query: SQL query string.

        Returns:
            Spark DataFrame containing query results.
        """
        return self.repository.execute_query(query)
