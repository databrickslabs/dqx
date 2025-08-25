from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import re
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class RuleBasedPKDetector:
    """
    Rule-based primary key detector for Spark SQL tables.
    """
    def __init__(self,
                 spark: SparkSession, 
                 column_exclusion_regex: str|None = None, 
                 fail_on_duplicate: bool = True, 
                 data_filter: str = "",
                 logger: logging.Logger = logger):
        """
        Initializes the RuleBasedPKDetector with a Spark session and optional parameters.
        :param spark: Spark session to use for SQL operations.
        :param column_exclusion_regex: Regex pattern to exclude certain columns from primary key detection.
        :param fail_on_duplicate: If True, will raise an error if duplicates are found in the table.
        :param data_filter: Optional filter to apply to the data when checking for primary keys.
        """
        self.spark = spark
        self.fail_on_duplicate = fail_on_duplicate
        self.column_exclusion_regex = column_exclusion_regex
        self.data_filter = data_filter
        self.logger = logger

    def _get_diff_unique_counts(self, table: str, fixed_columns: list[str], columns: list[str]) -> list[tuple[str, int]]:
        """
        Returns the difference between total and distinct counts for each column combination.
        """
        pk_check_columns = [fixed_columns + [i] for i in columns]
        counts_int_sql = ', '.join([
            f"""count(*) - count(distinct {'||'.join(["coalesce(cast(`"+col+"` as string),'')" for col in columns])}) as distinct_cnt{i}"""
            for i, columns in enumerate(pk_check_columns)
        ])
        counts_sql = rf"SELECT {counts_int_sql} FROM {table} {'WHERE ' + self.data_filter if self.data_filter else ''}"
        self.logger.debug(f"Counts SQL: {counts_sql}")
        diff_counts = self.spark.sql(counts_sql).collect()[0]
        return list(zip(pk_check_columns, diff_counts))

    def _get_col_with_highest_info(self, diff_counts: list[tuple[str, int]]) -> tuple[str, int]:
        """
        Finds the column that, when added to the fixed columns, results in the lowest difference between total and distinct counts.
        """
        df = self.spark.createDataFrame(
            diff_counts,
            schema='primary_keys array<string>, diff long'
        )
        df = df.orderBy('diff', ascending=True).first()
        return df.primary_keys[-1], df.diff
    
    def _get_generated_columns(self, create_table_stmt: str) -> set[str]:
        """
        Returns a set of generated columns for the given table.
        """
        generated_columns = []
        for column_def in create_table_stmt.split('\n'):
            if 'GENERATED' in column_def:
                generated_columns.append(column_def.strip().split()[0])
        return generated_columns

    def find_pk_for_table_from_columns(self, table_name: str, columns: list[str]) -> tuple[list[str], bool]:
        """
        Iteratively finds a set of columns that can serve as a primary key from the given list of columns.
        :param table_name: Name of the table to find the primary key for.
        :param columns: List of column names to consider for primary key detection.
        :return: List of column names that can serve as a primary key and flag indicating if there is any duplicates with the primary keys selected.
        :raises: `ValueError` if there are duplicates in the table is __fail_on_duplicates__ param is set.
        """
        column_list = columns.copy()
        self.logger.debug(f"Columns considered for primary key detection: {column_list}")
        final_pk_list: list[str] = list()
        is_pk_found = False
        prev_diff = None
        while column_list and not is_pk_found:
            diff_list = self._get_diff_unique_counts(table_name, final_pk_list, column_list)
            new_pk_col, diff = self._get_col_with_highest_info(diff_list)
            if diff == prev_diff:
                if self.fail_on_duplicate:
                    error_msg = f"Duplicates found in {table_name} for columns {final_pk_list}"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                else:
                    warning_msg = f"Duplicates found in {table_name} for columns {final_pk_list}"
                    self.logger.error(warning_msg)
                    return final_pk_list, True
            self.logger.debug(f"Selected {new_pk_col} with difference {diff} between count and distinct count")
            final_pk_list.append(new_pk_col)
            is_pk_found = diff == 0
            prev_diff = diff
            column_list = list(filter(lambda x: x not in final_pk_list, column_list))
            if not is_pk_found:
                self.logger.debug(f"Finding new column for the primary key list")
        return final_pk_list, False

    def find_pk_for_table(self, table: str, is_ignore_generated_columns: bool) -> tuple[list[str], bool]:
        """
        Finds the primary key columns for a given table, also neglects the columns matching the **ignore_columns_regex** for pk consideration.
        :param table: Name of the table to find the primary key for.
        :param is_ignore_generated_columns: Whether to ignore delta auto generated columns when finding the primary keys.
        :return: List of column names that can serve as a primary key and flag indicating if there is any duplicates with the primary keys selected.
        :raises: `ValueError` if there are duplicates in the table is __fail_on_duplicates__ param is set.
        """
        create_table_stmt = self.spark.sql(f'SHOW CREATE TABLE {table}').collect()[0].createtab_stmt
        if is_ignore_generated_columns:
            generated_columns = self._get_generated_columns(create_table_stmt)
            ignore_columns_regex = f'{self.column_exclusion_regex}{"|" if generated_columns else ""}{"|".join(generated_columns)}'
        else:
            ignore_columns_regex = self.column_exclusion_regex
        column_list = sorted([
            x for x in self.spark.table(table).columns
            if not re.match(ignore_columns_regex, x.lower())
        ])
        pk_columns, is_duplicate_date = self.find_pk_for_table_from_columns(table, column_list)
        return pk_columns, is_duplicate_date