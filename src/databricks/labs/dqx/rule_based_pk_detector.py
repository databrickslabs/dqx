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
    Rule-based primary key detector for Spark SQL tables/Views.
    """
    def __init__(self,
                 data_filter: str = "",
                 fail_on_duplicate: bool = True, 
                 logger: logging.Logger = logger):
        """
        Initializes the RuleBasedPKDetector with optional parameters.

        Args:
            data_filter: Optional SQL filter to apply to the data when checking for primary keys.
            fail_on_duplicate: If True, raises an error if duplicates are found in the table.
            logger: Logger instance for logging messages.

        Returns:
            None
        """
        self.data_filter = data_filter
        self.fail_on_duplicate = fail_on_duplicate
        self.logger = logger

    def _get_diff_unique_counts(self, table: str, fixed_columns: list[str], columns: list[str]) -> list[tuple[str, int]]:
        """
        Returns the difference between total and distinct counts for each column combination.

        Args:
            table: Name of the table to check.
            fixed_columns: List of columns already selected as part of the key.
            columns: List of candidate columns to test for uniqueness.

        Returns:
            List of tuples containing column combinations and their difference between total and distinct counts.
        """
        pk_combinations = [fixed_columns + [col] for col in columns]

        distinct_count_exprs = []
        for idx, combo in enumerate(pk_combinations):
            concat_expr = '||'.join([f"coalesce(cast(`{col}` as string),'')" for col in combo])
            distinct_count_exprs.append(
                f"count(*) - count(distinct {concat_expr}) as distinct_cnt{idx}"
            )
        counts_sql_expr = ', '.join(distinct_count_exprs)

        sql_query = f"SELECT {counts_sql_expr} FROM {table}"
        if self.data_filter:
            sql_query += f" WHERE {self.data_filter}"
        self.logger.debug(f"Counts SQL: {sql_query}")

        diff_counts = spark.sql(sql_query).collect()[0]

        return list(zip(pk_combinations, diff_counts))

    def _get_col_with_highest_info(self, diff_counts: list[tuple[str, int]]) -> tuple[str, int]:
        """
        Finds the column that, when added to the fixed columns, results in the lowest difference between total and distinct counts.

        Args:
            diff_counts: List of tuples containing column combinations and their difference between total and distinct counts.

        Returns:
            Tuple containing the column name with the highest information gain and its difference value.
        """
        df = spark.createDataFrame(
            diff_counts,
            schema='primary_keys array<string>, diff long'
        )
        best_row = df.orderBy('diff', ascending=True).first()

        return best_row.primary_keys[-1], best_row.diff
    
    def _get_generated_columns(self, create_table_stmt: str) -> set[str]:
        """
        Returns a set of generated columns for the given table.

        Args:
            create_table_stmt: The CREATE TABLE statement as a string.

        Returns:
            List of generated column names.
        """
        generated_columns = []
        for column_def in create_table_stmt.split('\n'):
            if 'GENERATED' in column_def:
                generated_columns.append(column_def.strip().split()[0])

        return generated_columns
    
    def _get_good_columns(self, table_name: str, columns_to_exclude: list[str], column_exclusion_regex: str, is_ignore_generated_columns: bool) -> list[str]:
        """
        Returns a list of good columns for the given table to identify Primary Key columns.

        Args:
            table_name: Name of the table to check.
            columns_to_exclude: List of column names to exclude from primary key consideration.
            column_exclusion_regex: Regex pattern to exclude columns from primary key consideration.
            is_ignore_generated_columns: Whether to ignore generated columns when finding primary keys.

        Returns:
            List of column names that are suitable for primary key detection (non-null, not excluded, not generated, etc.).
        """
        df = spark.table(table_name)
        print(f"Size of Table: ({df.count()}, {len(df.columns)})")

        if is_ignore_generated_columns and column_exclusion_regex:
            create_table_stmt = spark.sql(f'SHOW CREATE TABLE {table_name}').collect()[0].createtab_stmt
            generated_columns = self._get_generated_columns(create_table_stmt)
            ignore_columns_regex = f'{column_exclusion_regex}{"|" if generated_columns else ""}{"|".join(generated_columns)}'
        elif is_ignore_generated_columns:
            create_table_stmt = spark.sql(f'SHOW CREATE TABLE {table_name}').collect()[0].createtab_stmt
            generated_columns = self._get_generated_columns(create_table_stmt)
            ignore_columns_regex = f'{" |" if generated_columns else ""}{"|".join(generated_columns)}'
        elif column_exclusion_regex:
            ignore_columns_regex = column_exclusion_regex
        else:
            ignore_columns_regex = ""

        if (len(ignore_columns_regex.strip()) == 0):
            regex_columns_to_exclude = []
        else:
            regex_columns_to_exclude = [c for c in df.columns if re.match(ignore_columns_regex, c)]
        self.logger.debug(f"regex_columns_to_exclude:{regex_columns_to_exclude}")
        timestamp_columns_to_exclude = [name for name, dtype in df.dtypes if dtype == 'timestamp']
        self.logger.debug(f"timestamp_columns_to_exclude:{timestamp_columns_to_exclude}")
        exclude_pattern = re.compile(r'^row[-_]?(id|ids|no|num)$', re.IGNORECASE)
        rowid_columns_to_exclude = [col for col in df.columns if exclude_pattern.match(col)]
        self.logger.debug(f"rowid_columns_to_exclude:{rowid_columns_to_exclude}")
        all_exclude_cols = set(regex_columns_to_exclude) | set(timestamp_columns_to_exclude) | set(rowid_columns_to_exclude) | set(columns_to_exclude)
        self.logger.debug(f"all_exclude_cols: {all_exclude_cols}")

        cols_to_check = [c for c in df.columns 
                         if c not in all_exclude_cols]
        null_count_expr = ", ".join([
            f"sum(case when (isnull(`{c}`) or isnan(`{c}`)) then 1 else 0 end) as `{c}`"
            for c in cols_to_check
        ])
        sql_query = rf"SELECT {null_count_expr} FROM {table_name} {'WHERE ' + self.data_filter if self.data_filter else ''}"
        self.logger.debug(f"sql_query: {sql_query}")
        null_counts_row = spark.sql(sql_query).collect()[0]
        null_counts = null_counts_row.asDict()
        good_cols = [c for c, null_count in null_counts.items() if null_count == 0]

        return good_cols

    def find_pk_for_table_from_columns(self, table_name: str, columns: list[str]) -> tuple[list[str], bool]:
        """
        Iteratively finds a set of columns that can serve as a primary key from the given list of columns.

        Args:
            table_name: Name of the table to find the primary key for.
            columns: List of column names to consider for primary key detection.

        Returns:
            Tuple containing:
                - List of column names that can serve as a primary key.
                - Boolean flag indicating if there are any duplicates with the selected primary keys.

        Raises:
            ValueError: If there are duplicates in the table and the __fail_on_duplicates__ parameter is set.
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
                    error_msg = f"Error: Duplicates found in {table_name} for columns {final_pk_list}"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                else:
                    warning_msg = f"Warning: Duplicates found in {table_name} for columns {final_pk_list}"
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

    def find_pk_for_table(self, table: str, columns_to_exclude: list[str] = [], column_exclusion_regex: str = "", is_ignore_generated_columns: bool = False) -> tuple[list[str], bool]:
        """
        Finds the primary key columns for a given table, neglecting columns matching the ignore_columns_regex for PK consideration.

        Args:
            table: Name of the table to find the primary key for.
            columns_to_exclude: List of column names to exclude from primary key detection.
            column_exclusion_regex: Regex pattern to exclude columns from primary key consideration.
            is_ignore_generated_columns: Whether to ignore delta auto-generated columns when finding the primary keys.

        Returns:
            Tuple containing:
                - List of column names that can serve as a primary key.
                - Boolean flag indicating if there are any duplicates with the selected primary keys.

        Raises:
            ValueError: If there are duplicates in the table and the __fail_on_duplicates__ parameter is set.
        """
        column_list = self._get_good_columns(table, columns_to_exclude, column_exclusion_regex, is_ignore_generated_columns)

        pk_columns, is_duplicate_date = self.find_pk_for_table_from_columns(table, column_list)

        print(f"Primary Key Columns: {pk_columns}")
        print(f"Has Duplicates: {is_duplicate_date}")
        return pk_columns, is_duplicate_date