"""
DataFrame backend abstraction layer for DQX.

This module provides a unified interface for working with different DataFrame
implementations (Spark, Pandas, PyArrow) while maintaining API consistency.
"""

import abc
from typing import Any, List, Dict, Union, Optional
from dataclasses import dataclass

try:
    import pandas as pd
    import pyarrow as pa
    import pyspark.sql as ps
    from pyspark.sql import DataFrame as SparkDataFrame
    from pandas import DataFrame as PandasDataFrame
except ImportError:
    # Handle cases where some backends are not available
    pass


class DataFrameBackend(abc.ABC):
    """Abstract base class for DataFrame backends."""
    
    @abc.abstractmethod
    def create_condition(self, condition_expr: Any, message: Union[str, Any], alias: str) -> Any:
        """Create a condition column.
        
        Args:
            condition_expr: Condition expression
            message: Message to output
            alias: Name for the resulting column
            
        Returns:
            Backend-specific condition object
        """
        pass
    
    @abc.abstractmethod
    def is_not_null(self, column: Union[str, Any]) -> Any:
        """Check if values in column are not null.
        
        Args:
            column: Column to check
            
        Returns:
            Backend-specific condition object
        """
        pass
    
    @abc.abstractmethod
    def is_not_empty(self, column: Union[str, Any]) -> Any:
        """Check if values in column are not empty.
        
        Args:
            column: Column to check
            
        Returns:
            Backend-specific condition object
        """
        pass
    
    @abc.abstractmethod
    def is_not_null_and_not_empty(self, column: Union[str, Any], trim_strings: bool = False) -> Any:
        """Check if values in column are not null and not empty.
        
        Args:
            column: Column to check
            trim_strings: Whether to trim strings
            
        Returns:
            Backend-specific condition object
        """
        pass
    
    @abc.abstractmethod
    def apply_checks(self, df: Any, checks: List[Any]) -> Any:
        """Apply data quality checks to a DataFrame.
        
        Args:
            df: DataFrame to check
            checks: List of checks to apply
            
        Returns:
            DataFrame with error and warning columns
        """
        pass
    
    @abc.abstractmethod
    def count(self, df: Any) -> int:
        """Get the count of rows in the DataFrame.
        
        Args:
            df: DataFrame to count
            
        Returns:
            Number of rows
        """
        pass
    
    @abc.abstractmethod
    def select(self, df: Any, columns: List[Union[str, Any]]) -> Any:
        """Select specific columns from the DataFrame.
        
        Args:
            df: DataFrame to select from
            columns: Columns to select
            
        Returns:
            DataFrame with selected columns
        """
        pass


class SparkBackend(DataFrameBackend):
    """Spark DataFrame backend implementation."""
    
    def __init__(self, spark_session=None):
        try:
            from pyspark.sql import SparkSession
            self.spark = spark_session or SparkSession.builder.getOrCreate()
        except ImportError:
            raise ImportError("Spark backend requires pyspark to be installed")
    
    def create_condition(self, condition_expr, message, alias):
        """Create a condition column using Spark functions."""
        import pyspark.sql.functions as F
        if isinstance(message, str):
            msg_col = F.lit(message)
        else:
            msg_col = message
        
        return (F.when(condition_expr, msg_col).otherwise(F.lit(None).cast("string"))).alias(alias)
    
    def is_not_null(self, column):
        """Check if values in column are not null using Spark functions."""
        import pyspark.sql.functions as F
        col_expr = F.col(column) if isinstance(column, str) else column
        condition = col_expr.isNull()
        return self.create_condition(
            condition, f"Column '{column}' value is null", f"{column}_is_null"
        )
    
    def is_not_empty(self, column):
        """Check if values in column are not empty using Spark functions."""
        import pyspark.sql.functions as F
        col_expr = F.col(column) if isinstance(column, str) else column
        condition = col_expr.cast("string") == F.lit("")
        return self.create_condition(
            condition, f"Column '{column}' value is empty", f"{column}_is_empty"
        )
    
    def is_not_null_and_not_empty(self, column, trim_strings=False):
        """Check if values in column are not null and not empty using Spark functions."""
        import pyspark.sql.functions as F
        col_expr = F.col(column) if isinstance(column, str) else column
        if trim_strings:
            col_expr = F.trim(col_expr)
        condition = col_expr.isNull() | (col_expr.cast("string").isNull() | (col_expr.cast("string") == F.lit("")))
        col_name = column if isinstance(column, str) else "column"
        return self.create_condition(
            condition, f"Column '{col_name}' value is null or empty", f"{col_name}_is_null_or_empty"
        )
    
    def apply_checks(self, df, checks):
        """Apply data quality checks to a Spark DataFrame."""
        result_df = df
        for check in checks:
            result_df = result_df.withColumn(check.alias, check.condition)
        return result_df
    
    def count(self, df):
        """Get the count of rows in the Spark DataFrame."""
        return df.count()
    
    def select(self, df, columns):
        """Select specific columns from the Spark DataFrame."""
        import pyspark.sql.functions as F
        col_exprs = []
        for col in columns:
            if isinstance(col, str):
                col_exprs.append(F.col(col))
            else:
                col_exprs.append(col)
        return df.select(*col_exprs)


class PandasBackend(DataFrameBackend):
    """Pandas DataFrame backend implementation."""
    
    def __init__(self):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas backend requires pandas to be installed")
    
    def create_condition(self, condition_expr, message, alias):
        """Create a condition column for Pandas DataFrame."""
        import pandas as pd
        if isinstance(message, str):
            # Create a Series with the message where condition is True, None otherwise
            return pd.Series([message if cond else None for cond in condition_expr], name=alias)
        else:
            # Handle case where message is a Series
            return pd.Series([msg if cond else None for cond, msg in zip(condition_expr, message)], name=alias)
    
    def is_not_null(self, column):
        """Check if values in column are not null using Pandas."""
        import pandas as pd
        
        def check_not_null(df):
            col_data = df[column] if isinstance(column, str) else column
            condition = col_data.isnull()
            col_name = column if isinstance(column, str) else "column"
            return self.create_condition(
                condition, f"Column '{col_name}' value is null", f"{col_name}_is_null"
            )
        return check_not_null
    
    def is_not_empty(self, column):
        """Check if values in column are not empty using Pandas."""
        import pandas as pd
        
        def check_not_empty(df):
            col_data = df[column] if isinstance(column, str) else column
            condition = (col_data.astype(str) == "")
            col_name = column if isinstance(column, str) else "column"
            return self.create_condition(
                condition, f"Column '{col_name}' value is empty", f"{col_name}_is_empty"
            )
        return check_not_empty
    
    def is_not_null_and_not_empty(self, column, trim_strings=False):
        """Check if values in column are not null and not empty using Pandas."""
        import pandas as pd
        
        def check_not_null_and_not_empty(df):
            col_data = df[column] if isinstance(column, str) else column
            if trim_strings:
                col_data = col_data.astype(str).str.strip()
            condition = col_data.isnull() | (col_data.astype(str) == "")
            col_name = column if isinstance(column, str) else "column"
            return self.create_condition(
                condition, f"Column '{col_name}' value is null or empty", f"{col_name}_is_null_or_empty"
            )
        return check_not_null_and_not_empty
    
    def apply_checks(self, df, checks):
        """Apply data quality checks to a Pandas DataFrame."""
        result_df = df.copy()
        for check in checks:
            # Apply the check function to the DataFrame
            check_result = check.condition(result_df)
            result_df[check.alias] = check_result
        return result_df
    
    def count(self, df):
        """Get the count of rows in the Pandas DataFrame."""
        return len(df)
    
    def select(self, df, columns):
        """Select specific columns from the Pandas DataFrame."""
        col_names = [col if isinstance(col, str) else col.name for col in columns]
        return df[col_names]


class PyArrowBackend(DataFrameBackend):
    """PyArrow DataFrame backend implementation."""
    
    def __init__(self):
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("PyArrow backend requires pyarrow to be installed")
    
    def create_condition(self, condition_expr, message, alias):
        """Create a condition column for PyArrow Table."""
        import pyarrow as pa
        import pyarrow.compute as pc
        
        if isinstance(message, str):
            # Create an array with the message where condition is True, None otherwise
            message_array = pa.array([message if cond else None for cond in condition_expr])
        else:
            # Handle case where message is an array
            message_array = pa.array([msg.as_py() if cond else None for cond, msg in zip(condition_expr, message)])
        
        return message_array
    
    def is_not_null(self, column):
        """Check if values in column are not null using PyArrow."""
        import pyarrow.compute as pc
        
        def check_not_null(table):
            col_data = table.column(column) if isinstance(column, str) else column
            condition = pc.is_null(col_data)
            col_name = column if isinstance(column, str) else "column"
            return self.create_condition(
                condition, f"Column '{col_name}' value is null", f"{col_name}_is_null"
            )
        return check_not_null
    
    def is_not_empty(self, column):
        """Check if values in column are not empty using PyArrow."""
        import pyarrow.compute as pc
        
        def check_not_empty(table):
            col_data = table.column(column) if isinstance(column, str) else column
            condition = pc.equal(pc.cast(col_data, pa.string()), "")
            col_name = column if isinstance(column, str) else "column"
            return self.create_condition(
                condition, f"Column '{col_name}' value is empty", f"{col_name}_is_empty"
            )
        return check_not_empty
    
    def is_not_null_and_not_empty(self, column, trim_strings=False):
        """Check if values in column are not null and not empty using PyArrow."""
        import pyarrow.compute as pc
        
        def check_not_null_and_not_empty(table):
            col_data = table.column(column) if isinstance(column, str) else column
            if trim_strings:
                col_data = pc.utf8_trim_whitespace(pc.cast(col_data, pa.string()))
            is_null = pc.is_null(col_data)
            is_empty = pc.equal(pc.cast(col_data, pa.string()), "")
            condition = pc.or_(is_null, is_empty)
            col_name = column if isinstance(column, str) else "column"
            return self.create_condition(
                condition, f"Column '{col_name}' value is null or empty", f"{col_name}_is_null_or_empty"
            )
        return check_not_null_and_not_empty
    
    def apply_checks(self, table, checks):
        """Apply data quality checks to a PyArrow Table."""
        import pyarrow as pa
        
        # Convert to pandas for easier manipulation, then back to PyArrow
        df = table.to_pandas()
        result_df = df.copy()
        
        for check in checks:
            # Apply the check function to the DataFrame
            check_result = check.condition(df)
            result_df[check.alias] = check_result
        
        # Convert back to PyArrow Table
        return pa.Table.from_pandas(result_df)
    
    def count(self, table):
        """Get the count of rows in the PyArrow Table."""
        return table.num_rows
    
    def select(self, table, columns):
        """Select specific columns from the PyArrow Table."""
        col_names = [col if isinstance(col, str) else col._name for col in columns]
        return table.select(col_names)


def get_backend(dataframe_type: str) -> DataFrameBackend:
    """Get the appropriate backend for the given dataframe type.
    
    Args:
        dataframe_type: Type of dataframe ('spark', 'pandas', 'pyarrow')
        
    Returns:
        Appropriate backend implementation
    """
    if dataframe_type == 'spark':
        return SparkBackend()
    elif dataframe_type == 'pandas':
        return PandasBackend()
    elif dataframe_type == 'pyarrow':
        return PyArrowBackend()
    else:
        raise ValueError(f"Unsupported dataframe type: {dataframe_type}")
