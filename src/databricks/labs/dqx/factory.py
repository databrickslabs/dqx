"""
Factory module for creating appropriate DQX engine instances based on DataFrame type.

This module provides functions to automatically detect the type of DataFrame being used
and return the appropriate DQX engine implementation.
"""

import logging
from typing import Any, Optional, Dict

try:
    import pandas as pd
    import pyarrow as pa
    import pyspark.sql as ps
    from pyspark.sql import DataFrame as SparkDataFrame
    from pandas import DataFrame as PandasDataFrame
except ImportError:
    pass

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.pandas_engine import PandasDQEngine
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def get_dq_engine(
    workspace_client: WorkspaceClient,
    dataframe: Optional[Any] = None,
    extra_params: Optional[Dict[str, Any]] = None
) -> Any:
    """Get the appropriate DQX engine based on the DataFrame type.
    
    Args:
        workspace_client: WorkspaceClient instance
        dataframe: Optional DataFrame to detect type from
        extra_params: Extra parameters for the engine
        
    Returns:
        Appropriate DQX engine implementation
    """
    # If no dataframe is provided, default to Spark engine
    if dataframe is None:
        try:
            return DQEngine(workspace_client, extra_params=extra_params)
        except Exception as e:
            logger.warning(f"Failed to create Spark engine: {e}")
            # Fallback to Pandas engine if Spark is not available
            return PandasDQEngine(workspace_client, extra_params=extra_params)
    
    # Detect DataFrame type and return appropriate engine
    if _is_spark_dataframe(dataframe):
        return DQEngine(workspace_client, extra_params=extra_params)
    elif _is_pandas_dataframe(dataframe):
        return PandasDQEngine(workspace_client, extra_params=extra_params)
    elif _is_pyarrow_table(dataframe):
        # For now, we'll use Pandas engine for PyArrow tables
        # In a full implementation, we would have a PyArrow-specific engine
        return PandasDQEngine(workspace_client, extra_params=extra_params)
    else:
        raise ValueError(f"Unsupported DataFrame type: {type(dataframe)}")


def _is_spark_dataframe(dataframe: Any) -> bool:
    """Check if the given object is a Spark DataFrame."""
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        return isinstance(dataframe, SparkDataFrame)
    except ImportError:
        return False


def _is_pandas_dataframe(dataframe: Any) -> bool:
    """Check if the given object is a Pandas DataFrame."""
    try:
        from pandas import DataFrame as PandasDataFrame
        return isinstance(dataframe, PandasDataFrame)
    except ImportError:
        return False


def _is_pyarrow_table(dataframe: Any) -> bool:
    """Check if the given object is a PyArrow Table."""
    try:
        import pyarrow as pa
        return isinstance(dataframe, pa.Table)
    except ImportError:
        return False
