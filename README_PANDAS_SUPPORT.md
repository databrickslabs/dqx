# DQX Pandas/PyArrow Support

This document describes the new backend abstraction layer for DQX that enables support for Pandas DataFrames and PyArrow tables in addition to the existing Spark DataFrame support.

## Overview

The DQX library has been extended with a backend abstraction layer that allows the same API to work with different DataFrame implementations:

1. **Spark DataFrames** - The original backend (default)
2. **Pandas DataFrames** - For local analysis without a Spark cluster
3. **PyArrow Tables** - For efficient local data processing

## Architecture

The implementation follows an adapter pattern with the following components:

1. **DataFrame Backend Abstraction** (`df_backend.py`)
   - Abstract base class defining common operations
   - Concrete implementations for Spark, Pandas, and PyArrow

2. **Engine Implementations**
   - `engine.py` - Original Spark-based engine (unchanged)
   - `pandas_engine.py` - New Pandas-compatible engine

3. **Factory Pattern** (`factory.py`)
   - Automatic engine selection based on DataFrame type

## Usage

### Automatic Engine Selection

```python
import pandas as pd
from databricks.labs.dqx.factory import get_dq_engine
from databricks.sdk import WorkspaceClient

# Create sample data
pandas_df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', None]})

# Get appropriate engine automatically
ws = WorkspaceClient()  # Your workspace client
engine = get_dq_engine(ws, pandas_df)

# Apply checks (same API for all backends)
result = engine.apply_checks(pandas_df, checks)
```

### Direct Engine Usage

```python
# For explicit control, you can directly instantiate engines
from databricks.labs.dqx.pandas_engine import PandasDQEngine

pandas_engine = PandasDQEngine(workspace_client)
result = pandas_engine.apply_checks(pandas_df, checks)
```

## Current Limitations

The implementation is largely complete with only minor limitations:

1. **Backend-Specific Features** - Some advanced Spark-specific features may not be available in Pandas/PyArrow backends
2. **Performance** - Pandas/PyArrow backends are optimized for local analysis and may not scale to very large datasets
3. **File I/O Operations** - Some file loading/saving operations are still placeholder implementations

## Next Steps

The implementation is largely complete. The following work could further enhance the implementation:

1. **Performance Optimizations**
   - Optimize Pandas engine operations for better performance
   - Add support for chunked processing of large datasets
   
2. **Enhanced File Operations**
   - Implement full-featured file loading and saving capabilities
   - Add support for additional file formats
   
3. **Advanced Features**
   - Implement additional methods in the Pandas engine for more complex operations
   - Add support for streaming data processing
   
4. **Expanded Backend Support**
   - Implement a dedicated PyArrow engine for optimal performance with PyArrow tables
   - Add support for other DataFrame libraries if needed

## Performance Considerations

The Pandas/PyArrow backends are designed for local data analysis on modern laptops:

- **Memory Usage** - Limited by available RAM (32GB recommended)
- **Performance** - Suitable for datasets that fit in memory
- **Scalability** - For larger datasets, Spark backend should be used

## Migration Guide

Existing DQX users can migrate to the new backends with minimal changes:

1. **No Code Changes Required** - Existing Spark-based code continues to work
2. **Gradual Migration** - Migrate individual components to Pandas/PyArrow as needed
3. **Factory Pattern** - Use `get_dq_engine()` for automatic backend selection
4. **Backend-Specific Features** - Access backend-specific functionality when needed

## Examples

See the demo notebooks for detailed usage examples:

- `demos/dqx_pandas_demo.py` - Basic Pandas usage
- `demos/dqx_factory_demo.py` - Automatic engine selection
