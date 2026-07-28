---
sidebar_label: errors
title: databricks.labs.dqx.errors
---

## DQXError Objects

```python
class DQXError(Exception)
```

Base class for all DQX exceptions.

## ParameterError Objects

```python
class ParameterError(DQXError)
```

Base class for parameter-related errors.

## InvalidConfigError Objects

```python
class InvalidConfigError(DQXError)
```

Raised when a configuration error occurs.

## InvalidParameterError Objects

```python
class InvalidParameterError(ParameterError)
```

Raised when a parameter is invalid (malformed, wrong type, not supported, ambiguous, or incompatible with other inputs).

## MissingParameterError Objects

```python
class MissingParameterError(ParameterError)
```

Raised when a required parameter is missing, i.e when the user fails to provide a required parameter (None/absent).

## UnsafeSqlQueryError Objects

```python
class UnsafeSqlQueryError(DQXError)
```

Raised when a SQL query is considered unsafe.

## InvalidCheckError Objects

```python
class InvalidCheckError(DQXError)
```

Raised when a check is invalid or not supported.

## CheckDownloadError Objects

```python
class CheckDownloadError(InvalidCheckError)
```

Raised when a data quality check cannot be downloaded (e.g., from Unity Catalog).

## ODCSContractError Objects

```python
class ODCSContractError(DQXError)
```

Raised when there is an error related to ODCS data contracts.

## ComputationError Objects

```python
class ComputationError(DQXError)
```

Raised when an internal computation fails (e.g. aggregation returned no result, empty data).

## ModelLoadError Objects

```python
class ModelLoadError(DQXError)
```

Raised when a model cannot be loaded (e.g. version mismatch, corrupted artifact).

## InvalidPhysicalTypeError Objects

```python
class InvalidPhysicalTypeError(ODCSContractError)
```

Raised when a schema property is missing physicalType or physicalType is not a valid Unity Catalog data type.

For schema validation we require every property to have physicalType set to a Unity Catalog
type (e.g. STRING, INT, ARRAY&lt;STRING&gt;, DECIMAL(10,2)). See:
https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes

