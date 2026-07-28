---
sidebar_label: transformers
title: databricks.labs.dqx.anomaly.transformers
---

Feature engineering for row anomaly detection.

Provides column type analysis and Spark-native feature transformations.
All transformations are applied in Spark (distributed) for scalability and
Spark Connect compatibility (no custom Python class serialization).

## ColumnTypeInfo Objects

```python
@dataclass
class ColumnTypeInfo()
```

Information about a column&#x27;s type and encoding strategy.

#### category

&#x27;numeric&#x27;, &#x27;categorical&#x27;, &#x27;datetime&#x27;, &#x27;boolean&#x27;, &#x27;unsupported&#x27;

#### encoding\_strategy

&#x27;onehot&#x27;, &#x27;frequency&#x27;, &#x27;cyclical&#x27;, &#x27;binary&#x27;, &#x27;none&#x27;

## SparkFeatureMetadata Objects

```python
@dataclass
class SparkFeatureMetadata()
```

Metadata for reconstructing Spark transformations during scoring.

Stores everything needed to apply the same transformations:
- Column types and encoding strategies
- Frequency maps for categorical encoding (high cardinality)
- OneHot distinct values (low cardinality)
- Engineered feature names (in order)

#### column\_infos

Serializable version of ColumnTypeInfo

#### categorical\_frequency\_maps

col_name -&gt; (value -&gt; frequency)

#### onehot\_categories

col_name -&gt; [distinct_values] for OneHot encoding

#### engineered\_feature\_names

Final feature names after engineering

#### categorical\_cardinality\_threshold

Threshold used for categorical encoding

#### to\_json

```python
def to_json() -> str
```

Serialize to JSON for storage.

#### from\_json

```python
@classmethod
def from_json(cls, json_str: str) -> "SparkFeatureMetadata"
```

Deserialize from JSON.

#### reconstruct\_column\_infos

```python
def reconstruct_column_infos(
        feature_metadata: SparkFeatureMetadata) -> list[ColumnTypeInfo]
```

Reconstruct ColumnTypeInfo objects from SparkFeatureMetadata.

Uses category to set spark_type so that any code (e.g. _classify_column or
isinstance checks) that runs on reconstructed infos during scoring behaves correctly.

**Arguments**:

- `feature_metadata` - SparkFeatureMetadata with column information
  

**Returns**:

  List of ColumnTypeInfo objects

## ColumnTypeClassifier Objects

```python
class ColumnTypeClassifier()
```

Analyzes DataFrame schema and categorizes columns for feature engineering.

Categories:
- numeric: int, long, float, double, decimal
- categorical: string (with reasonable cardinality)
- datetime: date, timestamp, timestampNTZ
- boolean: boolean
- unsupported: array, map, struct, binary, etc.

#### analyze\_columns

```python
def analyze_columns(
        df: DataFrame,
        columns: list[str]) -> tuple[list[ColumnTypeInfo], list[str]]
```

Analyze columns and return type information and warnings.

**Returns**:

  Tuple of (column_type_infos, warnings)

#### apply\_feature\_engineering

```python
def apply_feature_engineering(
    df: DataFrame,
    column_infos: list[ColumnTypeInfo],
    categorical_cardinality_threshold: int = 20,
    frequency_maps: dict[str, dict[str, float]] | None = None,
    onehot_categories: dict[str, list[str]] | None = None
) -> tuple[DataFrame, SparkFeatureMetadata]
```

Apply feature engineering transformations in Spark (distributed).

**Returns**:

  - DataFrame with engineered numeric features
  - Metadata for reconstructing transformations during scoring
  
  Transformations applied:
  1. Categorical: OneHot (low-card) or Frequency encoding (high-card)
  2. Datetime: Extract hour_sin/cos, dow_sin/cos, month_sin/cos, is_weekend
  3. Boolean: Map to 0/1
  4. Numeric: Keep as-is
  5. Null indicators: Add column_is_null for columns with nulls
  6. Imputation: Fill nulls with 0 (numeric), &quot;MISSING&quot; (categorical), epoch (datetime), 0 (boolean)
  

**Arguments**:

- `df` - Input DataFrame with original columns
- `column_infos` - Column type information from ColumnTypeClassifier
- `categorical_cardinality_threshold` - Threshold for OneHot vs Frequency encoding
- `frequency_maps` - Pre-computed frequency maps (for scoring). If None, compute from df (for training).
- `onehot_categories` - Pre-computed OneHot distinct values (for scoring). If None, compute from df (for training).

