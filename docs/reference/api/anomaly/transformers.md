# databricks.labs.dqx.anomaly.transformers

Feature engineering for row anomaly detection.

Provides column type analysis and Spark-native feature transformations. All transformations are applied in Spark (distributed) for scalability and Spark Connect compatibility (no custom Python class serialization).

## ColumnTypeInfo Objects[​](#columntypeinfo-objects "Direct link to ColumnTypeInfo Objects")

```python
@dataclass
class ColumnTypeInfo()

```

Information about a column's type and encoding strategy.

#### category[​](#category "Direct link to category")

'numeric', 'categorical', 'datetime', 'boolean', 'unsupported'

#### encoding\_strategy[​](#encoding_strategy "Direct link to encoding_strategy")

'onehot', 'frequency', 'cyclical', 'binary', 'none'

## SparkFeatureMetadata Objects[​](#sparkfeaturemetadata-objects "Direct link to SparkFeatureMetadata Objects")

```python
@dataclass
class SparkFeatureMetadata()

```

Metadata for reconstructing Spark transformations during scoring.

Stores everything needed to apply the same transformations:

* Column types and encoding strategies
* Frequency maps for categorical encoding (high cardinality)
* OneHot distinct values (low cardinality)
* Engineered feature names (in order)

#### column\_infos[​](#column_infos "Direct link to column_infos")

Serializable version of ColumnTypeInfo

#### categorical\_frequency\_maps[​](#categorical_frequency_maps "Direct link to categorical_frequency_maps")

col\_name -> (value -> frequency)

#### onehot\_categories[​](#onehot_categories "Direct link to onehot_categories")

col\_name -> \[distinct\_values] for OneHot encoding

#### engineered\_feature\_names[​](#engineered_feature_names "Direct link to engineered_feature_names")

Final feature names after engineering

#### categorical\_cardinality\_threshold[​](#categorical_cardinality_threshold "Direct link to categorical_cardinality_threshold")

Threshold used for categorical encoding

### to\_json[​](#to_json "Direct link to to_json")

```python
def to_json() -> str

```

Serialize to JSON for storage.

### from\_json[​](#from_json "Direct link to from_json")

```python
@classmethod
def from_json(cls, json_str: str) -> "SparkFeatureMetadata"

```

Deserialize from JSON.

### reconstruct\_column\_infos[​](#reconstruct_column_infos "Direct link to reconstruct_column_infos")

```python
def reconstruct_column_infos(
        feature_metadata: SparkFeatureMetadata) -> list[ColumnTypeInfo]

```

Reconstruct ColumnTypeInfo objects from SparkFeatureMetadata.

Uses category to set spark\_type so that any code (e.g. \_classify\_column or isinstance checks) that runs on reconstructed infos during scoring behaves correctly.

**Arguments**:

* `feature_metadata` - SparkFeatureMetadata with column information

**Returns**:

List of ColumnTypeInfo objects

## ColumnTypeClassifier Objects[​](#columntypeclassifier-objects "Direct link to ColumnTypeClassifier Objects")

```python
class ColumnTypeClassifier()

```

Analyzes DataFrame schema and categorizes columns for feature engineering.

Categories:

* numeric: int, long, float, double, decimal
* categorical: string (with reasonable cardinality)
* datetime: date, timestamp, timestampNTZ
* boolean: boolean
* unsupported: array, map, struct, binary, etc.

### analyze\_columns[​](#analyze_columns "Direct link to analyze_columns")

```python
def analyze_columns(
        df: DataFrame,
        columns: list[str]) -> tuple[list[ColumnTypeInfo], list[str]]

```

Analyze columns and return type information and warnings.

**Returns**:

Tuple of (column\_type\_infos, warnings)

### apply\_feature\_engineering[​](#apply_feature_engineering "Direct link to apply_feature_engineering")

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

* DataFrame with engineered numeric features
* Metadata for reconstructing transformations during scoring

Transformations applied:

1. Categorical: OneHot (low-card) or Frequency encoding (high-card)
2. Datetime: Extract hour\_sin/cos, dow\_sin/cos, month\_sin/cos, is\_weekend
3. Boolean: Map to 0/1
4. Numeric: Keep as-is
5. Null indicators: Add column\_is\_null for columns with nulls
6. Imputation: Fill nulls with 0 (numeric), "MISSING" (categorical), epoch (datetime), 0 (boolean)

**Arguments**:

* `df` - Input DataFrame with original columns
* `column_infos` - Column type information from ColumnTypeClassifier
* `categorical_cardinality_threshold` - Threshold for OneHot vs Frequency encoding
* `frequency_maps` - Pre-computed frequency maps (for scoring). If None, compute from df (for training).
* `onehot_categories` - Pre-computed OneHot distinct values (for scoring). If None, compute from df (for training).
