---
sidebar_label: config
title: databricks.labs.dqx.config
---

## InputConfig Objects

```python
@dataclass
class InputConfig()
```

Configuration class for input data sources (e.g. tables or files).

## OutputConfig Objects

```python
@dataclass
class OutputConfig()
```

Configuration class for output data sinks (e.g. tables or files).

#### \_\_post\_init\_\_

```python
def __post_init__()
```

Normalize trigger configuration by converting string boolean representations to actual booleans.
This is required due to the limitation of the config deserializer.

## ProfilerConfig Objects

```python
@dataclass
class ProfilerConfig()
```

Configuration class for profiler.

#### summary\_stats\_file

file containing profile summary statistics

#### sample\_fraction

fraction of data to sample (30%)

#### sample\_seed

seed for sampling

#### limit

limit the number of records to profile

#### filter

filter to apply to the data before profiling

#### criticality

default criticality for generated rules (&quot;error&quot; or &quot;warn&quot;)

## IsolationForestConfig Objects

```python
@dataclass
class IsolationForestConfig()
```

Algorithm parameters for Spark ML IsolationForest.

## TemporalAnomalyConfig Objects

```python
@dataclass
class TemporalAnomalyConfig()
```

Configuration for temporal feature extraction.

## FeatureEngineeringConfig Objects

```python
@dataclass
class FeatureEngineeringConfig()
```

Configuration for multi-type feature engineering in anomaly detection.

#### max\_input\_columns

Soft limit - warns but proceeds if exceeded

#### max\_engineered\_features

Soft limit on total engineered features

#### categorical\_cardinality\_threshold

OneHot if &lt;=20, Frequency if &gt;20

## AnomalyParams Objects

```python
@dataclass
class AnomalyParams()
```

Optional tuning parameters for row anomaly detection.

**Attributes**:

- `sample_fraction` - Fraction of data to sample for training (default 0.3).
- `max_rows` - Maximum rows to use for training (default 1,000,000).
- `train_ratio` - Train/validation split ratio (default 0.8).
- `ensemble_size` - Number of models in ensemble (default 3). Set to None for single model.
  Ensemble models provide:
  - More robust anomaly scores (averaged across models)
  - Confidence scores via standard deviation
  - Better generalization
- `Performance` - Optimized ensemble scoring makes this negligible overhead.
- `algorithm_config` - Isolation Forest parameters (contamination, num_trees, seed).
- `feature_engineering` - Feature engineering parameters (temporal features, scaling, etc.).

#### ensemble\_size

Default 3-model ensemble for robustness, tie-breaking, and confidence scores

## AnomalyConfig Objects

```python
@dataclass
class AnomalyConfig()
```

Configuration for row anomaly detection.

#### columns

Auto-discovered if omitted

#### segment\_by

Auto-discovered if omitted (when columns also omitted)

#### model\_name

Optional in workflows; defaults to dqx_anomaly_&lt;run_config.name&gt;

## RunConfig Objects

```python
@dataclass
class RunConfig()
```

Configuration class for the data quality checks

#### name

name of the run configuration

#### quarantine\_config

quarantined data table

#### metrics\_config

summary metrics table

#### checks\_user\_requirements

user input for AI-assisted rule generation

#### warehouse\_id

warehouse id to use in the dashboard

#### reference\_tables

reference tables to use in the checks

#### anomaly\_config

optional anomaly detection configuration

## LLMModelConfig Objects

```python
@dataclass
class LLMModelConfig()
```

Configuration for LLM model

#### api\_key

when used with Profiler Workflow, this should be a secret: secret_scope/secret_key

#### api\_base

when used with Profiler Workflow, this should be a secret: secret_scope/secret_key

## LLMConfig Objects

```python
@dataclass(frozen=True)
class LLMConfig()
```

Configuration for LLM usage

## ExtraParams Objects

```python
@dataclass(frozen=True)
class ExtraParams()
```

Class to represent extra parameters for DQEngine.

## WorkspaceConfig Objects

```python
@dataclass
class WorkspaceConfig()
```

Configuration class for the workspace

#### extra\_params

extra parameters to pass to the jobs, e.g. result_column_names

#### profiler\_max\_parallelism

max parallelism for profiling multiple tables

#### quality\_checker\_max\_parallelism

max parallelism for quality checking multiple tables

#### custom\_metrics

custom summary metrics tracked by the observer when applying checks

#### as\_dict

```python
def as_dict() -> dict
```

Convert the WorkspaceConfig to a dictionary for serialization.
This method ensures that all fields, including boolean False values, are properly serialized.
Used by blueprint&#x27;s installation when saving the config (Installation.save()).

**Returns**:

  A dictionary representation of the WorkspaceConfig.

#### get\_run\_config

```python
def get_run_config(run_config_name: str | None = "default") -> RunConfig
```

Get the run configuration for a given run name, or the default configuration if no run name is provided.

**Arguments**:

- `run_config_name` - The name of the run configuration to get, e.g. input table or job name (use &quot;default&quot; if not provided).
  

**Returns**:

  The run configuration.
  

**Raises**:

- `InvalidConfigError` - If no run configurations are available or if the specified run configuration name is
  not found.

## BaseChecksStorageConfig Objects

```python
@dataclass
class BaseChecksStorageConfig(abc.ABC)
```

Marker base class for storage configuration.

**Arguments**:

- `location` - The file path or table name where checks are stored.

## FileChecksStorageConfig Objects

```python
@dataclass
class FileChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a file.

**Arguments**:

- `location` - The file path where the checks are stored.

## WorkspaceFileChecksStorageConfig Objects

```python
@dataclass
class WorkspaceFileChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a workspace file.

**Arguments**:

- `location` - The workspace file path where the checks are stored.

## TableChecksStorageConfig Objects

```python
@dataclass
class TableChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a table.

**Arguments**:

- `location` - The table name where the checks are stored.
- `run_config_name` - The name of the run configuration to use for checks, e.g. input table or job name (use &quot;default&quot; if not provided).
- `mode` - The mode for writing checks to a table (&#x27;append&#x27; or &#x27;overwrite&#x27;, default &#x27;append&#x27;).
  - **overwrite**: Replaces all rows for this run_config_name when the fingerprint differs.
  Skips write when the fingerprint already exists.
  - **append**: Adds new rows when the fingerprint differs; multiple versions can coexist.
  Skips write when the fingerprint already exists.
- `rule_set_fingerprint` - Optional SHA-256 fingerprint of the rule set to load.
  When provided, loads rules matching this specific fingerprint instead of the latest batch.
  When None (default), loads the latest batch.

#### run\_config\_name

to filter checks by run config

#### rule\_set\_fingerprint

to filter checks by rule set fingerprint

## LakebaseChecksStorageConfig Objects

```python
@dataclass
class LakebaseChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a Lakebase table.

**Arguments**:

- `location` - Fully qualified name of the Lakebase table to store checks in the format &#x27;database.schema.table&#x27;.
- `instance_name` - Name of the Lakebase instance.
- `client_id` - ID of the Databricks service principal to use for the Lakebase connection.
- `port` - The Lakebase port (default is &#x27;5432&#x27;).
- `run_config_name` - Name of the run configuration to use for checks (default is &#x27;default&#x27;).
- `mode` - The mode for writing checks to a table (&#x27;append&#x27; or &#x27;overwrite&#x27;, default &#x27;append&#x27;).
  - **overwrite**: Replaces all rows for this run_config_name when the fingerprint differs.
  Skips write when the fingerprint already exists.
  - **append**: Adds new rows when the fingerprint differs; multiple versions can coexist.
  Skips write when the fingerprint already exists.
- `rule_set_fingerprint` - Optional SHA-256 fingerprint of the rule set to load.
  When provided, loads rules matching this specific fingerprint instead of the latest batch.
  When None (default), loads the latest batch.

## VolumeFileChecksStorageConfig Objects

```python
@dataclass
class VolumeFileChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a Unity Catalog volume file.

**Arguments**:

- `location` - The Unity Catalog volume file path where the checks are stored.

## InstallationChecksStorageConfig Objects

```python
@dataclass
class InstallationChecksStorageConfig(WorkspaceFileChecksStorageConfig,
                                      TableChecksStorageConfig,
                                      VolumeFileChecksStorageConfig,
                                      LakebaseChecksStorageConfig)
```

Configuration class for storing checks in an installation.

**Arguments**:

- `location` - The installation path where the checks are stored (e.g., table name, file path).
  Not used when using installation method, as it is retrieved from the installation config,
  unless overwrite_location is enabled.
- `run_config_name` - The name of the run configuration to use for checks, e.g. input table or job name (use &quot;default&quot; if not provided).
- `product_name` - The product name for retrieving checks from the installation (default is &#x27;dqx&#x27;).
- `assume_user` - Whether to assume the user is the owner of the checks (default is True).
- `install_folder` - The installation folder where DQX is installed.
  DQX will be installed in a default directory if no custom folder is provided:
  * User&#x27;s home directory: &quot;/Users/&lt;your_user&gt;/.dqx&quot;
  * Global directory if `DQX_FORCE_INSTALL=global`: &quot;/Applications/dqx&quot;
- `overwrite_location` - Whether to overwrite the location from run config if provided (default is False).

#### location

retrieved from the installation config

#### run\_config\_name

to retrieve run config

