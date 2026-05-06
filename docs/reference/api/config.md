# databricks.labs.dqx.config

## InputConfig Objects[​](#inputconfig-objects "Direct link to InputConfig Objects")

```python
@dataclass
class InputConfig()

```

Configuration class for input data sources (e.g. tables or files).

## OutputConfig Objects[​](#outputconfig-objects "Direct link to OutputConfig Objects")

```python
@dataclass
class OutputConfig()

```

Configuration class for output data sinks (e.g. tables or files).

### \_\_post\_init\_\_[​](#__post_init__ "Direct link to __post_init__")

```python
def __post_init__()

```

Normalize trigger configuration by converting string boolean representations to actual booleans. This is required due to the limitation of the config deserializer.

## ProfilerConfig Objects[​](#profilerconfig-objects "Direct link to ProfilerConfig Objects")

```python
@dataclass
class ProfilerConfig()

```

Configuration class for profiler.

#### summary\_stats\_file[​](#summary_stats_file "Direct link to summary_stats_file")

file containing profile summary statistics

#### sample\_fraction[​](#sample_fraction "Direct link to sample_fraction")

fraction of data to sample (30%)

#### sample\_seed[​](#sample_seed "Direct link to sample_seed")

seed for sampling

#### limit[​](#limit "Direct link to limit")

limit the number of records to profile

#### filter[​](#filter "Direct link to filter")

filter to apply to the data before profiling

#### criticality[​](#criticality "Direct link to criticality")

default criticality for generated rules ("error" or "warn")

## IsolationForestConfig Objects[​](#isolationforestconfig-objects "Direct link to IsolationForestConfig Objects")

```python
@dataclass
class IsolationForestConfig()

```

Algorithm parameters for Spark ML IsolationForest.

## TemporalAnomalyConfig Objects[​](#temporalanomalyconfig-objects "Direct link to TemporalAnomalyConfig Objects")

```python
@dataclass
class TemporalAnomalyConfig()

```

Configuration for temporal feature extraction.

## FeatureEngineeringConfig Objects[​](#featureengineeringconfig-objects "Direct link to FeatureEngineeringConfig Objects")

```python
@dataclass
class FeatureEngineeringConfig()

```

Configuration for multi-type feature engineering in anomaly detection.

#### max\_input\_columns[​](#max_input_columns "Direct link to max_input_columns")

Soft limit - warns but proceeds if exceeded

#### max\_engineered\_features[​](#max_engineered_features "Direct link to max_engineered_features")

Soft limit on total engineered features

#### categorical\_cardinality\_threshold[​](#categorical_cardinality_threshold "Direct link to categorical_cardinality_threshold")

OneHot if <=20, Frequency if >20

## AnomalyParams Objects[​](#anomalyparams-objects "Direct link to AnomalyParams Objects")

```python
@dataclass
class AnomalyParams()

```

Optional tuning parameters for row anomaly detection.

**Attributes**:

* `sample_fraction` - Fraction of data to sample for training (default 0.3).

* `max_rows` - Maximum rows to use for training (default 1,000,000).

* `train_ratio` - Train/validation split ratio (default 0.8).

* `ensemble_size` - Number of models in ensemble (default 3). Set to None for single model. Ensemble models provide:

  <!-- -->

  * More robust anomaly scores (averaged across models)
  * Confidence scores via standard deviation
  * Better generalization

* `Performance` - Optimized ensemble scoring makes this negligible overhead.

* `algorithm_config` - Isolation Forest parameters (contamination, num\_trees, seed).

* `feature_engineering` - Feature engineering parameters (temporal features, scaling, etc.).

#### ensemble\_size[​](#ensemble_size "Direct link to ensemble_size")

Default 3-model ensemble for robustness, tie-breaking, and confidence scores

## AnomalyConfig Objects[​](#anomalyconfig-objects "Direct link to AnomalyConfig Objects")

```python
@dataclass
class AnomalyConfig()

```

Configuration for row anomaly detection.

#### columns[​](#columns "Direct link to columns")

Auto-discovered if omitted

#### segment\_by[​](#segment_by "Direct link to segment_by")

Auto-discovered if omitted (when columns also omitted)

#### model\_name[​](#model_name "Direct link to model_name")

Optional in workflows; defaults to dqx\_anomaly\_\<run\_config.name>

## RunConfig Objects[​](#runconfig-objects "Direct link to RunConfig Objects")

```python
@dataclass
class RunConfig()

```

Configuration class for the data quality checks

#### name[​](#name "Direct link to name")

name of the run configuration

#### quarantine\_config[​](#quarantine_config "Direct link to quarantine_config")

quarantined data table

#### metrics\_config[​](#metrics_config "Direct link to metrics_config")

summary metrics table

#### checks\_user\_requirements[​](#checks_user_requirements "Direct link to checks_user_requirements")

user input for AI-assisted rule generation

#### warehouse\_id[​](#warehouse_id "Direct link to warehouse_id")

warehouse id to use in the dashboard

#### reference\_tables[​](#reference_tables "Direct link to reference_tables")

reference tables to use in the checks

#### anomaly\_config[​](#anomaly_config "Direct link to anomaly_config")

optional anomaly detection configuration

## LLMModelConfig Objects[​](#llmmodelconfig-objects "Direct link to LLMModelConfig Objects")

```python
@dataclass
class LLMModelConfig()

```

Configuration for LLM model

#### api\_key[​](#api_key "Direct link to api_key")

when used with Profiler Workflow, this should be a secret: secret\_scope/secret\_key

#### api\_base[​](#api_base "Direct link to api_base")

when used with Profiler Workflow, this should be a secret: secret\_scope/secret\_key

## LLMConfig Objects[​](#llmconfig-objects "Direct link to LLMConfig Objects")

```python
@dataclass(frozen=True)
class LLMConfig()

```

Configuration for LLM usage

## ExtraParams Objects[​](#extraparams-objects "Direct link to ExtraParams Objects")

```python
@dataclass(frozen=True)
class ExtraParams()

```

Class to represent extra parameters for DQEngine.

## WorkspaceConfig Objects[​](#workspaceconfig-objects "Direct link to WorkspaceConfig Objects")

```python
@dataclass
class WorkspaceConfig()

```

Configuration class for the workspace

#### extra\_params[​](#extra_params "Direct link to extra_params")

extra parameters to pass to the jobs, e.g. result\_column\_names

#### profiler\_max\_parallelism[​](#profiler_max_parallelism "Direct link to profiler_max_parallelism")

max parallelism for profiling multiple tables

#### quality\_checker\_max\_parallelism[​](#quality_checker_max_parallelism "Direct link to quality_checker_max_parallelism")

max parallelism for quality checking multiple tables

#### custom\_metrics[​](#custom_metrics "Direct link to custom_metrics")

custom summary metrics tracked by the observer when applying checks

### as\_dict[​](#as_dict "Direct link to as_dict")

```python
def as_dict() -> dict

```

Convert the WorkspaceConfig to a dictionary for serialization. This method ensures that all fields, including boolean False values, are properly serialized. Used by blueprint's installation when saving the config (Installation.save()).

**Returns**:

A dictionary representation of the WorkspaceConfig.

### get\_run\_config[​](#get_run_config "Direct link to get_run_config")

```python
def get_run_config(run_config_name: str | None = "default") -> RunConfig

```

Get the run configuration for a given run name, or the default configuration if no run name is provided.

**Arguments**:

* `run_config_name` - The name of the run configuration to get, e.g. input table or job name (use "default" if not provided).

**Returns**:

The run configuration.

**Raises**:

* `InvalidConfigError` - If no run configurations are available or if the specified run configuration name is not found.

## BaseChecksStorageConfig Objects[​](#basechecksstorageconfig-objects "Direct link to BaseChecksStorageConfig Objects")

```python
@dataclass
class BaseChecksStorageConfig(abc.ABC)

```

Marker base class for storage configuration.

**Arguments**:

* `location` - The file path or table name where checks are stored.

## FileChecksStorageConfig Objects[​](#filechecksstorageconfig-objects "Direct link to FileChecksStorageConfig Objects")

```python
@dataclass
class FileChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a file.

**Arguments**:

* `location` - The file path where the checks are stored.

## WorkspaceFileChecksStorageConfig Objects[​](#workspacefilechecksstorageconfig-objects "Direct link to WorkspaceFileChecksStorageConfig Objects")

```python
@dataclass
class WorkspaceFileChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a workspace file.

**Arguments**:

* `location` - The workspace file path where the checks are stored.

## TableChecksStorageConfig Objects[​](#tablechecksstorageconfig-objects "Direct link to TableChecksStorageConfig Objects")

```python
@dataclass
class TableChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a table.

**Arguments**:

* `location` - The table name where the checks are stored.

* `run_config_name` - The name of the run configuration to use for checks, e.g. input table or job name (use "default" if not provided).

* `mode` - The mode for writing checks to a table ('append' or 'overwrite', default 'append').

  <!-- -->

  * **overwrite**: Replaces all rows for this run\_config\_name when the fingerprint differs. Skips write when the fingerprint already exists.
  * **append**: Adds new rows when the fingerprint differs; multiple versions can coexist. Skips write when the fingerprint already exists.

* `rule_set_fingerprint` - Optional SHA-256 fingerprint of the rule set to load. When provided, loads rules matching this specific fingerprint instead of the latest batch. When None (default), loads the latest batch.

#### run\_config\_name[​](#run_config_name "Direct link to run_config_name")

to filter checks by run config

#### rule\_set\_fingerprint[​](#rule_set_fingerprint "Direct link to rule_set_fingerprint")

to filter checks by rule set fingerprint

## LakebaseChecksStorageConfig Objects[​](#lakebasechecksstorageconfig-objects "Direct link to LakebaseChecksStorageConfig Objects")

```python
@dataclass
class LakebaseChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a Lakebase table.

**Arguments**:

* `location` - Fully qualified name of the Lakebase table to store checks in the format 'database.schema.table'.

* `instance_name` - Name of the Lakebase instance.

* `client_id` - ID of the Databricks service principal to use for the Lakebase connection.

* `port` - The Lakebase port (default is '5432').

* `run_config_name` - Name of the run configuration to use for checks (default is 'default').

* `mode` - The mode for writing checks to a table ('append' or 'overwrite', default 'append').

  <!-- -->

  * **overwrite**: Replaces all rows for this run\_config\_name when the fingerprint differs. Skips write when the fingerprint already exists.
  * **append**: Adds new rows when the fingerprint differs; multiple versions can coexist. Skips write when the fingerprint already exists.

* `rule_set_fingerprint` - Optional SHA-256 fingerprint of the rule set to load. When provided, loads rules matching this specific fingerprint instead of the latest batch. When None (default), loads the latest batch.

## VolumeFileChecksStorageConfig Objects[​](#volumefilechecksstorageconfig-objects "Direct link to VolumeFileChecksStorageConfig Objects")

```python
@dataclass
class VolumeFileChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a Unity Catalog volume file.

**Arguments**:

* `location` - The Unity Catalog volume file path where the checks are stored.

## InstallationChecksStorageConfig Objects[​](#installationchecksstorageconfig-objects "Direct link to InstallationChecksStorageConfig Objects")

```python
@dataclass
class InstallationChecksStorageConfig(WorkspaceFileChecksStorageConfig,
                                      TableChecksStorageConfig,
                                      VolumeFileChecksStorageConfig,
                                      LakebaseChecksStorageConfig)

```

Configuration class for storing checks in an installation.

**Arguments**:

* `location` - The installation path where the checks are stored (e.g., table name, file path). Not used when using installation method, as it is retrieved from the installation config, unless overwrite\_location is enabled.

* `run_config_name` - The name of the run configuration to use for checks, e.g. input table or job name (use "default" if not provided).

* `product_name` - The product name for retrieving checks from the installation (default is 'dqx').

* `assume_user` - Whether to assume the user is the owner of the checks (default is True).

* `install_folder` - The installation folder where DQX is installed. DQX will be installed in a default directory if no custom folder is provided:

  <!-- -->

  * User's home directory: "/Users/\<your\_user>/.dqx"
  * Global directory if `DQX_FORCE_INSTALL=global`: "/Applications/dqx"

* `overwrite_location` - Whether to overwrite the location from run config if provided (default is False).

#### location[​](#location "Direct link to location")

retrieved from the installation config

#### run\_config\_name[​](#run_config_name-1 "Direct link to run_config_name")

to retrieve run config
