# databricks.labs.dqx.config

## DQSecret Objects[​](#dqsecret-objects "Direct link to DQSecret Objects")

```python
@dataclass(frozen=True)
class DQSecret()

```

A reference to a Databricks secret stored in a secret scope.

Provides a canonical *scope/key* string representation that can be passed as a credential reference in DQX configuration without embedding the secret value in plain text.

Resolves against **classic Databricks workspace secret scopes** (the *dbutils.secrets* / Secrets API), **not** Unity Catalog secrets. The value is read at delivery time from the workspace that the engine's *WorkspaceClient* targets, so the scope and key must exist in that same workspace.

**Arguments**:

* `scope` - The Databricks secret scope name.
* `key` - The key within the secret scope.

### as\_reference[​](#as_reference "Direct link to as_reference")

```python
def as_reference() -> str

```

Return the canonical *scope/key* reference string.

**Returns**:

A string of the form `scope/key`.

### from\_reference[​](#from_reference "Direct link to from_reference")

```python
@classmethod
def from_reference(cls, ref: str) -> "DQSecret"

```

Parse a *scope/key* reference string into a *DQSecret*.

The string is split on the **first** `/` only, so a key that itself contains slashes is handled correctly.

**Arguments**:

* `ref` - A reference string in the form `scope/key`.

**Returns**:

A new *DQSecret* instance.

**Raises**:

* `InvalidParameterError` - If *ref* does not contain a `/`, or if either the scope or key part is empty.

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

#### sample\_seed[​](#sample_seed "Direct link to sample_seed")

seed for sampling

#### sample\_by\_column[​](#sample_by_column "Direct link to sample_by_column")

column with keys to sample by

#### sample\_by\_values\_limit[​](#sample_by_values_limit "Direct link to sample_by_values_limit")

max distinct sample\_by\_column values to collect when sampling uniformly

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

### is\_table\_location[​](#is_table_location "Direct link to is_table_location")

```python
def is_table_location(location: str) -> bool

```

Return True if *location* is a Delta table name (catalog.schema.table), not a file path.

A location is a table when it matches the table-identifier pattern AND does not end with a known checks-serializer file extension (.json/.yml/...). This is the single source of truth for the table-vs-file distinction; *io.py* and *checks\_storage.py* re-export it.

**Arguments**:

* `location` - The location string to classify.

**Returns**:

True if *location* names a table, False if it is a file path or otherwise not a table.

## BaseChecksStorageConfig Objects[​](#basechecksstorageconfig-objects "Direct link to BaseChecksStorageConfig Objects")

```python
class BaseChecksStorageConfig(BaseModel, ABC)

```

Marker base class for storage configuration.

**Arguments**:

* `location` - The file path or table name where checks are stored.

### replace[​](#replace "Direct link to replace")

```python
def replace(**changes: Any) -> "BaseChecksStorageConfig"

```

Return a new config instance with the given field overrides, fully re-validated.

Unlike *model\_copy(update=...)*, which shallow-copies the instance and skips all validators, this rebuilds the config through the constructor so the *model\_validator* checks (e.g. *mode* and *location* format) re-run against the updated fields. Use this instead of *model\_copy* when overriding fields, so an invalid override is rejected at the point of the change rather than failing later during a save/load operation.

**Arguments**:

* `**changes` - Field values to override on the new instance.

**Returns**:

A new, fully validated config of the same concrete type.

## FileChecksStorageConfig Objects[​](#filechecksstorageconfig-objects "Direct link to FileChecksStorageConfig Objects")

```python
class FileChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a file.

**Arguments**:

* `location` - The file path where the checks are stored.

## WorkspaceFileChecksStorageConfig Objects[​](#workspacefilechecksstorageconfig-objects "Direct link to WorkspaceFileChecksStorageConfig Objects")

```python
class WorkspaceFileChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a workspace file.

**Arguments**:

* `location` - The workspace file path where the checks are stored.

## TableChecksStorageConfig Objects[​](#tablechecksstorageconfig-objects "Direct link to TableChecksStorageConfig Objects")

```python
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
class VolumeFileChecksStorageConfig(BaseChecksStorageConfig)

```

Configuration class for storing checks in a Unity Catalog volume file.

**Arguments**:

* `location` - The Unity Catalog volume file path where the checks are stored.

## InstallationChecksStorageConfig Objects[​](#installationchecksstorageconfig-objects "Direct link to InstallationChecksStorageConfig Objects")

```python
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

## TableActionsStorageConfig Objects[​](#tableactionsstorageconfig-objects "Direct link to TableActionsStorageConfig Objects")

```python
class TableActionsStorageConfig(BaseChecksStorageConfig)

```

Configuration class for persisting DQ action definitions to a Unity Catalog table.

**Arguments**:

* `location` - Fully qualified UC table name (e.g. *catalog.schema.table*) where action definitions are stored.
* `run_config_name` - Name of the run configuration these actions belong to (default is *default*).
* `mode` - Write mode for the table (*append* or *overwrite*, default *append*).

## LakebaseActionsStorageConfig Objects[​](#lakebaseactionsstorageconfig-objects "Direct link to LakebaseActionsStorageConfig Objects")

```python
class LakebaseActionsStorageConfig(BaseChecksStorageConfig)

```

Configuration class for persisting DQ action definitions to a Lakebase (PostgreSQL) table.

The *location* must be a fully qualified three-part name in the form *database.schema.table*.

**Arguments**:

* `location` - Fully qualified Lakebase table name in the format *database.schema.table*.
* `instance_name` - Name of the Lakebase instance.
* `client_id` - ID of the Databricks service principal for the Lakebase connection.
* `port` - Lakebase port (default is *5432*).
* `run_config_name` - Name of the run configuration these actions belong to (default is *default*).
* `mode` - Write mode for the table (*append* or *overwrite*, default *append*).

### database\_name[​](#database_name "Direct link to database_name")

```python
@property
def database_name() -> str

```

The database portion of the three-part location.

### schema\_name[​](#schema_name "Direct link to schema_name")

```python
@property
def schema_name() -> str

```

The schema portion of the three-part location.

### table\_name[​](#table_name "Direct link to table_name")

```python
@property
def table_name() -> str

```

The table portion of the three-part location.

## ActionEventsConfig Objects[​](#actioneventsconfig-objects "Direct link to ActionEventsConfig Objects")

```python
class ActionEventsConfig(BaseChecksStorageConfig)

```

Configuration class for storing DQ action events in a Unity Catalog table.

**Arguments**:

* `location` - Fully qualified UC table name (e.g. *catalog.schema.events*) where action events are written.
* `mode` - Reserved for API symmetry with the other storage configs. Action events form an append-only audit log, so the events table is always appended to regardless of this value.
* `run_config_name` - Run configuration the events belong to. Events are stamped with, and loaded filtered by, this value so a shared events table keeps per-run-config alert suppression independent (default is *default*).
