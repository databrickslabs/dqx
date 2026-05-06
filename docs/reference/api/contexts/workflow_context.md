# databricks.labs.dqx.contexts.workflow\_context

## WorkflowContext Objects[​](#workflowcontext-objects "Direct link to WorkflowContext Objects")

```python
class WorkflowContext(GlobalContext)

```

WorkflowContext class that provides a context for workflows, including workspace configuration,

### config[​](#config "Direct link to config")

```python
@cached_property
def config() -> WorkspaceConfig

```

Loads and returns the workspace configuration.

### spark[​](#spark "Direct link to spark")

```python
@cached_property
def spark() -> SparkSession

```

Returns spark session.

### run\_config\_name[​](#run_config_name "Direct link to run_config_name")

```python
@cached_property
def run_config_name() -> str | None

```

Returns run configuration name.

### runnable\_for\_patterns[​](#runnable_for_patterns "Direct link to runnable_for_patterns")

```python
@cached_property
def runnable_for_patterns() -> bool

```

Returns run configuration name.

### runnable\_for\_run\_config[​](#runnable_for_run_config "Direct link to runnable_for_run_config")

```python
@cached_property
def runnable_for_run_config() -> bool

```

Returns run configuration name.

### patterns[​](#patterns "Direct link to patterns")

```python
@cached_property
def patterns() -> str | None

```

Returns semicolon delimited list of location patterns to use.

### exclude\_patterns[​](#exclude_patterns "Direct link to exclude_patterns")

```python
@cached_property
def exclude_patterns() -> str | None

```

Returns semicolon delimited list of location patterns to exclude.

### output\_table\_suffix[​](#output_table_suffix "Direct link to output_table_suffix")

```python
@cached_property
def output_table_suffix() -> str

```

Returns suffix to use for output tables.

### quarantine\_table\_suffix[​](#quarantine_table_suffix "Direct link to quarantine_table_suffix")

```python
@cached_property
def quarantine_table_suffix() -> str

```

Returns suffix to use for quarantine tables.

### run\_config[​](#run_config "Direct link to run_config")

```python
@cached_property
def run_config() -> RunConfig

```

Loads and returns the run configuration.

### product\_info[​](#product_info "Direct link to product_info")

```python
@cached_property
def product_info() -> ProductInfo

```

Returns the ProductInfo instance for the runtime. If `product_name` is provided in `named_parameters`, it overrides the default product name. This is useful for testing or when the product name needs to be dynamically set at runtime.

### workspace\_client[​](#workspace_client "Direct link to workspace_client")

```python
@cached_property
def workspace_client() -> WorkspaceClient

```

Returns the WorkspaceClient instance.

### installation[​](#installation "Direct link to installation")

```python
@cached_property
def installation() -> Installation

```

Returns the installation instance for the runtime.

### resolved\_patterns[​](#resolved_patterns "Direct link to resolved_patterns")

```python
@cached_property
def resolved_patterns() -> tuple[list[str], list[str]]

```

Returns a tuple of patterns and exclude patterns lists.

### profiler[​](#profiler "Direct link to profiler")

```python
@cached_property
def profiler() -> ProfilerRunner

```

Returns the ProfilerRunner instance.

### quality\_checker[​](#quality_checker "Direct link to quality_checker")

```python
@cached_property
def quality_checker() -> QualityCheckerRunner

```

Returns the QualityCheckerRunner instance.

### prepare\_run\_config[​](#prepare_run_config "Direct link to prepare_run_config")

```python
def prepare_run_config(run_config: RunConfig) -> RunConfig

```

Apply common path prefixing to a run configuration in-place and return it. Ensures custom check function paths and checks location are absolute in the Databricks Workspace.

**Arguments**:

* `run_config` - The run configuration to prepare.

**Returns**:

The prepared run configuration.
