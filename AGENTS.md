# AI Agent Guidelines for DQX

## Quick Command Reference

```bash
make dev            # Create .venv and install dependencies
make fmt            # Format code — run before every commit
make lint           # ruff + mypy
make test           # Unit tests
make integration    # Integration tests (needs Databricks workspace)
make e2e            # End-to-end tests
make coverage       # Coverage report
make help           # All available targets

.venv/bin/pytest tests/unit/test_build_rules.py -v          # single file
.venv/bin/pytest tests/unit/test_check_funcs.py::test_foo   # single test
```

---

## Project Overview

**DQX** is a Databricks Labs Python library for PySpark data quality checks — batch and streaming. It provides rule-based checks, LLM-driven rule generation, ML anomaly detection, data profiling, PII detection, and a Databricks CLI installer.

**Data flow:** `DQRule` → `check_func` → PySpark `Column` expression → appended to DataFrame as `_errors` / `_warnings` columns.

```
src/databricks/labs/dqx/
  ├── engine.py            # DQEngine / DQEngineCore — primary public API
  ├── base.py              # DQEngineBase, DQEngineCoreBase (abstract)
  ├── rule.py              # DQRule (abstract), DQRowRule, DQDatasetRule, DQForEachColRule, Criticality
  ├── check_funcs.py       # Built-in checks: null, range, regex, referential, aggregate…
  ├── manager.py           # DQRuleManager — build/manage rule collections
  ├── config.py            # WorkspaceConfig, RunConfig, AnomalyParams, LLMModelConfig, ExtraParams
  ├── checks_storage.py    # WorkspaceFileChecksStorageHandler, VolumeFileChecksStorageHandler
  ├── checks_serializer.py / checks_resolver.py / checks_validator.py
  ├── cli.py               # Databricks Labs CLI commands (@dqx.command)
  ├── profiler/            # Data profiling, rule generation, DLT pipeline generation
  ├── anomaly/             # ML row-level anomaly detection (MLflow, ensemble, drift, explainability)
  ├── llm/                 # LLM-based rule generation via DSPy (DQLLMEngine)
  ├── pii/                 # PII detection — optional dep: presidio + spaCy
  ├── geo/                 # Geospatial check functions
  ├── schema/              # DQ result and info schemas
  ├── installer/           # Workspace install/uninstall, workflow & dashboard installer
  ├── quality_checker/     # E2E workflow runner
  ├── contexts/            # workspace_context, workflow_context, cli_context
  └── dashboards/          # Lakeview dashboard support
tests/
  ├── unit/                # No Spark / workspace — runs in seconds
  ├── integration/         # Needs live workspace + databricks-connect
  ├── e2e/                 # Full workflow tests
  ├── integration_anomaly/ # MLflow + Unity Catalog
  └── perf/                # Benchmarks
```

---

## Critical Rules

### 1. Never skip `@register_rule` on check functions

Without it the function is absent from `CHECK_FUNC_REGISTRY` and invisible to `apply_checks_by_metadata`. Always decorate.

### 2. Never import optional dependencies in core modules

| Module | Requires |
|---|---|
| `pii/` | `pip install -e ".[pii]"` + spaCy model download |
| `llm/` | `dspy` |
| `anomaly/` | `mlflow` |

Never import these unconditionally in `engine.py`, `check_funcs.py`, or any non-optional module.

### 3. Never instantiate `DQRule` directly

`DQRule` is abstract. Use `DQRowRule` for row-level rules and `DQDatasetRule` for dataset-level rules. If wanting to apply the same rule to multiple columns at once use `DQForEachColRule`.

### 4. Never modify `ExtraParams` in-place

`ExtraParams` is `@dataclass(frozen=True)`. Use `dataclasses.replace(extra_params, field=value)`.

### 5. Never serialize configs with `dataclasses.asdict()`

Use `ConfigSerializer` — it preserves nested types. `dataclasses.asdict()` loses them.

### 6. GPG-sign all commits

```bash
git config --global commit.gpgsign true
git verify-commit <hash>   # verify after first commit
```

See `CONTRIBUTING.md` for key setup.

---

## Coding Guidelines

### Check Functions

```python
# ✅ correct
from databricks.labs.dqx.rule import register_rule
from pyspark.sql import Column
import pyspark.sql.functions as F

@register_rule("row")                          # "row" = row-level, "dataset" = group of rows
def is_not_empty(column: str | Column) -> Column:
    col = F.col(column) if isinstance(column, str) else column
    return col.isNotNull() & (F.trim(col) != "")

# ❌ wrong — missing decorator, missing return type, returns DataFrame
def is_not_empty(column):
    return df.filter(...)
```

Rules:
1. Decorate with `@register_rule("row")` (row-level) or `@register_rule("dataset")` (group of rows)
2. Return a PySpark `Column` — **never** a `DataFrame`
3. Use `SingleColumnMixin` / `MultipleColumnsMixin` for column resolution

### Rule Construction

```python
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule, Criticality
from databricks.labs.dqx import check_funcs as checks

# ✅ use concrete subclasses
DQRowRule(check_func=checks.is_not_null, column="id", criticality=Criticality.ERROR, name="id_not_null")
DQForEachColRule(check_func=checks.is_not_null, columns=["id", "name", "date"])

# ❌ DQRule is abstract — instantiation raises TypeError
DQRule(check_func=checks.is_not_null, ...)
```

### Type Hints

Every parameter and return value must be annotated. Enforced by mypy (`make lint`).

```python
# ✅
def resolve(column: str | Column, spark: SparkSession) -> Column: ...

# ❌
def resolve(column, spark): ...
```

| Rule | ✅ Do | ❌ Don't |
|---|---|---|
| Generics | `list[str]`, `dict[str, int]` | `List[str]`, `Dict[str, int]` |
| Optional | `str \| None` | `Optional[str]` |
| Union | `str \| int` | `Union[str, int]` |
| Callables | `collections.abc.Callable` | `typing.Callable` |
| Unknown types | `object` or `Protocol` | `Any` |

**Avoid `Any`.** When it is truly unavoidable (e.g., untyped external ML library), add:
```python
model: Any  # type: ignore[assignment] — mlflow has no stubs
```
`Any` in `anomaly/` is a known legacy exception. New code outside that module must not introduce it.

### Blueprint Patterns

`databricks-labs-blueprint` provides CLI, installation, parallelism, and rate-limiting. Use it — don't reinvent.

| Need | Import | Usage |
|---|---|---|
| Module logger | `import logging` | `logger = logging.getLogger(__name__)` |
| CLI entrypoint logger | `blueprint.entrypoint.get_logger` | `logger = get_logger(__file__)` |
| Package logger setup | `blueprint.logger.install_logger` | call once in `__init__.py` |
| Parallel tasks | `blueprint.parallel.Threads` | `Threads.strict("label", tasks)` |
| Aggregate errors | `blueprint.parallel.ManyError` | catch at top level |
| Rate limiting | `blueprint.limiter.rate_limited` | `@rate_limited(max_requests=100)` |
| Config persistence | `blueprint.installation.Installation` | `installation.load(WorkspaceConfig)` |
| Test config | `blueprint.installation.MockInstallation` | in-memory, no workspace needed |
| Test prompts | `blueprint.tui.MockPrompts` | regex → response map |
| Product versioning | `blueprint.wheels.ProductInfo` | `ProductInfo.for_testing(WorkspaceConfig)` |

**Parallel execution:**
```python
from databricks.labs.blueprint.parallel import ManyError, Threads

Threads.strict("label", tasks)                       # raises ManyError if any fail
results, errors = Threads.gather("label", tasks)     # returns (successes, failures)
# ❌ don't use concurrent.futures directly
```

**Installation (config persistence):**
```python
from databricks.labs.blueprint.installation import Installation, MockInstallation

# production
installation = Installation(ws, "dqx")
config = installation.load(WorkspaceConfig)
installation.save(config)

# unit tests — never use real Installation in unit tests
mock = MockInstallation({"version": "1.0.0"})
```

**MockPrompts (test interactive CLI):**
```python
from databricks.labs.blueprint.tui import MockPrompts

prompts = MockPrompts({r"Do you want to uninstall.*": "yes", r".*": ""})
```

### CLI Commands

```python
# WorkspaceClient auto-injected; keyword-only args become CLI flags
@dqx.command
def my_command(w: WorkspaceClient, *, install_folder: str = "", ctx: WorkspaceContext | None = None):
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    ...
```

### Config Dataclasses

All configs use `@dataclass`. Serialize via `ConfigSerializer`. Frozen configs use `dataclasses.replace()`.

### Telemetry

Wrap new public engine methods with `@telemetry_logger`. Call `log_telemetry()` / `log_dataframe_telemetry()` from `telemetry.py`.

---

## Testing

### Test Layer Rules

| Layer | Location | Requirements | When to write |
|---|---|---|---|
| Unit | `tests/unit/` | None — no Spark, no workspace | Always: every new function/class |
| Integration | `tests/integration/` | Live workspace + `databricks-connect` | CLI commands, storage handlers |
| E2E | `tests/e2e/` | Full workspace | Complete workflows |
| Anomaly | `tests/integration_anomaly/` | Unity Catalog + MLflow | `anomaly/` module changes |
| Perf | `tests/perf/` | Workspace | Performance-sensitive paths |

Minimum per new check function: one positive test + one negative test.
Minimum per new CLI command: one integration test with `MockInstallation` + `MockPrompts`.

### pytester `factory` Pattern

Use `factory` from `databricks.labs.pytester.fixtures.baseline` for every integration fixture that creates a Databricks resource. It guarantees cleanup on test failure.

```python
from databricks.labs.pytester.fixtures.baseline import factory

@pytest.fixture
def make_check_file(ws, make_directory, checks_yaml_content):
    def create(**kwargs) -> str:
        path = str(make_directory().absolute()) + "/checks.yml"
        ws.workspace.upload(path=path, content=checks_yaml_content.encode(), overwrite=True)
        return path

    def delete(path: str) -> None:
        ws.workspace.delete(path)

    yield from factory("file", create, delete)

# ❌ don't delete manually in test body — factory handles it even on failure
```

Auto-provided fixtures (from pytester, no need to define): `ws` (`WorkspaceClient`), `spark` (`SparkSession`), `make_random`, `make_directory`, `env_or_skip`.

---

## Common Tasks

### Apply checks to a DataFrame

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, Criticality
from databricks.labs.dqx import check_funcs as checks

engine = DQEngine(WorkspaceClient())

rules = [
    DQRowRule(check_func=checks.is_not_null, column="id", criticality=Criticality.ERROR),
    DQRowRule(check_func=checks.is_not_null_and_not_empty, column="name", criticality=Criticality.WARN),
]

# Returns DataFrame with _errors and _warnings columns appended
checked_df = engine.apply_checks(df, rules)

# Or split into valid / invalid
valid_df, invalid_df = engine.apply_checks_and_split(df, rules)
```

### Load checks from file and apply

```python
# Load from workspace YAML/JSON
checks_list = engine.load_checks_from_local_file("checks.yml")
checked_df = engine.apply_checks_by_metadata(df, checks_list)
```

### Add a new built-in check function

1. Add to `src/databricks/labs/dqx/check_funcs.py` with `@register_rule("raw")` or `@register_rule("dataset")`
2. Return a PySpark `Column`
3. Add to `__all__` if public
4. Add unit tests in `tests/unit/test_check_funcs_<category>.py`

### Add a new CLI command

1. Add `@dqx.command` function to `cli.py`
2. Inject `WorkspaceClient`; use `WorkspaceContext` for workspace operations
3. Add integration test in `tests/integration/test_cli.py` using `MockInstallation` + `MockPrompts`

---

## Documentation

- **[README.md](./README.md)** — Quick-start
- **[docs/](./docs/)** — Full site (Docusaurus): [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)
- **[CONTRIBUTING.md](./CONTRIBUTING.md)** — Developer setup, GPG signing, contribution workflow
- **[CHANGELOG.md](./CHANGELOG.md)** — Release history

---

**Stack**: Python 3.10+ · PySpark · Databricks SDK · databricks-labs-blueprint · databricks-labs-pytester · DSPy · Presidio · MLflow
