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
make docs-install   # Install docs dependencies (yarn)
make docs-build     # Build docs (pydoc-markdown + Docusaurus)
make help           # All available targets

.venv/bin/pytest tests/unit/test_build_rules.py -v          # single file
.venv/bin/pytest tests/unit/test_check_funcs.py::test_foo   # single test

git config --global commit.gpgsign true  # GPG-sign all commits (required)
git verify-commit <hash>   # verify after first commit
```

### Dependency installs and lock files

- Use **`make dev`** from the repo root to create `.venv` and install Python dependencies. Do **not** run `uv sync`, `uv lock`, or `uv add` for normal setup — that bypasses `UV_FROZEN=1` and may modify `uv.lock` or bake in internal registry URLs.
- To **update lock files** after intentional dependency changes: `make lock-dependencies` (root `uv.lock` and `.build-constraints.txt`) and/or `make lock-app-dependencies` (`app/uv.lock`) of the app.

---

## Project Overview

**DQX** is a Databricks Labs Python library for PySpark data quality checks — batch and streaming. It provides rule-based checks, LLM-driven rule generation, ML anomaly detection, data profiling, PII detection, and a Databricks CLI installer.

**Data flow:** `DQRule` → `check_func` → PySpark `Column` expression → appended to DataFrame as `_errors` / `_warnings` columns.

```
src/databricks/labs/dqx/
  ├── engine.py            # DQEngine / DQEngineCore — primary public API
  ├── base.py              # DQEngineBase, DQEngineCoreBase (abstract)
  ├── rule.py              # DQRule (abstract), DQRowRule, DQDatasetRule, DQForEachColRule, Criticality
  ├── rule_fingerprint.py  # Rule fingerprinting (compute_rule_fingerprint, compute_rule_set_fingerprint_by_metadata)
  ├── check_funcs.py       # Built-in checks: null, range, regex, referential, aggregate…
  ├── manager.py           # DQRuleManager — build/manage rule collections
  ├── config.py            # WorkspaceConfig, RunConfig, AnomalyParams, LLMModelConfig, ExtraParams
  ├── checks_storage.py    # WorkspaceFileChecksStorageHandler, VolumeFileChecksStorageHandler
  ├── checks_serializer.py / checks_resolver.py / checks_validator.py / checks_formats.py
  ├── config_serializer.py # ConfigSerializer — use instead of dataclasses.asdict()
  ├── cli.py               # Databricks Labs CLI commands (@dqx.command)
  ├── errors.py            # For example: MissingParameterError, InvalidParameterError, UnsafeSqlQueryError — use instead of built-in exceptions
  ├── telemetry.py         # telemetry_logger, log_telemetry, log_dataframe_telemetry
  ├── utils.py             # shared helpers (column name resolution, SQL safety checks, etc.)
  ├── executor.py / io.py / table_manager.py / workflows_runner.py
  ├── metrics_listener.py / metrics_observer.py / reporting_columns.py
  ├── profiling_utils.py   # Profiling utilities (stats, column metadata helpers)
  ├── profiler/            # Data profiling, rule generation, DLT pipeline generation
  ├── anomaly/             # ML row-level anomaly detection (MLflow, ensemble, drift, explainability)
  ├── llm/                 # LLM-based rule generation via DSPy (DQLLMEngine)
  ├── pii/                 # PII detection — optional dep: presidio + spaCy
  ├── geo/                 # Geospatial check functions
  ├── datacontract/        # Data contract rule generation
  ├── schema/              # DQ result and info schemas
  ├── installer/           # Workspace install/uninstall, workflow, dashboard, warehouse installer
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

## Production Code & Testing Standards

### Code Quality Principles

- Apply the **DRY principle**: avoid duplicating logic, validation, or configuration. Extract shared utilities only when the same logic appears in three or more places.
- Apply **dependency injection**: pass `WorkspaceClient`, `SparkSession`, and other external dependencies as constructor or function arguments rather than constructing them inside classes.
- Maintain **separation of concerns**: keep business logic decoupled from I/O, persistence, or framework details.
- Prefer **composition over inheritance** for new code. Where the existing mixin hierarchy (e.g. `DQRuleTypeMixin`) already provides the right extension point, use it — do not introduce additional inheritance layers on top.

### Pure Functions & Side Effects

- Write **pure functions** wherever possible: no hidden state, no side effects, deterministic outputs. All check functions in `check_funcs.py` must be pure — they receive column references and return a `Column` expression only.
- Encapsulate side effects (workspace API calls, file I/O, network) at well-defined boundaries, not inside check logic or rule construction.

### Testing Requirements

- **Cover all changes with tests.** New check functions and rule logic → unit tests. Workspace interactions → integration tests. Bug fixes → regression tests.
- **Unit tests** (`tests/unit/`) run without Spark or a live workspace and must stay fast.
- **Integration tests** (`tests/integration/`) require a real workspace and spark session; do not add workspace API calls to unit tests.
- Test **behaviour, not implementation details**: assert on outputs and observable state, not on private methods or internal data structures.
- Use **dependency injection to enable testing**: construct dependencies with `create_autospec` rather than patching internal module state.
- Use **pytest fixtures** (`conftest.py`) to share setup and teardown logic across tests. Unit-level fixtures live in `tests/unit/conftest.py`; integration-level fixtures in `tests/integration/conftest.py`. Do not duplicate fixture logic inline in individual tests.
- For workspace resource creation and cleanup in integration tests, use the pytester `factory` helper — see [## Testing](#testing) for the established patterns.
- If a test requires a real `SparkSession`, it is an **integration test** — place it in `tests/integration/`, not `tests/unit/`. Unit tests must never start or depend on a Spark session; use `create_autospec(SparkSession)` for any unit-level Spark dependency.
- Avoid `unittest.mock.patch` and `pytest.monkeypatch` unless the target is a module-level constant or a third-party boundary with no injectable seam. Patching internal symbols couples tests to implementation details.
- Tests must be **deterministic and isolated**: no timing dependencies, randomness, shared mutable state, or real network calls in unit tests.

### Agent Behaviour

- Keep responses **concise and practical**.
- Provide **production-grade solutions**, not prototype or demo code.
- Avoid speculative or unrequested changes.
- Align with the project's coding conventions and testing standards above.

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

`DQRule` is abstract. Use `DQRowRule` for row-level rules and `DQDatasetRule` for dataset-level rules. To apply the same rule to multiple columns at once, use `DQForEachColRule` — note that `DQForEachColRule` does **not** inherit from `DQRule`; it is a factory that produces `DQRule` instances via `get_rules()`.

### 4. Never modify `ExtraParams` in-place

`ExtraParams` is `@dataclass(frozen=True)`. Use `dataclasses.replace(extra_params, field=value)`.

### 5. Never serialize configs with `dataclasses.asdict()`

Use `ConfigSerializer` — it preserves nested types. `dataclasses.asdict()` loses them.

### 6. Never disable linting to silence issues

Fix the code instead of adding `# pylint: disable`, `# type: ignore`, `# noqa`, or per-file ignores. Use project-wide exceptions in `pyproject.toml` only when there is no viable fix (e.g., third-party API compatibility).

---

## Security Requirements

**Mandatory:** Agents must comply with the following security requirements. These are non-negotiable; any added or modified code must not introduce security gaps. Violations must be rejected or remediated before merge.
- **SQL injection**: User-provided or templated SQL must be validated with `is_sql_query_safe()` from `utils.py` before execution. Raise `UnsafeSqlQueryError` when the query is unsafe. Do not embed user input directly into SQL strings; use parameterized queries or validated template substitution (e.g., `re.escape` for keys).
- **Sensitive content**: Do not include in the repository, history, issues, PRs, workflows, or CI logs: PII, customer identifiers, internal URLs, internal system names, architecture details, secrets, keys, or tokens. Use structured logging with redaction. Before release, commit history must be reviewed and remediated or rewritten if needed.
- **Untrusted input**: When parsing JSON, YAML, or other formats from external sources, handle parse failures gracefully and avoid exposing raw error details that could leak internal structure.
- **Path traversal**: Validate file and workspace paths; avoid constructing paths from unsanitized user input.
- **Regex (ReDoS)**: Be aware of Regular Expression Denial of Service—complex patterns with nested quantifiers, alternation, or backtracking (e.g. `(a+)+`, `(a|a)*`) can cause catastrophic backtracking on adversarial input and hang the process. Validate or limit regex inputs from users; prefer simple patterns or use libraries with ReDoS-safe engines when handling untrusted strings.
- **Dependencies**: Do not add dependencies with known vulnerabilities. Prefer well-maintained, widely used packages. Ensure dependencies and their licenses are compliant for intended distribution (e.g., external or OSS).
- **Documentation and metadata**: Ensure metadata and documentation are safe for public distribution. Documentation must clearly describe intended use, supported environments, and any restrictions.
- **Open source hygiene**: Do not remove LICENSE, NOTICE, or attribution files. Maintainer contact should be identifiable.
- **Security controls**: Do not disable or bypass secret scanning, software composition analysis, or vulnerability scanning. Releases process should use CI-built tagged artifacts.
- **GitHub workflows**: Avoid `pull_request_target` unless strictly necessary. The danger arises when this trigger (which bypasses GitHub's approval gate and runs in the base-branch context with secrets) is *combined with* checkout of untrusted fork code (e.g. `refs/pull/.../merge`) — that PR code can then exfiltrate secrets. Prefer `pull_request` for CI. If `pull_request_target` is required (e.g., for label-based workflows), never checkout or run PR-provided code when secrets are available; use `workflow_run` or `workflow_dispatch` for sensitive operations instead.
- **Pickle / model deserialization**: `cloudpickle` and `mlflow.sklearn.load_model()` execute arbitrary code on deserialization. Only load models from trusted, authenticated MLflow registries within the controlled Databricks workspace. Never pass user-supplied model URIs to load functions without validating they resolve to a known, controlled registry path.
- **LLM prompt injection**: User-supplied inputs to the `llm/` module (business descriptions, column names, sample data) are embedded in LLM prompts and can be crafted to manipulate model behaviour. Treat all such inputs as untrusted. LLM-generated output — function names, SQL fragments, rule arguments — must be validated before execution: function names must resolve through `CHECK_FUNC_REGISTRY`; any generated SQL must pass `is_sql_query_safe()`.
- **Log injection**: Never embed user-supplied values (column names, rule names, file paths, contract identifiers) directly in log messages via f-strings without sanitization. Newlines and control characters in these values can forge log entries or corrupt log pipelines. Strip or escape newlines before logging untrusted strings; prefer structured logging fields over interpolated messages (CWE-117).
- **SSRF (Server-Side Request Forgery)**: User-controlled URL fields such as *api_base* in *LLMModelConfig* are passed to outbound HTTP clients. Validate that any user-supplied URL targets an expected, allowlisted host before use. Do not allow arbitrary internal network endpoints to be reached via user config (CWE-918).
- **Secrets management**: Design credential-accepting fields (API keys, tokens, passwords) to support secret scope references (e.g., `secret_scope/secret_key`) rather than raw strings. Document in docstrings that plaintext values are for local development only. Never log credential fields, even partially — redact them at the point of construction, not at the call site.
- **LLM denial of service and information disclosure**: Malicious or pathological prompts can trigger unbounded or very expensive inference calls (OWASP LLM04) — enforce token/budget limits on all LLM calls. LLM responses may inadvertently echo back sensitive inputs such as schema metadata or data samples (OWASP LLM06) — treat LLM output as untrusted data; do not relay raw LLM responses to end users or logs without review.

---

## Coding Guidelines

### Check Functions

```python
# ✅ correct
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition, get_normalized_column_and_expr
from databricks.labs.dqx.rule import register_rule


@register_rule("row")
def is_not_null(column: str | Column) -> Column:
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    return make_condition(
      col_expr.isNull(), 
      f"Column '{col_expr_str}' value is null", 
      f"{col_str_norm}_is_null"
    )


# ❌ wrong — missing decorator, missing return type, returns DataFrame, bypasses make_condition
def is_not_null(column):
    return df.filter(...)
```

Rules:
1. Decorate with `@register_rule("row")` (row-level) or `@register_rule("dataset")` (group of rows)
2. Return a PySpark `Column` — **never** a `DataFrame`
3. Use `make_condition(condition, message, name)` from `check_funcs.py` to build the return value.

### Rule Construction

Rules can be defined programmatically (DQX classes) or declaratively (dict metadata/YAML/JSON). Both are equivalent — choose based on context.

**Programmatic API — use when building rules in code using DQX classes:**
```python
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule, Criticality
from databricks.labs.dqx import check_funcs as checks

# ✅ use concrete subclasses
DQRowRule(check_func=checks.is_not_null, column="id", criticality=Criticality.ERROR, name="id_not_null")
DQForEachColRule(check_func=checks.is_not_null, columns=["id", "name", "date"])

# ❌ DQRule is abstract — instantiation raises TypeError
DQRule(check_func=checks.is_not_null, ...)
```

**Metadata API — build rules declaratively using list[dict] or YAML/JSON:**

```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: id
- check:
    function: is_not_null
    for_each_column:
    - id
    - name
    - date
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

### Docstrings

Use [Google Style Python Docstrings](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) for public functions, classes, and modules. See [Writing Docstrings](https://databrickslabs.github.io/dqx/docs/dev/contributing/#writing-docstrings) for full guidance.

- **No backticks** around object names — use italics instead (e.g., *arg1*, *column*). Backticks cause rendering issues in API docs.
- **Double curly braces** — mask with backslashes, e.g. `{{`. For code examples, use triple backticks.

### Blueprint Patterns

`databricks-labs-blueprint` provides CLI, installation, parallelism, and rate-limiting. Use it — don't reinvent.

| Need | Import | Usage |
|---|---|---|
| Module logger | `import logging` | `logger = logging.getLogger(__name__)`; use f-strings for log messages (e.g. `logger.warning(f"value={x}")`), not `%`-style args |
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

### Apply checks to a DataFrame (define checks programmatically)

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

### Apply checks to a DataFrame (define checks declaratively)

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

engine = DQEngine(WorkspaceClient())

checks = yaml.safe_load("""
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: id
    - criticality: warn
      check:
        function: is_not_null_and_not_empty
        arguments:
          column: name
""")

checked_df = engine.apply_checks_by_metadata(df, checks)

# Or split into valid / invalid (quarantine)
valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)
```

### Load checks from file and apply

```python
# Load from workspace YAML/JSON
checks_list = engine.load_checks_from_local_file("checks.yml")
checked_df = engine.apply_checks_by_metadata(df, checks_list)
```

### Add a new built-in check function

1. Add to `src/databricks/labs/dqx/check_funcs.py` with `@register_rule("row")` or `@register_rule("dataset")`
2. Return a PySpark `Column`
3. Add to `__all__` if public
4. Add unit tests in `tests/unit/test_check_funcs_<category>.py`

### Add a new CLI command

1. Add `@dqx.command` function to `cli.py`
2. Inject `WorkspaceClient`; use `WorkspaceContext` for workspace operations
3. Add integration test in `tests/integration/test_cli.py` using `MockInstallation` + `MockPrompts`

---

## Documentation

- **[README.md](./README.md)** — Project description
- **[docs/](./docs/)** — Full site including contribution workflow (Docusaurus): [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)
- **[CHANGELOG.md](./CHANGELOG.md)** — Release history

---

**Stack**: Python 3.10+ · PySpark · Databricks SDK · databricks-labs-blueprint · databricks-labs-pytester · DSPy · Presidio · MLflow
