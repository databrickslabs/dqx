# dqx-mcp-coverage-bootstrap (TEST-ONLY)

Collects real code coverage for the MCP integration suite, whose code runs **remotely** — the
server in a deployed Databricks App and the runner in a serverless job — where pytest-side
`coverage` measures nothing.

**Never installed by a production deploy.** Installation is the only switch: no production module
imports `coverage`, checks an env flag, or exposes a flush endpoint.

| Runtime | How this wheel gets installed | How the data comes back |
|---|---|---|
| Runner job | the `dev-coverage` bundle target adds it to `runner_environment_dependencies` (`mcp-server/databricks.yml`) | job containers have a `/Volumes` FUSE mount → plain file copy |
| App | `mcp-server/scripts/ci_deploy.sh` appends it to `requirements.txt` for the deploy only (trap-restored) | Apps have **no** volume mount → Files API upload |

## How it works

`dqx_mcp_coverage.pth` lands at the site-packages **root**, so `site.py` execs it at interpreter
start and the tracer is live *before* `server.app` / `dqx_mcp_runner.runner` import — module-level
lines included.

Configuration is derived from what each runtime can actually provide, so no new bundle plumbing
exists: the runner reads the `--results-volume` / `--run-id` arguments its task already receives
(a serverless job environment **cannot** set env vars), and the app reads its existing
`DQX_CATALOG` / `DQX_TMP_SCHEMA` config env. Data lands in `<results_volume>/coverage`, a volume
that already grants read/write to both service principals. `DQX_COVERAGE_DIR` overrides the
destination for local runs.

## Two things not to "simplify"

- **Ship a `.pth`, not `sitecustomize.py`.** `sitecustomize` is a single global module name and the
  first one on `sys.path` wins, so ours would either shadow a runtime-provided one or be silently
  shadowed — the latter yielding a mysterious 0%. `.pth` files are additive; `pytest-cov` ships
  `pytest-cov.pth` for exactly this reason. The `.pth` must sit at the site-packages root (see the
  `force-include` in `pyproject.toml`), not inside the package.
- **Do not pass `sigterm=True` to `Coverage()`.** Its handler re-raises SIGTERM via `os.kill()`
  immediately after saving, which kills the process before `atexit` runs — the data would be saved
  to `/tmp` and never uploaded. The app's final flush instead comes from a graceful stop
  (`ws.apps.stop_and_wait`, which the test calls before teardown) plus a periodic checkpoint
  thread that bounds loss to one interval if the 15s SIGTERM budget is exceeded.

## Local use

See the recipe in `tests/integration_mcp/README.md` (or the plan's verification section): set
`DQX_MCP_COVERAGE_DIR` when running `make mcp-integration`, then from the **repo root**:

```bash
uv run coverage combine --rcfile=tests/integration_mcp/.coveragerc --keep
uv run coverage report --rcfile=tests/integration_mcp/.coveragerc -m
```

Combining from the repo root matters: a `[paths]` rule whose rewritten target does not exist on
disk is silently skipped, which shows up as absolute container paths in the report.
