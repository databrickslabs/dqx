# dqx-mcp-coverage-bootstrap (TEST-ONLY)

Collects real code coverage for the MCP integration suite, whose code runs **remotely** — the
server in a deployed Databricks App and the runner in a serverless job — where pytest-side
`coverage` measures nothing.

**Never installed by a production deploy.** Installation is the only switch: no production module
imports `coverage`, checks an env flag, or exposes a flush endpoint.

| Runtime | How this wheel gets installed | How the data comes back |
|---|---|---|
| Runner job | the `dev-coverage` bundle target adds it to `runner_environment_dependencies` (`mcp-server/databricks.yml`) | the same target points the wheel task at `dqx-mcp-runner-coverage` (see `runner_entry.py`), which calls the pristine runner and then `flush_at_task_end()`; job containers have a `/Volumes` FUSE mount → plain file write |
| App | `mcp-server/scripts/ci_deploy.sh` appends it to `requirements.txt` for the deploy only (trap-restored) | Apps have **no** volume mount → Files API upload |

## How it works

`dqx_mcp_coverage.pth` lands at the site-packages **root**, so `site.py` execs it at interpreter
start and the tracer is live *before* `server.app` / `dqx_mcp_runner.runner` import — module-level
lines included.

Configuration is derived from what each runtime can actually provide, so no new bundle plumbing
exists: the runner reads the `--results-volume` argument its task already receives (a serverless job
environment **cannot** set env vars), and the app reads its existing `DQX_CATALOG` /
`DQX_TMP_SCHEMA` config env. Data lands in `<results_volume>/coverage`, a volume that already grants
read/write to both service principals. `DQX_COVERAGE_DIR` overrides the destination for local runs.

**The runner's delivery is an entry point, not a timer.** When the `.pth` executes, the task's
arguments are not in `sys.argv` yet, so nothing at interpreter start can resolve the results volume.
A timer-driven flush therefore could not deliver reliably: measured at a 15s interval with no
wrapper, only 7 of 21 runner runs produced a data file at all, and which ones did tracked process
lifetime rather than outcome — so the reported coverage of unchanged code wandered between 49% and
75%. `flush_at_task_end()` runs as ordinary code on `main()`'s own stack once the work is done, when
`argv` is populated. Output files are keyed to the **interpreter** (`pid` + a random suffix), not to
a run id, because one cumulative data file is what the process actually has.

## Two things not to "simplify"

- **Ship a `.pth`, not `sitecustomize.py`.** `sitecustomize` is a single global module name and the
  first one on `sys.path` wins, so ours would either shadow a runtime-provided one or be silently
  shadowed — the latter yielding a mysterious 0%. `.pth` files are additive; `pytest-cov` ships
  `pytest-cov.pth` for exactly this reason. The `.pth` must sit at the site-packages root (see the
  `force-include` in `pyproject.toml`), not inside the package.
- **Do not pass `sigterm=True` to `Coverage()`.** Its handler re-raises SIGTERM via `os.kill()`
  immediately after saving, which kills the process before `atexit` runs — the data would be saved
  to `/tmp` and never uploaded. The app's final flush instead comes from a graceful stop
  (`ws.apps.stop_and_wait`, which the test calls before teardown) plus a periodic checkpoint thread
  that bounds loss to one interval if the 15s SIGTERM budget is exceeded. Leaving `sigterm` off is
  also what keeps our own `save()` calls the only writers of the data file, which is what makes it
  safe to read that file under `_lock` — see the module docstring.
- **The checkpoint thread is OFF by default, and is app-only.** It exists because the app is a
  uvicorn process with no entry point to wrap; the `dev-coverage` app environment sets
  `DQX_COVERAGE_CHECKPOINT_SECONDS=15` explicitly. Turning it on for the runner would not help (see
  above) and would reintroduce a publisher that can race the deterministic one.

## Local use

See the recipe in `tests/integration_mcp/README.md` (or the plan's verification section): set
`DQX_MCP_COVERAGE_DIR` when running `make mcp-integration`, then from the **repo root**:

```bash
uv run coverage combine --rcfile=tests/integration_mcp/.coveragerc --keep
uv run coverage report --rcfile=tests/integration_mcp/.coveragerc -m
```

Combining from the repo root matters: a `[paths]` rule whose rewritten target does not exist on
disk is silently skipped, which shows up as absolute container paths in the report.
