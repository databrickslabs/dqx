"""TEST-ONLY: start coverage.py before any DQX MCP module is imported.

``dqx_mcp_coverage.pth`` imports this module at interpreter start (site.py execs ``*.pth``
lines), so the tracer is live before ``server.app`` / ``dqx_mcp_runner.runner`` are imported
and their module-level lines are measured.

**Installation is the switch.** This wheel is only installed by coverage-enabled deploys (the
``dev-coverage`` bundle target for the runner job; a deploy-time ``requirements.txt`` append for
the app — see ``mcp-server/scripts/ci_deploy.sh``). Production deploys never contain it, so no
production source needs to be coverage-aware. Nothing here may break the app or fail a data job:
every path is wrapped and failures are logged, never raised.

Why a ``.pth`` and not ``sitecustomize.py``: see README.md.
"""

import atexit
import os
import shutil
import sys
import threading
import time

# The live data file always lives on local disk: coverage's data file is SQLite, and SQLite over
# a FUSE mount is a known locking hazard. Only finished copies go to the UC volume.
_LOCAL_DIR = "/tmp"  # noqa: S108 — container-local scratch, not a shared host

# A checkpoint bounds data loss if the process is SIGKILLed (Databricks Apps allow only ~15s
# after SIGTERM) or if atexit never runs. 0 disables the thread.
_CHECKPOINT_SECONDS = float(os.getenv("DQX_COVERAGE_CHECKPOINT_SECONDS", "15"))

_cov = None
_data_file = ""
_lock = threading.Lock()


def _log(message: str) -> None:
    """Log to stderr — Apps and jobs both capture it.

    ``[dqx-coverage] tracing started`` is the primary signal that instrumentation is live; a
    silent 0% report is almost always this line missing.
    """
    sys.stderr.write(f"[dqx-coverage] {message}\n")
    sys.stderr.flush()


def _argv_value(flag: str) -> str:
    """Read a ``--flag value`` (or ``--flag=value``) from argv without consuming it."""
    for i, arg in enumerate(sys.argv):
        if arg == flag and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]
    return ""


def _results_volume() -> str:
    """Resolve the results volume the way each runtime already knows it.

    Runner: the wheel task is already passed ``--results-volume``, so no new task argument (and
    therefore no matching argparse entry in the pristine runner) is needed. App: mirrors
    ``mcp-server/server/utils.py::_get_results_volume`` using the app's existing config env.
    """
    from_argv = _argv_value("--results-volume")
    if from_argv:
        return from_argv.rstrip("/")
    catalog = os.getenv("DQX_CATALOG", "")
    schema = os.getenv("DQX_TMP_SCHEMA", "dqx_mcp_tmp")
    return f"/Volumes/{catalog}/{schema}/mcp_results" if catalog else ""


def _destination_dir() -> str:
    """UC-volume directory the data files are written to (empty = cannot resolve)."""
    override = os.getenv("DQX_COVERAGE_DIR", "").rstrip("/")
    if override:
        return override
    base = _results_volume()
    return f"{base}/coverage" if base else ""


def _role_and_key() -> tuple[str, str]:
    """Identify this process so concurrent writers never overwrite each other's file."""
    run_id = _argv_value("--run-id")
    if run_id:
        return "runner", run_id
    app_name = os.getenv("DATABRICKS_APP_NAME") or "app"
    return "app", f"{app_name}.{os.getpid()}"


def _persist(local_path: str, remote_path: str) -> None:
    """Copy the saved data file to the UC volume.

    Serverless jobs have a ``/Volumes`` FUSE mount, so a plain filesystem copy works there.
    Databricks Apps do **not** (go/apps/faq: "Can I mount a Unity Catalog volume in my app?" —
    "Not today"; request XTA-11566), so fall back to the Files API. One code path serves both and
    self-heals if either platform assumption changes.
    """
    try:
        os.makedirs(os.path.dirname(remote_path), exist_ok=True)
        shutil.copyfile(local_path, remote_path)
        _log(f"copied -> {remote_path}")
        return
    except OSError as exc:
        _log(f"FUSE copy unavailable ({exc}); using the Files API")

    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    ws.files.create_directory(os.path.dirname(remote_path))  # upload does not create parents
    with open(local_path, "rb") as handle:
        ws.files.upload(remote_path, handle, overwrite=True)
    _log(f"uploaded -> {remote_path}")


def _flush(final: bool) -> None:
    """Save the collected data and persist it to the volume. Never raises.

    *final* stops the tracer first (process is exiting); a checkpoint leaves it running. Only
    ``save()`` is called mid-run — never ``stop()``/``start()`` from the checkpoint thread, which
    would blind other threads while they execute.
    """
    if _cov is None:
        return
    try:
        with _lock:
            if final:
                _cov.stop()
            _cov.save()
            destination = _destination_dir()
            if not destination:
                _log("no destination could be resolved; data left in /tmp only")
                return
            role, key = _role_and_key()
            # The name must start with '.coverage.' (and never be exactly '.coverage') or
            # `coverage combine` will not glob it.
            _persist(_data_file, f"{destination}/.coverage.{role}.{key}")
    except Exception as exc:  # noqa: BLE001 — coverage must never break the app or fail a job
        _log(f"flush failed (non-fatal): {exc!r}")


def _checkpoint_loop() -> None:
    """Periodically flush so an ungraceful death costs at most one interval."""
    while True:
        time.sleep(_CHECKPOINT_SECONDS)
        _flush(final=False)


def _start() -> None:
    global _cov, _data_file  # noqa: PLW0603 — module-level singleton for the atexit hook

    import coverage

    role, key = _role_and_key()
    _data_file = os.path.join(_LOCAL_DIR, f".coverage.{role}.{key}")
    _cov = coverage.Coverage(
        data_file=_data_file,
        # Ship our own config so whatever .coveragerc/pyproject.toml happens to sit in the
        # container's working directory cannot override it.
        config_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), "coveragerc"),
        # sigterm=True is deliberately NOT set: its handler re-raises SIGTERM via os.kill()
        # immediately after saving, which kills the process before atexit runs — so the upload
        # would never happen. Graceful shutdown + the checkpoint thread cover that instead.
    )
    _cov.start()
    # Registered after start(), so with atexit's LIFO ordering this runs BEFORE coverage's own
    # atexit hook — hence _flush does its own stop()/save() rather than relying on coverage's.
    atexit.register(_flush, True)
    if _CHECKPOINT_SECONDS > 0:
        threading.Thread(target=_checkpoint_loop, daemon=True, name="dqx-coverage").start()
    _log(f"tracing started (role={role}, data_file={_data_file})")


try:
    _start()
except Exception as exc:  # noqa: BLE001 — a broken bootstrap must not stop the app or job
    _log(f"NOT started (non-fatal, expect 0% coverage): {exc!r}")
