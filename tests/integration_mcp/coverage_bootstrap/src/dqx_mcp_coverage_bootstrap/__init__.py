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
import io
import os
import shutil
import sys
import threading
import time

# The live data file always lives on local disk: coverage's data file is SQLite, and SQLite over
# a FUSE mount is a known locking hazard. Only finished copies go to the UC volume.
_LOCAL_DIR = "/tmp"  # noqa: S108 — container-local scratch, not a shared host

# The checkpoint is the PRIMARY persistence mechanism for the runner, not a safety net.
#
# The runner's task runs inside a Databricks python shell (``db_ipykernel_launcher.py``), and the
# task arguments are NOT in ``sys.argv`` when site.py execs the .pth. The results volume — and hence
# the upload destination — is only derivable once they are, so an early tick can only log
# "no destination could be resolved" and leave the data in /tmp. At the previous 15s interval a
# short-lived interpreter could spend its whole life in that window and ship nothing: measured over
# one suite run, only 7 of 21 runner runs produced a data file, and the split tracked process
# lifetime rather than outcome. At 5s every interpreter gets a later tick once argv is populated —
# measured 20/20, with 6 of the 20 still logging one early no-destination tick first.
#
# Each tick overwrites one cumulative file with a superset, so the interval bounds only how much of
# the tail can be lost, at a cost of one local save plus one volume copy. 0 disables the thread.
_CHECKPOINT_SECONDS = float(os.getenv("DQX_COVERAGE_CHECKPOINT_SECONDS", "5"))

_cov = None
_data_file = ""
# Every diagnostic this module emits, in order. Uploaded next to the data file so the bootstrap's
# own story is readable from the volume: a Databricks job's retained output carries neither raw
# stderr nor records logged before the task configures logging, which left three separate
# investigations of lost coverage with nothing to read.
_messages: list[str] = []
_lock = threading.Lock()
# Fixed at startup: see _instance_key. The data file is cumulative for the interpreter, so every
# flush overwrites one file with a superset — last write wins, and nothing depends on run boundaries.
_key = ""


def _log(message: str) -> None:
    """Emit a diagnostic through BOTH the logging module and raw stderr.

    ``[dqx-coverage] tracing started`` is the primary signal that instrumentation is live; a silent
    0% report is almost always this line missing.

    Why both: a Databricks job run's retained output (``jobs get-run-output``) carries records from
    the **logging** module but not raw ``sys.stderr`` writes, so a stderr-only diagnostic is
    invisible exactly where a failing runner needs to be diagnosed — which is how a lost-coverage
    bug stayed unexplained across three full suite runs. The app, whose stderr *is* captured,
    keeps working either way, and stderr is retained so a pre-logging-config failure still surfaces.
    """
    _messages.append(message)
    sys.stderr.write(f"[dqx-coverage] {message}\n")
    sys.stderr.flush()
    try:
        import logging

        logging.getLogger("dqx-coverage").warning(message)
    except Exception:  # noqa: BLE001 — diagnostics must never break the app or fail a job
        pass


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


def _instance_key() -> str:
    """Stable identity for THIS interpreter, decided once and never re-derived from argv.

    The runner does not get a process per job run: its task executes inside a long-lived Databricks
    python shell (``db_ipykernel_launcher.py``) that serves many runs, and at interpreter start —
    when the ``.pth`` executes — the task's ``--run-id`` is not in ``sys.argv`` at all. Keying the
    output file on the run id therefore produced a NEW file name per run for one cumulative data
    file, while the real lifetime being measured is the interpreter's.

    A pid alone can repeat across containers, so a random suffix keeps concurrent writers apart.
    """
    from uuid import uuid4

    app_name = os.getenv("DATABRICKS_APP_NAME")
    role = "app" if app_name else "proc"
    label = app_name or "runner"
    return f"{role}.{label}.{os.getpid()}.{uuid4().hex[:8]}"


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


def _persist_text(remote_path: str, text: str) -> None:
    """Write *text* to the volume, FUSE first then the Files API. Never raises."""
    try:
        payload = text.encode()
        try:
            os.makedirs(os.path.dirname(remote_path), exist_ok=True)
            with open(remote_path, "wb") as handle:
                handle.write(payload)
            return
        except OSError:
            pass
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()
        ws.files.create_directory(os.path.dirname(remote_path))
        ws.files.upload(remote_path, io.BytesIO(payload), overwrite=True)
    except Exception:  # noqa: BLE001 — a diagnostic must never break the app or fail a job
        pass


def _write_breadcrumb() -> None:
    """Publish the diagnostics collected so far, so they are readable even if no flush ever runs.

    Called at startup and after each flush. The filename deliberately does NOT start with
    ``.coverage.`` — ``coverage combine`` globs that prefix and would choke on a text file.
    """
    destination = _destination_dir()
    if not destination:
        return
    _persist_text(f"{destination}/dqxcov-log.{_key}.txt", "\n".join(_messages) + "\n")


def _flush(final: bool) -> None:
    """Save the collected data and persist it to the volume. Never raises.

    *final* stops the tracer first (process is exiting); a checkpoint leaves it running. Only
    ``save()`` is called mid-run — never ``stop()``/``start()`` from the checkpoint thread, which
    would blind other threads while they execute.
    """
    if _cov is None:
        return
    try:
        _flush_locked(final)
    finally:
        # In a finally so the breadcrumb is published on every path, including the guarded
        # already-finalized return — otherwise the last message never reaches the volume.
        _write_breadcrumb()


def _flush_locked(final: bool) -> None:
    """The body of :func:`_flush`, minus the breadcrumb. Never raises."""
    cov = _cov
    if cov is None:
        return
    try:
        with _lock:
            if final:
                # Only the app ever reaches this: its process really does exit. The runner shares a
                # long-lived interpreter with later runs, so stopping the tracer there would blind
                # every operation that followed.
                cov.stop()
            cov.save()
            destination = _destination_dir()
            if not destination:
                _log("no destination could be resolved; data left in /tmp only")
                return
            # The name must start with '.coverage.' (and never be exactly '.coverage') or
            # `coverage combine` will not glob it.
            _persist(_data_file, f"{destination}/.coverage.{_key}")
    except Exception as exc:  # noqa: BLE001 — coverage must never break the app or fail a job
        _log(f"flush failed (non-fatal): {exc!r}")


def _checkpoint_loop() -> None:
    """Periodically flush so an ungraceful death costs at most one interval."""
    while True:
        time.sleep(_CHECKPOINT_SECONDS)
        _flush(final=False)


def _start() -> None:
    global _cov, _data_file, _key  # noqa: PLW0603 — module-level singletons for the flush hooks

    import coverage

    _key = _instance_key()
    _data_file = os.path.join(_LOCAL_DIR, f".coverage.{_key}")
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
    _log(f"tracing started (key={_key}, data_file={_data_file}, argv0={sys.argv[0]!r})")
    _write_breadcrumb()


try:
    _start()
except Exception as exc:  # noqa: BLE001 — a broken bootstrap must not stop the app or job
    _log(f"NOT started (non-fatal, expect 0% coverage): {exc!r}")
