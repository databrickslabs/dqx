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
import collections
import io
import os
import shutil
import sys
import threading
import time

# The live data file always lives on local disk: coverage's data file is SQLite, and SQLite over
# a FUSE mount is a known locking hazard. Only finished copies go to the UC volume.
_LOCAL_DIR = "/tmp"  # noqa: S108 — container-local scratch, not a shared host

# Periodic flush interval, in seconds. OFF by default, and deliberately so.
#
# The runner does not need it: the dev-coverage bundle target points the wheel task at a wrapping
# entry point (see runner_entry) that persists coverage from ordinary code once the work is done.
# That is deterministic — it runs on main()'s own call stack, with the task arguments already in
# sys.argv so the results volume resolves.
#
# A timer cannot offer that. The .pth executes at interpreter start, before the task arguments
# exist, so an early tick can only log "no destination could be resolved" and leave the data in
# /tmp. Measured with a 15s interval and no wrapper: 7 of 21 runner runs produced a data file at
# all, and which ones did tracked process lifetime rather than outcome — the reported coverage of
# unchanged code therefore wandered between 49% and 75%.
#
# The app is the exception and sets this explicitly (see the dev-coverage app_environment): it is a
# uvicorn process with no entry point to wrap, relying on a graceful SIGTERM plus atexit, and the
# interval bounds the loss if the platform's ~15s shutdown budget is exceeded.
_CHECKPOINT_SECONDS = float(os.getenv("DQX_COVERAGE_CHECKPOINT_SECONDS", "0"))

_cov = None
_data_file = ""
# Every diagnostic this module emits, in order. Uploaded next to the data file so the bootstrap's
# own story is readable from the volume: a Databricks job's retained output carries neither raw
# stderr nor records logged before the task configures logging, which left three separate
# investigations of lost coverage with nothing to read.
#
# Bounded, because the whole buffer is re-uploaded on every flush: unbounded, a long-lived app
# checkpointing all suite long would ship a quadratic amount of breadcrumb traffic. The cap is far
# above what a healthy process emits (a handful of lines), so it only ever truncates a pathological
# repeating failure — and then the tail, which is the part worth reading, is what survives.
_MAX_MESSAGES = 400
_messages: collections.deque[str] = collections.deque(maxlen=_MAX_MESSAGES)
# Last breadcrumb text actually uploaded, so an unchanged one is not re-shipped. Together with the
# quiet routine copy below this keeps a long-lived app's breadcrumb traffic flat instead of growing
# with the run: in steady state nothing new is logged, so nothing is re-uploaded.
_last_breadcrumb = ""
_lock = threading.Lock()
# Set by flush_at_task_end. The checkpoint exists only because nothing else reliably persists a
# runner's data; once the wrapping entry point has done so deterministically there is nothing left
# for the thread to do, and another tick would only re-upload the same bytes.
_checkpoint_stopped = False
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


_announced_copies: set[str] = set()


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
        # Announced once per destination. A periodic checkpoint copies to the same path every tick,
        # and logging each one grew the breadcrumb — which is re-uploaded whole — with run length.
        if remote_path not in _announced_copies:
            _announced_copies.add(remote_path)
            _log(f"copied -> {remote_path}")
        return
    except OSError as exc:
        _log(f"FUSE copy unavailable ({exc}); using the Files API")

    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    ws.files.create_directory(os.path.dirname(remote_path))  # upload does not create parents
    with open(local_path, "rb") as handle:
        ws.files.upload(remote_path, handle, overwrite=True)
    if remote_path not in _announced_copies:
        _announced_copies.add(remote_path)
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
    # Snapshot under the lock rather than joining the live buffer: _log appends from whichever thread
    # is running, and building the text outside the lock would race it. The join happens while
    # evaluating _persist_text's argument, i.e. OUTSIDE that function's own try — so an exception here
    # would escape _write_breadcrumb, propagate out of _flush's finally, and (see _checkpoint_loop)
    # take the checkpoint thread with it permanently.
    global _last_breadcrumb  # noqa: PLW0603 — module-level singleton, mirrors the other hooks
    with _lock:
        snapshot = list(_messages)
    text = "\n".join(snapshot) + "\n"
    if text == _last_breadcrumb:
        return
    _last_breadcrumb = text
    _persist_text(f"{destination}/dqxcov-log.{_key}.txt", text)


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
        # In a finally so the breadcrumb is published even when the flush itself gets nowhere — the
        # "no destination could be resolved" return and the caught-exception path both land here, and
        # those are exactly the cases worth being able to read afterwards.
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
    while not _checkpoint_stopped:
        time.sleep(_CHECKPOINT_SECONDS)
        if _checkpoint_stopped:
            return
        try:
            _flush(final=False)
        except Exception as exc:  # noqa: BLE001 — the thread must outlive any single bad tick
            # Without this, one escaped exception ends the thread for the life of the process and
            # silently reinstates the atexit-only failure this whole mechanism exists to avoid.
            _log(f"checkpoint tick failed (non-fatal): {exc!r}")


def flush_at_task_end() -> None:
    """Persist now, from ordinary code, and retire the checkpoint. Never raises.

    Called by the coverage-wrapping wheel entry point (see runner_entry) once the runner's work is
    done. This is the deterministic path: it runs on main()'s own call stack with the interpreter
    unquestionably alive and — crucially — with the task arguments already in ``sys.argv``, so the
    results volume resolves. Compare the checkpoint thread, whose early ticks fire before argv is
    populated and can only leave the data in /tmp.

    Deliberately a non-final flush: the tracer is left running so nothing that executes after this
    point is silently untraced, and so a second call cannot blind an interpreter that turns out to be
    shared.
    """
    global _checkpoint_stopped  # noqa: PLW0603 — module-level singleton, mirrors the other hooks
    _checkpoint_stopped = True
    # Logged so the breadcrumb attributes the upload. Without this the deterministic flush and a
    # checkpoint tick are indistinguishable in the record, which is precisely the ambiguity that made
    # an earlier attempt at this look like it worked when it had never run at all.
    _log("task-end flush (deterministic; checkpoint retired)")
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
