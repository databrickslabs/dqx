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

Concurrency, gathered here because it is easy to get wrong:

* ``_upload_lock`` serializes every remote publish. Two writers on one remote path is not a
  "stale bytes" problem: the FUSE write truncates then streams, the Files API PUT has no
  ordering, and there is no rename primitive to publish atomically. A torn ``.coverage.*`` makes
  ``coverage combine`` raise ``DataError``, and the CI merge step runs it under ``|| true`` — so
  one interleaved write silently zeroes the WHOLE report while the job stays green. Concurrent
  same-path writes must be impossible, not merely unlikely.
* ``_lock`` guards the tracer and its data file, and is **never** held across a network call.
  Reading the file under it is load-bearing: coverage opens its SQLite with ``journal_mode=off``
  and ``synchronous=off``, so there is no atomic commit and a copy taken while a writer is
  mid-save can be structurally invalid, not merely out of date. That is safe only because our
  ``save()`` calls are the sole disk writers — ``Coverage`` is built without ``auto_data`` and
  without ``sigterm=True``, so its own atexit hook does not save. Setting either would introduce
  a writer that does not hold ``_lock``.
* Lock order is ``_upload_lock`` then ``_lock``, never the reverse.
* No forking. With ``suffix=None`` a forked child re-derives the same ``/tmp`` path and
  ``erase()``s it, and ``_key`` is fixed pre-fork so every child would publish to one remote
  path. ``mcp-server/server/main.py`` calls ``uvicorn.run`` with no ``workers``; setting
  ``workers>1`` would break this design and no in-process lock would help.
"""

import atexit
import io
import os
import sys
import threading

# The live data file always lives on local disk: coverage's data file is SQLite, and SQLite over
# a FUSE mount is a known locking hazard. Only finished copies go to the UC volume.
_LOCAL_DIR = "/tmp"  # noqa: S108 — container-local scratch, not a shared host

# How long the final (atexit) flush waits for an in-flight publish before publishing beside it.
# Short on purpose: atexit is LIFO and this module registers from the .pth at interpreter start, so
# every hook registered later runs first and much of the platform's ~15s budget is already gone.
_FINAL_WAIT_SECONDS = 2.0

_cov = None
_data_file = ""
# Fixed at startup: see _instance_key. The data file is cumulative for the interpreter, so every
# flush overwrites one file with a superset — last write wins, and nothing depends on run boundaries.
_key = ""
_lock = threading.Lock()
_upload_lock = threading.Lock()
# An Event rather than a bool: `wait(timeout)` returns the moment it is set, whereas a flag checked
# after `time.sleep` is only noticed a whole interval later. That mattered — a tick could wake during
# interpreter finalization and run Python (including _log's lazy `import logging`) while the import
# machinery was being torn down.
_stop_event = threading.Event()
# Cached SDK client, created on first use. Deliberately NOT warmed at import: doing client setup
# there is how an earlier version ended up doing network work inside the .pth exec, before uvicorn
# booted. For the app the first checkpoint tick warms it long before shutdown, so the atexit path
# does no client setup either way.
_ws = None
# Remote directories already created. A check-then-add race here is harmless — the worst outcome is
# calling create_directory twice, which is idempotent — so this deliberately takes no lock.
_created_dirs: set[str] = set()


def _log(message: str, warning: bool = False) -> None:
    """Emit a diagnostic to stderr and through the logging module. Never raises.

    Both channels, because they are retained differently. Verified against a real runner job's
    retained output (``jobs get-run-output``): messages emitted once the task is running appear on
    both, while anything emitted at interpreter start — ``tracing started`` — appears on neither,
    because the .pth runs before the task's log capture begins.

    Routine events log at INFO and only real problems at WARNING (*warning*), so a long-lived app
    does not emit a steady stream of spurious warnings from the flush path. The messages saying the
    mechanism itself is broken stay at WARNING deliberately: they can fire before the task calls
    ``basicConfig``, where ``lastResort`` drops INFO, and they are the lines whose absence explains
    a silent 0% report.
    """
    try:
        # Inside the guard, not outside it: by teardown ``sys.stderr`` can be closed or detached (the
        # runner's task runs in a Databricks python shell whose log capture is torn down with it),
        # and a ValueError from here would propagate out of runner_entry's bare ``finally`` —
        # replacing the operation's real exception and failing an otherwise successful run.
        sys.stderr.write(f"[dqx-coverage] {message}\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001 — diagnostics must never break the app or fail a job
        pass
    try:
        import logging

        logger = logging.getLogger("dqx-coverage")
        if warning:
            logger.warning(message)
        else:
            logger.info(message)
    except Exception:  # noqa: BLE001 — same reason
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


def _is_app() -> bool:
    """Whether this interpreter is the Databricks App rather than a runner job task.

    This decides how a publish is written, so it must not be inferred from a failed filesystem
    write: if ``/Volumes/...`` happened to be locally creatable, the data would land on ephemeral
    disk and be reported as delivered while never reaching the volume.
    """
    return bool(os.getenv("DATABRICKS_APP_NAME"))


def _instance_key() -> str:
    """Stable identity for THIS interpreter, decided once and never re-derived from argv.

    The runner's task runs inside a Databricks python shell and, when the ``.pth`` executes, the
    task's ``--run-id`` is not in ``sys.argv`` at all. Keying the output file on the run id
    therefore produced a new filename per run for one cumulative data file, while the real lifetime
    being measured is the interpreter's.

    A pid alone can repeat across containers, so a random suffix keeps concurrent writers apart.
    """
    from uuid import uuid4

    app_name = os.getenv("DATABRICKS_APP_NAME")
    role = "app" if app_name else "proc"
    label = app_name or "runner"
    return f"{role}.{label}.{os.getpid()}.{uuid4().hex[:8]}"


def _client():
    """The cached SDK client, created on first use."""
    global _ws  # noqa: PLW0603 — module-level singleton, created lazily and reused
    if _ws is None:
        from databricks.sdk import WorkspaceClient

        _ws = WorkspaceClient()
    return _ws


def _publish(payload: bytes, remote_path: str) -> bool:
    """Write *payload* to the volume. Never raises. Returns whether it landed.

    Callers must hold ``_upload_lock`` and must NOT hold ``_lock`` — with one deliberate exception,
    the shutdown fallback in ``_final_flush``, which could not get the lock and publishes to a
    unique sibling path instead. That is safe only because the path is unique: two unlocked writers
    may briefly race ``_client()``'s lazy assignment (constructing one client twice, wasteful but
    harmless) and ``_created_dirs`` (an idempotent create), never one remote object.

    The write path is chosen by runtime, not by catching an exception: serverless job tasks have a
    ``/Volumes`` FUSE mount so a filesystem write works, while Databricks Apps do not
    (go/apps/faq: "Can I mount a Unity Catalog volume in my app?" — "Not today"; request
    XTA-11566) and use the Files API.
    """
    directory = os.path.dirname(remote_path)
    try:
        if not _is_app():
            try:
                os.makedirs(directory, exist_ok=True)
                with open(remote_path, "wb") as handle:
                    handle.write(payload)
                _log(f"copied {len(payload)} bytes -> {remote_path}")
                return True
            except OSError as exc:
                # A real fallback, not the normal path: the runner's mount should be there.
                _log(f"no FUSE write at {remote_path} ({exc}); using the Files API", warning=True)
        ws = _client()
        if directory not in _created_dirs:
            ws.files.create_directory(directory)  # upload does not create parents
            _created_dirs.add(directory)
        ws.files.upload(remote_path, io.BytesIO(payload), overwrite=True)
        _log(f"uploaded {len(payload)} bytes -> {remote_path}")
        return True
    except Exception as exc:  # noqa: BLE001 — must never break the app or fail a job
        _log(f"could not write {remote_path}: {exc!r}", warning=True)
        return False


def _snapshot(final: bool) -> tuple[bytes, str] | None:
    """Save the tracer's data and read it back under ``_lock``. Never raises.

    Returns ``(payload, remote_path)``, or None when there is nothing to publish. No network here:
    holding ``_lock`` across a round trip is what let the final flush queue behind an in-flight
    publish and lose the shutdown budget.
    """
    cov = _cov
    if cov is None:
        return None
    try:
        with _lock:
            if final:
                # Only the app reaches this: its process really does exit. Stopping the tracer in a
                # runner would blind anything that ran afterwards.
                cov.stop()
            cov.save()
            destination = _destination_dir()
            if not destination:
                # Routine on an early checkpoint tick, where the runner's task arguments are not in
                # sys.argv yet. Callers for whom it is NOT routine say so — see flush_at_task_end.
                _log("no destination could be resolved; data left in /tmp only")
                return None
            with open(_data_file, "rb") as handle:
                payload = handle.read()
            # The name must start with '.coverage.' (and never be exactly '.coverage') or
            # `coverage combine` will not glob it.
            return payload, f"{destination}/.coverage.{_key}"
    except Exception as exc:  # noqa: BLE001 — coverage must never break the app or fail a job
        _log(f"snapshot failed (non-fatal): {exc!r}", warning=True)
        return None


def _flush(final: bool, remote_suffix: str = "") -> bool:
    """Snapshot then publish. Never raises. Returns whether the data reached the volume.

    Callers must hold ``_upload_lock`` (see ``_publish`` for the single documented exception); the
    module docstring says why it is not acquired here.
    *remote_suffix* publishes to a sibling name, used by the final flush when it cannot get the
    lock in time and must not race the canonical object.
    """
    snapshot = _snapshot(final)
    if snapshot is None:
        return False
    payload, remote_path = snapshot
    return _publish(payload, remote_path + remote_suffix)


def _checkpoint_seconds() -> float:
    """The periodic flush interval, in seconds. OFF by default, and deliberately so.

    The runner does not need it: the dev-coverage bundle target points the wheel task at a wrapping
    entry point (see runner_entry) that persists coverage from ordinary code once the work is done.
    That is deterministic — it runs on main()'s own call stack, with the task arguments already in
    ``sys.argv`` so the results volume resolves.

    A timer cannot offer that. The .pth executes at interpreter start, before the task arguments
    exist, so an early tick can only log "no destination could be resolved" and leave the data in
    /tmp. Measured with a 15s interval and no wrapper: 7 of 21 runner runs produced a data file at
    all, and which ones did tracked process lifetime rather than outcome — so the reported coverage
    of unchanged code wandered between 49% and 75%.

    The app is the exception and sets this explicitly (see the dev-coverage app_environment): it is a
    uvicorn process with no entry point to wrap, relying on a graceful SIGTERM plus atexit, and the
    interval bounds the loss if the platform's ~15s shutdown budget is exceeded.

    Read here rather than at module scope: an env var that renders empty or non-numeric would raise
    while site.py is exec'ing the .pth, where the failure is swallowed — coverage would never start
    AND the "NOT started" diagnostic this module promises would never be emitted, leaving only a
    site.py traceback and a silent 0%. Called from ``_start()``, inside its try.
    """
    raw = os.getenv("DQX_COVERAGE_CHECKPOINT_SECONDS", "")
    try:
        return float(raw) if raw else 0.0
    except ValueError:
        _log(f"ignoring non-numeric DQX_COVERAGE_CHECKPOINT_SECONDS={raw!r}; checkpoint off", warning=True)
        return 0.0


def _checkpoint_loop(interval: float) -> None:
    """Periodically publish, so an ungraceful death costs at most one interval."""
    while not _stop_event.wait(interval):
        # Skip rather than queue: a tick's payload is a subset of whatever publish is already in
        # flight, and waiting here is how a tick could delay the final flush past the shutdown
        # budget.
        if not _upload_lock.acquire(blocking=False):
            continue
        try:
            _flush(final=False)
        except Exception as exc:  # noqa: BLE001 — the thread must outlive any single bad tick
            # Without this, one escaped exception ends the thread for the life of the process and
            # silently reinstates the atexit-only failure this whole mechanism exists to avoid.
            _log(f"checkpoint tick failed (non-fatal): {exc!r}", warning=True)
        finally:
            _upload_lock.release()


def _final_flush() -> None:
    """The atexit hook: stop checkpointing, then publish what we have. Never raises."""
    _stop_event.set()
    if _upload_lock.acquire(timeout=_FINAL_WAIT_SECONDS):
        try:
            _flush(final=True)
        finally:
            _upload_lock.release()
        return
    # A publish is still running and the budget is short. Writing the canonical path anyway could
    # interleave with it and corrupt the object, which aborts `coverage combine` and costs the
    # entire report — so publish beside it. `coverage combine` globs '.coverage.*' and picks this up
    # as simply another data file.
    # Deliberately WITHOUT _upload_lock — we just failed to get it. Safe only because the sibling
    # path is unique to this write, so nothing can interleave on one object; see _publish's contract.
    _log("upload busy at shutdown; publishing to a sibling path", warning=True)
    _flush(final=True, remote_suffix=".final")


def flush_at_task_end() -> None:
    """Persist now, from ordinary code, and retire the checkpoint. Never raises.

    Called by the coverage-wrapping wheel entry point (see runner_entry) once the runner's work is
    done. This is the deterministic path: it runs on main()'s own call stack with the interpreter
    unquestionably alive and — crucially — with the task arguments already in ``sys.argv``, so the
    results volume resolves. Compare the checkpoint thread, whose early ticks fire before argv is
    populated and can only leave the data in /tmp.

    Deliberately a non-final flush: the tracer is left running so nothing executing after this point
    is silently untraced. It waits for the upload lock rather than skipping, unlike a checkpoint
    tick — there is no shutdown budget here, and this publish is the whole point.

    The atexit hook still publishes afterwards, so a runner writes the same path twice. That is not
    redundant: because this flush leaves the tracer running, the second payload is a superset
    including whatever executed during teardown, and each write is a whole cumulative file so the
    last one simply wins. Suppressing it would need "already published" state, and that is exactly
    the dedup that previously let a failed write mark itself delivered and never retry.
    """
    _stop_event.set()
    _log("task-end flush (deterministic; checkpoint retired)")
    with _upload_lock:
        landed = _flush(final=False)
    if not landed:
        # Not benign here, unlike an early tick: the work is finished, so if this did not reach the
        # volume then this run's coverage is lost.
        _log("task-end flush did not reach the volume; this run's coverage is lost", warning=True)


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
        # would never happen. Graceful shutdown plus the checkpoint thread cover that instead, and
        # leaving it off is also what keeps our save() calls the only disk writers (see the module
        # docstring).
    )
    _cov.start()
    # Registered after start(), so with atexit's LIFO ordering this runs BEFORE coverage's own
    # atexit hook — hence _final_flush does its own stop()/save().
    atexit.register(_final_flush)
    interval = _checkpoint_seconds()
    if interval > 0:
        threading.Thread(target=_checkpoint_loop, args=(interval,), daemon=True, name="dqx-coverage").start()
    # WARNING, not INFO: this fires from the .pth before the task configures logging, where
    # lastResort drops INFO, and it is the line whose absence explains a silent 0% report.
    _log(f"tracing started (key={_key}, data_file={_data_file}, argv0={sys.argv[0]!r})", warning=True)


try:
    _start()
except Exception as exc:  # noqa: BLE001 — a broken bootstrap must not stop the app or job
    _log(f"NOT started (non-fatal, expect 0% coverage): {exc!r}", warning=True)
