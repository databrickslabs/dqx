"""Start the local development environment for DQX Studio.

Replaces ``apx dev start`` end-to-end. Spawns two foreground subprocesses
and wires Vite as the public-facing reverse proxy for the FastAPI
backend, matching the legacy contract:

* **UI**: http://localhost:9001
* **API**: http://localhost:9001/api/*  → uvicorn on 9002
* **Swagger**: http://localhost:9001/docs → uvicorn on 9002

Rationale for the layout:

* uvicorn runs on a fixed internal port (``--port 9002``) with
  ``--reload`` for backend hot reload — same wiring ``apx dev start``
  used internally.
* Vite serves the SPA on the documented public port 9001 and proxies
  the FastAPI paths via its built-in ``server.proxy`` (configured in
  ``vite.config.ts``). The proxy target port is passed through the
  ``DQX_APP_BACKEND_PORT`` env var so the two stay in sync if a
  developer needs to remap (e.g. port conflict).
* Both processes run in the foreground, with their stdout/stderr
  streaming through to the caller's terminal. ``Ctrl+C`` (SIGINT) is
  forwarded to both children for a clean shutdown.
* If either child exits unexpectedly, the orchestrator terminates the
  sibling and exits with that child's status code — surfaces broken
  state immediately instead of leaving half a dev server running.

We deliberately do NOT regenerate ``api.ts`` automatically on backend
changes (apx watched the OpenAPI schema and re-ran orval on every
backend save). The auto-regen path added complexity for marginal value
in practice; developers run ``make app-regen-api`` after touching
pydantic models. ``uvicorn --reload`` still picks up backend code
changes for runtime, so only the typed client lags until manual
regeneration.

Usage::

    python scripts/dev.py

Stop via Ctrl+C, or from a sibling shell::

    pkill -f scripts/dev.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent.parent
NODE_BIN = APP_DIR / "node_modules" / ".bin"

UVICORN_PORT = 9002
VITE_PORT = 9001
UVICORN_APP = "databricks_labs_dqx_app.backend.app:app"

_children: list[subprocess.Popen[bytes]] = []
_shutting_down = False


def _shutdown(*_: object) -> None:
    """Terminate every spawned child (and its descendants), idempotently.

    Registered as the handler for SIGINT and SIGTERM, and also called
    from the main loop when a child exits unexpectedly. The
    ``_shutting_down`` flag suppresses re-entry — a fast Ctrl+C
    burst would otherwise iterate over a partially-killed children
    list and double-signal procs that were already terminated.

    Signals are sent to each child's *process group* (``os.killpg``)
    rather than the child PID alone. This is load-bearing for uvicorn
    ``--reload``: the immediate child is a watcher that spawns the real
    server in a grandchild PID; ``terminate()`` on the watcher alone
    leaves the server orphaned for several seconds. Each child was
    spawned with its own process group (``start_new_session=True``) so
    the killpg has a well-defined target and doesn't escape into the
    parent shell.
    """
    global _shutting_down  # noqa: PLW0603
    if _shutting_down:
        return
    _shutting_down = True
    print("\n▶ shutting down dev servers...", flush=True)
    for c in _children:
        if c.poll() is None:
            try:
                os.killpg(os.getpgid(c.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + 5.0
    for c in _children:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            c.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            print(f"  ! pid {c.pid} ignored SIGTERM; sending SIGKILL", flush=True)
            try:
                os.killpg(os.getpgid(c.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            c.wait()


def _spawn(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.Popen[bytes]:
    """Spawn a child in its own process group, stdio passed through.

    ``start_new_session=True`` puts the child into a fresh session
    (and therefore a fresh process group), so ``_shutdown`` can send
    one signal to the whole subtree without affecting the parent
    shell. The lifecycle guarantee: every grandchild spawned by the
    child (e.g. uvicorn's reloader spawning the actual server) shares
    the group and gets terminated alongside the parent.
    """
    print(f"  $ {' '.join(cmd)}", flush=True)
    child = subprocess.Popen(  # noqa: S603
        cmd,
        cwd=APP_DIR,
        env=env,
        start_new_session=True,
    )
    _children.append(child)
    return child


def _start_uvicorn() -> subprocess.Popen[bytes]:
    """Launch the FastAPI backend on ``UVICORN_PORT`` with auto-reload.

    Runs via ``uv run --exact --all-extras`` so the dev shell doesn't
    need a pre-activated venv — same invocation style the Makefile uses
    for ``app-check`` / ``app-build``. ``--reload-dir src`` keeps the
    watcher scoped to first-party source, avoiding spurious restarts
    when ``.build/`` artifacts or ``node_modules/`` get touched.
    """
    return _spawn(
        [
            "uv",
            "run",
            "--exact",
            "--all-extras",
            "uvicorn",
            UVICORN_APP,
            "--host",
            "localhost",
            "--port",
            str(UVICORN_PORT),
            "--reload",
            "--reload-dir",
            "src",
        ]
    )


def _start_vite() -> subprocess.Popen[bytes]:
    """Launch the Vite dev server on ``VITE_PORT`` with HMR.

    The backend port is passed through ``DQX_APP_BACKEND_PORT`` so
    ``vite.config.ts``'s ``server.proxy`` forwards ``/api`` / ``/docs``
    / ``/openapi.json`` to the right uvicorn instance. ``--strictPort``
    fails fast on a port clash instead of silently bumping to 9002 and
    breaking the proxy contract.
    """
    env = os.environ.copy()
    env["DQX_APP_BACKEND_PORT"] = str(UVICORN_PORT)
    return _spawn(
        [
            str(NODE_BIN / "vite"),
            "--port",
            str(VITE_PORT),
            "--strictPort",
        ],
        env=env,
    )


def main() -> int:
    if not (NODE_BIN / "vite").exists():
        sys.exit("error: node_modules not installed — run `make app-install` first")

    # Install handlers BEFORE spawning so a Ctrl+C during startup still
    # kills any already-spawned child instead of orphaning it.
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"▶ Starting uvicorn (FastAPI) on http://localhost:{UVICORN_PORT}", flush=True)
    _start_uvicorn()

    print(f"▶ Starting vite (UI + reverse proxy) on http://localhost:{VITE_PORT}", flush=True)
    _start_vite()

    print(
        f"\n✓ Dev server ready: http://localhost:{VITE_PORT}\n"
        f"  - UI:      http://localhost:{VITE_PORT}\n"
        f"  - API:     http://localhost:{VITE_PORT}/api/v1/*\n"
        f"  - Swagger: http://localhost:{VITE_PORT}/docs\n"
        f"  (Ctrl+C to stop both servers)\n",
        flush=True,
    )

    # Poll until either child exits, then bring everything down.
    # ``wait()`` on a single child would block us from noticing the
    # sibling dying; a short sleep keeps shutdown latency under 0.5s
    # without burning CPU.
    while True:
        for c in _children:
            rc = c.poll()
            if rc is not None:
                print(f"\n⚠ subprocess pid={c.pid} exited with {rc}", flush=True)
                _shutdown()
                return rc
        time.sleep(0.5)


if __name__ == "__main__":
    raise SystemExit(main())
