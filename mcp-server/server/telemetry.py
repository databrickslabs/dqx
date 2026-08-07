"""Adoption telemetry for the DQX MCP server.

Mirrors ``src/databricks/labs/dqx/telemetry.py`` — same mechanism, same dedup semantics — but
reimplemented here because that module imports pyspark at module scope and the MCP server has
neither pyspark nor the DQX library as a dependency (the server only submits jobs; see
``mcp-server/pyproject.toml``).

**What this sends.** Nothing to any DQX-owned endpoint. The signal is a ``User-Agent`` extra on a
single cheap call to the *customer's own* workspace control plane, which already logs every request
it receives. So no data, metadata, or telemetry leaves the customer's account.

**What it measures.** Which MCP tools are used, not how often: each ``(key, value)`` is sent at most
once per process, exactly as the DQX library does. A signal therefore means "this tool has been used
on this deployment" for the lifetime of an app replica, and is deliberately *not* an invocation
counter — that keeps the load on the control plane bounded and stops a brownout from re-stalling
every subsequent request.

The ``dqx_mcp`` key distinguishes MCP-driven usage from a notebook user calling DQX directly: the
runner job installs the real DQX library, so its ``engine/*`` signals are indistinguishable from any
other caller's without this.
"""

import functools
import logging
import os
import threading
from collections import OrderedDict
from collections.abc import Callable

logger = logging.getLogger(__name__)

# Bound the control-plane ping so a workspace brownout cannot stall a tool call. The SDK's default
# retry window is 300s; on repeated 503s it blocks for that long and then raises. Telemetry is
# best-effort, so giving up in seconds is right. Matches the DQX library.
_TELEMETRY_TIMEOUT_SECONDS = 5

# Hard-bound the dedup cache and evict oldest-first (FIFO), as the library does. The MCP key space is
# naturally tiny (one entry per tool name plus a version), so eviction should never trigger here —
# the cap exists so a future high-cardinality signal cannot grow memory without limit.
_TELEMETRY_CACHE_MAX_SIZE = 10_000
_sent_telemetry: "OrderedDict[tuple[str, str], None]" = OrderedDict()

# Namespaces the signal so MCP usage is separable from direct DQX library usage.
TELEMETRY_KEY = "dqx_mcp"


def reset_telemetry_cache() -> None:
    """Clear the per-process dedup cache so previously sent signals can be sent again (tests)."""
    _sent_telemetry.clear()


def _telemetry_disabled() -> bool:
    """True when DQX_MCP_DISABLE_TELEMETRY is set to a truthy value.

    An explicit off switch: the mechanism is harmless (a header on a call to the customer's own
    control plane) but an operator should not have to patch the image to stop it.
    """
    return os.environ.get("DQX_MCP_DISABLE_TELEMETRY", "").strip().lower() in {"1", "true", "yes"}


def log_telemetry(ws, key: str, value: str) -> None:
    """Record a *(key, value)* adoption signal via the SDK's User-Agent. Never raises.

    Args:
        ws: WorkspaceClient whose config carries the signal.
        key: telemetry key (e.g. ``dqx_mcp``).
        value: telemetry value (e.g. the tool name).
    """
    if _telemetry_disabled():
        return
    # Mark on *attempt*, not success: at most one control-plane call per signal per process, so a
    # brownout cannot re-stall every later request. Same rationale as the DQX library.
    if (key, value) in _sent_telemetry:
        return
    _sent_telemetry[(key, value)] = None
    if len(_sent_telemetry) > _TELEMETRY_CACHE_MAX_SIZE:
        _sent_telemetry.popitem(last=False)

    try:
        new_config = ws.config.copy().with_user_agent_extra(key, value)
        new_config.retry_timeout_seconds = _TELEMETRY_TIMEOUT_SECONDS
        new_config.http_timeout_seconds = _TELEMETRY_TIMEOUT_SECONDS
        logger.debug(f"Added User-Agent extra {key}={value}")
        # Recreate from the same type to preserve type information, then make one cheap call whose
        # only purpose is to put the header on the wire. Its result is deliberately discarded.
        client = type(ws)(config=new_config)
        client.clusters.select_spark_version()
    except Exception as e:  # telemetry must never surface in a tool response
        logger.debug(f"Telemetry not sent for {key}={value}: {e}")


def with_telemetry(tool: Callable) -> Callable:
    """Wrap an MCP tool so its first invocation records a ``dqx_mcp/<tool_name>`` signal.

    Applied once in ``load_tools`` to every registered tool, so a new tool is instrumented by
    construction rather than by remembering to annotate it.

    The signal is sent BEFORE the tool body runs, so a tool that fails still reports that it was
    reached — the point is which surfaces users invoke, not which succeeded. The client is resolved
    lazily and any failure (including no workspace credentials at all) is swallowed, so this stays
    invisible to callers.
    """

    @functools.wraps(tool)
    def wrapper(*args, **kwargs):
        _send_async(tool.__name__)
        return tool(*args, **kwargs)

    return wrapper


def _send_async(tool_name: str) -> None:
    """Fire the signal on a daemon thread so it never adds latency to a tool call.

    The ping is an HTTP round-trip to the control plane: ~0.6s on a healthy workspace and up to the
    5s timeout on an unhealthy one. Doing that inline made every first-call-per-tool pay for
    instrumentation, which is the wrong trade — the user's request must not wait on telemetry.
    Daemon so it can never hold up interpreter shutdown; the dedup cache still bounds this to one
    thread per distinct signal per process.
    """
    if _telemetry_disabled():
        return

    def _run() -> None:
        try:
            # Imported here, not at module scope: keeps SDK construction off the import path and
            # avoids a circular import (utils imports nothing from this module).
            from .utils import get_sp_client_for_telemetry

            log_telemetry(get_sp_client_for_telemetry(), TELEMETRY_KEY, tool_name)
        except Exception as e:  # never let instrumentation break anything
            logger.debug(f"Telemetry skipped for {tool_name}: {e}")

    try:
        threading.Thread(target=_run, name=f"dqx-mcp-telemetry-{tool_name}", daemon=True).start()
    except Exception as e:  # thread creation can fail under resource pressure
        logger.debug(f"Telemetry thread not started for {tool_name}: {e}")
