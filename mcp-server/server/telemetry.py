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
import re
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

# Guards _sent_telemetry. Tools are served concurrently, so an unguarded read-then-write lets N
# simultaneous first invocations of the same tool all observe "not sent yet" and each fire a ping.
_cache_lock = threading.Lock()

# Namespaces the signal so MCP usage is separable from direct DQX library usage.
TELEMETRY_KEY = "dqx_mcp"

# Fallback when DQX_VERSION is not injected. Reported rather than omitted: an unknown-but-present
# version still satisfies the pipeline's `release_version IS NOT NULL` filter, so the signal is
# counted instead of silently dropped, and the placeholder makes a missing deploy-time value obvious
# in the data rather than invisible.
_UNKNOWN_VERSION = "0.0.0"

# Characters a release version may contain. Deliberately strict: this value is interpolated into an
# HTTP header, and a header value containing a newline is rejected outright by the HTTP client
# (InvalidHeader) — which would disable telemetry for every tool rather than mis-report one field.
# `.strip()` alone does not protect against that: it removes surrounding whitespace but leaves an
# interior newline intact.
_SEMVER_ISH_RE = re.compile(r"^[0-9A-Za-z.+_-]{1,64}$")


def _dqx_version() -> str:
    """The DQX release this deployment runs against.

    Sourced from the DQX_VERSION app config value, which the bundle sets from the same pin the runner
    job installs — so the reported version is the one that actually executes the checks, not the
    server's own build number. The MCP server cannot read it from the library the way DQX does,
    because it deliberately does not depend on DQX.

    A value that is not version-shaped falls back to the placeholder rather than being sent as-is: see
    _SEMVER_ISH_RE for why a malformed value is worse than a missing one. The placeholder still
    satisfies the telemetry pipeline's `release_version IS NOT NULL` filter, so the signal is counted
    either way.
    """
    raw = os.environ.get("DQX_VERSION", "").strip()
    return raw if _SEMVER_ISH_RE.match(raw) else _UNKNOWN_VERSION


def reset_telemetry_cache() -> None:
    """Clear the per-process dedup cache so previously sent signals can be sent again (tests)."""
    with _cache_lock:
        _sent_telemetry.clear()


def _claim_signal(key: str, value: str) -> bool:
    """Atomically claim the right to send *(key, value)*, returning False if already claimed.

    Check-and-insert under one lock, so concurrent first invocations of the same tool produce exactly
    one send rather than one per caller. Claiming on *attempt* rather than success is deliberate — the
    same choice the DQX library makes — so a brownout costs one stalled ping, not one per later call.
    """
    with _cache_lock:
        if (key, value) in _sent_telemetry:
            return False
        _sent_telemetry[(key, value)] = None
        if len(_sent_telemetry) > _TELEMETRY_CACHE_MAX_SIZE:
            _sent_telemetry.popitem(last=False)
        return True


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
    # Claiming here as well as in _send_async keeps this function correct when called directly; the
    # claim is idempotent, so the second check is a cheap no-op on the wrapped path.
    if not _claim_signal(key, value):
        return

    try:
        new_config = ws.config.copy().with_user_agent_extra(key, value)
        # Also stamp dqx/<version>, which is what the telemetry pipeline reads into its
        # `release_version` column. The DQX library gets this from its own package
        # (src/databricks/labs/dqx/__init__.py calls ua.with_product/with_extra at import time), but
        # the MCP server does not import DQX, so its User-Agent would otherwise carry no dqx token at
        # all — leaving release_version NULL. Every chart on the DQX Telemetry dashboard filters
        # `release_version IS NOT NULL`, so without this the signal is collected and then discarded.
        new_config = new_config.with_user_agent_extra("dqx", _dqx_version())
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
    Daemon so it can never hold up interpreter shutdown.

    The dedup slot is claimed *before* the thread is created, so a tool that has already reported
    costs nothing at all on later calls — no thread, no client construction. Doing this inside the
    thread instead would spawn one per request and let concurrent first calls race into N pings.
    """
    if _telemetry_disabled():
        return
    if not _claim_signal(TELEMETRY_KEY, tool_name):
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
