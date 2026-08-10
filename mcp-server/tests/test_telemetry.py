"""Unit tests for the MCP server's adoption telemetry.

The properties that matter are the ones the DQX library learned the hard way: a signal is sent at
most once per process, a failure never reaches the caller, and the whole thing can be switched off.
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _clean_cache(monkeypatch):
    """Each test starts with an empty dedup cache and telemetry enabled."""
    from server.telemetry import reset_telemetry_cache

    monkeypatch.delenv("DQX_MCP_DISABLE_TELEMETRY", raising=False)
    reset_telemetry_cache()
    yield
    reset_telemetry_cache()


def _client() -> MagicMock:
    """A WorkspaceClient stand-in whose config records the user-agent extras it was given."""
    ws = MagicMock()
    ws.config.copy.return_value = ws.config
    ws.config.with_user_agent_extra.return_value = ws.config
    return ws


class TestLogTelemetry:
    def test_sends_the_signal_as_a_user_agent_extra(self):
        from server.telemetry import log_telemetry

        ws = _client()
        with patch("server.telemetry.type", create=True):
            log_telemetry(ws, "dqx_mcp", "run_checks")

        extras = {c.args for c in ws.config.with_user_agent_extra.call_args_list}
        assert ("dqx_mcp", "run_checks") in extras, extras

    def test_sends_each_signal_at_most_once_per_process(self):
        """Adoption signal, not an invocation counter — the library dedups the same way."""
        from server.telemetry import log_telemetry

        ws = _client()
        for _ in range(5):
            log_telemetry(ws, "dqx_mcp", "run_checks")

        signals = [c.args for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "dqx_mcp"]
        assert signals == [("dqx_mcp", "run_checks")], signals

    def test_distinct_values_are_sent_separately(self):
        from server.telemetry import log_telemetry

        ws = _client()
        log_telemetry(ws, "dqx_mcp", "run_checks")
        log_telemetry(ws, "dqx_mcp", "profile_table")

        signals = [c.args[1] for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "dqx_mcp"]
        assert sorted(signals) == ["profile_table", "run_checks"], signals

    def test_a_failed_ping_is_swallowed(self):
        """Telemetry must never surface in a tool response."""
        from server.telemetry import log_telemetry

        ws = _client()
        ws.config.copy.side_effect = RuntimeError("control plane down")

        log_telemetry(ws, "dqx_mcp", "run_checks")  # must not raise

    def test_marks_on_attempt_so_a_brownout_is_not_retried_every_call(self):
        """A failure still consumes the dedup slot, bounding the cost of an unhealthy workspace."""
        from server.telemetry import log_telemetry

        ws = _client()
        ws.config.copy.side_effect = RuntimeError("boom")
        for _ in range(4):
            log_telemetry(ws, "dqx_mcp", "run_checks")

        assert ws.config.copy.call_count == 1

    def test_disabled_by_env(self, monkeypatch):
        from server.telemetry import log_telemetry

        monkeypatch.setenv("DQX_MCP_DISABLE_TELEMETRY", "true")
        ws = _client()
        log_telemetry(ws, "dqx_mcp", "run_checks")

        ws.config.copy.assert_not_called()

    def test_cache_is_bounded(self, monkeypatch):
        """Hard memory bound, evicting oldest-first, as the library does."""
        import server.telemetry as tel

        monkeypatch.setattr(tel, "_TELEMETRY_CACHE_MAX_SIZE", 3)
        ws = _client()
        for i in range(10):
            tel.log_telemetry(ws, "dqx_mcp", f"tool_{i}")

        assert len(tel._sent_telemetry) <= 3


class TestReleaseVersion:
    """The dqx/<version> token the telemetry pipeline reads into its release_version column."""

    def test_stamps_the_configured_dqx_version(self, monkeypatch):
        from server.telemetry import log_telemetry

        monkeypatch.setenv("DQX_VERSION", "0.15.0")
        ws = _client()
        log_telemetry(ws, "dqx_mcp", "run_checks")

        extras = {c.args for c in ws.config.with_user_agent_extra.call_args_list}
        assert ("dqx", "0.15.0") in extras, extras
        assert ("dqx_mcp", "run_checks") in extras, extras

    def test_rejects_a_version_that_could_break_the_header(self, monkeypatch):
        """A newline in DQX_VERSION would make the HTTP client reject the whole request.

        The value is interpolated into a User-Agent header, and an interior newline survives
        .strip() — an InvalidHeader would then disable telemetry for every tool rather than
        mis-report one field. Verified against the real SDK that such a header does raise.
        """
        from server.telemetry import log_telemetry

        monkeypatch.setenv("DQX_VERSION", "0.15\n0")
        ws = _client()
        log_telemetry(ws, "dqx_mcp", "run_checks")

        versions = [c.args[1] for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "dqx"]
        assert versions == ["0.0.0"], versions

    def test_reports_a_placeholder_rather_than_omitting_the_version(self, monkeypatch):
        """A NULL release_version is filtered out of every dashboard chart, so never omit it."""
        from server.telemetry import log_telemetry

        monkeypatch.delenv("DQX_VERSION", raising=False)
        ws = _client()
        log_telemetry(ws, "dqx_mcp", "run_checks")

        versions = [c.args[1] for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "dqx"]
        assert versions == ["0.0.0"], versions


class TestWithTelemetry:
    def test_returns_the_wrapped_result_and_preserves_metadata(self):
        """FastMCP derives a tool's name and schema from the function, so both must survive."""
        from server.telemetry import with_telemetry

        def sample_tool(table_name: str) -> str:
            """Sample docstring."""
            return f"ran on {table_name}"

        wrapped = with_telemetry(sample_tool)
        with patch("server.utils.get_sp_client_for_telemetry", return_value=_client()):
            assert wrapped("cat.sch.tbl") == "ran on cat.sch.tbl"
        assert wrapped.__name__ == "sample_tool"
        assert wrapped.__doc__ == "Sample docstring."

    def test_tool_still_runs_when_telemetry_cannot_resolve_a_client(self):
        """No workspace credentials (or any other failure) must not break the tool."""
        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        wrapped = with_telemetry(sample_tool)
        with patch("server.utils.get_sp_client_for_telemetry", side_effect=RuntimeError("no creds")):
            assert wrapped() == "ok"

    def test_records_the_tool_name(self):
        from server.telemetry import with_telemetry

        def profile_table() -> str:
            return "ok"

        wrapped = with_telemetry(profile_table)
        with patch("server.telemetry._send_async") as mock_send:
            wrapped()

        mock_send.assert_called_once_with("profile_table")

    def test_signal_is_sent_even_when_the_tool_raises(self):
        """Which surfaces get invoked is the signal, not which ones succeeded."""
        from server.telemetry import with_telemetry

        def failing_tool() -> None:
            raise ValueError("tool failed")

        wrapped = with_telemetry(failing_tool)
        with patch("server.telemetry._send_async") as mock_send, pytest.raises(ValueError):
            wrapped()

        mock_send.assert_called_once_with("failing_tool")

    def test_never_blocks_the_tool_on_a_slow_control_plane(self):
        """The ping runs off-thread: a hung workspace must not add latency to a tool call.

        Regression guard — doing this inline made every first-call-per-tool wait for an HTTP
        round-trip (~0.6s healthy, up to the 5s timeout otherwise).
        """
        import time

        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        def _hang(*_args, **_kwargs):
            time.sleep(30)  # never completes within the assertion window

        wrapped = with_telemetry(sample_tool)
        with patch("server.utils.get_sp_client_for_telemetry", side_effect=_hang):
            started = time.monotonic()
            assert wrapped() == "ok"
            assert time.monotonic() - started < 1.0

    def test_no_thread_is_spawned_once_the_signal_has_been_sent(self):
        """The point of the dedup cache: a reported tool costs nothing on every later call."""
        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        wrapped = with_telemetry(sample_tool)
        with patch("server.utils.get_sp_client_for_telemetry", return_value=_client()):
            with patch("server.telemetry.threading.Thread") as thread:
                for _ in range(5):
                    assert wrapped() == "ok"

        assert thread.call_count == 1, f"spawned {thread.call_count} threads for one signal"

    def test_concurrent_first_calls_send_exactly_one_signal(self):
        """N simultaneous first invocations must not race into N control-plane pings."""
        import threading as _threading

        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        wrapped = with_telemetry(sample_tool)
        start = _threading.Barrier(8)
        threads_made: list[object] = []
        lock = _threading.Lock()

        real_thread = _threading.Thread

        def _record(*args, **kwargs):
            with lock:
                threads_made.append(object())
            return real_thread(*args, **kwargs)

        def _call() -> None:
            start.wait()  # maximise the overlap on the dedup check
            wrapped()

        with patch("server.utils.get_sp_client_for_telemetry", return_value=_client()):
            with patch("server.telemetry.threading.Thread", side_effect=_record):
                callers = [real_thread(target=_call) for _ in range(8)]
                for c in callers:
                    c.start()
                for c in callers:
                    c.join(timeout=10)

        assert len(threads_made) == 1, f"{len(threads_made)} pings fired for one signal"

    def test_tool_still_runs_when_the_thread_cannot_start(self):
        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        wrapped = with_telemetry(sample_tool)
        with patch("server.telemetry.threading.Thread", side_effect=RuntimeError("no threads")):
            assert wrapped() == "ok"
