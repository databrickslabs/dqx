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
            log_telemetry(ws, "mcp", "run_checks")

        extras = {c.args for c in ws.config.with_user_agent_extra.call_args_list}
        assert ("mcp", "run_checks") in extras, extras

    def test_sends_each_signal_at_most_once_per_process(self):
        """Adoption signal, not an invocation counter — the library dedups the same way."""
        from server.telemetry import log_telemetry

        ws = _client()
        for _ in range(5):
            log_telemetry(ws, "mcp", "run_checks")

        signals = [c.args for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "mcp"]
        assert signals == [("mcp", "run_checks")], signals

    def test_distinct_values_are_sent_separately(self):
        from server.telemetry import log_telemetry

        ws = _client()
        log_telemetry(ws, "mcp", "run_checks")
        log_telemetry(ws, "mcp", "profile_table")

        signals = [c.args[1] for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "mcp"]
        assert sorted(signals) == ["profile_table", "run_checks"], signals

    def test_a_failed_ping_is_swallowed(self):
        """Telemetry must never surface in a tool response."""
        from server.telemetry import log_telemetry

        ws = _client()
        ws.config.copy.side_effect = RuntimeError("control plane down")

        log_telemetry(ws, "mcp", "run_checks")  # must not raise

    def test_marks_on_attempt_so_a_brownout_is_not_retried_every_call(self):
        """A failure still consumes the dedup slot, bounding the cost of an unhealthy workspace."""
        from server.telemetry import log_telemetry

        ws = _client()
        ws.config.copy.side_effect = RuntimeError("boom")
        for _ in range(4):
            log_telemetry(ws, "mcp", "run_checks")

        assert ws.config.copy.call_count == 1

    def test_disabled_by_env(self, monkeypatch):
        from server.telemetry import log_telemetry

        monkeypatch.setenv("DQX_MCP_DISABLE_TELEMETRY", "true")
        ws = _client()
        log_telemetry(ws, "mcp", "run_checks")

        ws.config.copy.assert_not_called()

    def test_cache_is_bounded(self, monkeypatch):
        """Hard memory bound, evicting oldest-first, as the library does."""
        import server.telemetry as tel

        monkeypatch.setattr(tel, "_TELEMETRY_CACHE_MAX_SIZE", 3)
        ws = _client()
        for i in range(10):
            tel.log_telemetry(ws, "mcp", f"tool_{i}")

        assert len(tel._sent_telemetry) <= 3


class TestReleaseVersion:
    """The dqx/<version> token the telemetry pipeline reads into its release_version column."""

    def test_stamps_the_configured_dqx_version(self, monkeypatch):
        from server.telemetry import log_telemetry

        monkeypatch.setenv("DQX_VERSION", "0.15.0")
        ws = _client()
        log_telemetry(ws, "mcp", "run_checks")

        extras = {c.args for c in ws.config.with_user_agent_extra.call_args_list}
        assert ("dqx", "0.15.0") in extras, extras
        assert ("mcp", "run_checks") in extras, extras

    def test_rejects_a_version_that_could_break_the_header(self, monkeypatch):
        """A newline in DQX_VERSION would make the HTTP client reject the whole request.

        The value is interpolated into a User-Agent header, and an interior newline survives
        .strip() — an InvalidHeader would then disable telemetry for every tool rather than
        mis-report one field. Verified against the real SDK that such a header does raise.
        """
        from server.telemetry import log_telemetry

        monkeypatch.setenv("DQX_VERSION", "0.15\n0")
        ws = _client()
        log_telemetry(ws, "mcp", "run_checks")

        versions = [c.args[1] for c in ws.config.with_user_agent_extra.call_args_list if c.args[0] == "dqx"]
        assert versions == ["0.0.0"], versions

    def test_reports_a_placeholder_rather_than_omitting_the_version(self, monkeypatch):
        """A NULL release_version is filtered out of every dashboard chart, so never omit it."""
        from server.telemetry import log_telemetry

        monkeypatch.delenv("DQX_VERSION", raising=False)
        ws = _client()
        log_telemetry(ws, "mcp", "run_checks")

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
        with patch("server.telemetry._send") as mock_send:
            wrapped()

        mock_send.assert_called_once_with("profile_table")

    def test_signal_is_sent_even_when_the_tool_raises(self):
        """Which surfaces get invoked is the signal, not which ones succeeded."""
        from server.telemetry import with_telemetry

        def failing_tool() -> None:
            raise ValueError("tool failed")

        wrapped = with_telemetry(failing_tool)
        with patch("server.telemetry._send") as mock_send, pytest.raises(ValueError):
            wrapped()

        mock_send.assert_called_once_with("failing_tool")

    def test_a_slow_control_plane_is_bounded_not_unbounded(self):
        """Inline means a tool DOES wait for the ping, so the bound is what keeps that safe.

        This replaces an earlier "never blocks" guard that held only while the ping ran on a daemon
        thread. That thread was removed: it put the dedup claim and the send on opposite sides of the
        cache check, so the signal was never actually sent (and the DQX library is synchronous too).
        The accepted cost is a bounded wait, paid once per tool per process — never the SDK's default
        300s retry window, which is the failure that made telemetry dangerous in the library (#1355).
        """
        import server.telemetry as tel

        assert tel._TELEMETRY_TIMEOUT_SECONDS <= 5, (
            "the inline ping must stay tightly bounded; without a low cap a control-plane brownout "
            "stalls the first call of every tool for the SDK's full retry window"
        )

        captured = {}

        def _capture(config=None, **_kwargs):
            captured["retry"] = config.retry_timeout_seconds
            captured["http"] = config.http_timeout_seconds
            return MagicMock()

        ws = _client()
        with patch("server.telemetry.type", return_value=_capture):
            tel.log_telemetry(ws, "mcp", "run_checks")

        # Both caps must be applied to the config that actually makes the call.
        assert captured["retry"] == tel._TELEMETRY_TIMEOUT_SECONDS, captured
        assert captured["http"] == tel._TELEMETRY_TIMEOUT_SECONDS, captured

    def test_the_wrapped_path_actually_sends_the_signal(self):
        """The end-to-end guard: drive the REAL wrapped path and assert the ping goes out.

        Regression guard for the bug this feature shipped with. An earlier version claimed the dedup
        slot before spawning a thread and then called log_telemetry inside it, which claimed the SAME
        (key, value) again, saw it taken, and returned before sending — so telemetry NEVER reached the
        wire on the production path, while every unit test still passed. They passed because they
        called log_telemetry directly with a fresh cache, or patched _send/Thread, so none of them
        exercised wrapper -> _send -> log_telemetry as a whole. This one patches nothing but the
        client resolution.
        """
        from server.telemetry import with_telemetry

        def profile_table() -> str:
            return "ok"

        ws = _client()
        wrapped = with_telemetry(profile_table)
        with patch("server.utils.get_sp_client_for_telemetry", return_value=ws):
            assert wrapped() == "ok"

        extras = {c.args for c in ws.config.with_user_agent_extra.call_args_list}
        assert ("mcp", "profile_table") in extras, f"signal never sent: {extras}"
        # The config was copied and stamped, which is what puts the header on the wire. (The client is
        # rebuilt via type(ws)(config=...), so the send itself lands on a different mock object.)
        assert ws.config.copy.called, "config never copied, so log_telemetry returned before sending"

    def test_the_wrapped_path_sends_once_across_repeated_calls(self):
        """Dedup still holds through the wrapper, so later calls cost nothing."""
        from server.telemetry import with_telemetry

        def profile_table() -> str:
            return "ok"

        ws = _client()
        wrapped = with_telemetry(profile_table)
        with patch("server.utils.get_sp_client_for_telemetry", return_value=ws):
            for _ in range(5):
                assert wrapped() == "ok"

        assert ws.config.copy.call_count == 1, "should build the signal exactly once for one tool"

    def test_concurrent_first_calls_send_exactly_one_signal(self):
        """N simultaneous first invocations must not race into N control-plane pings."""
        import threading as _threading

        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        ws = _client()
        wrapped = with_telemetry(sample_tool)
        start = _threading.Barrier(8)

        def _call() -> None:
            start.wait()  # maximise the overlap on the dedup check
            wrapped()

        with patch("server.utils.get_sp_client_for_telemetry", return_value=ws):
            callers = [_threading.Thread(target=_call) for _ in range(8)]
            for c in callers:
                c.start()
            for c in callers:
                c.join(timeout=10)

        assert ws.config.copy.call_count == 1, f"{ws.config.copy.call_count} pings fired for one signal"

    def test_a_failing_client_never_breaks_the_tool(self):
        """Inline means the tool now shares a thread with the ping — a failure must stay invisible."""
        from server.telemetry import with_telemetry

        def sample_tool() -> str:
            return "ok"

        wrapped = with_telemetry(sample_tool)
        with patch("server.utils.get_sp_client_for_telemetry", side_effect=RuntimeError("no creds")):
            assert wrapped() == "ok"
