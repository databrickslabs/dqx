"""Tests for ``SchedulerService`` — the weekly view-GC logic.

We focus on the pieces that landed most recently:

- ``_next_saturday_01_utc`` cron-style boundary maths.
- ``_gc_orphan_views`` orchestration: candidate filtering, in-use
  exclusion, and bounded drop count.

The async loop itself (``_loop`` / ``_tick``) is exercised via
integration tests; here we keep the surface narrow and deterministic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.services.scheduler_service import (
    _GC_AGE_HOURS,
    _GC_HOUR_UTC,
    _GC_WEEKDAY_SAT,
    SchedulerService,
)


def _make_service() -> SchedulerService:
    """Construct a SchedulerService without invoking ``__init__``.

    The real constructor builds two ``SqlExecutor`` instances (which
    requires a ``WorkspaceClient`` and a warehouse id), neither of which
    is needed for these tests. We bypass it via ``object.__new__`` and
    set the handful of attributes the methods we test actually read.
    """
    svc = object.__new__(SchedulerService)
    svc._catalog = "main"
    svc._schema = "dqx"
    svc._tmp_schema = "dqx_tmp"
    svc._sql = MagicMock(name="SqlExecutor (main schema)")
    svc._tmp_sql = MagicMock(name="SqlExecutor (tmp schema)")
    svc._next_view_gc_at = datetime(2099, 1, 1, tzinfo=timezone.utc)
    return svc


# ---------------------------------------------------------------------------
# _next_saturday_01_utc — cron-style boundary maths
# ---------------------------------------------------------------------------


class TestNextSaturday01Utc:
    def test_constants_are_correct(self):
        # Sanity-check the module-level constants drive the right schedule.
        assert _GC_WEEKDAY_SAT == 5  # Python's weekday: Mon=0..Sun=6
        assert _GC_HOUR_UTC == 1
        assert _GC_AGE_HOURS == 24

    def test_friday_morning_returns_next_saturday_at_01(self):
        # Fri 2026-05-01 10:00 UTC → next Sat is 2026-05-02 01:00 UTC.
        now = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
        nxt = SchedulerService._next_saturday_01_utc(now)
        assert nxt == datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)

    def test_saturday_at_midnight_returns_today_at_01(self):
        # Sat 2026-05-02 00:30 UTC → today (Sat) at 01:00.
        now = datetime(2026, 5, 2, 0, 30, tzinfo=timezone.utc)
        nxt = SchedulerService._next_saturday_01_utc(now)
        assert nxt == datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)

    def test_saturday_at_01_exactly_rolls_to_next_week(self):
        # The boundary case: now == target → must roll forward (>= would
        # double-fire). The doc string promises "strictly after now".
        now = datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)
        nxt = SchedulerService._next_saturday_01_utc(now)
        assert nxt == datetime(2026, 5, 9, 1, 0, tzinfo=timezone.utc)

    def test_saturday_after_01_rolls_to_next_week(self):
        # Sat 2026-05-02 02:00 UTC → next Sat 2026-05-09 01:00 UTC.
        now = datetime(2026, 5, 2, 2, 0, tzinfo=timezone.utc)
        nxt = SchedulerService._next_saturday_01_utc(now)
        assert nxt == datetime(2026, 5, 9, 1, 0, tzinfo=timezone.utc)

    @pytest.mark.parametrize(
        "weekday,today,expected_offset_days",
        [
            (0, datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc), 5),  # Mon → Sat = +5
            (1, datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc), 4),  # Tue → +4
            (2, datetime(2026, 4, 29, 9, 0, tzinfo=timezone.utc), 3),  # Wed → +3
            (3, datetime(2026, 4, 30, 9, 0, tzinfo=timezone.utc), 2),  # Thu → +2
            (4, datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc), 1),  # Fri → +1
            (6, datetime(2026, 5, 3, 9, 0, tzinfo=timezone.utc), 6),  # Sun → +6
        ],
    )
    def test_every_weekday_lands_on_saturday(self, weekday, today, expected_offset_days):
        assert today.weekday() == weekday
        nxt = SchedulerService._next_saturday_01_utc(today)
        assert nxt.weekday() == _GC_WEEKDAY_SAT
        assert nxt.hour == 1 and nxt.minute == 0 and nxt.second == 0 and nxt.microsecond == 0
        assert (nxt.date() - today.date()).days == expected_offset_days

    def test_microseconds_normalised(self):
        now = datetime(2026, 5, 1, 10, 30, 22, 123456, tzinfo=timezone.utc)
        nxt = SchedulerService._next_saturday_01_utc(now)
        assert nxt.microsecond == 0
        assert nxt.second == 0


# ---------------------------------------------------------------------------
# _gc_orphan_views — happy path + edge cases
# ---------------------------------------------------------------------------


class TestGcOrphanViews:
    def test_no_candidates_returns_quickly(self):
        svc = _make_service()
        svc._tmp_sql.query.return_value = []
        svc._gc_orphan_views()
        # No DROP VIEW issued.
        svc._tmp_sql.execute.assert_not_called()

    def test_drops_eligible_views_skipping_in_use(self):
        svc = _make_service()
        # Three candidates: tmp_view_aaaa1111, tmp_view_bbbb2222, tmp_view_cccc3333
        svc._tmp_sql.query.return_value = [
            ("tmp_view_aaaa1111",),
            ("tmp_view_bbbb2222",),
            ("tmp_view_cccc3333",),
        ]
        # tmp_view_bbbb2222 is currently in use by a RUNNING dryrun
        svc._sql.query.return_value = [("main.dqx_tmp.tmp_view_bbbb2222",)]
        svc._gc_orphan_views()

        executed = [c.args[0] for c in svc._tmp_sql.execute.call_args_list]
        assert any("tmp_view_aaaa1111" in s for s in executed)
        assert any("tmp_view_cccc3333" in s for s in executed)
        assert not any("tmp_view_bbbb2222" in s for s in executed)

    def test_invalid_view_names_are_filtered_out(self):
        svc = _make_service()
        # The regex requires tmp_view_<8-32 lowercase hex>; everything else
        # is skipped to keep the GC laser-focused.
        svc._tmp_sql.query.return_value = [
            ("tmp_view_DEADBEEF",),  # uppercase hex → not matched
            ("tmp_view_short",),  # too short / non-hex
            ("tmp_view_aaaa1111",),  # valid
            ("real_user_view",),  # not the right prefix
            ("tmp_view_zzzzzzzzzz",),  # non-hex characters
        ]
        svc._sql.query.return_value = []
        svc._gc_orphan_views()

        executed = [c.args[0] for c in svc._tmp_sql.execute.call_args_list]
        assert len(executed) == 1
        assert "tmp_view_aaaa1111" in executed[0]

    def test_in_use_query_failure_does_not_abort_cleanup(self):
        svc = _make_service()
        svc._tmp_sql.query.return_value = [("tmp_view_aaaa1111",)]
        svc._sql.query.side_effect = RuntimeError("warehouse cold")
        # Should still drop the candidate using age-only criteria.
        svc._gc_orphan_views()
        svc._tmp_sql.execute.assert_called_once()

    def test_individual_drop_failure_continues_loop(self):
        svc = _make_service()
        svc._tmp_sql.query.return_value = [
            ("tmp_view_aaaa1111",),
            ("tmp_view_bbbb2222",),
        ]
        svc._sql.query.return_value = []
        svc._tmp_sql.execute.side_effect = [RuntimeError("locked"), None]
        # Both DROPs are attempted even when the first one blows up.
        svc._gc_orphan_views()
        assert svc._tmp_sql.execute.call_count == 2

    def test_list_query_failure_returns_silently(self):
        svc = _make_service()
        svc._tmp_sql.query.side_effect = RuntimeError("warehouse down")
        svc._gc_orphan_views()
        svc._tmp_sql.execute.assert_not_called()
        # Must not have attempted the in-use cross-check either.
        svc._sql.query.assert_not_called()


# ---------------------------------------------------------------------------
# _maybe_gc_orphan_views — gate-keeper behaviour
# ---------------------------------------------------------------------------


class TestMaybeGcOrphanViews:
    @pytest.mark.asyncio
    async def test_skips_when_not_yet_due(self):
        svc = _make_service()
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        svc._next_view_gc_at = future

        await svc._maybe_gc_orphan_views(future - timedelta(seconds=1))

        # Schedule untouched, no SQL run.
        assert svc._next_view_gc_at == future
        svc._tmp_sql.query.assert_not_called()

    @pytest.mark.asyncio
    async def test_advances_schedule_before_running(self):
        svc = _make_service()
        # Schedule a fire on 2026-05-02 01:00 UTC.
        due = datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)
        svc._next_view_gc_at = due

        # No candidates → no work to do, but the schedule MUST advance.
        svc._tmp_sql.query.return_value = []
        await svc._maybe_gc_orphan_views(due + timedelta(seconds=1))

        # Next fire is the following Saturday at 01:00.
        assert svc._next_view_gc_at == datetime(2026, 5, 9, 1, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_failure_in_gc_does_not_propagate(self):
        svc = _make_service()
        due = datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)
        svc._next_view_gc_at = due

        svc._tmp_sql.query.side_effect = RuntimeError("nope")
        # Must not raise; just log and reschedule.
        await svc._maybe_gc_orphan_views(due + timedelta(minutes=1))
        assert svc._next_view_gc_at > due
