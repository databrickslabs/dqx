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

import pytest

from databricks_labs_dqx_app.backend.services.scheduler_service import (
    _FAILURE_BACKOFF,
    _GC_AGE_HOURS,
    _GC_HOUR_UTC,
    _GC_WEEKDAY_SAT,
    _TMP_VIEW_ID_LEN,
    _TMP_VIEW_NAME_RE,
    SchedulerService,
)


# ---------------------------------------------------------------------------
# Fixture
#
# The shared :func:`make_scheduler` factory (in ``conftest.py``) routes
# through the real ``SchedulerService(...)`` constructor and injects mocks
# via the public ``oltp_sql=`` parameter. We use ``distinct_sql=True`` and
# ``distinct_tmp_sql=True`` because the view-GC tests need to inspect
# ``_sql`` (in-use cross-check) and ``_tmp_sql`` (DROP VIEW execution) as
# independently addressable mocks. The previous ``object.__new__`` +
# private-attribute pattern coupled the tests to internal field names; the
# helper makes the rename surface (``oltp_sql=`` constructor parameter)
# the test contract instead.
# ---------------------------------------------------------------------------


@pytest.fixture
def gc_scheduler(make_scheduler):
    """Construct a scheduler with distinct ``_sql`` and ``_tmp_sql`` mocks.

    Returns ``(svc, mocks)``. ``mocks.sql`` is the analytical Delta
    executor (for the in-use cross-check); ``mocks.tmp`` is the tmp-
    schema executor (for SHOW VIEWS / DROP VIEW). ``_next_view_gc_at``
    starts at year 2099 (the make_scheduler factory schedules a real
    next-Saturday value; tests that exercise the GC schedule
    explicitly override this).
    """
    svc, mocks = make_scheduler(
        catalog="main",
        schema="dqx",
        tmp_schema="dqx_tmp",
        distinct_sql=True,
        distinct_tmp_sql=True,
    )
    # Park the next-fire well into the future so tests opt into firing
    # by overriding ``_next_view_gc_at`` explicitly. (This one field
    # has no constructor seam because in production it's computed from
    # the current wall clock — the override is an inherent test seam.)
    svc._next_view_gc_at = datetime(2099, 1, 1, tzinfo=timezone.utc)
    return svc, mocks


# ---------------------------------------------------------------------------
# _next_saturday_01_utc — cron-style boundary maths
# ---------------------------------------------------------------------------


class TestNextSaturday01Utc:
    def test_constants_are_correct(self):
        # Sanity-check the module-level constants drive the right schedule.
        assert _GC_WEEKDAY_SAT == 5  # Python's weekday: Mon=0..Sun=6
        assert _GC_HOUR_UTC == 1
        # 48h gives long-running validation jobs plenty of headroom; if
        # this is ever bumped down the per-run cleanup needs to be
        # audited first.
        assert _GC_AGE_HOURS == 48

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
# _compute_next_run — monthly day-of-month clamping
# ---------------------------------------------------------------------------


class TestComputeNextRunMonthly:
    @staticmethod
    def _cfg(dom: int, hour: int = 0, minute: int = 0) -> dict:
        return {"frequency": "monthly", "day_of_month": dom, "hour": hour, "minute": minute}

    def test_day_31_in_30_day_month_fires_on_last_day_not_28(self):
        # April has 30 days. Day 31 must clamp to the 30th, not silently 28.
        after = datetime(2026, 4, 10, tzinfo=timezone.utc)
        nxt = SchedulerService._compute_next_run(self._cfg(31), after)
        assert nxt == datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)

    def test_day_31_in_february_non_leap_clamps_to_28(self):
        after = datetime(2026, 2, 5, tzinfo=timezone.utc)
        nxt = SchedulerService._compute_next_run(self._cfg(31), after)
        assert nxt == datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc)

    def test_day_29_in_february_leap_year_keeps_29(self):
        # 2028 is a leap year — the 29th is a real date and must be honoured.
        after = datetime(2028, 2, 5, tzinfo=timezone.utc)
        nxt = SchedulerService._compute_next_run(self._cfg(29), after)
        assert nxt == datetime(2028, 2, 29, 0, 0, tzinfo=timezone.utc)

    def test_normal_day_is_preserved(self):
        after = datetime(2026, 6, 1, tzinfo=timezone.utc)
        nxt = SchedulerService._compute_next_run(self._cfg(15, hour=9), after)
        assert nxt == datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)

    def test_rollover_into_short_month_reclamps_without_error(self):
        # after is past this month's occurrence, forcing a roll into April
        # (30 days). Day 31 must re-clamp to the 30th rather than raising
        # ValueError from replace(day=31) on a 30-day month.
        after = datetime(2026, 3, 31, 23, 0, tzinfo=timezone.utc)
        nxt = SchedulerService._compute_next_run(self._cfg(31), after)
        assert nxt == datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)

    def test_december_rollover_into_january(self):
        after = datetime(2026, 12, 31, 23, 0, tzinfo=timezone.utc)
        nxt = SchedulerService._compute_next_run(self._cfg(31), after)
        assert nxt == datetime(2027, 1, 31, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _advance_after_failure — retry-loop prevention
# ---------------------------------------------------------------------------


class TestAdvanceAfterFailure:
    def test_advances_to_next_occurrence_with_failed_status(self, make_scheduler):
        svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp")
        now = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)

        svc._advance_after_failure("daily_check", {"frequency": "daily", "hour": 9, "minute": 0}, now, "run123")

        mocks.oltp.upsert.assert_called_once()
        kwargs = mocks.oltp.upsert.call_args.kwargs
        value_cols = kwargs["value_cols"]
        assert value_cols["status"] == "failed"
        assert value_cols["last_run_id"] == "run123"
        # next_run_at is pushed to the next daily occurrence (tomorrow 09:00),
        # strictly in the future relative to ``now`` so the schedule stops
        # re-firing every tick.
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_falls_back_to_backoff_when_compute_fails(self, make_scheduler, monkeypatch):
        svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp")
        now = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)

        def _boom(*_a, **_k):
            raise ValueError("bad cron")

        monkeypatch.setattr(SchedulerService, "_compute_next_run", staticmethod(_boom))

        svc._advance_after_failure("broken", {"frequency": "weekly"}, now, "run456")

        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "failed"
        expected = (now + _FAILURE_BACKOFF).isoformat()
        assert expected in value_cols["next_run_at"].expr

    def test_swallows_persistence_failure(self, make_scheduler):
        svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp")
        mocks.oltp.upsert.side_effect = RuntimeError("db down")
        now = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)

        # Must not raise — the caller (_tick) relies on this being best-effort.
        svc._advance_after_failure("daily_check", {"frequency": "daily", "hour": 9, "minute": 0}, now, "run789")


# ---------------------------------------------------------------------------
# _gc_orphan_views — happy path + edge cases
# ---------------------------------------------------------------------------


class TestGcOrphanViews:
    def test_no_candidates_returns_quickly(self, gc_scheduler):
        svc, mocks = gc_scheduler
        mocks.tmp.query.return_value = []
        svc._gc_orphan_views()
        # No DROP VIEW issued.
        mocks.tmp.execute.assert_not_called()

    def test_drops_eligible_views_skipping_in_use(self, gc_scheduler):
        svc, mocks = gc_scheduler
        # Three candidates: tmp_view_aaaa1111, tmp_view_bbbb2222, tmp_view_cccc3333
        mocks.tmp.query.return_value = [
            ("tmp_view_aaaa1111",),
            ("tmp_view_bbbb2222",),
            ("tmp_view_cccc3333",),
        ]
        # tmp_view_bbbb2222 is currently in use by a RUNNING dryrun
        mocks.sql.query.return_value = [("main.dqx_tmp.tmp_view_bbbb2222",)]
        svc._gc_orphan_views()

        executed = [c.args[0] for c in mocks.tmp.execute.call_args_list]
        assert any("tmp_view_aaaa1111" in s for s in executed)
        assert any("tmp_view_cccc3333" in s for s in executed)
        assert not any("tmp_view_bbbb2222" in s for s in executed)

    def test_invalid_view_names_are_filtered_out(self, gc_scheduler):
        svc, mocks = gc_scheduler
        # The regex requires tmp_view_<8-32 lowercase hex>; everything else
        # is skipped to keep the GC laser-focused.
        mocks.tmp.query.return_value = [
            ("tmp_view_DEADBEEF",),  # uppercase hex → not matched
            ("tmp_view_short",),  # too short / non-hex
            ("tmp_view_aaaa1111",),  # valid
            ("real_user_view",),  # not the right prefix
            ("tmp_view_zzzzzzzzzz",),  # non-hex characters
        ]
        mocks.sql.query.return_value = []
        svc._gc_orphan_views()

        executed = [c.args[0] for c in mocks.tmp.execute.call_args_list]
        assert len(executed) == 1
        assert "tmp_view_aaaa1111" in executed[0]

    def test_in_use_query_failure_does_not_abort_cleanup(self, gc_scheduler):
        svc, mocks = gc_scheduler
        mocks.tmp.query.return_value = [("tmp_view_aaaa1111",)]
        mocks.sql.query.side_effect = RuntimeError("warehouse cold")
        # Should still drop the candidate using age-only criteria.
        svc._gc_orphan_views()
        mocks.tmp.execute.assert_called_once()

    def test_individual_drop_failure_continues_loop(self, gc_scheduler):
        svc, mocks = gc_scheduler
        mocks.tmp.query.return_value = [
            ("tmp_view_aaaa1111",),
            ("tmp_view_bbbb2222",),
        ]
        mocks.sql.query.return_value = []
        mocks.tmp.execute.side_effect = [RuntimeError("locked"), None]
        # Both DROPs are attempted even when the first one blows up.
        svc._gc_orphan_views()
        assert mocks.tmp.execute.call_count == 2

    def test_list_query_failure_returns_silently(self, gc_scheduler):
        svc, mocks = gc_scheduler
        mocks.tmp.query.side_effect = RuntimeError("warehouse down")
        svc._gc_orphan_views()
        mocks.tmp.execute.assert_not_called()
        # Must not have attempted the in-use cross-check either.
        mocks.sql.query.assert_not_called()


# ---------------------------------------------------------------------------
# _maybe_gc_orphan_views — gate-keeper behaviour
# ---------------------------------------------------------------------------


class TestMaybeGcOrphanViews:
    @pytest.mark.asyncio
    async def test_skips_when_not_yet_due(self, gc_scheduler):
        svc, mocks = gc_scheduler
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        svc._next_view_gc_at = future

        await svc._maybe_gc_orphan_views(future - timedelta(seconds=1))

        # Schedule untouched, no SQL run.
        assert svc._next_view_gc_at == future
        mocks.tmp.query.assert_not_called()

    @pytest.mark.asyncio
    async def test_advances_schedule_before_running(self, gc_scheduler):
        svc, mocks = gc_scheduler
        # Schedule a fire on 2026-05-02 01:00 UTC.
        due = datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)
        svc._next_view_gc_at = due

        # No candidates → no work to do, but the schedule MUST advance.
        mocks.tmp.query.return_value = []
        await svc._maybe_gc_orphan_views(due + timedelta(seconds=1))

        # Next fire is the following Saturday at 01:00.
        assert svc._next_view_gc_at == datetime(2026, 5, 9, 1, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_failure_in_gc_does_not_propagate(self, gc_scheduler):
        svc, mocks = gc_scheduler
        due = datetime(2026, 5, 2, 1, 0, tzinfo=timezone.utc)
        svc._next_view_gc_at = due

        mocks.tmp.query.side_effect = RuntimeError("nope")
        # Must not raise; just log and reschedule.
        await svc._maybe_gc_orphan_views(due + timedelta(minutes=1))
        assert svc._next_view_gc_at > due


# ---------------------------------------------------------------------------
# tmp_view_<id> generator <-> regex round-trip
#
# This test exists because the GC's ``_TMP_VIEW_NAME_RE`` is the only thing
# standing between us and a runaway DROP VIEW: anything that doesn't match
# the regex is silently skipped. If a future refactor of
# ``_generate_tmp_view_id`` (or its callers ``_create_view`` /
# ``_create_view_from_sql``) changes the suffix shape — e.g. to
# ``uuid4().hex`` (32 chars), uppercase hex, base32, prefix change — the
# regex would silently exclude every newly-created view from cleanup and
# orphans would accumulate forever. We catch that here by feeding the
# regex with names produced through the actual generator.
# ---------------------------------------------------------------------------


class TestTmpViewNameRegexMatchesGenerator:
    def test_id_length_constant_is_within_regex_bounds(self):
        # The regex tolerates a small drift around _TMP_VIEW_ID_LEN, but
        # the configured length itself must be inside the band.
        assert 8 <= _TMP_VIEW_ID_LEN <= 32

    def test_regex_matches_generator_output(self):
        # 1_000 samples is overkill for a 12-hex-char space (~10^14
        # combinations) but it makes a sub-millisecond test that exercises
        # every UUID-derived path. If the generator is ever changed to
        # something that yields a non-hex char or a different length, at
        # least one of these will trip the assertion.
        for _ in range(1000):
            view_id = SchedulerService._generate_tmp_view_id()
            view_name = f"tmp_view_{view_id}"
            assert _TMP_VIEW_NAME_RE.match(view_name), (
                f"View name produced by _generate_tmp_view_id() does not match "
                f"_TMP_VIEW_NAME_RE; one of them drifted: {view_name!r}"
            )

    def test_generator_emits_expected_shape(self):
        # Belt-and-suspenders sanity check on the generator itself, so a
        # later refactor that "still happens to match the regex" but
        # broke an expected invariant (e.g. accidentally returning
        # uppercase) is also caught.
        view_id = SchedulerService._generate_tmp_view_id()
        assert len(view_id) == _TMP_VIEW_ID_LEN
        assert view_id == view_id.lower()
        assert all(c in "0123456789abcdef" for c in view_id)

    def test_regex_rejects_obvious_drift_examples(self):
        # If the regex is ever loosened to match these, GC will start
        # touching things it shouldn't. Lock the negative space too.
        for bad in [
            "tmp_view_DEADBEEF12",  # uppercase hex
            "tmp_view_short",  # too short / non-hex
            "tmp_view_zzzzzzzzzzzz",  # non-hex characters
            "tmp_view_" + "a" * 33,  # one over the regex max
            "tmp_view_" + "a" * 7,  # one under the regex min
            "real_user_view_12345678",  # wrong prefix
            "_tmp_view_12345678",  # leading underscore
            "tmp_view_12345678 ",  # trailing whitespace
        ]:
            assert _TMP_VIEW_NAME_RE.match(bad) is None, f"Regex unexpectedly accepted: {bad!r}"
