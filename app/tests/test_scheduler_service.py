"""Tests for ``SchedulerService`` — the weekly view-GC logic and product ticks.

We focus on the pieces that landed most recently:

- ``_next_saturday_01_utc`` cron-style boundary maths.
- ``_gc_orphan_views`` orchestration: candidate filtering, in-use
  exclusion, and bounded drop count.
- Data Products product ticks (design spec §4.3, Task 5): 5-field cron
  evaluation (``_parse_cron_field``/``_compute_next_cron_run``) and the
  per-product due-ness/bookkeeping dance (``_tick_one_product``/
  ``_tick_products``).

The async loop itself (``_loop`` / ``_tick``) is exercised via
integration tests; here we keep the surface narrow and deterministic.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingRunError,
    BindingRunResult,
    BindingRunService,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductRunResult,
    DataProductRunSubmission,
    DataProductService,
    NoRunnableMembersError,
)
from databricks_labs_dqx_app.backend.services.scheduler_service import (
    _CRON_WEEKDAY_NAMES,
    _FAILURE_BACKOFF,
    _GC_AGE_HOURS,
    _GC_HOUR_UTC,
    _GC_WEEKDAY_SAT,
    _SCORE_REFRESH_TTL,
    _TMP_VIEW_ID_LEN,
    _TMP_VIEW_NAME_RE,
    SchedulerService,
)
from databricks_labs_dqx_app.backend.services.scheduler_service import logger as scheduler_logger
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService


@pytest.fixture
def scheduler_caplog(caplog):
    """``caplog`` wired up for the scheduler's logger.

    ``scheduler_logger`` is built via ``get_logger("scheduler")``, which
    sets ``propagate = False`` (to avoid duplicate console output) and
    attaches its own ``StreamHandler``. That means records never reach the
    root logger, so plain ``caplog.at_level(...)`` (which only attaches to
    root by default) sees nothing. Attaching ``caplog.handler`` directly to
    ``scheduler_logger`` sidesteps ``propagate`` entirely.
    """
    previous_level = scheduler_logger.level
    scheduler_logger.addHandler(caplog.handler)
    scheduler_logger.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        scheduler_logger.removeHandler(caplog.handler)
        scheduler_logger.setLevel(previous_level)


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


# ---------------------------------------------------------------------------
# _parse_cron_field / _compute_next_cron_run — Data Products Task 5
# ---------------------------------------------------------------------------


class TestParseCronField:
    def test_wildcard_returns_full_range(self):
        assert SchedulerService._parse_cron_field("*", 0, 4) == {0, 1, 2, 3, 4}

    def test_single_value(self):
        assert SchedulerService._parse_cron_field("9", 0, 23) == {9}

    def test_comma_list(self):
        assert SchedulerService._parse_cron_field("1,3,5", 0, 23) == {1, 3, 5}

    def test_range(self):
        assert SchedulerService._parse_cron_field("1-5", 0, 23) == {1, 2, 3, 4, 5}

    def test_step_over_wildcard(self):
        assert SchedulerService._parse_cron_field("*/15", 0, 59) == {0, 15, 30, 45}

    def test_step_over_range(self):
        assert SchedulerService._parse_cron_field("0-10/5", 0, 59) == {0, 5, 10}

    def test_weekday_names_resolve_via_map(self):
        assert SchedulerService._parse_cron_field("MON-FRI", 0, 7, _CRON_WEEKDAY_NAMES) == {1, 2, 3, 4, 5}

    def test_weekday_names_case_insensitive(self):
        assert SchedulerService._parse_cron_field("mon", 0, 7, _CRON_WEEKDAY_NAMES) == {1}

    @pytest.mark.parametrize("bad", ["", "foo", "60", "-1", "5-1", "1/0"])
    def test_invalid_field_raises(self, bad):
        with pytest.raises(ValueError):
            SchedulerService._parse_cron_field(bad, 0, 59)


class TestComputeNextCronRun:
    def test_daily_after_time_rolls_to_tomorrow(self):
        # "0 9 * * *" — every day at 09:00. Just past today's 09:00 should
        # roll to tomorrow, not fire again today.
        after = datetime(2026, 5, 1, 9, 0, 1, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("0 9 * * *", after, None)
        assert result == datetime(2026, 5, 2, 9, 0, tzinfo=timezone.utc)

    def test_daily_before_time_fires_today(self):
        after = datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("0 9 * * *", after, None)
        assert result == datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc)

    def test_weekday_range_skips_weekend(self):
        # "0 6 * * MON-FRI" — 2026-05-01 is a Friday; next occurrence after
        # Friday 06:00 must skip Sat/Sun and land on Monday.
        after = datetime(2026, 5, 1, 6, 0, 1, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("0 6 * * MON-FRI", after, None)
        assert result == datetime(2026, 5, 4, 6, 0, tzinfo=timezone.utc)
        assert result.isoweekday() == 1  # Monday

    def test_dom_and_dow_both_restricted_matches_either(self):
        # POSIX rule: when BOTH day-of-month and day-of-week are
        # restricted, a day matches if EITHER matches. "0 0 1 * MON" fires
        # on the 1st of the month OR any Monday.
        after = datetime(2026, 5, 1, 0, 0, 1, tzinfo=timezone.utc)  # just past May 1st (a Friday)
        result = SchedulerService._compute_next_cron_run("0 0 1 * MON", after, None)
        assert result == datetime(2026, 5, 4, 0, 0, tzinfo=timezone.utc)  # next Monday, not June 1st

    def test_timezone_converts_local_wall_clock_to_utc(self):
        # "0 9 * * *" in America/New_York (UTC-4 during EDT in May) means
        # 13:00 UTC, not 09:00 UTC.
        after = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("0 9 * * *", after, "America/New_York")
        assert result == datetime(2026, 5, 1, 13, 0, tzinfo=timezone.utc)

    def test_unknown_timezone_falls_back_to_utc(self):
        after = datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("0 9 * * *", after, "Not/A_Real_Zone")
        assert result == datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc)

    def test_unsatisfiable_expression_raises_without_hanging(self):
        # April never has 31 days — must terminate via _CRON_MAX_STEPS
        # rather than looping forever.
        after = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError):
            SchedulerService._compute_next_cron_run("0 0 31 4 *", after, None)

    def test_wrong_field_count_raises(self):
        with pytest.raises(ValueError):
            SchedulerService._compute_next_cron_run("0 9 * *", datetime.now(timezone.utc), None)

    def test_leap_day_from_non_leap_year_rolls_to_next_leap_year(self):
        # "0 0 29 2 *" only has a real date in leap years. Starting from a
        # non-leap year must skip 2026/2027 (no Feb 29) and land on the
        # next leap year (2028) — and must terminate rather than looping
        # forever hunting for a day that doesn't exist in most years.
        after = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("0 0 29 2 *", after, None)
        assert result == datetime(2028, 2, 29, 0, 0, tzinfo=timezone.utc)

    def test_dst_spring_forward_gap_time_fires_once_without_hanging(self):
        # America/New_York DST starts 2026-03-08 at 02:00 local (clocks
        # jump to 03:00), so the wall-clock time 02:30 never occurs that
        # day. The evaluator must still terminate and produce a single,
        # deterministic UTC instant instead of hanging or raising.
        after = datetime(2026, 3, 8, 5, 30, tzinfo=timezone.utc)  # 00:30 EST, before the gap
        result = SchedulerService._compute_next_cron_run("30 2 * * *", after, "America/New_York")
        assert result == datetime(2026, 3, 8, 7, 30, tzinfo=timezone.utc)

        # Firing again from that instant must advance to the *next* day
        # (now in EDT, UTC-4) rather than re-firing the same gap gets hit
        # a second time.
        next_result = SchedulerService._compute_next_cron_run("30 2 * * *", result, "America/New_York")
        assert next_result == datetime(2026, 3, 9, 6, 30, tzinfo=timezone.utc)

    def test_dst_fall_back_ambiguous_time_fires_once(self):
        # America/New_York DST ends 2026-11-01 at 02:00 local (clocks fall
        # back to 01:00), so wall-clock 01:30 occurs twice that day (once
        # in EDT, once in EST). The evaluator must fire exactly once for
        # that day, not twice.
        after = datetime(2026, 10, 31, 20, 0, tzinfo=timezone.utc)
        result = SchedulerService._compute_next_cron_run("30 1 * * *", after, "America/New_York")
        assert result.date().isoformat() == "2026-11-01"

        next_result = SchedulerService._compute_next_cron_run("30 1 * * *", result, "America/New_York")
        # The next occurrence must be the following day, not another
        # 01:30 instance on the same ambiguous day.
        assert next_result.date().isoformat() == "2026-11-02"


# ---------------------------------------------------------------------------
# Product ticks — _tick_one_product / _tick_products (Data Products Task 5)
# ---------------------------------------------------------------------------


def _make_product_scheduler(make_scheduler, *, dp_service=None):
    dp_service = dp_service if dp_service is not None else create_autospec(DataProductService, instance=True)
    svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp", data_product_service=dp_service)
    return svc, mocks, dp_service


def _tracker_row(schedule_name: str, next_run_at: str, status: str = "success") -> tuple:
    return (schedule_name, "2026-04-30T09:00:00+00:00", next_run_at, "run_old", status)


class TestTickOneProduct:
    def test_due_product_fires_once_and_advances_bookkeeping(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("product:prod1", "2026-05-01T09:00:00+00:00")]
        dp_service.run.return_value = DataProductRunResult(
            run_set_id="rs1",
            submitted=[
                DataProductRunSubmission(
                    binding_id="b1", table_fqn="c.s.t", run_id="r1", job_run_id=1, view_fqn="c.tmp.v1", binding_version=1
                )
            ],
            skipped=[],
        )
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        dp_service.run.assert_called_once_with(
            "prod1", source="approved", user_email="scheduler", trigger="scheduled"
        )
        mocks.oltp.upsert.assert_called_once()
        kwargs = mocks.oltp.upsert.call_args.kwargs
        assert kwargs["key_cols"] == {"schedule_name": "product:prod1"}
        value_cols = kwargs["value_cols"]
        assert value_cols["status"] == "success"
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_not_due_product_is_skipped(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("product:prod1", "2026-05-02T09:00:00+00:00")]
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        dp_service.run.assert_not_called()
        mocks.oltp.upsert.assert_not_called()

    def test_first_tick_seeds_tracker_without_firing_when_not_yet_due(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []  # no existing tracker row
        now = datetime(2026, 5, 1, 8, 0, 0, tzinfo=timezone.utc)  # before today's 09:00

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        dp_service.run.assert_not_called()
        # Seeds a "pending" tracker row so the next tick knows the target time.
        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "pending"
        assert "2026-05-01T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_run_failure_records_failed_status_and_advances_next_run(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("product:prod1", "2026-05-01T09:00:00+00:00")]
        dp_service.run.side_effect = RuntimeError("job submission failed")
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        # Must not raise — a run failure is best-effort, mirroring
        # _advance_after_failure on the scope-config path.
        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "failed"
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_zero_runnable_members_records_failed_status_not_an_exception(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("product:prod1", "2026-05-01T09:00:00+00:00")]
        dp_service.run.side_effect = NoRunnableMembersError("zero runnable members")
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "failed"
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_missed_tick_catch_up_fires_once_and_advances_from_now(self, make_scheduler):
        # next_run_at is 6 days overdue (e.g. the scheduler was down). A
        # single catch-up run must fire — not one per missed day — and the
        # new next_run_at must be computed from *now*, not replayed forward
        # from the stale next_run_at.
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("product:prod1", "2026-04-25T09:00:00+00:00")]
        dp_service.run.return_value = DataProductRunResult(run_set_id="rs1", submitted=[], skipped=[])
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        dp_service.run.assert_called_once()
        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "success"
        # Advances from *now* (2026-05-01) to tomorrow, not from the stale
        # next_run_at (2026-04-25) forward one day at a time.
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr


class TestTickOneProductMalformedCronBackoff:
    """Regression coverage for the eternal-log-spam bug (LOW fix #1).

    Before the fix, a published product with a malformed ``schedule_cron``
    and no tracker row yet would raise inside ``_tick_one_product`` before
    any tracker was seeded, so the identical unguarded exception was
    re-raised (and logged as a full stack trace) on *every* tick forever,
    with ``next_run_at`` never advancing. The fix seeds a backoff tracker
    (mirroring ``_advance_product_after_failure``) so the schedule retries
    on the ``_FAILURE_BACKOFF`` cadence, warns instead of dumping a full
    traceback on repeat encounters, and never raises.
    """

    def test_first_encounter_seeds_backoff_tracker_with_exception_log(self, make_scheduler, scheduler_caplog):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []  # no tracker row exists yet
        now = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "not a cron", "schedule_tz": "UTC"}, now)

        dp_service.run.assert_not_called()
        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "pending"
        expected_backoff = (now + _FAILURE_BACKOFF).isoformat()
        assert expected_backoff in value_cols["next_run_at"].expr
        # First-ever encounter gets a full exception log (stack trace),
        # not just a bare warning — this is the "log once" half of the fix.
        assert any(r.levelname == "ERROR" and r.exc_info for r in scheduler_caplog.records)

    def test_repeat_encounter_warns_without_full_exception_spam(self, make_scheduler, scheduler_caplog):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        # A tracker row already exists (seeded by a prior failed attempt)
        # but next_run_at is still unset because the cron is still broken.
        mocks.oltp.query.return_value = [("product:prod1", None, None, None, "pending")]
        now = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "not a cron", "schedule_tz": "UTC"}, now)

        dp_service.run.assert_not_called()
        mocks.oltp.upsert.assert_called_once()
        expected_backoff = (now + _FAILURE_BACKOFF).isoformat()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert expected_backoff in value_cols["next_run_at"].expr
        # No full-traceback log spam once a tracker already exists —
        # this is the crux of the eternal-log-spam fix.
        assert not any(r.levelname == "ERROR" for r in scheduler_caplog.records)
        assert any(r.levelname == "WARNING" for r in scheduler_caplog.records)

    def test_tick_never_raises_for_malformed_cron(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []
        now = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)

        # Must not raise — this is the core regression: an unguarded raise
        # here previously propagated up through _tick_products every tick.
        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "garbage", "schedule_tz": "UTC"}, now)

        mocks.oltp.upsert.assert_called_once()


class TestTickProducts:
    def test_noop_without_data_product_service(self, make_scheduler):
        svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp")
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_products(now)  # must not raise, must not touch the OLTP executor

        mocks.oltp.query.assert_not_called()

    def test_query_predicate_filters_by_cron_and_approved_snapshot_version(self, make_scheduler):
        # Changed: the predicate no longer gates on ``status = 'approved'``.
        # A Table Space pending re-approval must keep running its existing
        # frozen (``version > 0``) schedule — see the ruling documented on
        # ``_load_scheduled_products``.
        svc, mocks, _dp = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []

        svc._tick_products(datetime.now(timezone.utc))

        sql = mocks.oltp.query.call_args.args[0]
        assert "schedule_cron IS NOT NULL" in sql
        assert "version > 0" in sql
        assert "status = 'approved'" not in sql

    def test_missing_table_is_tolerated(self, make_scheduler):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        mocks.oltp.query.side_effect = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        svc._tick_products(datetime.now(timezone.utc))  # must not raise

        dp_service.run.assert_not_called()

    def test_one_product_failure_does_not_prevent_the_next(self, make_scheduler, monkeypatch):
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        svc._load_scheduled_products = lambda: [  # type: ignore[method-assign]
            {"product_id": "bad", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"},
            {"product_id": "good", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"},
        ]

        calls: list[str] = []

        def _tick_one(product, now):
            calls.append(product["product_id"])
            if product["product_id"] == "bad":
                raise RuntimeError("boom")

        monkeypatch.setattr(svc, "_tick_one_product", _tick_one)

        svc._tick_products(datetime.now(timezone.utc))  # must not raise

        assert calls == ["bad", "good"]

    @pytest.mark.parametrize(
        "status,version,expect_fires",
        [
            ("pending_approval", 3, True),  # space rolled to review by a member's republish — vN keeps running
            ("rejected", 2, True),  # rejected-with-a-prior-approved-version keeps running vN
            ("approved", 1, True),  # baseline: approved keeps running
            ("draft", 0, False),  # never approved — nothing frozen to run
            ("pending_approval", 0, False),  # pending on the FIRST-ever approval — still nothing to run
        ],
    )
    def test_scheduling_eligibility_matrix_is_version_gated_not_status_gated(
        self, make_scheduler, status, version, expect_fires
    ):
        """Table Space analogue of the monitored-table matrix in ``TestTickMonitoredTables``.

        Emulates the production ``WHERE schedule_cron IS NOT NULL AND
        version > 0`` predicate in Python (no live DB in these unit tests)
        across every review ``status`` a space can carry, resolving each
        member per pin / latest-approved version exactly as
        ``DataProductService.run`` always has when it does fire.
        """
        svc, mocks, dp_service = _make_product_scheduler(make_scheduler)
        product = {"product_id": "p1", "status": status, "version": version, "schedule_cron": "0 9 * * *"}

        would_be_returned = product["version"] > 0
        svc._load_scheduled_products = lambda: (  # type: ignore[method-assign]
            [{"product_id": product["product_id"], "schedule_cron": product["schedule_cron"], "schedule_tz": "UTC"}]
            if would_be_returned
            else []
        )
        assert would_be_returned == expect_fires

        fired: list[str] = []
        original = svc._tick_one_product
        svc._tick_one_product = lambda p, now: fired.append(p["product_id"])  # type: ignore[method-assign]

        svc._tick_products(datetime.now(timezone.utc))

        assert (fired == ["p1"]) == expect_fires
        svc._tick_one_product = original


# ---------------------------------------------------------------------------
# Monitored-table ticks — _tick_one_table / _tick_monitored_tables (P21 item 14)
# ---------------------------------------------------------------------------


def _make_table_scheduler(make_scheduler, *, br_service=None):
    br_service = br_service if br_service is not None else create_autospec(BindingRunService, instance=True)
    svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp", binding_run_service=br_service)
    return svc, mocks, br_service


def _binding_run_result() -> BindingRunResult:
    return BindingRunResult(run_set_id="rs1", run_id="r1", job_run_id=1, view_fqn="c.tmp.v1")


class TestTickOneTable:
    def test_due_table_fires_once_and_advances_bookkeeping(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("table:b1", "2026-05-01T09:00:00+00:00")]
        br_service.run_binding.return_value = _binding_run_result()
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        br_service.run_binding.assert_called_once_with(
            "b1", source="approved", version=None, user_email="scheduler", trigger="scheduled"
        )
        mocks.oltp.upsert.assert_called_once()
        kwargs = mocks.oltp.upsert.call_args.kwargs
        assert kwargs["key_cols"] == {"schedule_name": "table:b1"}
        value_cols = kwargs["value_cols"]
        assert value_cols["status"] == "success"
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_not_due_table_is_skipped(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("table:b1", "2026-05-02T09:00:00+00:00")]
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        br_service.run_binding.assert_not_called()
        mocks.oltp.upsert.assert_not_called()

    def test_first_tick_seeds_tracker_without_firing_when_not_yet_due(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []
        now = datetime(2026, 5, 1, 8, 0, 0, tzinfo=timezone.utc)

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        br_service.run_binding.assert_not_called()
        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "pending"
        assert "2026-05-01T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_binding_run_error_records_failed_status_not_an_exception(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("table:b1", "2026-05-01T09:00:00+00:00")]
        br_service.run_binding.side_effect = BindingRunError("never approved")
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "failed"
        assert "2026-05-02T09:00:00+00:00" in value_cols["next_run_at"].expr

    def test_run_failure_records_failed_status_and_advances(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = [_tracker_row("table:b1", "2026-05-01T09:00:00+00:00")]
        br_service.run_binding.side_effect = RuntimeError("job submission failed")
        now = datetime(2026, 5, 1, 9, 0, 5, tzinfo=timezone.utc)

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, now)

        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "failed"

    def test_malformed_cron_seeds_backoff_and_never_raises(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []
        now = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "not a cron", "schedule_tz": "UTC"}, now)

        br_service.run_binding.assert_not_called()
        mocks.oltp.upsert.assert_called_once()
        value_cols = mocks.oltp.upsert.call_args.kwargs["value_cols"]
        assert value_cols["status"] == "pending"
        assert (now + _FAILURE_BACKOFF).isoformat() in value_cols["next_run_at"].expr


class TestTickMonitoredTables:
    def test_noop_without_binding_run_service(self, make_scheduler):
        svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp")
        svc._tick_monitored_tables(datetime.now(timezone.utc))
        mocks.oltp.query.assert_not_called()

    def test_query_predicate_filters_by_cron_and_approved_snapshot_version(self, make_scheduler):
        # Changed: the predicate no longer gates on ``status = 'approved'``.
        # A following table rolled to ``pending_approval`` by a followed
        # rule's republish (auto-upgrade OFF) must keep running its
        # existing frozen (``version > 0``) schedule — see the ruling
        # documented on ``_load_scheduled_tables``.
        svc, mocks, _br = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.return_value = []

        svc._tick_monitored_tables(datetime.now(timezone.utc))

        sql = mocks.oltp.query.call_args.args[0]
        assert "dq_monitored_tables" in sql
        assert "schedule_cron IS NOT NULL" in sql
        assert "version > 0" in sql
        assert "status = 'approved'" not in sql

    def test_missing_table_is_tolerated(self, make_scheduler):
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        mocks.oltp.query.side_effect = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        svc._tick_monitored_tables(datetime.now(timezone.utc))  # must not raise

        br_service.run_binding.assert_not_called()

    @pytest.mark.parametrize(
        "status,version,expect_fires",
        [
            ("pending_approval", 3, True),  # follower rolled to review by a republish — vN keeps running
            ("rejected", 2, True),  # rejected-with-a-prior-approved-version keeps running vN
            ("approved", 1, True),  # baseline: approved keeps running
            ("draft", 0, False),  # never approved — nothing frozen to run
            ("pending_approval", 0, False),  # pending on the FIRST-ever approval — still nothing to run
        ],
    )
    def test_scheduling_eligibility_matrix_is_version_gated_not_status_gated(
        self, make_scheduler, status, version, expect_fires
    ):
        """Mirrors the SQL predicate's ``version > 0`` gate for every ``status`` value.

        The real gate lives in the WHERE clause (asserted separately above);
        this test emulates the DB-side filter in Python — using the exact
        same predicate the production SQL applies — so the ruling's full
        status/version matrix is exercised without a live database. It
        proves ``_tick_monitored_tables`` fires purely off of whatever
        ``_load_scheduled_tables`` (i.e. the SQL layer) returns, with no
        additional Python-side status check that could reintroduce the
        pause the ruling rejected.
        """
        svc, mocks, br_service = _make_table_scheduler(make_scheduler)
        binding = {"binding_id": "b1", "status": status, "version": version, "schedule_cron": "0 9 * * *"}

        # Emulate ``WHERE schedule_cron IS NOT NULL AND version > 0`` — the
        # production predicate in ``_load_scheduled_tables`` — filtering the
        # fixture table before it ever reaches the tick.
        would_be_returned = binding["version"] > 0
        svc._load_scheduled_tables = lambda: (  # type: ignore[method-assign]
            [{"binding_id": binding["binding_id"], "schedule_cron": binding["schedule_cron"], "schedule_tz": "UTC"}]
            if would_be_returned
            else []
        )
        assert would_be_returned == expect_fires

        fired: list[str] = []
        monkeypatch_target = svc._tick_one_table
        svc._tick_one_table = lambda table, now: fired.append(table["binding_id"])  # type: ignore[method-assign]

        svc._tick_monitored_tables(datetime.now(timezone.utc))

        assert (fired == ["b1"]) == expect_fires
        svc._tick_one_table = monkeypatch_target


# ---------------------------------------------------------------------------
# _trigger_run — view_fqn quoting for the task runner (Data Products fix)
# ---------------------------------------------------------------------------


class TestTriggerRunViewFqnQuoting:
    """The runner does ``spark.table(view_fqn)`` for row-level scheduled runs,
    so an exotic real table name must arrive backtick-quoted; simple names and
    synthetic SQL-check keys must stay byte-identical."""

    _DEFAULT_CHECKS = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]

    def _prepare(self, make_scheduler, table_fqn, *, checks=None):
        svc, _mocks = make_scheduler(catalog="main", schema="dqx")
        svc._job_id = "123"  # ``int(self._job_id)`` runs before jobs.run_now
        svc._resolve_scope = lambda cfg: [table_fqn]  # type: ignore[method-assign]
        svc._get_approved_rule = lambda fqn: {"checks": checks or self._DEFAULT_CHECKS}  # type: ignore[method-assign]
        svc._load_custom_metrics = lambda: []  # type: ignore[method-assign]
        return svc

    def _submitted_view_fqn(self, svc):
        svc._trigger_run("nightly", {}, "run")
        assert svc._ws.jobs.run_now.call_count == 1
        return svc._ws.jobs.run_now.call_args.kwargs["job_parameters"]["view_fqn"]

    def test_simple_fqn_passed_unquoted(self, make_scheduler):
        svc = self._prepare(make_scheduler, "main.default.orders")
        assert self._submitted_view_fqn(svc) == "main.default.orders"

    def test_exotic_fqn_is_backtick_quoted(self, make_scheduler):
        # Regression: a real UC schema/table literally named "'ftr_mv_test'"
        # (quote chars included) fails spark.table() unquoted, marking every
        # scheduled run FAILED. It must arrive backtick-quoted.
        fqn = "main.'ftr_mv_test'.'ftr_gold_mv_bkp'"
        svc = self._prepare(make_scheduler, fqn)
        assert self._submitted_view_fqn(svc) == "`main`.`'ftr_mv_test'`.`'ftr_gold_mv_bkp'`"

    def test_synthetic_sql_check_key_passed_raw(self, make_scheduler):
        # Cross-table SQL checks use the table-less synthetic namespace; the
        # runner builds a temp view from the embedded query and never
        # spark.table()s the key, so it must not be quoted.
        checks = [{"check": {"function": "sql_query", "arguments": {"query": "SELECT 1"}}}]
        svc = self._prepare(make_scheduler, "__sql_check__/my_check", checks=checks)
        svc._extract_sql_query = lambda entry_checks: "SELECT 1"  # type: ignore[method-assign]
        assert self._submitted_view_fqn(svc) == "__sql_check__/my_check"


# ---------------------------------------------------------------------------
# Score-cache refresh on observed run completion
# ---------------------------------------------------------------------------


def _make_score_scheduler(make_scheduler, **kwargs):
    score_cache = create_autospec(ScoreCacheService, instance=True)
    svc, mocks = make_scheduler(
        catalog="main",
        schema="dqx",
        tmp_schema="dqx_tmp",
        distinct_sql=True,
        score_cache_service=score_cache,
        **kwargs,
    )
    return svc, mocks, score_cache


class TestScoreCacheRefreshOnCompletion:
    """The server-side completion trigger for the ``dq_score_cache`` refresh.

    The browser-side refresh-scores POST only fires when a user watches a
    run complete; the scheduler must therefore refresh the cache itself
    when a run it launched reaches its terminal ``dq_validation_runs``
    row — otherwise scheduled runs completing with no browser open leave
    the list scores stale/NULL forever.
    """

    NOW = datetime(2026, 5, 1, 9, 5, 0, tzinfo=timezone.utc)

    def test_completion_triggers_exactly_one_refresh_with_the_right_fqns(self, make_scheduler):
        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r1")
        svc._track_run_for_score_refresh("r2")
        mocks.sql.query.return_value = [("r1", "main.sales.orders"), ("r2", "main.sales.customers")]

        svc._refresh_scores_for_completed_runs(self.NOW)

        score_cache.refresh_all_for_tables.assert_called_once_with(["main.sales.customers", "main.sales.orders"])
        # Completed runs are untracked — the next tick must not refresh again.
        assert svc._pending_score_runs == {}
        svc._refresh_scores_for_completed_runs(self.NOW)
        score_cache.refresh_all_for_tables.assert_called_once()

    def test_no_refresh_while_runs_are_still_running(self, make_scheduler):
        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r1")
        mocks.sql.query.return_value = []  # only the RUNNING placeholder exists

        svc._refresh_scores_for_completed_runs(self.NOW)

        score_cache.refresh_all_for_tables.assert_not_called()
        assert "r1" in svc._pending_score_runs  # re-checked next tick

    def test_partial_completion_refreshes_only_the_finished_run(self, make_scheduler):
        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r1")
        svc._track_run_for_score_refresh("r2")
        mocks.sql.query.return_value = [("r1", "main.sales.orders")]

        svc._refresh_scores_for_completed_runs(self.NOW)

        score_cache.refresh_all_for_tables.assert_called_once_with(["main.sales.orders"])
        assert set(svc._pending_score_runs) == {"r2"}

    def test_refresh_failure_is_swallowed_and_never_retried_in_a_loop(self, make_scheduler):
        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r1")
        mocks.sql.query.return_value = [("r1", "main.sales.orders")]
        score_cache.refresh_all_for_tables.side_effect = RuntimeError("warehouse hiccup")

        # Best-effort: must not raise out of the tick step.
        svc._refresh_scores_for_completed_runs(self.NOW)

        # The run stays untracked — a persistent warehouse failure must not
        # turn into an every-tick retry loop; the browser refresh or the
        # run's next completion catches up.
        assert svc._pending_score_runs == {}

    def test_synthetic_sql_check_keys_never_trigger_a_refresh(self, make_scheduler):
        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r1")
        mocks.sql.query.return_value = [("r1", "__sql_check__/my_check")]

        svc._refresh_scores_for_completed_runs(self.NOW)

        score_cache.refresh_all_for_tables.assert_not_called()
        assert svc._pending_score_runs == {}

    def test_noop_without_a_score_cache_service(self, make_scheduler):
        svc, mocks = make_scheduler(catalog="main", schema="dqx", tmp_schema="dqx_tmp", distinct_sql=True)

        svc._track_run_for_score_refresh("r1")
        assert svc._pending_score_runs == {}

        svc._refresh_scores_for_completed_runs(self.NOW)
        mocks.sql.query.assert_not_called()

    def test_runs_whose_terminal_row_never_lands_expire_after_the_ttl(self, make_scheduler, scheduler_caplog):
        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r_old")
        svc._pending_score_runs["r_old"] = self.NOW - _SCORE_REFRESH_TTL - timedelta(minutes=1)

        svc._refresh_scores_for_completed_runs(self.NOW)

        assert svc._pending_score_runs == {}
        mocks.sql.query.assert_not_called()  # nothing left to look up
        score_cache.refresh_all_for_tables.assert_not_called()
        assert "never reached a terminal state" in scheduler_caplog.text

    # -- launch-path tracking ------------------------------------------------

    def test_table_tick_tracks_its_launched_run(self, make_scheduler):
        br_service = create_autospec(BindingRunService, instance=True)
        svc, mocks, _score_cache = _make_score_scheduler(make_scheduler, binding_run_service=br_service)
        mocks.oltp.query.return_value = [_tracker_row("table:b1", "2026-05-01T09:00:00+00:00")]
        br_service.run_binding.return_value = _binding_run_result()

        svc._tick_one_table({"binding_id": "b1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, self.NOW)

        assert set(svc._pending_score_runs) == {"r1"}

    def test_product_tick_tracks_every_submitted_member_run(self, make_scheduler):
        dp_service = create_autospec(DataProductService, instance=True)
        svc, mocks, _score_cache = _make_score_scheduler(make_scheduler, data_product_service=dp_service)
        mocks.oltp.query.return_value = [_tracker_row("product:prod1", "2026-05-01T09:00:00+00:00")]
        dp_service.run.return_value = DataProductRunResult(
            run_set_id="rs1",
            submitted=[
                DataProductRunSubmission(
                    binding_id="b1", table_fqn="c.s.t1", run_id="r1", job_run_id=1, view_fqn="c.tmp.v1", binding_version=1
                ),
                DataProductRunSubmission(
                    binding_id="b2", table_fqn="c.s.t2", run_id="r2", job_run_id=2, view_fqn="c.tmp.v2", binding_version=1
                ),
            ],
            skipped=[],
        )

        svc._tick_one_product({"product_id": "prod1", "schedule_cron": "0 9 * * *", "schedule_tz": "UTC"}, self.NOW)

        assert set(svc._pending_score_runs) == {"r1", "r2"}

    def test_scope_config_trigger_tracks_real_tables_but_not_synthetic_keys(self, make_scheduler):
        svc, _mocks, _score_cache = _make_score_scheduler(make_scheduler)
        svc._job_id = "123"
        svc._resolve_scope = lambda cfg: ["main.default.orders", "__sql_check__/my_check"]  # type: ignore[method-assign]
        svc._get_approved_rule = lambda fqn: {  # type: ignore[method-assign]
            "checks": (
                [{"check": {"function": "sql_query", "arguments": {"query": "SELECT 1"}}}]
                if fqn.startswith("__sql_check__/")
                else [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
            )
        }
        svc._load_custom_metrics = lambda: []  # type: ignore[method-assign]

        errors = svc._trigger_run("nightly", {}, "run")

        assert errors == []
        assert svc._ws.jobs.run_now.call_count == 2
        # Only the real table's run (index 0) is tracked — the synthetic
        # cross-table key has no score-cache row to refresh.
        assert set(svc._pending_score_runs) == {"run_0"}

    # -- tick integration ----------------------------------------------------

    def test_tick_runs_the_refresh_step_and_isolates_its_failure(self, make_scheduler):
        import asyncio

        svc, mocks, score_cache = _make_score_scheduler(make_scheduler)
        svc._track_run_for_score_refresh("r1")
        mocks.oltp.query.return_value = []  # no schedule configs
        mocks.sql.query.return_value = [("r1", "main.sales.orders")]

        asyncio.run(svc._tick())
        score_cache.refresh_all_for_tables.assert_called_once_with(["main.sales.orders"])

        # And a hard failure inside the step never breaks the tick.
        svc._track_run_for_score_refresh("r2")
        score_cache.refresh_all_for_tables.side_effect = RuntimeError("boom")
        asyncio.run(svc._tick())  # must not raise
