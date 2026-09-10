"""Tests for the profiler sampling admin setting.

The setting controls how much of a source table the profiler reads and is the
default for both profiling entry points (the Profiler page and a monitored
table's Profile tab). Layers exercised here:

* ``AppSettingsService.get_profiler_sample`` / setter — storage round-trip and
  the fall-backs that keep a corrupt row from widening the scan.
* ``ViewService`` sampling SQL — the shape of each kind, including the
  deliberate refusal to use ``TABLESAMPLE (n ROWS)`` (Spark implements that as
  a plain ``LIMIT``, so it would return the *first* n rows, not a random n).
* ``routes.v1.compute`` GET/PUT endpoints and their validation.
* The profiler routes' option pinning — DQX's ``DEFAULT_PROFILE_OPTIONS`` would
  otherwise apply ``sample_fraction: 0.3`` and ``limit: 1000`` and silently
  shrink every run to ~300 rows.
* Regression guard: DQ runs must NOT inherit the profiler's sampling.
"""

from unittest.mock import create_autospec

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.common.authorization import CAN_RUN_ROLES, UserRole
from databricks_labs_dqx_app.backend.models import ProfileRunIn, ProfilerSampleOverride
from databricks_labs_dqx_app.backend.routes.v1 import compute as compute_routes
from databricks_labs_dqx_app.backend.routes.v1.compute import (
    ProfilerSampleIn,
    get_profiler_sample,
    save_profiler_sample,
)
from databricks_labs_dqx_app.backend.routes.v1.profiler import (
    recorded_sample_limit,
    resolve_sample,
    sample_profile_options,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import (
    PROFILER_SAMPLE_KIND_DEFAULT,
    PROFILER_SAMPLE_RECORDS_MAX,
    PROFILER_SAMPLE_VALUE_DEFAULT,
    PROFILER_SAMPLE_VALUE_DEFAULT_BY_KIND,
    AppSettingsService,
    ProfilerSample,
)
from databricks_labs_dqx_app.backend.services.view_service import (
    build_sample_select,
    needs_row_count,
)

# ---------------------------------------------------------------------------
# AppSettingsService — storage layer
# ---------------------------------------------------------------------------


class TestAppSettingsProfilerSample:
    @pytest.fixture
    def svc(self, sql_executor_mock):
        return AppSettingsService(sql_executor_mock), sql_executor_mock

    @staticmethod
    def _stored(sql, kind, value):
        """Stub the two get_setting reads (kind, then value)."""
        sql.query.side_effect = [[(kind,)], [(value,)]]

    def test_unset_falls_back_to_compiled_default(self, svc):
        s, sql = svc
        sql.query.return_value = []
        sample = s.get_profiler_sample()
        assert sample.kind == PROFILER_SAMPLE_KIND_DEFAULT
        assert sample.value == PROFILER_SAMPLE_VALUE_DEFAULT

    def test_reads_records_round_trip(self, svc):
        s, sql = svc
        self._stored(sql, "records", "25000")
        assert s.get_profiler_sample() == ProfilerSample(kind="records", value=25000)

    def test_reads_percent_round_trip(self, svc):
        s, sql = svc
        self._stored(sql, "percent", "10")
        assert s.get_profiler_sample() == ProfilerSample(kind="percent", value=10)

    def test_full_reads_back_as_zero_value(self, svc):
        """``full`` has no meaningful value, and must not read the value key."""
        s, sql = svc
        sql.query.return_value = [("full",)]
        sample = s.get_profiler_sample()
        assert sample.kind == "full"
        assert sample.value == 0
        assert sample.is_full_table

    def test_unknown_kind_falls_back_to_default(self, svc):
        s, sql = svc
        self._stored(sql, "everything", "5")
        assert s.get_profiler_sample().kind == PROFILER_SAMPLE_KIND_DEFAULT

    def test_percent_above_100_is_clamped(self, svc):
        """A corrupt row must never widen the scan beyond the unit's range."""
        s, sql = svc
        self._stored(sql, "percent", "5000")
        assert s.get_profiler_sample().value == 100

    def test_records_above_ceiling_is_clamped(self, svc):
        s, sql = svc
        self._stored(sql, "records", str(PROFILER_SAMPLE_RECORDS_MAX * 10))
        assert s.get_profiler_sample().value == PROFILER_SAMPLE_RECORDS_MAX

    def test_garbage_records_value_falls_back_in_row_units(self, svc):
        s, sql = svc
        self._stored(sql, "records", "loads")
        assert s.get_profiler_sample().value == PROFILER_SAMPLE_VALUE_DEFAULT_BY_KIND["records"]

    def test_garbage_percent_value_falls_back_in_percent_units(self, svc):
        """A row-sized global fallback would clamp to 100% — i.e. the whole
        table, the opposite of a cap. The fallback must match the unit."""
        s, sql = svc
        self._stored(sql, "percent", "loads")
        sample = s.get_profiler_sample()
        assert sample.value == PROFILER_SAMPLE_VALUE_DEFAULT_BY_KIND["percent"]
        assert sample.value < 100

    def test_default_is_ten_percent(self, svc):
        s, sql = svc
        sql.query.return_value = []
        assert s.get_profiler_sample() == ProfilerSample(kind="percent", value=10)

    def test_zero_and_negative_values_clamp_up_to_one(self, svc):
        s, sql = svc
        self._stored(sql, "records", "-10")
        assert s.get_profiler_sample().value == 1

    def test_save_returns_saved_policy(self, svc):
        s, _sql = svc
        assert s.save_profiler_sample("percent", 25) == ProfilerSample(kind="percent", value=25)

    def test_save_full_stores_zero_value(self, svc):
        s, _sql = svc
        assert s.save_profiler_sample("full", 999) == ProfilerSample(kind="full", value=0)

    def test_save_clamps_out_of_range_percent(self, svc):
        s, _sql = svc
        assert s.save_profiler_sample("percent", 400).value == 100

    def test_save_rejects_unknown_kind(self, svc):
        s, _sql = svc
        with pytest.raises(ValueError, match="Unknown profiler sample kind"):
            s.save_profiler_sample("most-of-it", 10)


# ---------------------------------------------------------------------------
# ViewService — sampling SQL
# ---------------------------------------------------------------------------


class TestBuildSampleSelect:
    """The sampling SQL builder is pure: SQL in, SQL out, no I/O."""

    SOURCE = "`c`.`s`.`t`"

    def test_none_selects_whole_table(self):
        assert build_sample_select(self.SOURCE, None, None) == f"SELECT * FROM {self.SOURCE}"

    def test_full_selects_whole_table(self):
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="full", value=0), 1_000_000)
        assert body == f"SELECT * FROM {self.SOURCE}"
        assert "TABLESAMPLE" not in body and "LIMIT" not in body

    def test_percent_uses_tablesample_percent(self):
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="percent", value=10), None)
        assert body == f"SELECT * FROM {self.SOURCE} TABLESAMPLE (10 PERCENT)"

    def test_records_over_samples_by_percent_then_caps(self):
        """A row cap must be a RANDOM n, so it samples by percentage first.

        50k of 1M rows is 5%, over-sampled by the 1.5 margin to 8% (ceil), then
        capped with LIMIT so the result is exactly 50k rows.
        """
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="records", value=50_000), 1_000_000)
        assert body == f"SELECT * FROM {self.SOURCE} TABLESAMPLE (8 PERCENT) LIMIT 50000"

    def test_records_never_uses_tablesample_rows(self):
        """Spark implements TABLESAMPLE (n ROWS) as LIMIT — never random."""
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="records", value=10), 1_000_000)
        assert "ROWS" not in body

    def test_records_uses_plain_limit_when_table_fits(self):
        """No sampling needed when the table is already inside the cap."""
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="records", value=50_000), 100)
        assert body == f"SELECT * FROM {self.SOURCE} LIMIT 50000"

    def test_records_falls_back_to_limit_without_a_row_count(self):
        """An unavailable count must degrade to a bounded scan, not a failure."""
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="records", value=1000), None)
        assert body == f"SELECT * FROM {self.SOURCE} LIMIT 1000"

    def test_tiny_fraction_still_samples_at_least_one_percent(self):
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="records", value=1), 1_000_000_000)
        assert body == f"SELECT * FROM {self.SOURCE} TABLESAMPLE (1 PERCENT) LIMIT 1"

    def test_unknown_kind_degrades_to_whole_table(self):
        body = build_sample_select(self.SOURCE, ProfilerSample(kind="somehow", value=5), 100)
        assert body == f"SELECT * FROM {self.SOURCE}"

    def test_is_pure_and_repeatable(self):
        sample = ProfilerSample(kind="records", value=5000)
        first = build_sample_select(self.SOURCE, sample, 100_000)
        assert build_sample_select(self.SOURCE, sample, 100_000) == first


class TestNeedsRowCount:
    """Only a row cap should make the app pay for a COUNT(*)."""

    def test_records_needs_a_count(self):
        assert needs_row_count(ProfilerSample(kind="records", value=100)) is True

    @pytest.mark.parametrize(
        "sample",
        [None, ProfilerSample(kind="full", value=0), ProfilerSample(kind="percent", value=10)],
    )
    def test_other_kinds_need_no_count(self, sample):
        assert needs_row_count(sample) is False


# ---------------------------------------------------------------------------
# Profiler routes — precedence and option pinning
# ---------------------------------------------------------------------------


class TestProfilerSampleResolution:
    @pytest.fixture
    def app_settings(self):
        svc = create_autospec(AppSettingsService, instance=True)
        svc.get_profiler_sample.return_value = ProfilerSample(kind="records", value=50_000)
        return svc

    def test_request_without_kind_uses_admin_setting(self, app_settings):
        body = ProfileRunIn(table_fqn="c.s.t")
        assert resolve_sample(body, app_settings) == ProfilerSample(kind="records", value=50_000)

    def test_request_with_kind_overrides_admin_setting(self, app_settings):
        body = ProfileRunIn(table_fqn="c.s.t", sample_kind="percent", sample_value=10)
        assert resolve_sample(body, app_settings) == ProfilerSample(kind="percent", value=10)
        app_settings.get_profiler_sample.assert_not_called()

    def test_full_override_needs_no_value(self, app_settings):
        body = ProfileRunIn(table_fqn="c.s.t", sample_kind="full")
        assert resolve_sample(body, app_settings).is_full_table

    def test_request_rejects_unknown_kind(self):
        with pytest.raises(ValidationError):
            ProfileRunIn(table_fqn="c.s.t", sample_kind="loads")

    def test_request_rejects_zero_value(self):
        with pytest.raises(ValidationError):
            ProfileRunIn(table_fqn="c.s.t", sample_kind="records", sample_value=0)

    def test_request_rejects_percent_above_100(self):
        """Any percentage at or above 100 is the whole table, so an out-of-range
        request is rejected rather than clamped — clamping would turn a request
        for a cap into a silent full scan."""
        with pytest.raises(ValidationError):
            ProfileRunIn(table_fqn="c.s.t", sample_kind="percent", sample_value=5000)

    def test_request_allows_a_large_row_cap(self):
        """The same field carries rows, where a big number is legitimate."""
        assert ProfileRunIn(table_fqn="c.s.t", sample_kind="records", sample_value=500_000).sample_value == 500_000

    def test_override_percent_is_clamped_defensively(self, app_settings):
        """Defence in depth for a caller that bypasses the model (internal call)."""
        body = ProfilerSampleOverride.model_construct(sample_kind="percent", sample_value=5000)

        assert resolve_sample(body, app_settings).value == 100

    def test_override_records_is_clamped_to_the_ceiling(self, app_settings):
        body = ProfilerSampleOverride.model_construct(
            sample_kind="records", sample_value=PROFILER_SAMPLE_RECORDS_MAX * 10
        )

        assert resolve_sample(body, app_settings).value == PROFILER_SAMPLE_RECORDS_MAX

    def test_override_kind_without_value_uses_that_kinds_default(self, app_settings):
        """A kind with no value must not collapse to a 1-row/1% sample."""
        body = ProfileRunIn(table_fqn="c.s.t", sample_kind="records")

        assert resolve_sample(body, app_settings).value == PROFILER_SAMPLE_VALUE_DEFAULT_BY_KIND["records"]


class TestProfileOptionPinning:
    """DQX defaults must never silently shrink a run to ~300 rows."""

    @pytest.mark.parametrize(
        "sample",
        [
            ProfilerSample(kind="full", value=0),
            ProfilerSample(kind="records", value=50_000),
            ProfilerSample(kind="percent", value=10),
        ],
    )
    def test_always_pins_off_dqx_sampling_defaults(self, sample):
        options = sample_profile_options(sample, None)
        assert options["sample_fraction"] is None
        assert options["limit"] == 0

    def test_preserves_caller_supplied_options(self):
        options = sample_profile_options(
            ProfilerSample(kind="full", value=0),
            {"filter": "country = 'GB'", "num_sigmas": 4},
        )
        assert options["filter"] == "country = 'GB'"
        assert options["num_sigmas"] == 4

    def test_caller_cannot_reinstate_the_1000_row_default(self):
        """A stale client sending limit=1000 must not resurrect the old cap."""
        options = sample_profile_options(ProfilerSample(kind="full", value=0), {"limit": 1000, "sample_fraction": 0.3})
        assert options["limit"] == 0
        assert options["sample_fraction"] is None


class TestRecordedSampleLimit:
    def test_records_reports_its_row_cap(self):
        assert recorded_sample_limit(ProfilerSample(kind="records", value=2500)) == 2500

    @pytest.mark.parametrize(
        "sample",
        [ProfilerSample(kind="full", value=0), ProfilerSample(kind="percent", value=10)],
    )
    def test_no_exact_cap_reports_zero(self, sample):
        """0 is the table's existing 'unlimited' convention."""
        assert recorded_sample_limit(sample) == 0


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------


class TestProfilerSampleRoutes:
    @pytest.fixture
    def app_settings(self):
        return create_autospec(AppSettingsService, instance=True)

    def test_get_returns_configured_policy_with_bounds(self, app_settings):
        app_settings.get_profiler_sample.return_value = ProfilerSample(kind="percent", value=10)
        out = get_profiler_sample(app_settings)
        assert (out.sample_kind, out.sample_value) == ("percent", 10)
        assert out.records_max == PROFILER_SAMPLE_RECORDS_MAX
        assert out.default_value == PROFILER_SAMPLE_VALUE_DEFAULT

    def test_put_persists_and_echoes_saved_policy(self, app_settings):
        app_settings.save_profiler_sample.return_value = ProfilerSample(kind="records", value=5000)
        out = save_profiler_sample(
            ProfilerSampleIn(sample_kind="records", sample_value=5000), app_settings, "admin@example.com"
        )
        assert (out.sample_kind, out.sample_value) == ("records", 5000)
        app_settings.save_profiler_sample.assert_called_once_with("records", 5000, user_email="admin@example.com")

    def test_put_full_needs_no_value(self, app_settings):
        app_settings.save_profiler_sample.return_value = ProfilerSample(kind="full", value=0)
        out = save_profiler_sample(ProfilerSampleIn(sample_kind="full"), app_settings, "a@e.com")
        assert out.sample_kind == "full"

    def test_put_rejects_missing_value_for_records(self, app_settings):
        with pytest.raises(HTTPException) as exc:
            save_profiler_sample(ProfilerSampleIn(sample_kind="records"), app_settings, "a@e.com")
        assert exc.value.status_code == 400

    def test_put_rejects_percent_above_100(self, app_settings):
        with pytest.raises(HTTPException) as exc:
            save_profiler_sample(ProfilerSampleIn(sample_kind="percent", sample_value=150), app_settings, "a@e.com")
        assert exc.value.status_code == 400

    def test_put_rejects_unknown_kind_at_the_model(self):
        with pytest.raises(ValidationError):
            ProfilerSampleIn(sample_kind="most", sample_value=10)


# ---------------------------------------------------------------------------
# Role gating
# ---------------------------------------------------------------------------


def _allowed_roles(operation_id: str) -> set[UserRole]:
    """Roles the ``require_role`` gate on *operation_id* admits."""
    for route in compute_routes.router.routes:
        if getattr(route, "operation_id", None) != operation_id:
            continue
        for dep in route.dependencies:
            closure = getattr(getattr(dep, "dependency", None), "__closure__", None) or ()
            for cell in closure:
                try:
                    value = cell.cell_contents
                except ValueError:
                    continue
                if isinstance(value, tuple) and value and all(isinstance(v, UserRole) for v in value):
                    return set(value)
        raise AssertionError(f"No require_role gate found on {operation_id}")
    raise AssertionError(f"No route found for operation_id={operation_id}")


class TestProfilerSampleRoleGating:
    def test_read_is_open_to_everyone_who_can_run_a_profile(self):
        """An ADMIN-only GET would 403 for a RULE_AUTHOR, whose profiler UI would
        then silently run with the placeholder instead of the configured policy."""
        assert _allowed_roles("getProfilerSample") == set(CAN_RUN_ROLES)
        assert UserRole.RULE_AUTHOR in _allowed_roles("getProfilerSample")

    def test_write_stays_admin_only(self):
        assert _allowed_roles("saveProfilerSample") == {UserRole.ADMIN}

    def test_read_is_not_open_to_viewers(self):
        assert UserRole.VIEWER not in _allowed_roles("getProfilerSample")


# ---------------------------------------------------------------------------
# Regression guard — DQ runs must not inherit profiler sampling
# ---------------------------------------------------------------------------


class TestDqRunsAreUnsampled:
    def test_no_sample_means_no_limit_or_tablesample(self):
        """Dry runs, binding runs and scheduled runs all create their view with
        no sample, so the view must scan the whole table — their pass rates
        describe the table rather than a subset."""
        body = build_sample_select("`c`.`s`.`t`", None, 1_000_000)
        assert "LIMIT" not in body
        assert "TABLESAMPLE" not in body
