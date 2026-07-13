"""Tests for the ``draft_run_sample_limit`` admin setting.

Draft monitored-table runs are capped by this setting (default 1000,
0 = unlimited); approved/published runs never sample at all — that force
is pinned in ``tests/test_binding_runs.py``. Layers exercised here:

* ``AppSettingsService.get_draft_run_sample_limit`` / setter — storage
  round-trip, unset/garbage/negative fall-back.
* ``routes.v1.config`` GET/PUT endpoints + ``DraftRunSampleLimitIn``
  bounds (ge=0, le=10_000_000).
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.routes.v1.config import (
    _DRAFT_SAMPLE_LIMIT_MAX,
    DraftRunSampleLimitIn,
    get_draft_run_sample_limit,
    save_draft_run_sample_limit,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import (
    DRAFT_RUN_SAMPLE_LIMIT_DEFAULT,
    AppSettingsService,
)


# ---------------------------------------------------------------------------
# AppSettingsService — storage layer
# ---------------------------------------------------------------------------


class TestAppSettingsDraftRunSampleLimit:
    @pytest.fixture
    def svc(self, sql_executor_mock):
        return AppSettingsService(sql_executor_mock), sql_executor_mock

    def test_get_returns_none_when_unset(self, svc):
        s, sql = svc
        sql.query.return_value = []
        assert s.get_draft_run_sample_limit() is None

    def test_get_parses_integer(self, svc):
        s, sql = svc
        sql.query.return_value = [("250",)]
        assert s.get_draft_run_sample_limit() == 250

    def test_get_zero_round_trips_as_zero_not_unset(self, svc):
        """0 = unlimited is a real value, distinct from 'not configured'."""
        s, sql = svc
        sql.query.return_value = [("0",)]
        assert s.get_draft_run_sample_limit() == 0

    def test_get_returns_none_on_garbage_value(self, svc):
        s, sql = svc
        sql.query.return_value = [("lots",)]
        assert s.get_draft_run_sample_limit() is None

    def test_get_treats_negative_as_unset(self, svc):
        s, sql = svc
        sql.query.return_value = [("-5",)]
        assert s.get_draft_run_sample_limit() is None

    def test_save_persists_integer_string_under_expected_key(self, svc):
        s, sql = svc
        assert s.save_draft_run_sample_limit(5000) == 5000
        kwargs = sql.upsert.call_args.kwargs
        keys = kwargs["key_cols"] if "key_cols" in kwargs else sql.upsert.call_args.args[1]
        values = kwargs["value_cols"] if "value_cols" in kwargs else sql.upsert.call_args.args[2]
        assert keys["setting_key"] == "draft_run_sample_limit"
        assert values["setting_value"] == "5000"

    def test_default_constant_is_1000(self):
        assert DRAFT_RUN_SAMPLE_LIMIT_DEFAULT == 1000


# ---------------------------------------------------------------------------
# Request model bounds
# ---------------------------------------------------------------------------


class TestDraftRunSampleLimitInBounds:
    def test_zero_accepted_means_unlimited(self):
        assert DraftRunSampleLimitIn(draft_run_sample_limit=0).draft_run_sample_limit == 0

    def test_negative_rejected(self):
        with pytest.raises(ValidationError):
            DraftRunSampleLimitIn(draft_run_sample_limit=-1)

    def test_ceiling_enforced(self):
        assert (
            DraftRunSampleLimitIn(draft_run_sample_limit=_DRAFT_SAMPLE_LIMIT_MAX).draft_run_sample_limit
            == _DRAFT_SAMPLE_LIMIT_MAX
        )
        with pytest.raises(ValidationError):
            DraftRunSampleLimitIn(draft_run_sample_limit=_DRAFT_SAMPLE_LIMIT_MAX + 1)


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


class TestDraftRunSampleLimitRoutes:
    @pytest.fixture
    def settings_svc(self):
        return create_autospec(AppSettingsService, instance=True)

    def test_get_falls_back_to_default_when_unset(self, settings_svc):
        settings_svc.get_draft_run_sample_limit.return_value = None
        out = get_draft_run_sample_limit(settings_svc)
        assert out.draft_run_sample_limit == DRAFT_RUN_SAMPLE_LIMIT_DEFAULT
        assert out.draft_run_sample_limit_set is False
        assert out.draft_run_sample_limit_default == DRAFT_RUN_SAMPLE_LIMIT_DEFAULT

    def test_get_returns_configured_value(self, settings_svc):
        settings_svc.get_draft_run_sample_limit.return_value = 0
        out = get_draft_run_sample_limit(settings_svc)
        assert out.draft_run_sample_limit == 0
        assert out.draft_run_sample_limit_set is True

    def test_save_persists_and_returns_effective_state(self, settings_svc):
        settings_svc.get_draft_run_sample_limit.return_value = 2500
        out = save_draft_run_sample_limit(
            DraftRunSampleLimitIn(draft_run_sample_limit=2500), settings_svc, "admin@x"
        )
        settings_svc.save_draft_run_sample_limit.assert_called_once_with(2500, user_email="admin@x")
        assert out.draft_run_sample_limit == 2500
        assert out.draft_run_sample_limit_set is True
