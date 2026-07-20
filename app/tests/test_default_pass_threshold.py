"""Tests for the ``default_pass_threshold`` admin setting.

Layers exercised here:

* ``AppSettingsService.get_default_pass_threshold`` / setter — tested via
  ``test_app_settings_service.py``; here we focus on the route layer.
* ``routes.v1.config`` GET/PUT endpoints + ``DefaultPassThresholdIn``
  bounds (ge=0, le=100).
* Admin-only RBAC — both operations are gated by ``require_role(ADMIN)``.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.routes.v1.config import (
    DefaultPassThresholdIn,
    get_default_pass_threshold,
    save_default_pass_threshold,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import (
    DEFAULT_PASS_THRESHOLD_DEFAULT,
    AppSettingsService,
)
from databricks_labs_dqx_app.backend.routes.v1 import config as config_routes


# ---------------------------------------------------------------------------
# Request model bounds
# ---------------------------------------------------------------------------


class TestDefaultPassThresholdInBounds:
    def test_zero_accepted(self):
        assert DefaultPassThresholdIn(default_pass_threshold=0).default_pass_threshold == 0

    def test_hundred_accepted(self):
        assert DefaultPassThresholdIn(default_pass_threshold=100).default_pass_threshold == 100

    def test_negative_rejected(self):
        with pytest.raises(ValidationError):
            DefaultPassThresholdIn(default_pass_threshold=-1)

    def test_above_100_rejected(self):
        with pytest.raises(ValidationError):
            DefaultPassThresholdIn(default_pass_threshold=150)


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


class TestDefaultPassThresholdRoutes:
    @pytest.fixture
    def settings_svc(self):
        return create_autospec(AppSettingsService, instance=True)

    def test_get_returns_default_70_when_unset(self, settings_svc):
        settings_svc.get_default_pass_threshold.return_value = 70
        out = get_default_pass_threshold(settings_svc)
        assert out.default_pass_threshold == 70
        assert out.default_pass_threshold_default == 70

    def test_get_returns_configured_value(self, settings_svc):
        settings_svc.get_default_pass_threshold.return_value = 90
        out = get_default_pass_threshold(settings_svc)
        assert out.default_pass_threshold == 90
        assert out.default_pass_threshold_default == DEFAULT_PASS_THRESHOLD_DEFAULT

    def test_save_calls_service_and_returns_effective_state(self, settings_svc):
        settings_svc.get_default_pass_threshold.return_value = 90
        out = save_default_pass_threshold(
            DefaultPassThresholdIn(default_pass_threshold=90), settings_svc, "admin@x"
        )
        settings_svc.save_default_pass_threshold.assert_called_once_with(90, user_email="admin@x")
        assert out.default_pass_threshold == 90

    def test_default_constant_is_70(self):
        assert DEFAULT_PASS_THRESHOLD_DEFAULT == 70


# ---------------------------------------------------------------------------
# RBAC — both GET and PUT must require ADMIN
# ---------------------------------------------------------------------------


def _route_required_roles(operation_id: str) -> set[UserRole]:
    """Extract the ``require_role(...)`` role set declared on a route."""
    for route in config_routes.router.routes:
        if getattr(route, "operation_id", None) != operation_id:
            continue
        for dep in route.dependencies:
            for cell in getattr(dep.dependency, "__closure__", None) or ():
                val = cell.cell_contents
                if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                    return set(val)
    raise AssertionError(f"No require_role dependency found for {operation_id}")


class TestDefaultPassThresholdRbac:
    def test_get_is_admin_only(self):
        roles = _route_required_roles("getDefaultPassThreshold")
        assert UserRole.ADMIN in roles
        assert UserRole.VIEWER not in roles

    def test_put_is_admin_only(self):
        roles = _route_required_roles("saveDefaultPassThreshold")
        assert UserRole.ADMIN in roles
        assert UserRole.VIEWER not in roles
