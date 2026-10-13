"""Construction/shape smoke tests for the demo-seed DI providers.

These assert the providers exist and expose the expected shape without needing
a request, an OBO token, or a live SQL warehouse — the heavier end-to-end
wiring is covered by the route test.
"""

import inspect
from unittest.mock import MagicMock

import pytest


def test_demo_providers_exist() -> None:
    from databricks_labs_dqx_app.backend import dependencies

    assert hasattr(dependencies, "get_demo_seed_service")
    assert hasattr(dependencies, "get_demo_status_store")


def _annotation_name(annotation: object) -> str:
    # A live class exposes ``__name__``; a string annotation (e.g. under a
    # ``from __future__ import annotations`` module) does not. Handle both.
    return getattr(annotation, "__name__", str(annotation))


def test_get_demo_seed_service_returns_demo_seed_service() -> None:
    from databricks_labs_dqx_app.backend import dependencies

    sig = inspect.signature(dependencies.get_demo_seed_service)
    assert "DemoSeedService" in _annotation_name(sig.return_annotation)


def test_get_demo_status_store_returns_demo_status_store() -> None:
    from databricks_labs_dqx_app.backend import dependencies

    sig = inspect.signature(dependencies.get_demo_status_store)
    assert "DemoStatusStore" in _annotation_name(sig.return_annotation)


def test_get_demo_seed_service_declares_expected_params() -> None:
    from databricks_labs_dqx_app.backend import dependencies

    params = set(inspect.signature(dependencies.get_demo_seed_service).parameters)
    # The provider must inject the full SP-only service graph.
    expected = {
        "sp_ws",
        "sp_sql",
        "oltp",
        "registry",
        "monitored_tables",
        "apply_rules",
        "materializer",
        "rules_catalog",
        "version_service",
        "data_products",
        "score_cache",
        "job_service",
        "run_set_service",
        "app_settings",
        "status",
        "reset_service",
        "schedule_config",
    }
    assert expected <= params


@pytest.mark.asyncio
async def test_get_demo_seed_service_forwards_installation_demo_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    from databricks_labs_dqx_app.backend import dependencies
    from databricks_labs_dqx_app.backend.demo import seed_service
    from databricks_labs_dqx_app.backend.runtime import rt
    from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime

    seed_constructor = MagicMock()
    monkeypatch.setattr(seed_service, "DemoSeedService", seed_constructor)
    monkeypatch.setattr(dependencies, "resolve_execution_principals", lambda _ws: ("runner", "cleanup"))
    monkeypatch.setattr(setup_runtime, "require_job_id", lambda: 7)
    params = inspect.signature(dependencies.get_demo_seed_service).parameters

    await dependencies.get_demo_seed_service(**{name: MagicMock() for name in params})

    assert seed_constructor.call_args.kwargs["schema"] == rt.require_resources().demo_schema
