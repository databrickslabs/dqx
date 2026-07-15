"""Construction/shape smoke tests for the demo-seed DI providers.

These assert the providers exist and expose the expected shape without needing
a request, an OBO token, or a live SQL warehouse — the heavier end-to-end
wiring is covered by the route test.
"""

import inspect


def test_demo_providers_exist() -> None:
    from databricks_labs_dqx_app.backend import dependencies

    assert hasattr(dependencies, "get_demo_seed_service")
    assert hasattr(dependencies, "get_demo_status_store")


def _annotation_name(annotation: object) -> str:
    # The module uses ``from __future__ import annotations`` so annotations are
    # strings; a live class exposes ``__name__``. Handle both.
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
    }
    assert expected <= params
