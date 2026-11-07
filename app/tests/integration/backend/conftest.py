"""Workspace fixtures for the backend integration suite, run by the acceptance harness in CI.

The parent conftests replace pytester's fixtures: ``ws`` authenticates from a CLI profile,
``sql_backend`` creates its own SQL warehouse and ``debug_env`` is an offline stand-in. This
suite restores pytester's environment-based versions (``DATABRICKS_HOST``,
``DATABRICKS_WAREHOUSE_ID``), which the acceptance action provides.
"""

import time
from collections.abc import Iterator
from typing import Any

import pytest
from databricks.sdk.config import Config
from databricks.labs.pytester.fixtures.baseline import ws
from databricks.labs.pytester.fixtures.environment import debug_env
from databricks.labs.pytester.fixtures.sql import sql_backend

__all__ = ["debug_env", "ws", "sql_backend"]


@pytest.fixture(autouse=True)
def _activate_test_runtime_resources() -> Iterator[None]:
    """Leave the runtime resources unset: this suite exercises the real app wiring, not unit stubs."""
    yield


# Transient-auth retry is observed in three flavors, all originating from the Databricks SDK's
# metadata-service credentials path under CI load. We retry at the auth resolution step so the
# wrapper covers every caller (WorkspaceClient, MLflow, streaming listeners, etc.) uniformly.
_AUTH_FLAKE_MARKERS = (
    "Metadata Service returned empty token",
    "metadata-service",
    "Read timed out",
    "Credential was not sent",
)


def _is_transient_auth_flake(exc: BaseException) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _AUTH_FLAKE_MARKERS)


@pytest.fixture(autouse=True)
def _retry_sdk_auth_flakes(monkeypatch):
    """Retry Databricks SDK auth on transient metadata-service failures.

    Test-only workaround for several observed flake flavors from the GHA OIDC metadata
    service: empty token, read timeout, and downstream 401 ("Credential was not sent").
    They surface at two distinct points in the SDK's auth path, so we wrap both:

    - *Config.init_auth* — runs during *WorkspaceClient* construction and probes the
      configured credentials provider once via *token_source.token()*. A timeout here
      raises before *authenticate* is ever called.
    - *Config.authenticate* — called per-request to refresh headers.

    Production code intentionally does not retry so real users see authentication errors
    immediately.
    """

    def _wrap(method_name: str) -> None:
        original = getattr(Config, method_name)

        def retrying(self: Any, *args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None
            for attempt in range(5):
                try:
                    return original(self, *args, **kwargs)
                except Exception as e:
                    if not _is_transient_auth_flake(e):
                        raise
                    last_exc = e
                    time.sleep(2**attempt)  # 1s, 2s, 4s, 8s, 16s (~31s max)
            assert last_exc is not None
            raise last_exc

        monkeypatch.setattr(Config, method_name, retrying)

    _wrap("init_auth")
    _wrap("authenticate")
