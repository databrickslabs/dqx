"""Smoke tests for the standalone demo seeding CLI."""

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from databricks_labs_dqx_app.backend.demo import seed_service
from databricks_labs_dqx_app.backend.services import resource_tagging_service


_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "seed_demo.py"


def _load_cli_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("demo_cli", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f"Could not build spec for {_SCRIPT_PATH}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_module_imports_and_has_main() -> None:
    mod = _load_cli_module()
    assert callable(mod.main), "seed_demo.py must expose a callable main()"


def test_cli_constructs_demo_seed_service_with_workspace_resource_tagger(monkeypatch: pytest.MonkeyPatch) -> None:
    """The standalone graph supplies the workspace-owned tag reconciler."""
    module = _load_cli_module()
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(user_name="admin@example.com", display_name=None)
    tagger = object()
    tagger_factory = MagicMock(return_value=tagger)
    seed_constructor = MagicMock()
    seed_constructor.return_value.run.return_value = SimpleNamespace(
        rules=0,
        tables=0,
        products=0,
        weeks=0,
        trend_points=0,
    )

    monkeypatch.setattr(module, "WorkspaceClient", lambda profile: workspace)
    monkeypatch.setattr(resource_tagging_service, "ResourceTaggingService", tagger_factory)
    monkeypatch.setattr(seed_service, "DemoSeedService", seed_constructor)
    monkeypatch.setattr("sys.argv", ["seed_demo.py", "--warehouse-id", "warehouse-id", "--weeks", "0"])

    assert module.main() == 0
    assert tagger_factory.call_args == call(workspace)
    assert seed_constructor.call_args.kwargs["resource_tagger"] is tagger
