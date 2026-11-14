"""Tests for ``services/ai_bootstrap.py``.

``ensure_ai_ready`` backfills embeddings for published rules. It must never
raise — any failure is logged and swallowed so a transient outage can never
crash app startup or the admin "enable AI" flow. It grants nothing: access to
AI model services is checked by the setup AI step (``setup/ai_access.py``).
"""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.ai_bootstrap import AiBootstrap
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService


@pytest.fixture
def embeddings():
    svc = create_autospec(RuleEmbeddingsService, instance=True)
    svc.backfill.return_value = 0
    return svc


@pytest.fixture
def registry():
    svc = create_autospec(RegistryService, instance=True)
    svc.list_rules.return_value = []
    return svc


@pytest.fixture
def bootstrap(embeddings, registry):
    return AiBootstrap(embeddings=embeddings, registry=registry)


class TestBackfillPublishedRules:
    """Best-effort re-embed of already-published rules (incl. built-ins)."""

    async def test_backfills_published_rules(self, bootstrap, registry, embeddings):
        rule_a, rule_b = object(), object()
        registry.list_rules.return_value = [rule_a, rule_b]

        await bootstrap.ensure_ai_ready()

        registry.list_rules.assert_called_once_with(status="approved")
        embeddings.backfill.assert_called_once_with([rule_a, rule_b])

    async def test_backfill_failure_does_not_raise(self, bootstrap, registry):
        registry.list_rules.side_effect = RuntimeError("SQL warehouse unreachable")

        await bootstrap.ensure_ai_ready()  # must not raise
