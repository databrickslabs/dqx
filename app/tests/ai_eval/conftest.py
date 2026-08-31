"""Gates and shared fixtures for the live suggester evaluation (Tier 2).

Lives in its own directory rather than behind a pytest marker because
``app/pyproject.toml`` sets ``--strict-markers`` with no markers declared, and
because ``make app-test`` should never be one forgotten ``-m`` away from
spending tokens. Directory separation makes the exclusion structural: the
``app-test`` target ignores this path outright, and the only way in is the
dedicated ``make app-ai-eval`` target.

Every test here is skipped unless ``DQX_EVAL_LIVE=1`` is set, so a checkout of
this branch on a machine that happens to hold workspace credentials still costs
nothing.
"""

import os

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from tests.suggester_eval_support import load_corpus, load_labels, load_recordings, load_tables

# Default to the same endpoints a fresh Studio deploy would use, so the numbers
# describe the shipped configuration rather than a hand-tuned one.
DEFAULT_EMBED_ENDPOINT = AppSettingsService.EMBEDDING_ENDPOINT_NAME_DEFAULT
DEFAULT_JUDGE_ENDPOINT = AppSettingsService.AI_ENDPOINT_NAME_DEFAULT


@pytest.fixture(scope="session", autouse=True)
def require_opt_in():
    """Skip the whole directory unless the caller explicitly asked for a live run."""
    if os.environ.get("DQX_EVAL_LIVE") != "1":
        pytest.skip("live suggester eval is opt-in; set DQX_EVAL_LIVE=1 (see make app-ai-eval)")


@pytest.fixture(scope="session")
def live_client() -> WorkspaceClient:
    """A real ``WorkspaceClient`` from the ambient Databricks configuration.

    Skips rather than fails when authentication is unavailable: a missing
    profile is a "cannot run here", not a defect in the suggester.
    """
    try:
        client = WorkspaceClient()
        client.current_user.me()
    except Exception as e:
        # Broad by contract: config parsing, token exchange, DNS and SCIM can
        # each fail differently, and every one of them means the same thing here
        # — this machine cannot reach a workspace, so skip rather than red.
        # Narrowing would only re-introduce a crash on the next unlisted error.
        pytest.skip(f"no usable Databricks authentication: {type(e).__name__}: {e}")
    return client


@pytest.fixture(scope="session")
def embed_endpoint() -> str:
    return os.environ.get("DQX_EVAL_EMBEDDING_ENDPOINT", DEFAULT_EMBED_ENDPOINT)


@pytest.fixture(scope="session")
def judge_endpoint() -> str:
    return os.environ.get("DQX_EVAL_JUDGE_ENDPOINT", DEFAULT_JUDGE_ENDPOINT)


@pytest.fixture(scope="session")
def corpus():
    return load_corpus()


@pytest.fixture(scope="session")
def tables():
    return load_tables()


@pytest.fixture(scope="session")
def labels():
    return load_labels()


@pytest.fixture(scope="session")
def recordings():
    """The Tier 1 replay cache, loaded so the live run can be compared against it."""
    return load_recordings()
