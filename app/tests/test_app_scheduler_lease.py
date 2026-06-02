"""Tests for ``backend.app._try_acquire_scheduler_lease``.

The function holds an fcntl advisory lock for the lifetime of the
worker so only one uvicorn worker runs the scheduler. The lock fd
MUST stay live as long as the process runs — if it gets garbage-
collected the OS releases the flock and a sibling worker can grab
the lease, resulting in two schedulers racing on the same job.

Pre-refactor the fd was pinned via ``globals()["_scheduler_lock_fd"] = fd``
— a magic-string lookup that's invisible to grep, type-checkers,
and IDE rename-refactor. The fix replaces that with a proper
module-level ``_scheduler_lock_fd: int | None = None`` global plus
``global _scheduler_lock_fd`` inside the function.

These tests pin the new convention so a regression to the
``globals()`` form would fail loudly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture
def fresh_lease_state(monkeypatch, tmp_path):
    """Per-test isolation: fresh lock path + reset global to None.

    Each test gets its own lock file under ``tmp_path`` so concurrent
    test runs don't contend on the production ``/tmp/.dqx_scheduler.lock``
    path, and the module global is reset before AND after the test so
    a successful acquisition in one test doesn't bleed into the next.
    """
    from databricks_labs_dqx_app.backend import app as app_mod

    monkeypatch.setattr(app_mod, "_SCHEDULER_LOCK_PATH", tmp_path / ".lease.lock")
    original_fd = app_mod._scheduler_lock_fd
    app_mod._scheduler_lock_fd = None
    yield app_mod
    # Best-effort cleanup: close any fd this test left behind so we don't
    # leak descriptors across the suite.
    leftover = app_mod._scheduler_lock_fd
    if isinstance(leftover, int) and leftover != original_fd:
        try:
            os.close(leftover)
        except OSError:
            pass
    app_mod._scheduler_lock_fd = original_fd


class TestSchedulerLeaseConvention:
    """The reviewer's specific anti-pattern to lock down."""

    def test_lock_fd_exists_as_module_level_global_not_dynamic_attribute(self):
        """``_scheduler_lock_fd`` MUST be a real module-level annotation.

        The pre-refactor code wrote it via ``globals()["_scheduler_lock_fd"] = fd``
        which works at runtime but is invisible to grep, type-checkers,
        and IDE rename. A regression that re-introduces the dynamic form
        would (a) not appear in ``vars(module)`` at import time and
        (b) leave the type annotation absent.
        """
        from databricks_labs_dqx_app.backend import app as app_mod

        # Present at import time — the module-level statement
        # ``_scheduler_lock_fd: int | None = None`` ran.
        assert "_scheduler_lock_fd" in vars(app_mod), (
            "_scheduler_lock_fd must be declared at module top level. "
            "If it only appears after _try_acquire_scheduler_lease() runs, "
            "someone re-introduced the globals()[...] = fd pattern."
        )

        # Has a typed declaration. ``__annotations__`` is populated by
        # the ``: int | None = None`` annotation in the module body.
        # The dynamic-globals form leaves no annotation behind.
        assert "_scheduler_lock_fd" in app_mod.__annotations__, (
            "_scheduler_lock_fd must have a module-level type annotation "
            "so basedpyright and reviewers can see the dependency."
        )

    def test_source_does_not_use_globals_dict_assignment(self):
        """Belt-and-suspenders: the literal anti-pattern must not return.

        Reads the source of ``_try_acquire_scheduler_lease`` and asserts
        the magic-string form is absent. Cheap to maintain — if a future
        refactor moves the global into a class or context-manager, this
        test still passes; it only fails on the specific regression the
        reviewer flagged.
        """
        import inspect

        from databricks_labs_dqx_app.backend import app as app_mod

        src = inspect.getsource(app_mod._try_acquire_scheduler_lease)
        assert 'globals()["_scheduler_lock_fd"]' not in src, (
            "Regression: the magic-string globals() assignment is back. "
            "Use ``global _scheduler_lock_fd; _scheduler_lock_fd = fd`` instead."
        )
        assert "globals()['_scheduler_lock_fd']" not in src


class TestSchedulerLeaseBehaviour:
    """The fd-pinning semantics that the refactor must preserve."""

    def test_acquire_returns_true_and_pins_fd_on_module_global(self, fresh_lease_state):
        """Happy path: lease is acquired, fd is non-None, lock file exists."""
        app_mod = fresh_lease_state

        acquired = app_mod._try_acquire_scheduler_lease()

        assert acquired is True
        # The fd is pinned to the module global so GC can't reap it
        # mid-process and silently drop the flock.
        assert isinstance(app_mod._scheduler_lock_fd, int)
        assert app_mod._scheduler_lock_fd >= 0
        # And the lock file exists where we monkeypatched it.
        assert Path(app_mod._SCHEDULER_LOCK_PATH).exists()

    def test_acquire_failure_does_not_clobber_existing_fd(self, fresh_lease_state, monkeypatch):
        """If ``flock`` raises after a prior successful acquisition the
        existing fd must survive — otherwise the previously-held lease
        evaporates.

        Simulates the failure path by monkeypatching ``fcntl.flock`` to
        always raise OSError on the second call.
        """
        app_mod = fresh_lease_state

        assert app_mod._try_acquire_scheduler_lease() is True
        original_fd = app_mod._scheduler_lock_fd
        assert isinstance(original_fd, int)

        import fcntl

        def boom(*args, **kwargs):
            raise OSError("would block")

        monkeypatch.setattr(fcntl, "flock", boom)
        acquired = app_mod._try_acquire_scheduler_lease()

        assert acquired is False
        # Critical: the previously-pinned fd must still be the same int.
        # A regression that did ``_scheduler_lock_fd = fd`` *before*
        # ``flock(...)`` could overwrite it with an unlocked fd; a
        # regression that used ``finally`` could close it.
        assert app_mod._scheduler_lock_fd == original_fd
