"""Thread-safe process registry for setup state and activation."""

import asyncio
import threading

from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState


class SetupRuntime:
    """Publish immutable setup reports and serialize activation work."""

    def __init__(self, initial_report: SetupReport | None = None) -> None:
        self._report = initial_report or SetupReport(state=SetupState.CHECKING, steps=())
        self._report_lock = threading.Lock()
        self.activation_lock = asyncio.Lock()
        self.job_id: int | None = None

    def report(self) -> SetupReport:
        """Return the latest immutable setup report."""
        with self._report_lock:
            return self._report

    def publish(self, report: SetupReport) -> None:
        """Atomically replace the report visible to request handlers."""
        with self._report_lock:
            self._report = report

    def require_job_id(self) -> int:
        """Return the setup-resolved task-runner job ID.

        Returns:
            The resolved positive Databricks job identifier.

        Raises:
            RuntimeError: If setup has not resolved a task-runner job yet.
        """
        if self.job_id is None:
            raise RuntimeError("DQX Studio task-runner job is not ready")
        return self.job_id


setup_runtime = SetupRuntime()
