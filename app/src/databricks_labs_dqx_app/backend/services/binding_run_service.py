"""Single-binding run submission (Data Products Task 3).

Resolves a monitored table's checks per design spec §4.1's matrix
(draft render / pinned frozen snapshot / latest approved snapshot), then
submits EXACTLY the same way ``routes/v1/dryrun.py:batch_run_from_catalog``
does today: create a view via :class:`ViewService` (including the
synthetic ``__sql_check__/`` SQL-view branch for cross-table rules), mint
an app-level ``run_id``, call ``JobService.submit_run`` /
``JobService.record_dryrun_started``. The runner/job-submission contract
is untouched — this service only decides WHICH checks flow into
``config["checks"]``.

Every submission mints (or joins) a :class:`~.run_sets.RunSetService`
run set — a run set of one for single-table runs — so run history can be
grouped consistently with product runs (Task 4).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import RunSetTrigger
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.services.view_service import ViewService

logger = logging.getLogger(__name__)

_SQL_CHECK_PREFIX = "__sql_check__/"
_DEFAULT_SAMPLE_SIZE = 1000

RunSource = Literal["approved", "draft"]


class BindingRunError(Exception):
    """Base class for :meth:`BindingRunService.run_binding` failures."""


class BindingNotFoundError(BindingRunError, LookupError):
    """The requested monitored-table binding does not exist."""


class MissingSnapshotError(BindingRunError, LookupError):
    """No frozen snapshot exists for the requested ``(binding_id, version)``."""


class NeverApprovedError(BindingRunError, ValueError):
    """``source='approved'`` with no pinned version, but the binding has never been approved."""


@dataclass
class BindingRunResult:
    """Outcome of :meth:`BindingRunService.run_binding`."""

    run_set_id: str
    run_id: str
    job_run_id: int
    view_fqn: str


def _extract_sql_query(checks: list[dict[str, Any]]) -> str | None:
    """Return the SQL query from the first ``sql_query`` check, or None.

    Mirrors ``routes/v1/dryrun.py:_extract_sql_query`` exactly — kept as a
    private copy rather than a shared import so this module has no
    dependency on the dryrun route module (Global Constraints: dryrun.py
    stays untouched).
    """
    for check in checks:
        fn = (check.get("check") or {}).get("function", "")
        if fn == "sql_query":
            return (check.get("check") or {}).get("arguments", {}).get("query")
    return None


class BindingRunService:
    """Resolves a monitored table's checks and submits a run via the existing job path."""

    def __init__(
        self,
        monitored_tables: MonitoredTableService,
        version_service: MonitoredTableVersionService,
        materializer: Materializer,
        view_service: ViewService,
        job_service: JobService,
        run_set_service: RunSetService,
        settings_service: AppSettingsService,
        runs_table: str,
    ) -> None:
        self._monitored_tables = monitored_tables
        self._version_service = version_service
        self._materializer = materializer
        self._view_service = view_service
        self._job_service = job_service
        self._run_set_service = run_set_service
        self._settings_service = settings_service
        self._runs_table = runs_table

    def run_binding(
        self,
        binding_id: str,
        source: RunSource,
        version: int | None,
        user_email: str,
        trigger: RunSetTrigger = "manual",
        run_set_id: str | None = None,
        sample_size: int = _DEFAULT_SAMPLE_SIZE,
    ) -> BindingRunResult:
        """Resolve checks for *binding_id* and submit a run.

        Resolution (design spec §4.1):
        - ``source == "draft"``: render the binding's current persisted
          applied-rules state (no writes); the run-set member records
          ``binding_version=None``.
        - ``source == "approved"`` and *version* is given: that frozen
          snapshot.
        - ``source == "approved"`` and *version* is None: the latest
          approved snapshot (``binding.version``); raises
          :class:`NeverApprovedError` if the binding has never been
          approved (``version == 0``).

        Mints a new run set when *run_set_id* is None (a run set of one);
        otherwise joins the caller-supplied run set (product fan-out).

        *sample_size* bounds the number of rows sampled for the run
        (default 1000); callers should enforce the same upper bound as
        the dryrun batch route (``BatchRunFromCatalogIn.sample_size``,
        <= 10,000) before calling this method.

        Raises:
            BindingNotFoundError: *binding_id* does not exist.
            MissingSnapshotError: *version* was pinned but no snapshot
                exists for it.
            NeverApprovedError: ``source == "approved"``, *version* is
                None, and the binding has never been approved.
            BindingRunError: the resolved checks are empty, or a
                synthetic cross-table binding is missing its
                ``sql_query``.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise BindingNotFoundError(f"Monitored table not found: {binding_id}")
        table_fqn = detail.table.table_fqn

        checks, binding_version = self._resolve_checks(binding_id, detail.table.version, source, version)
        if not checks:
            raise BindingRunError(f"No checks resolved for binding {binding_id} (source={source})")

        run_id = uuid4().hex[:16]
        is_synthetic = table_fqn.startswith(_SQL_CHECK_PREFIX)
        sql_query: str | None = None
        if is_synthetic:
            sql_query = _extract_sql_query(checks)
            if not sql_query:
                raise BindingRunError(f"{table_fqn}: cross-table rule is missing its sql_query")
            view_fqn = self._view_service.create_view_from_sql(sql_query)
        else:
            view_fqn = self._view_service.create_view(table_fqn)

        # Write order matters here (fail-closed): a run-set member row must
        # never be persisted unless it can point at an existing
        # ``dq_validation_runs`` row. So ``record_dryrun_started`` — which
        # inserts that row — runs BEFORE the run set is minted/joined and
        # BEFORE the member is added. If it throws, no run-set state has
        # been written yet, so there is nothing to roll back beyond the
        # temp view. If the LATER run-set create/add_member step throws,
        # the validation-run row already exists standalone (not part of
        # any run set) — that is an accepted, lesser-severity gap (the
        # invariant is member => validation row, not the reverse) and
        # requires no cleanup of ``dq_validation_runs`` itself. The one
        # additional dangling state introduced by minting our own run set
        # is a run set left with zero members if ``add_member`` then
        # fails; that is cleaned up explicitly below via ``delete_empty``,
        # but only when we minted the run set ourselves (a caller-supplied
        # *run_set_id* may already have other members and must not be
        # touched).
        try:
            config: dict[str, Any] = {
                "checks": checks,
                "sample_size": sample_size,
                "source_table_fqn": table_fqn,
                "is_sql_check": sql_query is not None,
            }
            custom_metrics = self._settings_service.get_custom_metrics()
            if custom_metrics:
                config["custom_metrics"] = custom_metrics

            job_run_id = self._job_service.submit_run(
                task_type="dryrun",
                view_fqn=view_fqn,
                config=config,
                run_id=run_id,
                requesting_user=user_email,
            )

            self._job_service.record_dryrun_started(
                table=self._runs_table,
                run_id=run_id,
                requesting_user=user_email,
                source_table_fqn=table_fqn,
                view_fqn=view_fqn,
                sample_size=sample_size,
                job_run_id=job_run_id,
            )

            minted_run_set = run_set_id is None
            resolved_run_set_id = run_set_id or self._run_set_service.create(
                product_id=None,
                product_version=None,
                source=source,
                trigger=trigger,
                created_by=user_email,
            )
            try:
                self._run_set_service.add_member(resolved_run_set_id, run_id, binding_id, binding_version)
            except Exception:
                if minted_run_set:
                    try:
                        self._run_set_service.delete_empty(resolved_run_set_id)
                    except Exception as cleanup_err:
                        logger.warning(
                            "Failed to roll back empty run set %s after add_member failure for %s: %s",
                            resolved_run_set_id,
                            binding_id,
                            cleanup_err,
                        )
                raise
        except Exception:
            try:
                self._view_service.drop_view(view_fqn)
            except Exception as cleanup_err:
                logger.warning("Failed to drop temp view %s after submit failure for %s: %s", view_fqn, binding_id, cleanup_err)
            raise

        return BindingRunResult(
            run_set_id=resolved_run_set_id, run_id=run_id, job_run_id=job_run_id, view_fqn=view_fqn
        )

    def _resolve_checks(
        self,
        binding_id: str,
        current_version: int,
        source: RunSource,
        version: int | None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        if source == "draft":
            return self._materializer.render_binding_checks(binding_id), None

        pinned = version if version is not None else current_version
        if version is None and current_version == 0:
            raise NeverApprovedError(
                f"Monitored table {binding_id} has never been approved; "
                "run source='draft' or pin a version instead."
            )
        try:
            checks = self._version_service.get_checks(binding_id, pinned)
        except LookupError as exc:
            raise MissingSnapshotError(str(exc)) from exc
        return checks, pinned
