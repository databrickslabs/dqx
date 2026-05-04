"""Unit tests for ``backend.routes.v1.profiler``.

The classifier is the only part of the route module that's pure Python
and worth pinning down with tests. Everything else (view creation, job
submission, status polling) goes through service singletons that are
covered by their own service-level tests, so we focus the suite on:

1. The error classifier — every supported error class on its own, plus a
   couple of "real-world" multiline SQL exception strings copied from
   logs to make sure the parser can lift the actionable bit out of the
   noise.
2. ``BatchProfileRunFailure`` / ``BatchProfileRunOut`` model wiring — a
   tiny smoke test to make sure the new ``errors`` field actually
   serialises / round-trips.
"""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.models import (
    BatchProfileRunFailure,
    BatchProfileRunOut,
    ProfileRunOut,
)
from databricks_labs_dqx_app.backend.routes.v1.profiler import _classify_table_error


# ---------------------------------------------------------------------------
# _classify_table_error — error class detection
# ---------------------------------------------------------------------------


class TestClassifyTableErrorPermissions:
    """``USE SCHEMA`` / ``USE CATALOG`` / ``SELECT`` failures from Unity Catalog."""

    def test_use_schema_missing_returns_403_with_actionable_detail(self) -> None:
        """The exact shape from the user-reported bug.

        The original SDK exception carries a wall of text including the
        verbatim CREATE VIEW SQL. The classifier should pluck out the
        "User does not have ..." piece so the UI can show a clean
        permission-denied diagnostic.
        """
        raw = (
            "SQL execution failed: [INSUFFICIENT_PERMISSIONS] Insufficient privileges: "
            "User does not have USE SCHEMA on Schema 'dipayan.core'. SQLSTATE: 42501\n"
            "SQL: CREATE OR REPLACE VIEW `dqx_studio`.`dqx_app_tmp`.`tmp_view_95b` AS SELECT * FROM `dipayan`.`core`.`bronze_billing` LIMIT 50000"
        )
        status, code, message = _classify_table_error(Exception(raw), "dipayan.core.bronze_billing")

        assert status == 403
        assert code == "INSUFFICIENT_PERMISSIONS"
        assert "dipayan.core.bronze_billing" in message
        assert "USE SCHEMA on Schema 'dipayan.core'" in message
        # The verbatim SQL noise should be stripped — clients don't need it.
        assert "CREATE OR REPLACE VIEW" not in message
        assert "SQLSTATE" not in message

    def test_select_grant_missing_returns_403(self) -> None:
        raw = (
            "[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have "
            "SELECT on Table 'main.public.orders'. SQLSTATE: 42501"
        )
        status, code, message = _classify_table_error(Exception(raw), "main.public.orders")
        assert status == 403
        assert code == "INSUFFICIENT_PERMISSIONS"
        assert "SELECT on Table 'main.public.orders'" in message

    def test_use_catalog_missing_returns_403(self) -> None:
        raw = (
            "[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have "
            "USE CATALOG on Catalog 'restricted'. SQLSTATE: 42501"
        )
        status, code, _ = _classify_table_error(Exception(raw), "restricted.x.y")
        assert status == 403
        assert code == "INSUFFICIENT_PERMISSIONS"

    def test_sqlstate_42501_alone_is_classified_as_permission(self) -> None:
        """Some surfaces drop the bracketed code but keep SQLSTATE: 42501."""
        raw = "Operation refused. SQLSTATE: 42501"
        status, code, _ = _classify_table_error(Exception(raw), "x.y.z")
        assert status == 403
        assert code == "INSUFFICIENT_PERMISSIONS"

    def test_permission_denied_phrasing_is_classified_as_permission(self) -> None:
        """``PERMISSION_DENIED`` is the gRPC variant we sometimes see."""
        raw = "PERMISSION_DENIED: caller cannot read sales.public.events"
        status, code, _ = _classify_table_error(Exception(raw), "sales.public.events")
        assert status == 403
        assert code == "INSUFFICIENT_PERMISSIONS"

    def test_falls_back_gracefully_when_marker_missing(self) -> None:
        """If 'Insufficient privileges:' marker isn't present we still
        classify as 403 (driven by INSUFFICIENT_PERMISSIONS), but the
        message just contains the raw error verbatim — no crash."""
        raw = "INSUFFICIENT_PERMISSIONS some other shape"
        status, code, message = _classify_table_error(Exception(raw), "a.b.c")
        assert status == 403
        assert code == "INSUFFICIENT_PERMISSIONS"
        assert "a.b.c" in message


class TestClassifyTableErrorNotFound:
    def test_table_or_view_not_found_returns_404(self) -> None:
        raw = "[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: main.x.gone"
        status, code, message = _classify_table_error(Exception(raw), "main.x.gone")
        assert status == 404
        assert code == "TABLE_OR_VIEW_NOT_FOUND"
        assert "main.x.gone" in message


class TestClassifyTableErrorUnknown:
    def test_arbitrary_runtime_error_returns_500(self) -> None:
        raw = "Spark driver crashed: out of memory"
        status, code, message = _classify_table_error(Exception(raw), "x.y.z")
        assert status == 500
        assert code == "UNKNOWN"
        assert "x.y.z" in message
        assert "out of memory" in message

    def test_empty_exception_message_does_not_crash(self) -> None:
        """An ``Exception("")`` should still produce a sensible response."""
        status, code, message = _classify_table_error(Exception(""), "x.y.z")
        assert status == 500
        assert code == "UNKNOWN"
        assert "x.y.z" in message


# ---------------------------------------------------------------------------
# BatchProfileRunOut + BatchProfileRunFailure — partial-failure shape
# ---------------------------------------------------------------------------


class TestBatchProfileRunOutShape:
    """The contract the UI relies on for partial-failure batch responses."""

    def test_default_errors_is_empty_list(self) -> None:
        """Backwards-compat: existing clients that don't read ``errors``
        should still see a missing/empty field, never None."""
        out = BatchProfileRunOut(
            runs=[ProfileRunOut(run_id="r1", job_run_id=42, view_fqn="cat.sch.tmp_view_x")],
        )
        assert out.errors == []
        # And a JSON round-trip preserves it.
        rebuilt = BatchProfileRunOut.model_validate(out.model_dump())
        assert rebuilt.errors == []

    def test_failures_round_trip_with_error_code(self) -> None:
        out = BatchProfileRunOut(
            runs=[],
            errors=[
                BatchProfileRunFailure(
                    table_fqn="dipayan.core.bronze_billing",
                    error="You don't have permission to read dipayan.core.bronze_billing: User does not have USE SCHEMA on Schema 'dipayan.core'",
                    error_code="INSUFFICIENT_PERMISSIONS",
                ),
            ],
        )
        dumped = out.model_dump()
        assert dumped["errors"][0]["error_code"] == "INSUFFICIENT_PERMISSIONS"
        rebuilt = BatchProfileRunOut.model_validate(dumped)
        assert rebuilt.errors[0].table_fqn == "dipayan.core.bronze_billing"
        assert rebuilt.errors[0].error_code == "INSUFFICIENT_PERMISSIONS"

    def test_failure_without_error_code_is_allowed(self) -> None:
        """``error_code`` is optional — a generic 500 might not have one."""
        f = BatchProfileRunFailure(table_fqn="a.b.c", error="boom")
        assert f.error_code is None


# ---------------------------------------------------------------------------
# Smoke parametrize — every documented error code maps to a status code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw_error", "expected_status", "expected_code"),
    [
        (
            "[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have USE SCHEMA on Schema 'a.b'. SQLSTATE: 42501",
            403,
            "INSUFFICIENT_PERMISSIONS",
        ),
        ("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: main.x.gone", 404, "TABLE_OR_VIEW_NOT_FOUND"),
        ("Some random runtime explosion", 500, "UNKNOWN"),
    ],
)
def test_classify_returns_documented_status_codes(raw_error: str, expected_status: int, expected_code: str) -> None:
    """Pin the public contract: each error class returns the documented status."""
    status, code, _ = _classify_table_error(Exception(raw_error), "x.y.z")
    assert status == expected_status
    assert code == expected_code
