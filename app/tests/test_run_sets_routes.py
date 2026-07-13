"""Tests for the ``/run-sets`` route handlers (Data Products Task 3).

Follows ``test_monitored_tables_routes.py``'s convention: call the route
functions directly with a mocked ``RunSetService`` rather than spinning up
a FastAPI ``TestClient``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.run_sets import get_run_set, list_run_sets
from databricks_labs_dqx_app.backend.services.run_sets import RunSetDetail, RunSetMemberDetail, RunSetSummary


class TestListRunSets:
    def test_maps_summaries_to_response_models(self):
        svc = MagicMock()
        svc.list_for_product.return_value = [
            RunSetSummary(
                run_set_id="rs-1",
                product_id="prod-1",
                product_version=1,
                source="approved",
                trigger="manual",
                created_by="alice@x",
                created_at=datetime(2026, 7, 7, tzinfo=timezone.utc),
                member_count=2,
                status="running",
            )
        ]
        result = list_run_sets(run_set_svc=svc, product_id="prod-1", limit=50)
        assert len(result) == 1
        assert result[0].run_set_id == "rs-1"
        assert result[0].member_count == 2
        assert result[0].status == "running"
        svc.list_for_product.assert_called_once_with("prod-1", limit=50)

    def test_empty_list(self):
        svc = MagicMock()
        svc.list_for_product.return_value = []
        assert list_run_sets(run_set_svc=svc, product_id="prod-1", limit=50) == []

    def test_error_maps_to_500(self):
        svc = MagicMock()
        svc.list_for_product.side_effect = RuntimeError("boom")
        with pytest.raises(HTTPException) as excinfo:
            list_run_sets(run_set_svc=svc, product_id="prod-1", limit=50)
        assert excinfo.value.status_code == 500


class TestGetRunSet:
    def test_maps_detail_to_response_model(self):
        svc = MagicMock()
        svc.get.return_value = RunSetDetail(
            run_set_id="rs-1",
            product_id=None,
            product_version=None,
            source="draft",
            trigger="manual",
            created_by="alice@x",
            created_at=datetime(2026, 7, 7, tzinfo=timezone.utc),
            status="success",
            members=[
                RunSetMemberDetail(
                    run_id="run-1",
                    binding_id="b1",
                    binding_version=None,
                    table_fqn="cat.schema.tbl",
                    status="SUCCESS",
                    total_rows=100,
                    valid_rows=90,
                    invalid_rows=10,
                    error_rows=5,
                    warning_rows=5,
                )
            ],
        )
        result = get_run_set("rs-1", run_set_svc=svc)
        assert result.run_set_id == "rs-1"
        assert result.status == "success"
        assert len(result.members) == 1
        assert result.members[0].binding_version is None
        assert result.members[0].table_fqn == "cat.schema.tbl"

    def test_missing_raises_404(self):
        svc = MagicMock()
        svc.get.side_effect = LookupError("Run set not found: rs-missing")
        with pytest.raises(HTTPException) as excinfo:
            get_run_set("rs-missing", run_set_svc=svc)
        assert excinfo.value.status_code == 404

    def test_unexpected_error_maps_to_500(self):
        svc = MagicMock()
        svc.get.side_effect = RuntimeError("boom")
        with pytest.raises(HTTPException) as excinfo:
            get_run_set("rs-1", run_set_svc=svc)
        assert excinfo.value.status_code == 500
