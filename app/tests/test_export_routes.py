"""Tests for the export routes — thin wrappers over ExportService.

Route functions are called directly with a mocked ExportService (the same
direct-call style as the other route tests), asserting the ExportOut envelope
is returned and that ExportError maps to a 404.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.export import (
    export_data_product,
    export_data_products,
    export_monitored_table,
    export_monitored_tables,
    export_registry_rule,
    export_registry_rules,
)
from databricks_labs_dqx_app.backend.services.export_service import ExportError, ExportResult


def _result(fmt="dqx"):
    return ExportResult(filename=f"x.{fmt}.yaml", content="- criticality: error\n", format=fmt)


class TestRegistryRoutes:
    def test_export_all(self):
        svc = MagicMock()
        svc.export_registry_rules.return_value = _result()
        out = export_registry_rules(svc=svc)
        assert out.filename == "x.dqx.yaml"
        assert out.format == "dqx"
        svc.export_registry_rules.assert_called_once()

    def test_export_single(self):
        svc = MagicMock()
        svc.export_registry_rule.return_value = _result()
        out = export_registry_rule("r1", svc=svc)
        assert out.content.startswith("- criticality")
        svc.export_registry_rule.assert_called_once_with("r1")

    def test_missing_rule_404(self):
        svc = MagicMock()
        svc.export_registry_rule.side_effect = ExportError("Registry rule not found: r1")
        with pytest.raises(HTTPException) as exc:
            export_registry_rule("r1", svc=svc)
        assert exc.value.status_code == 404


class TestMonitoredTableRoutes:
    def test_export_all_default_dqx(self):
        svc = MagicMock()
        svc.export_monitored_tables.return_value = _result()
        out = export_monitored_tables(svc=svc)
        assert out.format == "dqx"
        # default format forwarded
        assert svc.export_monitored_tables.call_args.args[0] == "dqx"

    def test_export_single_odcs(self):
        svc = MagicMock()
        svc.export_monitored_table.return_value = _result("odcs")
        out = export_monitored_table("b1", svc=svc, format="odcs")
        assert out.format == "odcs"
        svc.export_monitored_table.assert_called_once_with("b1", "odcs")

    def test_missing_table_404(self):
        svc = MagicMock()
        svc.export_monitored_table.side_effect = ExportError("Monitored table not found: b1")
        with pytest.raises(HTTPException) as exc:
            export_monitored_table("b1", svc=svc, format="dqx")
        assert exc.value.status_code == 404


class TestDataProductRoutes:
    def test_export_all(self):
        svc = MagicMock()
        svc.export_data_products.return_value = _result("odcs")
        out = export_data_products(svc=svc, format="odcs")
        assert out.format == "odcs"
        svc.export_data_products.assert_called_once_with("odcs")

    def test_export_single(self):
        svc = MagicMock()
        svc.export_data_product.return_value = _result()
        out = export_data_product("p1", svc=svc, format="dqx")
        assert out.filename == "x.dqx.yaml"
        svc.export_data_product.assert_called_once_with("p1", "dqx")

    def test_missing_product_404(self):
        svc = MagicMock()
        svc.export_data_product.side_effect = ExportError("Table space not found: p1")
        with pytest.raises(HTTPException) as exc:
            export_data_product("p1", svc=svc, format="dqx")
        assert exc.value.status_code == 404
