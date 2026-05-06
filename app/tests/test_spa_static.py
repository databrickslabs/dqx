"""Tests for ``SPAStaticFiles`` — the SPA index.html fallback.

The behaviour we care about:
- Real static files (``app.js``, ``logo.png``, etc.) get served as-is.
- Client-side routes (``/rules``, ``/runs/123``) fall back to ``index.html``.
- Asset-extension paths (``.js``, ``.css``, …) NEVER fall back — those
  must continue to 404 so missing assets surface clearly.
- Non-404 errors propagate untouched.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.spa_static import SPAStaticFiles


@pytest.fixture
def spa_directory():
    """Create a temp dir laid out like a real Vite build output."""
    tmp = Path(tempfile.mkdtemp(prefix="dqx-spa-"))
    (tmp / "index.html").write_text("<html><body>SPA root</body></html>")
    (tmp / "app.js").write_text("console.log('hi');\n")
    (tmp / "static").mkdir()
    (tmp / "static" / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n")  # minimal PNG header
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def client(spa_directory):
    app = FastAPI()
    app.mount("/", SPAStaticFiles(directory=str(spa_directory), html=True), name="spa")
    return TestClient(app)


# ---------------------------------------------------------------------------
# Static-asset behaviour (parent StaticFiles unchanged)
# ---------------------------------------------------------------------------


class TestStaticAssets:
    def test_index_served_at_root(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "SPA root" in resp.text

    def test_real_js_file_served(self, client):
        resp = client.get("/app.js")
        assert resp.status_code == 200
        assert "console.log" in resp.text

    def test_real_png_file_served(self, client):
        resp = client.get("/static/logo.png")
        assert resp.status_code == 200
        assert resp.content.startswith(b"\x89PNG")


# ---------------------------------------------------------------------------
# SPA fallback for client-side routes
# ---------------------------------------------------------------------------


class TestSpaFallback:
    @pytest.mark.parametrize("path", ["/rules", "/runs/abc-123", "/dashboards/foo/bar"])
    def test_unknown_route_falls_back_to_index_html(self, client, path):
        resp = client.get(path)
        assert resp.status_code == 200
        assert "SPA root" in resp.text

    @pytest.mark.parametrize(
        "path",
        [
            "/missing.js",
            "/missing.css",
            "/static/missing.png",
            "/missing.svg",
            "/missing.woff2",
            "/missing.json",
        ],
    )
    def test_missing_asset_extension_keeps_404(self, client, path):
        # The whole point of the asset-ext guard: a missing build asset
        # should NOT silently get index.html — it should 404 so we notice.
        resp = client.get(path)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Asset extension list — keep the contract explicit
# ---------------------------------------------------------------------------


class TestAssetExtensions:
    def test_module_level_constant_is_a_tuple_of_dots(self):
        from databricks_labs_dqx_app.backend.spa_static import _ASSET_EXTS

        assert isinstance(_ASSET_EXTS, tuple)
        for ext in _ASSET_EXTS:
            assert ext.startswith("."), f"{ext!r} should start with a dot"
            assert ext == ext.lower(), f"{ext!r} should be lowercase"

    def test_includes_critical_extensions(self):
        from databricks_labs_dqx_app.backend.spa_static import _ASSET_EXTS

        # If any of these regress, we'd start serving index.html for a
        # missing JS bundle — that breaks production silently.
        for ext in [".js", ".mjs", ".css", ".map", ".json"]:
            assert ext in _ASSET_EXTS
