"""Route tests for /api/v1/config/branding."""

import base64

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import get_app_settings_service, get_user_role
from databricks_labs_dqx_app.backend.routes.v1.branding import router
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

PNG = b"\x89PNG\r\n\x1a\n" + b"\x02" * 16
SVG = b"<svg xmlns='http://www.w3.org/2000/svg'></svg>"


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


@pytest.fixture
def settings(sql_executor_mock) -> AppSettingsService:
    data: dict[str, str] = {}

    def _upsert(_table, *, key_cols, value_cols, **_kwargs):
        data[key_cols["setting_key"]] = value_cols["setting_value"]

    def _query(sql):
        for key, value in data.items():
            if f"'{key}'" in sql:
                return [(value,)]
        return []

    def _execute(sql, **_kwargs):
        for key in list(data):
            if sql.startswith("DELETE") and f"'{key}'" in sql:
                del data[key]

    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.upsert.side_effect = _upsert
    sql_executor_mock.query.side_effect = _query
    sql_executor_mock.execute.side_effect = _execute
    return AppSettingsService(sql=sql_executor_mock)


def _client(settings: AppSettingsService, role: UserRole) -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/config/branding")
    app.dependency_overrides[get_app_settings_service] = lambda: settings
    app.dependency_overrides[get_user_email] = lambda: "user@x"
    app.dependency_overrides[get_user_role] = lambda: role
    return TestClient(app)


@pytest.fixture
def admin(settings) -> TestClient:
    return _client(settings, UserRole.ADMIN)


@pytest.fixture
def viewer(settings) -> TestClient:
    return _client(settings, UserRole.VIEWER)


class TestRead:
    def test_default_branding(self, viewer):
        resp = viewer.get("/api/v1/config/branding")
        assert resp.status_code == 200
        assert resp.json() == {
            "company_name": None,
            "preset": None,
            "logo_mode": "shared",
            "light": {"colors": {}},
            "dark": {"customised": False, "colors": {}},
            "logos": {"light": None, "dark": None},
        }
        assert resp.headers["etag"]

    def test_etag_304(self, viewer):
        etag = viewer.get("/api/v1/config/branding").headers["etag"]
        resp = viewer.get("/api/v1/config/branding", headers={"If-None-Match": etag})
        assert resp.status_code == 304


class TestRoles:
    @pytest.mark.parametrize(
        "method,path,body",
        [
            ("put", "/api/v1/config/branding/company-name", {"company_name": "Acme"}),
            (
                "put",
                "/api/v1/config/branding/theme",
                {"preset": None, "light": {"colors": {}}, "dark": {"customised": False, "colors": {}}},
            ),
            ("put", "/api/v1/config/branding/logo-mode", {"logo_mode": "separate"}),
            ("put", "/api/v1/config/branding/logo/light", {"content_type": "image/png", "data_base64": ""}),
            ("delete", "/api/v1/config/branding/logo/light", None),
            ("delete", "/api/v1/config/branding", None),
        ],
    )
    def test_viewer_cannot_mutate(self, viewer, method, path, body):
        resp = viewer.request(method.upper(), path, json=body)
        assert resp.status_code == 403


class TestMutations:
    def test_company_name(self, admin):
        resp = admin.put("/api/v1/config/branding/company-name", json={"company_name": " Acme "})
        assert resp.status_code == 200 and resp.json()["company_name"] == "Acme"

    def test_company_name_too_long(self, admin):
        resp = admin.put("/api/v1/config/branding/company-name", json={"company_name": "x" * 61})
        assert resp.status_code == 400

    def test_theme_bad_colour(self, admin):
        body = {"preset": None, "light": {"colors": {"header": "red;}"}}, "dark": {"customised": False, "colors": {}}}
        assert admin.put("/api/v1/config/branding/theme", json=body).status_code == 400

    def test_theme_ok(self, admin):
        body = {
            "preset": "nord",
            "light": {"colors": {"brand": "#5e81ac"}},
            "dark": {"customised": True, "colors": {"brand": "#88c0d0"}},
        }
        out = admin.put("/api/v1/config/branding/theme", json=body).json()
        assert out["preset"] == "nord"
        assert out["light"]["colors"] == {"brand": "#5E81AC"}
        assert out["dark"] == {"customised": True, "colors": {"brand": "#88C0D0"}}

    def test_logo_upload_and_fetch(self, admin, viewer):
        out = admin.put(
            "/api/v1/config/branding/logo/light", json={"content_type": "image/png", "data_base64": _b64(PNG)}
        ).json()
        digest = out["logos"]["light"]
        assert digest
        resp = viewer.get(f"/api/v1/config/branding/logo/light?v={digest}")
        assert resp.status_code == 200
        assert resp.content == PNG
        assert resp.headers["content-type"] == "image/png"
        assert resp.headers["x-content-type-options"] == "nosniff"
        assert "immutable" in resp.headers["cache-control"]

    def test_svg_disguised_as_png_rejected(self, admin):
        resp = admin.put(
            "/api/v1/config/branding/logo/light", json={"content_type": "image/png", "data_base64": _b64(SVG)}
        )
        assert resp.status_code == 400

    def test_unknown_slot(self, admin):
        resp = admin.put(
            "/api/v1/config/branding/logo/sepia", json={"content_type": "image/png", "data_base64": _b64(PNG)}
        )
        assert resp.status_code == 400

    def test_missing_logo_404(self, viewer):
        assert viewer.get("/api/v1/config/branding/logo/dark").status_code == 404

    def test_delete_logo(self, admin):
        admin.put("/api/v1/config/branding/logo/dark", json={"content_type": "image/png", "data_base64": _b64(PNG)})
        out = admin.delete("/api/v1/config/branding/logo/dark").json()
        assert out["logos"]["dark"] is None

    def test_reset(self, admin):
        admin.put("/api/v1/config/branding/company-name", json={"company_name": "Acme"})
        out = admin.delete("/api/v1/config/branding").json()
        assert out["company_name"] is None


class TestLogoMimeAllowlist:
    def test_non_image_mime_in_db_served_as_404(self, admin, viewer, settings):
        settings.save_branding_logo("light", "text/html", b"<script>alert(1)</script>", user_email="a@x")
        assert viewer.get("/api/v1/config/branding/logo/light").status_code == 404
