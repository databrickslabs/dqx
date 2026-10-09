"""Custom styling API: company name, header logos and colour theme."""

import hashlib
import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.common.branding import BrandingValidationError, decode_logo
from databricks_labs_dqx_app.backend.dependencies import get_app_settings_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BrandingCompanyNameIn,
    BrandingDarkOut,
    BrandingLogoIn,
    BrandingLogoModeIn,
    BrandingLogosOut,
    BrandingModeColorsOut,
    BrandingOut,
    BrandingThemeIn,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService, branding_logo_hashes

router = APIRouter()

SettingsDep = Annotated[AppSettingsService, Depends(get_app_settings_service)]
EmailDep = Annotated[str, Depends(get_user_email)]
_SERVED_LOGO_MIMES = frozenset({"image/png", "image/jpeg", "image/webp"})
_ADMIN = [require_role(UserRole.ADMIN)]


def _to_out(svc: AppSettingsService) -> BrandingOut:
    branding = svc.get_branding()
    light = branding["light"]
    dark = branding["dark"]
    if not isinstance(light, dict) or not isinstance(dark, dict):
        raise RuntimeError("Stored branding is malformed.")
    return BrandingOut(
        company_name=branding["company_name"] if isinstance(branding["company_name"], str) else None,
        preset=branding["preset"] if isinstance(branding["preset"], str) else None,
        logo_mode=str(branding["logo_mode"]),
        light=BrandingModeColorsOut(colors=dict(light["colors"])),
        dark=BrandingDarkOut(customised=bool(dark["customised"]), colors=dict(dark["colors"])),
        logos=BrandingLogosOut(**branding_logo_hashes(branding)),
    )


def _bad_request(e: BrandingValidationError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=BrandingOut, operation_id="getBranding")
def get_branding(request: Request, response: Response, svc: SettingsDep) -> BrandingOut | Response:
    """Return the company branding. Available to every signed-in user."""
    out = _to_out(svc)
    etag = '"' + hashlib.sha256(json.dumps(out.model_dump(), sort_keys=True).encode()).hexdigest()[:16] + '"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "no-cache"
    return out


@router.put("/company-name", response_model=BrandingOut, operation_id="saveBrandingCompanyName", dependencies=_ADMIN)
def save_company_name(body: BrandingCompanyNameIn, svc: SettingsDep, email: EmailDep) -> BrandingOut:
    """Set or clear the company name (admin only)."""
    try:
        svc.save_branding_company_name(body.company_name, user_email=email)
    except BrandingValidationError as e:
        raise _bad_request(e) from e
    logger.info(f"Saved branding company name (by={email})")
    return _to_out(svc)


@router.put("/theme", response_model=BrandingOut, operation_id="saveBrandingTheme", dependencies=_ADMIN)
def save_theme(body: BrandingThemeIn, svc: SettingsDep, email: EmailDep) -> BrandingOut:
    """Save the colour theme (admin only)."""
    try:
        svc.save_branding_theme(
            body.preset, body.light.colors, body.dark.customised, body.dark.colors, user_email=email
        )
    except BrandingValidationError as e:
        raise _bad_request(e) from e
    logger.info(f"Saved branding theme (by={email})")
    return _to_out(svc)


@router.put("/logo-mode", response_model=BrandingOut, operation_id="saveBrandingLogoMode", dependencies=_ADMIN)
def save_logo_mode(body: BrandingLogoModeIn, svc: SettingsDep, email: EmailDep) -> BrandingOut:
    """Choose one shared logo or separate light and dark logos (admin only)."""
    try:
        svc.save_branding_logo_mode(body.logo_mode, user_email=email)
    except BrandingValidationError as e:
        raise _bad_request(e) from e
    return _to_out(svc)


@router.put("/logo/{slot}", response_model=BrandingOut, operation_id="uploadBrandingLogo", dependencies=_ADMIN)
def upload_logo(slot: str, body: BrandingLogoIn, svc: SettingsDep, email: EmailDep) -> BrandingOut:
    """Upload a PNG, JPEG or WebP logo of up to 256 KB (admin only)."""
    try:
        mime, raw = decode_logo(body.content_type, body.data_base64)
        svc.save_branding_logo(slot, mime, raw, user_email=email)
    except BrandingValidationError as e:
        raise _bad_request(e) from e
    logger.info(f"Saved {slot} branding logo ({len(raw)} bytes, by={email})")
    return _to_out(svc)


@router.delete("/logo/{slot}", response_model=BrandingOut, operation_id="deleteBrandingLogo", dependencies=_ADMIN)
def delete_logo(slot: str, svc: SettingsDep, email: EmailDep) -> BrandingOut:
    """Remove a logo (admin only)."""
    try:
        svc.delete_branding_logo(slot, user_email=email)
    except BrandingValidationError as e:
        raise _bad_request(e) from e
    logger.info(f"Removed {slot} branding logo")
    return _to_out(svc)


@router.get("/logo/{slot}", operation_id="getBrandingLogo", response_class=Response)
def get_logo(slot: str, svc: SettingsDep, v: str | None = None) -> Response:
    """Return a logo's bytes.

    When *v* (the content hash) is given it must match the stored logo, and the response
    can then be cached forever. Without *v* the response must be revalidated.
    """
    try:
        logo = svc.get_branding_logo(slot)
    except BrandingValidationError as e:
        raise _bad_request(e) from e
    if logo is None or logo.mime not in _SERVED_LOGO_MIMES:
        raise HTTPException(status_code=404, detail="No logo is set.")
    if v is not None and v != logo.hash:
        raise HTTPException(status_code=404, detail="This logo version is no longer available.")
    cache_control = "public, max-age=31536000, immutable" if v is not None else "no-cache"
    return Response(
        content=logo.data,
        media_type=logo.mime,
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": cache_control},
    )


@router.delete("", response_model=BrandingOut, operation_id="resetBranding", dependencies=_ADMIN)
def reset_branding(svc: SettingsDep, email: EmailDep) -> BrandingOut:
    """Reset all styling to DQX Default (admin only)."""
    svc.reset_branding()
    logger.info(f"Reset branding to DQX Default (by={email})")
    return _to_out(svc)
