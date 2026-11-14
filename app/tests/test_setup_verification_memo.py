"""Behavior tests for the administrator verification memo used by unattended setup checks."""

import json
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.setup.verification_memo import (
    VerificationMemo,
    VerifiedRequirement,
    requirement_fingerprint,
)


class _Settings:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get_setting(self, key: str) -> str | None:
        return self.values.get(key)

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        self.values[key] = value


class _FailingSettings:
    def get_setting(self, key: str) -> str | None:
        raise RuntimeError("lakebase unavailable")

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        raise RuntimeError("lakebase unavailable")


_NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


def _requirements(*principals: str) -> list[VerifiedRequirement]:
    return [VerifiedRequirement(principal, "CATALOG", "main", "USE_CATALOG") for principal in principals]


def test_fingerprint_ignores_order_and_principal_case() -> None:
    first = requirement_fingerprint("unity_catalog", "main", _requirements("app-sp", "Data-Team"))
    second = requirement_fingerprint("unity_catalog", "main", _requirements("data-team", "APP-SP"))

    assert first == second


def test_fingerprint_changes_with_principals_catalog_and_scope() -> None:
    base = requirement_fingerprint("unity_catalog", "main", _requirements("app-sp", "data-team"))

    assert base != requirement_fingerprint("unity_catalog", "main", _requirements("app-sp", "other-team"))
    assert base != requirement_fingerprint("unity_catalog", "other", _requirements("app-sp", "data-team"))
    assert base != requirement_fingerprint("task_runner", "main", _requirements("app-sp", "data-team"))


def test_recorded_fingerprint_is_verified_and_stores_only_hash_and_timestamp() -> None:
    settings = _Settings()
    memo = VerificationMemo(settings, clock=lambda: _NOW)
    fingerprint = requirement_fingerprint("unity_catalog", "main", _requirements("app-sp", "data-team"))

    memo.record("unity_catalog", fingerprint)

    assert memo.is_verified("unity_catalog", fingerprint)
    (stored,) = settings.values.values()
    assert json.loads(stored) == {"fingerprint": fingerprint, "verified_at": _NOW.isoformat()}
    assert "data-team" not in stored
    assert "app-sp" not in stored


def test_unrecorded_or_different_fingerprint_is_not_verified() -> None:
    memo = VerificationMemo(_Settings(), clock=lambda: _NOW)
    fingerprint = requirement_fingerprint("unity_catalog", "main", _requirements("app-sp"))

    assert not memo.is_verified("unity_catalog", fingerprint)
    memo.record("unity_catalog", fingerprint)
    assert not memo.is_verified("unity_catalog", "0" * 64)
    assert not memo.is_verified("task_runner", fingerprint)


def test_corrupt_memo_is_not_verified() -> None:
    settings = _Settings()
    memo = VerificationMemo(settings, clock=lambda: _NOW)
    memo.record("unity_catalog", "abc")
    (key,) = settings.values
    settings.values[key] = "{not json"

    assert not memo.is_verified("unity_catalog", "abc")


def test_store_failures_fail_closed_without_raising() -> None:
    memo = VerificationMemo(_FailingSettings(), clock=lambda: _NOW)

    memo.record("unity_catalog", "abc")
    assert not memo.is_verified("unity_catalog", "abc")
