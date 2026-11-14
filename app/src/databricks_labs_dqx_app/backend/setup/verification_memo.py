"""Remember administrator-verified grant requirements for unattended setup checks.

The app service principal usually cannot inspect other principals' catalog grants,
so unattended startup reconciles would otherwise block every user after a restart.
This memo records only a fingerprint of a requirement set that a check fully
verified, together with the verification time; it never stores tokens or raw
principal lists.
"""

import hashlib
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.setup.configuration import SetupSettings

_KEY_PREFIX = "setup_verified_"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VerifiedRequirement:
    """One privilege a principal must hold on a securable.

    Args:
        principal: Principal that must hold the privilege.
        kind: Securable type, for example CATALOG, SCHEMA or VOLUME.
        securable: Dotted securable name.
        privilege: Required privilege name, for example USE_CATALOG.
    """

    principal: str
    kind: str
    securable: str
    privilege: str


def requirement_fingerprint(scope: str, catalog: str, requirements: Iterable[VerifiedRequirement]) -> str:
    """Return a stable SHA-256 fingerprint of a requirement set.

    Principals are compared case-insensitively and the requirement order is ignored.

    Args:
        scope: Check scope the requirements belong to, for example *unity_catalog*.
        catalog: Catalog that hosts the Studio storage.
        requirements: Privileges the check requires.

    Returns:
        The hexadecimal SHA-256 digest of the canonical requirement set.
    """
    canonical = {
        "scope": scope,
        "catalog": catalog,
        "requirements": sorted(
            {
                (requirement.principal.casefold(), requirement.kind, requirement.securable, requirement.privilege)
                for requirement in requirements
            }
        ),
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class VerificationMemo:
    """Persist the last fully verified requirement fingerprint per check scope.

    Store failures never raise: an unreadable memo counts as unverified, so checks
    fail closed, and a failed write only means the next unattended run re-checks.

    Args:
        settings: Application settings store used for persistence.
        clock: Returns the current time; defaults to UTC now.
    """

    def __init__(self, settings: SetupSettings, *, clock: Callable[[], datetime] | None = None) -> None:
        self._settings = settings
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def is_verified(self, scope: str, fingerprint: str) -> bool:
        """Return whether *fingerprint* matches the last full verification of *scope*.

        Args:
            scope: Check scope to look up.
            fingerprint: Fingerprint of the current requirement set.
        """
        try:
            stored = self._settings.get_setting(_KEY_PREFIX + scope)
        except Exception:
            logger.warning("Could not read the setup verification memo; re-verification is required.")
            return False
        if not stored:
            return False
        try:
            record = json.loads(stored)
        except ValueError:
            return False
        return isinstance(record, dict) and record.get("fingerprint") == fingerprint

    def record(self, scope: str, fingerprint: str) -> None:
        """Record that the requirement set with *fingerprint* was fully verified now.

        Args:
            scope: Check scope that passed.
            fingerprint: Fingerprint of the verified requirement set.
        """
        value = json.dumps({"fingerprint": fingerprint, "verified_at": self._clock().isoformat()})
        try:
            self._settings.save_setting(_KEY_PREFIX + scope, value)
        except Exception:
            logger.warning("Could not save the setup verification memo; the next restart will re-verify.")
