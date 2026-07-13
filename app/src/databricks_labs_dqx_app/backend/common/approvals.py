"""Approvals-workflow mode — the admin knob governing the submit→approve gate.

A single, app-wide 3-state setting (issue #94) that decides what happens when
an author submits rules / registry rules / monitored tables / table spaces for
review:

* ``enabled`` (default) — today's behaviour: a submit moves the object to
  ``pending_approval`` and an approver/admin must approve it separately.
* ``auto_bypass`` — the approval gate is still ON, BUT a submit auto-approves
  within the same call when the acting user could have approved it themselves,
  i.e. :func:`should_auto_approve` with ``can_edit_and_approve=True``. Everyone
  else's submit still lands in ``pending_approval``. This spares approvers from
  rubber-stamping their own work while keeping the gate for authors.
* ``disabled`` — no approval step at all: every submit auto-approves regardless
  of who the caller is.

The auto-bypass predicate itself (``can_edit_and_approve``) is object-aware and
lives on :class:`~backend.services.permissions_service.PermissionsService`
(:meth:`~backend.services.permissions_service.PermissionsService.can_edit_and_approve`)
because it needs the object's grants; this module keeps only the pure, mode ->
decision mapping so it is trivially unit-testable and free of I/O.

When a submit auto-approves, the acting user is recorded as the approver with an
``(auto)`` marker (see :func:`mark_auto_approver`) so the audit trail stays
honest — history shows *who* triggered the approval and that it was automatic,
never a silent system approval.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ApprovalMode:
    """The three approvals-workflow modes, stored as a plain string setting."""

    ENABLED = "enabled"
    AUTO_BYPASS = "auto_bypass"
    DISABLED = "disabled"

    #: Every accepted value — used to validate an incoming/stored mode string.
    ALL: frozenset[str] = frozenset({ENABLED, AUTO_BYPASS, DISABLED})

    #: The compiled-in default when the setting has never been saved.
    DEFAULT: str = ENABLED


#: Suffix appended to the recorded approver identity on an automatic approval.
AUTO_APPROVER_SUFFIX = " (auto)"


def normalize_approvals_mode(raw: str | None) -> str:
    """Coerce a stored/incoming mode string to a valid :class:`ApprovalMode`.

    An unset (``None``/empty) or unrecognised value falls back to
    :data:`ApprovalMode.DEFAULT` (``enabled``) — the safe default keeps the
    approval gate on rather than silently disabling review because of a corrupt
    row. Matching is case/whitespace-insensitive.
    """
    if raw is None:
        return ApprovalMode.DEFAULT
    candidate = raw.strip().lower()
    if candidate in ApprovalMode.ALL:
        return candidate
    logger.warning("Unrecognised approvals mode %r; falling back to %s", raw, ApprovalMode.DEFAULT)
    return ApprovalMode.DEFAULT


def should_auto_approve(mode: str, *, can_edit_and_approve: bool) -> bool:
    """Decide whether a submit should auto-approve within the same call.

    Pure decision function — the caller resolves ``can_edit_and_approve`` (the
    object-aware auto-bypass predicate) and passes it in.

    Args:
        mode: The effective :class:`ApprovalMode` value.
        can_edit_and_approve: Whether the acting user could approve this object
            themselves (admin, or holds ``approve_rules`` AND edit rights). Only
            consulted in ``auto_bypass`` mode.

    Returns:
        ``True`` when the submit should transition straight to ``approved``:
        always in ``disabled`` mode, and in ``auto_bypass`` mode only when the
        caller can edit+approve. Always ``False`` in ``enabled`` mode.
    """
    normalized = normalize_approvals_mode(mode)
    if normalized == ApprovalMode.DISABLED:
        return True
    if normalized == ApprovalMode.AUTO_BYPASS:
        return can_edit_and_approve
    return False


def mark_auto_approver(user_email: str) -> str:
    """Return the approver identity to record for an automatic approval.

    Appends :data:`AUTO_APPROVER_SUFFIX` so the persisted approver/audit value
    is e.g. ``alice@example.com (auto)`` — honest about *who* triggered the
    approval and that it happened automatically (no separate approver acted).
    Idempotent: never double-marks an already-marked identity.
    """
    if user_email.endswith(AUTO_APPROVER_SUFFIX):
        return user_email
    return f"{user_email}{AUTO_APPROVER_SUFFIX}"
