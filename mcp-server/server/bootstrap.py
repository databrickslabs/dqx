"""Startup work the app does once, before serving: publish the runner wheel.

The runner job installs its wheel from an absolute ``/Volumes`` path (see the job's
``dependencies`` in databricks.yml). Nothing uploads it at deploy time — `bundle deploy` writes
artifacts *before* it creates resources, so a volume used as ``workspace.artifact_path`` could never
be bundle-managed, which is what previously forced a pre-deploy shell script and a post-deploy
notebook job. Publishing from the app instead lets every UC object be bundle-managed with native
grants; this mirrors the DQX Studio app.

Everything here is **best effort**: a failure is logged and the app still serves. The failure mode is
visible and specific — data tools fail with a library-install error naming the missing wheel — and
the fix (usually a missing ``USE CATALOG`` grant for the app's service principal) is in the docs.
"""

import asyncio
import hashlib
import io
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Marker file recording the digest of what was last published, so an unchanged wheel is not
# re-uploaded on every app restart.
_HASH_MARKER = ".wheels_hash"


def _wheels_volume() -> str:
    """UC volume directory the runner wheel is published to (empty ⇒ publishing disabled)."""
    return os.environ.get("DQX_WHEELS_VOLUME", "").rstrip("/")


def _expected_wheel_name() -> str:
    """Filename the runner job expects, from the same bundle var the job's dependencies use."""
    return os.environ.get("DQX_RUNNER_WHEEL_FILENAME", "").strip()


def find_runner_wheel() -> Path | None:
    """Locate the built runner wheel shipped alongside the app source.

    Databricks Apps run with the working directory set to the deployed source, and the bundle syncs
    ``.build/`` (see ``sync.include``), so the wheel is normally at ``./.build/``. Walking up from
    this file covers a local ``uvicorn server.app:combined_app`` where the cwd is elsewhere.

    When several wheels are present, the one the runner job actually installs wins — the job lists
    ``DQX_RUNNER_WHEEL_FILENAME`` by absolute path, so publishing anything else guarantees a library
    install failure. Only if that exact name is absent does this fall back to the sole match, and a
    genuinely ambiguous directory is reported rather than resolved by guesswork: picking by name sort
    is not version order (``0.9.0`` sorts after ``0.10.0``), so there is no correct guess to make.
    """
    roots: list[Path] = [Path.cwd(), Path.cwd() / ".build"]
    for parent in Path(__file__).resolve().parents:
        if (parent / "requirements.txt").exists():
            roots.extend([parent, parent / ".build"])
            break
    expected = _expected_wheel_name()
    for root in roots:
        if not root.is_dir():
            continue
        matches = sorted(root.glob("dqx_mcp_runner-*.whl"))
        if not matches:
            continue
        if expected:
            exact = [m for m in matches if m.name == expected]
            if exact:
                return exact[0]
        if len(matches) > 1:
            logger.warning(
                f"Found {len(matches)} runner wheels in {root} and none matches the expected "
                f"{expected or '<DQX_RUNNER_WHEEL_FILENAME unset>'}: {[m.name for m in matches]}. "
                "Publishing none of them — rebuild with a clean .build/, or align the "
                "runner_wheel_filename bundle variable with runner/pyproject.toml."
            )
            return None
        return matches[0]
    logger.warning(f"No runner wheel found; searched {[str(r) for r in roots]}")
    return None


def compute_wheel_hash(wheel: Path) -> str:
    """Digest that changes whenever the wheel's name, size or contents change."""
    digest = hashlib.sha256()
    digest.update(wheel.name.encode())
    digest.update(wheel.stat().st_size.to_bytes(8, "big"))
    with open(wheel, "rb") as handle:
        while chunk := handle.read(1 << 16):
            digest.update(chunk)
    return digest.hexdigest()


def _read_remote_hash(ws, volume: str) -> str | None:
    """Digest recorded by the previous publish, or None if absent/unreadable."""
    try:
        resp = ws.files.download(f"{volume}/{_HASH_MARKER}")
        return resp.contents.read().decode().strip() if resp.contents is not None else None
    except Exception:  # a missing/unreadable marker just means "publish again"
        return None


def publish_runner_wheel() -> str | None:
    """Publish the runner wheel to the wheels volume if it is not already there.

    Returns the published path, or None when nothing was published (disabled, no wheel found, or
    the volume already holds this exact wheel). Never raises.
    """
    volume = _wheels_volume()
    if not volume:
        logger.info("DQX_WHEELS_VOLUME is not set; skipping runner-wheel publish")
        return None

    wheel = find_runner_wheel()
    if wheel is None:
        return None

    expected = _expected_wheel_name()
    if expected and wheel.name != expected:
        # Publish nothing on drift. The runner job installs `expected` by absolute path, so that path
        # stays absent either way and every data tool fails on library install — publishing a wheel
        # the job cannot install is strictly worse than publishing none, because it also poisons the
        # hash marker: the marker records the hash of what WAS uploaded, so a later restart with the
        # same .build/ short-circuits as "unchanged, already published" and never re-uploads, which
        # blocks the deploy from self-healing once the pin is corrected. Same contract as the
        # ambiguous-.build/ path in find_runner_wheel: refuse rather than guess.
        logger.error(
            f"Runner wheel version drift: built {wheel.name} but the runner job expects {expected}. "
            "Publishing nothing — the job installs the expected filename by absolute path, so a "
            "differently-named wheel cannot be used and would only mask the mismatch. Bump the "
            "runner_wheel_filename bundle variable to match runner/pyproject.toml and redeploy."
        )
        return None

    from databricks.sdk import WorkspaceClient  # app SP credentials are injected by the platform

    ws = WorkspaceClient()
    destination = f"{volume}/{wheel.name}"
    try:
        local_hash = compute_wheel_hash(wheel)
        if local_hash == _read_remote_hash(ws, volume):
            logger.info(f"Runner wheel unchanged ({local_hash[:12]}…); already published at {destination}")
            return destination

        with open(wheel, "rb") as handle:
            ws.files.upload(destination, handle, overwrite=True)
        ws.files.upload(f"{volume}/{_HASH_MARKER}", io.BytesIO(local_hash.encode()), overwrite=True)
        logger.info(f"Published runner wheel to {destination} ({local_hash[:12]}…)")
        return destination
    except Exception as exc:  # never prevent the app from serving
        logger.warning(
            f"Could not publish the runner wheel to {destination}: {exc}. Data tools will fail to "
            "install the runner until this succeeds — check that the app's service principal has "
            "USE CATALOG on the catalog and WRITE VOLUME on the wheels volume, then restart the app.",
            exc_info=True,
        )
        return None


def publish_extra_wheels() -> list[str]:
    """Publish any additional local wheels the runner job installs from the volume. Never raises.

    Only relevant to coverage-enabled test deploys: the `dev-coverage` target adds the test-only
    coverage bootstrap wheel to the runner job's dependencies, and a job dependency must be an
    absolute /Volumes path (there is no workspace.artifact_path to upload it for us). Production
    deploys build no such wheel, so this is a no-op for them — the glob simply finds nothing.

    Kept separate from publish_runner_wheel: that one is pinned by filename and refuses to publish on
    drift, whereas these are auxiliary and matched by pattern. Failures are logged and swallowed, so a
    missing extra wheel degrades coverage rather than breaking the app.
    """
    volume = _wheels_volume()
    if not volume:
        return []

    roots: list[Path] = [Path.cwd(), Path.cwd() / ".build"]
    for parent in Path(__file__).resolve().parents:
        if (parent / "requirements.txt").exists():
            roots.extend([parent, parent / ".build"])
            break

    published: list[str] = []
    for root in roots:
        if not root.is_dir():
            continue
        for wheel in sorted(root.glob("dqx_mcp_coverage_bootstrap-*.whl")):
            destination = f"{volume}/{wheel.name}"
            try:
                from databricks.sdk import WorkspaceClient

                ws = WorkspaceClient()
                with open(wheel, "rb") as handle:
                    ws.files.upload(destination, handle, overwrite=True)
                logger.info(f"Published auxiliary wheel to {destination}")
                published.append(destination)
            except Exception as exc:  # coverage instrumentation must never break the app
                logger.warning(f"Could not publish {wheel.name} to {volume}: {exc}")
        if published:
            break
    return published


async def run_startup_tasks() -> None:
    """Run the app's startup work off the event loop. Never raises."""
    try:
        await asyncio.to_thread(publish_runner_wheel)
        await asyncio.to_thread(publish_extra_wheels)
    except Exception as exc:  # belt and braces; publish_runner_wheel already guards
        logger.warning(f"Startup tasks failed (continuing): {exc}", exc_info=True)
