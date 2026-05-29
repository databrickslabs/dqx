"""Re-align a wheel's METADATA / RECORD / dist-info dir to its filename.

Background
----------
``apx build`` post-processes the freshly-built application wheel by
injecting a build timestamp into the **filename** (e.g. renaming
``databricks_labs_dqx_app-0.13.0.post38.dev0+65c9602-py3-none-any.whl``
to
``databricks_labs_dqx_app-0.13.0.post38.dev0+65c9602.post20260506102508-py3-none-any.whl``)
but does **not** update the wheel's internal ``METADATA``, ``RECORD`` or
``*.dist-info`` directory name.

The result is a wheel whose filename advertises a new version each
build but whose package metadata still reports the unchanged
git-derived version.  ``pip`` consults the metadata, sees the version
is already installed, and silently skips reinstall — so a Databricks
App container's persistent venv keeps running stale code on every
redeploy.

This script repairs the wheel produced by ``apx build`` by:

1. parsing the version segment out of the filename
2. unpacking the wheel
3. renaming the ``*.dist-info`` directory to use that version
4. rewriting ``METADATA`` so ``Version:`` matches
5. regenerating ``RECORD`` checksums
6. repacking the wheel in place

After running this each rebuild produces a wheel with a unique
metadata version, forcing pip to install fresh code on every deploy.

Usage
-----
    python _align_wheel_version.py path/to/wheel.whl

The script overwrites the input wheel; safe to run idempotently.
"""

from __future__ import annotations

import base64
import hashlib
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

# Wheel filename format (PEP 427):
#   {distribution}-{version}(-{build tag})?-{python tag}-{abi tag}-{platform tag}.whl
# Distribution and version may both contain dots; the version segment
# starts right after the leading "{distribution}-" and ends at the
# next "-py" / "-cp" / "-pp" tag.  Everything after that until the
# trailing ``.whl`` is the compatibility tag triple.
_WHEEL_FILENAME_RE = re.compile(
    r"^(?P<dist>[A-Za-z0-9_]+)-(?P<version>[^-]+(?:\+[^-]+)?)-(?P<tags>(?:py|cp|pp|ip)\d.*?)\.whl$"
)


def _filename_version(wheel: Path) -> tuple[str, str]:
    """Return ``(distribution, version)`` parsed from the wheel filename.

    Uses a regex rather than ``packaging.utils.parse_wheel_filename``
    because the latter normalises the distribution name and rejects
    local versions that contain dots — both of which are present in the
    wheels apx produces.
    """
    m = _WHEEL_FILENAME_RE.match(wheel.name)
    if not m:
        raise SystemExit(f"unrecognised wheel filename: {wheel.name}")
    return m.group("dist"), m.group("version")


def _record_entry(rel_path: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{rel_path},sha256={digest},{len(data)}"


def align(wheel_path: Path) -> None:
    """Rewrite ``wheel_path`` so its metadata version matches its filename."""
    dist, filename_version = _filename_version(wheel_path)

    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)
        with zipfile.ZipFile(wheel_path, "r") as zf:
            zf.extractall(tmp)

        old_dist_infos = [p for p in tmp.iterdir() if p.is_dir() and p.name.endswith(".dist-info")]
        if not old_dist_infos:
            raise SystemExit(f"no .dist-info dir in {wheel_path.name}")
        if len(old_dist_infos) > 1:
            raise SystemExit(f"multiple .dist-info dirs in {wheel_path.name}: {old_dist_infos}")
        old_dist_info = old_dist_infos[0]

        # Hatchling preserves underscores in the distribution prefix
        # of the dist-info directory, so reuse that prefix verbatim
        # rather than recomputing from ``dist`` (which packaging.utils
        # would normalise to dashes).  The directory name is
        # ``{prefix}-{version}.dist-info``; strip the literal suffix
        # then split off the version.
        stem = old_dist_info.name[: -len(".dist-info")]
        existing_prefix = stem.rsplit("-", 1)[0]
        new_dist_info = tmp / f"{existing_prefix}-{filename_version}.dist-info"
        # Skip the rename when the dist-info dir is already correctly
        # named (e.g. apx didn't inject a build tag on this rebuild so
        # the filename version matches what's already inside the
        # wheel). Without this guard, the ``rmtree`` below would delete
        # the source directory — both paths point at the same dir — and
        # the subsequent ``rename`` would crash with FileNotFoundError.
        # The METADATA/RECORD rewrites further down are themselves
        # idempotent, so we still run them to repair any drift.
        if new_dist_info != old_dist_info:
            if new_dist_info.exists():
                shutil.rmtree(new_dist_info)
            old_dist_info.rename(new_dist_info)

        metadata = new_dist_info / "METADATA"
        if not metadata.exists():
            raise SystemExit(f"missing METADATA in {wheel_path.name}")
        text = metadata.read_text(encoding="utf-8")
        new_text, n = re.subn(
            r"^Version: .*$",
            f"Version: {filename_version}",
            text,
            count=1,
            flags=re.MULTILINE,
        )
        if n == 0:
            raise SystemExit(f"no Version: line in METADATA of {wheel_path.name}")
        metadata.write_text(new_text, encoding="utf-8")

        record = new_dist_info / "RECORD"
        record_rel = record.relative_to(tmp).as_posix()
        entries: list[str] = []
        for f in sorted(tmp.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(tmp).as_posix()
            if rel == record_rel:
                continue
            entries.append(_record_entry(rel, f.read_bytes()))
        entries.append(f"{record_rel},,")
        record.write_text("\n".join(entries) + "\n", encoding="utf-8")

        tmp_wheel = wheel_path.with_suffix(wheel_path.suffix + ".tmp")
        with zipfile.ZipFile(tmp_wheel, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(tmp.rglob("*")):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp).as_posix())

        tmp_wheel.replace(wheel_path)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {Path(argv[0]).name} <wheel.whl>", file=sys.stderr)
        return 2
    wheel = Path(argv[1])
    if not wheel.is_file():
        print(f"not a file: {wheel}", file=sys.stderr)
        return 1
    align(wheel)
    print(f"  aligned wheel metadata to filename version: {wheel.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
