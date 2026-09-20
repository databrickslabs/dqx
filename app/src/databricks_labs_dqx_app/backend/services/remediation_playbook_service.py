"""Thin passthrough onto the remediation-playbook Unity Catalog Volume.

The playbook is the deterministic half of the quarantine-remediation
pipeline: a rule owner writes exactly how a table's failed checks get
fixed as a YAML "runbook", so an external agentic re-ingestion job
(``03a_playbook_remediate``) doesn't have to ask an LLM for that table's
quarantined rows. The pipeline reads these runbooks directly from a
Volume — ``/Volumes/<catalog>/dqx_studio/remediation_playbooks/
<schema>_<table>.yaml``, one file per dataset, filename matching the
quarantine table's own naming convention (``<src_schema>_<src_table>``).

This service is deliberately **not** a database CRUD layer: there is no
app-side draft/approved state, no row to keep in sync — whatever's on
the Volume is what the pipeline uses. ``runbook_yaml`` is opaque to the
app (checked only for valid YAML syntax, never its strategy shape), so a
new remediation strategy the pipeline team invents never requires an app
change.
"""

from __future__ import annotations

import io
import logging
import re
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import validate_fqn

logger = logging.getLogger(__name__)

_SCHEMA = "dqx_studio"
_VOLUME_NAME = "remediation_playbooks"
_FILENAME_RE = re.compile(r"^[a-zA-Z0-9_]+\.ya?ml$")


def _filename_for_table(table_fqn: str) -> str:
    """Derive the runbook filename from a table FQN: schema.table -> schema_table.yaml.

    Matches the quarantine table naming convention
    (``<catalog>.quarantine.<src_schema>_<src_table>``) so the filename
    lines up with what the consuming pipeline already expects.
    """
    validate_fqn(table_fqn)
    _catalog, schema, table = table_fqn.split(".")
    return f"{schema}_{table}.yaml"


class RemediationPlaybookService:
    """List/read/save/delete dataset runbooks on the remediation-playbook Volume."""

    def __init__(self, ws: WorkspaceClient, sql: SqlExecutor, catalog: str) -> None:
        self._ws = ws
        self._sql = sql
        self._catalog = catalog

    @property
    def _volume_path(self) -> str:
        return f"/Volumes/{self._catalog}/{_SCHEMA}/{_VOLUME_NAME}"

    def _ensure_volume(self) -> None:
        """Best-effort: create the volume if it doesn't exist yet.

        The ``dqx_studio`` schema itself is always present (created by the
        app's own bundle), so this really only needs to create the
        ``remediation_playbooks`` volume under it — idempotent
        ``IF NOT EXISTS`` DDL, safe to attempt on every save. A permission
        failure here (e.g. the app SP lacks CREATE VOLUME) surfaces as a
        clear error from the subsequent upload instead of a confusing
        volume-not-found one.
        """
        try:
            self._sql.execute_no_schema(
                f"CREATE VOLUME IF NOT EXISTS `{self._catalog}`.`{_SCHEMA}`.`{_VOLUME_NAME}`"
            )
        except Exception:
            logger.warning("Could not ensure remediation-playbook volume exists (non-fatal)", exc_info=True)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list_entries(self) -> list[dict[str, Any]]:
        try:
            entries = list(self._ws.files.list_directory_contents(self._volume_path))
        except NotFound:
            return []
        except Exception:
            logger.warning("Could not list %s (non-fatal)", self._volume_path, exc_info=True)
            return []

        out: list[dict[str, Any]] = []
        for e in entries:
            if e.is_directory or not e.name or not _FILENAME_RE.match(e.name):
                continue
            out.append(
                {
                    "filename": e.name,
                    "table_fqn": self._table_fqn_hint(e.name),
                    "runbook_yaml": "",
                    "modified_at": self._iso_millis(e.last_modified),
                    "size_bytes": e.file_size,
                }
            )
        out.sort(key=lambda d: d["filename"])
        return out

    def get(self, filename: str) -> dict[str, Any] | None:
        self._validate_filename(filename)
        path = f"{self._volume_path}/{filename}"
        try:
            resp = self._ws.files.download(path)
            meta = self._ws.files.get_metadata(path)
        except NotFound:
            return None
        content = resp.contents.read().decode("utf-8") if resp.contents else ""
        return {
            "filename": filename,
            "table_fqn": self._table_fqn_hint(filename),
            "runbook_yaml": content,
            # get_metadata's last_modified is an HTTP-date *string*
            # (unlike DirectoryEntry's epoch-millis int used in
            # list_entries) — passed through as-is rather than parsed,
            # since it's a display value only.
            "modified_at": meta.last_modified,
            "size_bytes": meta.content_length,
        }

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def save(self, table_fqn: str, runbook_yaml: str, user_email: str) -> dict[str, Any]:
        filename = _filename_for_table(table_fqn)
        if runbook_yaml.strip():
            try:
                yaml.safe_load(runbook_yaml)
            except yaml.YAMLError as e:
                raise ValueError(f"runbook_yaml is not valid YAML: {e}") from e

        self._ensure_volume()
        path = f"{self._volume_path}/{filename}"
        self._ws.files.upload(path, io.BytesIO(runbook_yaml.encode("utf-8")), overwrite=True)
        logger.info("Saved remediation runbook %s (table=%s, user=%s)", filename, table_fqn, user_email)

        entry = self.get(filename)
        if entry is None:
            raise RuntimeError(f"Runbook {filename} was uploaded but could not be re-read")
        return entry

    def update_content(self, filename: str, runbook_yaml: str, user_email: str) -> dict[str, Any]:
        """Overwrite an *existing* runbook's content in place, by filename.

        Deliberately does not go through ``_filename_for_table`` — the
        filename's ``schema_table`` stem isn't losslessly reversible back
        to a real ``table_fqn`` (either part may itself contain an
        underscore), so re-deriving one from the other on every edit is
        both unnecessary (the file's location isn't changing) and a
        source of exactly the bug this method exists to avoid: silently
        writing to the wrong path, or rejecting a valid edit because the
        reconstructed name fails FQN validation.
        """
        self._validate_filename(filename)
        path = f"{self._volume_path}/{filename}"
        try:
            self._ws.files.get_metadata(path)
        except NotFound as e:
            raise LookupError(f"Runbook '{filename}' not found") from e

        if runbook_yaml.strip():
            try:
                yaml.safe_load(runbook_yaml)
            except yaml.YAMLError as e:
                raise ValueError(f"runbook_yaml is not valid YAML: {e}") from e

        self._ws.files.upload(path, io.BytesIO(runbook_yaml.encode("utf-8")), overwrite=True)
        logger.info("Updated remediation runbook %s (user=%s)", filename, user_email)

        entry = self.get(filename)
        if entry is None:
            raise RuntimeError(f"Runbook {filename} was updated but could not be re-read")
        return entry

    def delete(self, filename: str, user_email: str) -> None:
        self._validate_filename(filename)
        path = f"{self._volume_path}/{filename}"
        try:
            self._ws.files.delete(path)
        except NotFound as e:
            raise ValueError(f"Runbook '{filename}' not found") from e
        logger.info("Deleted remediation runbook %s (user=%s)", filename, user_email)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_filename(filename: str) -> None:
        if not _FILENAME_RE.match(filename):
            raise ValueError(f"Invalid runbook filename: {filename!r}")

    def _table_fqn_hint(self, filename: str) -> str:
        """Best-effort table_fqn for display: schema_table.yaml -> catalog.schema.table.

        Splits on the *first* underscore only, matching the common
        medallion convention (schema is a single word — bronze/silver/
        gold) that every real example of this filename shape uses. Not
        guaranteed correct for a schema name that itself contains an
        underscore — this is a display hint only, never used to resolve
        a real table reference or round-tripped back into a save
        (updates to an existing runbook go through
        :meth:`update_content`, keyed by filename, not table_fqn).
        """
        stem = filename.rsplit(".", 1)[0]
        schema, _, table = stem.partition("_")
        if not table:
            return f"{self._catalog}.{stem}"
        return f"{self._catalog}.{schema}.{table}"

    @staticmethod
    def _iso_millis(epoch_millis: int | None) -> str | None:
        if epoch_millis is None:
            return None
        from datetime import datetime, timezone

        return datetime.fromtimestamp(epoch_millis / 1000, tz=timezone.utc).isoformat()
