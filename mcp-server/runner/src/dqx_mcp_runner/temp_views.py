"""Validate and clean up server-generated views without importing Spark.

The runner anchors its temporary namespace to the results-volume path already
supplied by the server's configuration. This is a consistency guard, not a new
authorization boundary or a caller-supplied allowlist.
"""

import logging
import re
from collections.abc import Mapping
from typing import Protocol

from dqx_mcp_runner.naming import IDENTIFIER_RE

logger = logging.getLogger("dqx-mcp-runner")
_TEMP_VIEW_NAME_RE = re.compile(r"v_[0-9]+_[0-9a-f]+")


class SqlExecutor(Protocol):
    """The Spark SQL operation needed for best-effort view cleanup."""

    def sql(self, query: str, /) -> object:
        """Execute a SQL statement."""


def trusted_view_namespace(results_volume: str) -> tuple[str, str] | None:
    """Read the temporary namespace from the configured results-volume path.

    Args:
        results_volume: Server-configured path under /Volumes/catalog/schema/volume.

    Returns:
        Lowercase catalog and schema identifiers, or None for an invalid path.
    """
    parts = results_volume.strip().strip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        return None
    catalog, schema = parts[1], parts[2]
    if not (IDENTIFIER_RE.fullmatch(catalog) and IDENTIFIER_RE.fullmatch(schema)):
        return None
    return catalog.lower(), schema.lower()


def parse_ref_views(params: Mapping[str, object]) -> dict[str, str]:
    """Read the original-reference-name to view-FQN map.

    Args:
        params: Server-prepared runner parameters.

    Returns:
        Reference views, or an empty dictionary when absent.

    Raises:
        ValueError: The map contains non-string names or values.
    """
    raw = params.get("ref_views")
    if raw is None:
        return {}
    if not isinstance(raw, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in raw.items()):
        raise ValueError("ref_views must be a mapping of reference name to view name.")
    return dict(raw)


def collect_temp_views(params: Mapping[str, object]) -> list[str]:
    """Collect monitored and reference views for independent, deduplicated cleanup.

    Args:
        params: Server-prepared runner parameters, possibly malformed.

    Returns:
        String-valued view names in first-occurrence order. Malformed values are
        ignored so they cannot prevent cleanup of other, valid views.
    """
    candidates: list[object] = [params.get("view_name")]
    ref_views = params.get("ref_views")
    if isinstance(ref_views, dict):
        candidates.extend(ref_views.values())
    return list(dict.fromkeys(view for view in candidates if isinstance(view, str) and view))


def _is_trusted_view(view_name: str, namespace: tuple[str, str] | None) -> bool:
    parts = view_name.split(".")
    if namespace is None or len(parts) != 3 or not all(IDENTIFIER_RE.fullmatch(part) for part in parts):
        return False
    return (parts[0].lower(), parts[1].lower()) == namespace and _TEMP_VIEW_NAME_RE.fullmatch(parts[2]) is not None


def ensure_views_in_namespace(views: list[str], namespace: tuple[str, str] | None) -> None:
    """Require generated view names inside the configured temporary namespace.

    Args:
        views: View FQNs the operation will read.
        namespace: Catalog and schema derived from the configured results volume.

    Raises:
        ValueError: A name is malformed, outside the namespace, or does not match
            the server's v_<epoch>_<hex> naming pattern.
    """
    for view in views:
        if not _is_trusted_view(view, namespace):
            raise ValueError(f"View {view!r} is not a generated view in the runner's temp-view namespace.")


def drop_temp_views(spark: SqlExecutor, views: list[str], namespace: tuple[str, str] | None) -> None:
    """Attempt every valid view drop without hiding the operation's original error.

    Args:
        spark: SQL executor, normally the operation's Spark session.
        views: Monitored and reference views, possibly containing duplicates.
        namespace: Catalog and schema derived from the configured results volume.
    """
    for view in dict.fromkeys(views):
        if not _is_trusted_view(view, namespace):
            logger.warning(f"Skipping drop of invalid or out-of-namespace temp view: {view!r}")
            continue
        safe_fqn = ".".join(f"`{part}`" for part in view.split("."))
        try:
            spark.sql(f"DROP VIEW IF EXISTS {safe_fqn}")
            logger.info(f"Dropped temp view {view}")
        except Exception:
            logger.warning(f"Failed to drop temp view {view}", exc_info=True)
