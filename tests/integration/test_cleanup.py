import logging
import time

from databricks.sdk.errors import PermissionDenied

from tests.constants import TEST_CATALOG

logger = logging.getLogger(__name__)

DUMMY_SCHEMA_PREFIX = "dummy_"
MAX_AGE_MS = 24 * 60 * 60 * 1000  # only drop objects older than 1 day


def test_drop_dummy_schemas_from_test_catalog(ws):
    """Maintenance sweep: drop every schema in the test catalog whose name starts with
    ``dummy_`` and that is older than one day, cascading the deletion to all contained
    tables and registered models.

    pytester's ``make_schema`` fixture names schemas ``dummy_s<random>`` and its tables
    ``dummy_t<random>``. Interrupted or failed integration runs can leave these (and any
    models registered under them) behind in the shared test catalog. This test removes
    them so the catalog does not accumulate orphaned objects. The one-day age cutoff
    avoids deleting schemas created by integration runs currently in progress.

    Best-effort: in a shared catalog some stale schemas may be owned by other principals,
    so a delete can raise ``PermissionDenied``. Those are skipped (logged) rather than
    failing the sweep; the final assertion only requires that the schemas we could manage
    were dropped.
    """
    cutoff_ms = int(time.time() * 1000) - MAX_AGE_MS
    schemas = _list_stale_dummy_schemas(ws, cutoff_ms)

    skipped: list[str] = []
    for schema in schemas:
        try:
            # Registered models are not removed by a forced schema delete, so drop them first.
            _drop_registered_models(ws, schema.catalog_name, schema.name)
            # force=True cascades the deletion to all remaining objects (tables, views, functions).
            ws.schemas.delete(full_name=schema.full_name, force=True)
            logger.info(f"Dropped schema {schema.full_name!r} and all contained objects")
        except PermissionDenied as exc:
            skipped.append(schema.full_name)
            logger.warning(f"Skipping schema {schema.full_name!r}: no permission to drop it ({exc})")

    remaining = {schema.full_name for schema in _list_stale_dummy_schemas(ws, cutoff_ms)}
    not_permission_skipped = remaining - set(skipped)
    assert not not_permission_skipped, (
        f"Stale schemas with prefix {DUMMY_SCHEMA_PREFIX!r} were not dropped "
        f"(and not permission-skipped): {sorted(not_permission_skipped)}"
    )


def _list_stale_dummy_schemas(ws, cutoff_ms: int):
    """Return ``dummy_``-prefixed schemas in the test catalog older than the cutoff."""
    return [
        schema
        for schema in ws.schemas.list(catalog_name=TEST_CATALOG)
        if (schema.name or "").startswith(DUMMY_SCHEMA_PREFIX)
        and schema.created_at is not None
        and schema.created_at < cutoff_ms
    ]


def _drop_registered_models(ws, catalog_name: str, schema_name: str) -> None:
    """Delete every registered (Unity Catalog) model in the given schema.

    A forced schema delete drops tables and functions but leaves registered models in
    place, so they must be removed explicitly before the schema can be dropped. Unlike
    MLflow's ``delete_registered_model``, the SDK refuses to delete a registered model
    that still has versions (``InvalidParameterValue: ... has N model versions``), so
    each version must be deleted first.
    """
    for model in ws.registered_models.list(catalog_name=catalog_name, schema_name=schema_name):
        for version in ws.model_versions.list(full_name=model.full_name):
            ws.model_versions.delete(full_name=model.full_name, version=version.version)
            logger.info(f"Deleted model version {model.full_name!r} v{version.version}")
        ws.registered_models.delete(full_name=model.full_name)
        logger.info(f"Deleted registered model {model.full_name!r}")
