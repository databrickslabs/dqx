"""ONE-TIME maintenance test — delete after it has run once.

The Lakebase suite creates one Unity Catalog catalog per test, named ``dqx-test-<run_id>-<rand>``
(see the ``make_lakebase_instance`` fixture). When a runner is killed mid-test the fixture teardown
never runs, leaking the catalog and its database instance. Catalogs count toward the per-metastore
catalog quota (1000); once exceeded, every Lakebase test fails at setup with
``Cannot create ... Catalog(s) in Metastore ... (estimated count: N, limit: 1000)``.

This test sweeps the leaked ``dqx-test-*`` catalogs (and their database instances) so the suite can
create catalogs again. It only ever touches the ``dqx-test-`` prefix and skips the current run's own
resources (a parallel test may still be using them). Runs inside CI, which has workspace access.

It attempts every delete and then FAILS if any request errored — a sweep that silently swallowed
errors would report success while leaving the metastore over quota.

Remove this file once it has cleared the backlog and the suite is green.
"""

import logging
import os
from collections.abc import Callable, Iterable

from databricks.sdk.errors import DatabricksError, NotFound

logger = logging.getLogger(__name__)

_PREFIX = "dqx-test-"


def test_cleanup_orphaned_lakebase_catalogs(ws):
    current_run_prefix = f"{_PREFIX}{os.getenv('GITHUB_RUN_ID', 'local')}-"
    catalog_names = _orphaned_names((c.name or "" for c in ws.catalogs.list()), current_run_prefix)
    catalog_failures = _delete_each(catalog_names, lambda n: ws.catalogs.delete(name=n, force=True), "catalog")

    instance_names = _orphaned_names((i.name or "" for i in ws.database.list_database_instances()), current_run_prefix)
    instance_failures = _delete_each(instance_names, lambda n: ws.database.delete_database_instance(name=n), "instance")

    failures = catalog_failures + instance_failures
    assert not failures, f"Failed to delete {len(failures)} orphaned resource(s): " + "; ".join(failures)


def _orphaned_names(names: Iterable[str], current_run_prefix: str) -> list[str]:
    """Leaked test resources: the dqx-test- prefix, excluding this run's own (still possibly in use)."""
    return [n for n in names if n.startswith(_PREFIX) and not n.startswith(current_run_prefix)]


def _delete_each(names: list[str], delete: Callable[[str], None], kind: str) -> list[str]:
    """Calls *delete* on every named resource to clean up orphaned objects."""
    failures: list[str] = []
    deleted = 0
    for name in names:
        try:
            delete(name)
            deleted += 1
            logger.info(f"Deleted orphaned Lakebase test {kind}: {name}")
        except NotFound:
            pass
        except DatabricksError as exc:
            logger.warning(f"Failed to delete {kind} {name}: {exc}")
            failures.append(f"{kind} {name}: {exc}")
    logger.info(f"Deleted {deleted} {kind}(s), {len(failures)} failure(s)")
    return failures
