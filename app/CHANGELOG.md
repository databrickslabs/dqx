# DQX Studio changelog

## 0.1.3

Pins DQX Core 0.17.2 (was 0.17.0) for the app and the task runner, on both DAB and Marketplace.

### Packaging and deployment

- New `make lock-marketplace DQX_VERSION=x.y.z` regenerates `app/marketplace_templates/uv.lock` for a DQX release, using `app/uv.lock` as version preferences, and points every URL at public PyPI. A different version also updates `dqx_version` in `app/databricks.yml`. This replaces the manual lock-refresh steps in `DEPLOYMENT.md`.

## 0.1.2

### Fixes

- Approving a monitored table no longer deletes every other table's approved checks. Cleanup now only touches the table being submitted ([#1640](https://github.com/databrickslabs/dqx/pull/1640), fixes [#1639](https://github.com/databrickslabs/dqx/issues/1639)).

### Packaging and deployment

- Studio no longer follows DQX Core automatically. It pins a published DQX (0.17.0), and the app and the task runner use that same version on both DAB and Marketplace. Before this, the Marketplace lock stayed on 0.16.0 while the runner pin moved to 0.17.x ([#1647](https://github.com/databrickslabs/dqx/pull/1647)).
- The pin now changes only in a Studio release, together with the Marketplace lock. `make fmt` no longer bumps it.
- DAB builds now ship the same pinned `pyproject.toml` and lock as Marketplace. DQX is no longer bundled into the build from the local checkout.
- The app starts with `uv run --frozen`.
- The build fails if the lock's DQX version doesn't match `dqx_version`.
- Updated `litellm` to 1.96.2, which clears 17 Dependabot alerts on the Marketplace lock.
- Required `certifi>=2026.10.19`, which drops the distrusted eMudhra root certificate ([#1632](https://github.com/databrickslabs/dqx/issues/1632)).
