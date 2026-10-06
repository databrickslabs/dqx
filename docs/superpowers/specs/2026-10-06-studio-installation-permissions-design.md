# DQX Studio Installation and Permissions

Status: proposed design, awaiting review. This document does not describe
already-shipped functionality.

## Intent

Make DAB deployment and Marketplace installation converge on the same usable
Studio permissions, with minimal administrator setup. An administrator chooses
the storage namespace and audience once; subsequent onboarding is group
membership plus the existing role policy. Users must be able to perform
authorized OBO operations, the separate task-runner service principal must be
able to execute its workload, and administrators must be able to install and run
Studio. Missing or unverifiable required permissions must not produce a ready
setup report.

The agreed changes are:

- Request warehouse `CAN_MANAGE` for the app service principal.
- Replace the mandatory Marketplace wheels-volume binding with catalog and
  prefix selection in the setup UI. Catalog is not a manifest binding or input.
  Retain a wheels volume inside the main Studio schema.
- Use a common prefix-derived Unity Catalog namespace in new installations.
- Keep a dedicated audience group for Marketplace. DAB additionally supports
  the explicit `users` compatibility mode.
- Keep administrator access separate from audience access.
- Preserve OBO enforcement, the distinct runner identity, and existing storage.

## Current Gaps

The current Marketplace manifest binds a warehouse with `CAN_USE`, Lakebase,
and a pre-existing volume. Startup derives the main catalog/schema from the
volume and cannot construct setup collaborators without that volume.

`DQX_USER_GROUPS` rejects `users` and `account users`. Configured temporary-schema
and Genie object grants are best effort, and do not block setup readiness.
Marketplace does not configure a Studio audience during setup or reconcile its
warehouse and app access.

Runner Unity Catalog checks already resolve the actual Jobs `run_as`, check
schema-level output privileges and wheel reads, and retain per-view grants for
OBO-created views. They must be retained and tested, not replaced by assumptions
about the configured runner ID.

On current `main`, oversized run configurations are staged in Lakebase, but
setup does not verify the runner's Lakebase access. PR #1564 moves staging to
Delta and removes that runtime dependency. This feature depends on that change
being merged and rebased into this branch before implementation is completed.
Do not claim runner readiness without verifying the staged-config path.

## Storage Contract

New installations select an existing catalog and a validated prefix. The
default prefix is `dqx_studio`.

```text
<catalog>
  <prefix>                  main Studio schema
    wheels                  managed Unity Catalog volume
  <prefix>_tmp              OBO temporary views
  <prefix>_genie             approved Genie views and metadata
```

There are three required Unity Catalog schemas, not a separate wheels schema.
The volume stores the application and task-runner wheels; it is not merely a
name binding.

Optional demo data stays isolated in `<prefix>_demo`, created only when demo
content is requested for a new installation. Do not grant audience access to
the main schema to make demo browsing work. Preserve existing bundle-managed
demo resources and their destroy protection on upgrades.

The prefix controls Unity Catalog storage. Lakebase is a separate namespace:
retain its configured `DQX_LAKEBASE_SCHEMA` so that selecting a UC prefix cannot
redirect or orphan transactional settings. Installations sharing a Lakebase
database must use distinct application schemas. This independent, stable
Lakebase namespace also permits storing setup choices before UC storage exists.

All identifiers must use the existing validation and quoting helpers. Reject
invalid names, over-length derived identifiers, and conflicting storage inputs
before DDL or grant operations. Never interpret prefix input as a path or SQL.

## Installation Configuration

The Marketplace manifest declares only the SQL warehouse (`CAN_MANAGE`) and
Lakebase bindings. It does not declare a catalog binding or catalog parameter.
The setup UI collects an existing catalog, prefix and audience after installation.
It validates the installing administrator's actual catalog permissions before
creating storage. Removing the volume binding also removes its automatic parent
catalog/schema access for the app SP, so setup must establish and verify that
access explicitly.

Use the existing app settings persistence rather than local container files.
Bootstrap the bound Lakebase connection and its application migrations before
requiring a Unity Catalog volume. Persist catalog, prefix, resolved storage
names, and audience in Lakebase after an authorized setup submission.

Deployment-supplied configuration is authoritative and shown as such in setup.
Marketplace catalog selection is always a setup input for a fresh installation,
not a hard-coded `DQX_CATALOG` default or manifest value. Subsequent restarts
resolve it and the other setup choices from saved settings. Existing volume
bindings remain a supported legacy input; they must resolve to the existing
storage rather than being ignored.

DAB exposes a prefix variable and a `STUDIO_PREFIX` Make argument. Schema-name
overrides remain available for existing installations. The Make helper forwards
variables through the existing deployment mechanism; no generated tracked YAML
or duplicate permission logic is introduced.

DAB exposes `STUDIO_USER_GROUP` through Make and retains the bundle
`studio_user_group` variable. New compatibility-mode deployments can use
`users`; a dedicated group uses its supplied name unchanged. Resolve the
workspace ACL principal and the UC principal separately:

| Audience input | Workspace ACL principal | UC principal |
| --- | --- | --- |
| `users` | `users` | `account users` |
| Dedicated account group | Group name | Same group name |

Do not claim these built-ins have identical scope: `account users` is
account-wide UC access, while `users` is the workspace-wide ACL audience.
Broad mode must be clearly identified in configuration and documentation.
Never implement it by nesting a built-in group into a custom group.

Marketplace requires an existing, workspace-assigned account group for its
audience. It must not silently fall back to broad access or create account
groups. No account-admin role is required by Studio installation itself;
identity provisioning remains an organization's separate responsibility.

## Administrator Contract

Workspace administration remains an installation prerequisite. It is not a
substitute for catalog grant authority, warehouse access, permission to use the
runner SP, or Lakebase migration authority.

Retain the workspace `admins` bootstrap path when a custom `admin_group` is
configured. The custom administrator group remains eligible for the in-app
`ADMIN` role and authorized setup, subject to actual resource capabilities.
Authorize setup mutations from fresh, trusted identity/group resolution, not
caller-supplied identities, forwarded group headers, or role mappings that
require an already-ready application.

Administrators need the same external OBO prerequisites as other users. Apply
the required warehouse and temporary/Genie access to the administrator groups
as well as the audience; do not assume an in-app admin role supplies UC access.
Keep audience membership distinct from installation or administrative authority.

## Permission Matrix

| Resource or operation | App SP | Actual runner SP | Audience | Administrator |
| --- | --- | --- | --- | --- |
| Selected UC catalog | `USE CATALOG`, `CREATE SCHEMA` | `USE CATALOG` | `USE CATALOG` | Usage, schema creation and grant/ownership authority during install |
| Main UC schema | Marketplace owner; DAB explicit application privileges and `MANAGE` | `USE SCHEMA`, `SELECT`, `MODIFY` | No schema-wide table access | Verified install access; scoped dashboard-publisher access if needed |
| Wheels volume | Owner, effective read/write | `READ VOLUME` | None | Provisioning/recovery authority, not routine wheel data access |
| Temporary UC schema | Marketplace owner; DAB explicit creation privileges and `MANAGE` | `USE SCHEMA` | `USE SCHEMA`, `CREATE TABLE` | Same OBO privileges as audience |
| OBO-created temporary view | Per-view `MANAGE` for cleanup | Per-view `SELECT` | Creator's existing access only | No automatic blanket access to others' views |
| Genie UC schema | Marketplace owner; DAB explicit creation privileges and `MANAGE` | No additional grant unless workload needs it | `USE SCHEMA` | Explicit consumer allowlist |
| Approved Genie objects | Application access | No blanket grant | Explicit `SELECT` allowlist | Explicit consumer allowlist |
| SQL warehouse | `CAN_MANAGE` | Not needed for Spark-only execution | `CAN_USE` | Effective use and installation authority |
| Task-runner job | `CAN_MANAGE` | Configured `run_as`; not app SP | No direct job ACL needed for app-mediated runs | Setup-management access to the job |
| App | Runtime service identity, not presumed ACL manager | None | `CAN_USE` | Installation management and usable app access |
| Genie space, when configured | Creation/reconciliation authority | None | `CAN_RUN` | Management/consumer access as appropriate |
| Dashboard, when configured | Depends on configured publisher | None | `CAN_READ` | Publisher data privileges or verified embedded credentials |
| Lakebase application schema | Connection, migration ownership and application CRUD | No runtime access after #1564 | No direct access | Bootstrap migration/provisioning authority |
| User source data | Only explicitly authorized scheduled workloads | Run-specific verified source access | Caller-specific OBO source access | Caller-specific source access |

Ownership permits management of Studio's namespace, but is not accepted as a
blanket substitute for every existing child object's data privileges or
ownership. Check effective capabilities and object ownership where required.
Do not take over an unrelated pre-existing schema or volume solely because its
name matches a prefix.

DAB schema resources are owned by the deployment identity; they do not support
a declarative app-SP owner override. Preserve that ownership model and explicitly
grant the app `ALL_PRIVILEGES` plus `MANAGE` on the dedicated Studio schemas.
`ALL_PRIVILEGES` alone is not permission-management authority. Semantic
permission parity does not require identical schema ownership in both paths.

Genie `SELECT` remains limited to the existing five approved views and
`dim_dq_rules` / `dim_dq_monitored_tables`. Never grant whole-schema Genie
`SELECT`, direct quarantine-table access, or access to `dq_user_table_entitlements`.
Runner wheel write access is unnecessary; table `SELECT`/`MODIFY` does not
substitute for volume privileges.

Infrastructure permissions do not replace in-app RBAC. Keep existing role
mapping semantics: administrators assign the audience's author, approver or
viewer policy once, and onboarding thereafter uses group membership. Audience
membership must never imply `ADMIN`. Tests for author OBO workflows must assign
the existing author role rather than bypassing authorization.

## Provisioning and Reconciliation

1. Resolve trusted setup administrator access, Lakebase binding, warehouse
   binding, application identity, and saved/deployment configuration.
2. Initialize the stable Lakebase application namespace and settings needed to
   save installation choices. With no choices, serve the setup UI without
   activating normal APIs, scheduling, or user workloads.
3. Validate catalog, prefix and audience. Inspect existing objects and refuse
   unrelated collisions. Persist accepted choices; existing storage locations
   become immutable after provisioning.
4. Verify catalog bootstrap capabilities. An authorized installing admin
   establishes app-SP `USE CATALOG` / `CREATE SCHEMA` and catalog usage for the
   runner, audience and administrator groups. Report precise instructions when
   the installer lacks grant authority; do not escalate privileges.
5. Marketplace creates the three UC schemas and wheels volume as the app SP.
   DAB continues to manage the resources declaratively under the deployer's
   schema ownership, with explicit app-SP schema management and volume data
   privileges. Existing objects must have sufficient effective capabilities
   established by an authorized administrator; never silently take ownership.
6. Resolve or create the Studio task-runner job. Preserve the separate runner
   SP assignment and the installer's Service Principal: User prerequisite.
   Resolve grants against the actual `run_as`, not just a requested UUID.
7. Reconcile runner output-schema and wheel-read grants, publish wheels,
   configure the job, and run application migrations. Verify staged-config
   read and cleanup behavior after #1564 as part of runner acceptance.
8. Apply and verify audience and administrator warehouse and temporary-schema
   permissions. Materialize the required Genie views/dimensions, then apply
   and verify the explicit object allowlist. Reconcile and verify ACLs on
   configured Genie spaces and dashboards.
9. Verify app sharing. DAB declares audience and administrator app ACLs.
   Marketplace requires the explicit Apps Permissions step and inspects its ACL
   using an identity with sufficient permission and supported authorization
   scopes before reporting ready. If the app identity cannot inspect its ACL,
   report that missing capability rather than accepting an unchecked completion
   checkbox. Do not assume the app SP can manage its own app ACL.
10. Publish ready only after required infrastructure, audience, administrator and
    runner checks pass. Start background workloads only afterwards.

Use additive ACL updates and scoped grant operations. Preserve unrelated
principals, resource bindings, memberships, owners and grants on shared
warehouse/Lakebase resources. Do not use replace-all permission calls for
audience sharing. A rebind to another warehouse must repeat app management and
audience/administrator use checks before switching the active configuration.

Retries must be serialized and idempotent. Partially created resources remain
available for a retry; never delete persistent storage to recover from a failed
grant. Once ready, an administrator's verify/reconcile action must recheck
permissions rather than return a cached ready report. Do not persist OBO tokens.

## Readiness Boundaries

Required-but-missing and required-but-uninspectable grants both keep setup
action-required, with different diagnostic codes. An attempted grant is not
proof of effective access. Schema ownership and group membership alone are not
proof that OBO or runner operations work.

Do not invent missing Marketplace dashboards. ACL checks for dashboards and
Genie spaces apply when those features have configured resources; disabled
features are explicitly not applicable, not falsely reported as shared.

Per-user SQL entitlement, OAuth consent and source-data privileges cannot be
proven for an entire group by a setup-time ACL check. Report these boundaries in
setup/documentation and check the active user's capabilities on the relevant
workflow. Do not automatically change organization entitlements or grant source
data broadly. Per-view runner `SELECT` and app cleanup `MANAGE` failures must
continue to block submission; scheduled source access remains independently
verified for the scheduler and actual runner.

## Upgrade Safety

Retain legacy volume bindings, explicit main/tmp/Genie names, existing Lakebase
schema names, and all bundle destroy protections. Do not automatically rename,
move or recreate data. In particular, existing DAB targets using `genie` must
pin that override rather than being redirected to `<prefix>_genie`.

Existing targets that relied on the implicit scoped-audience default must pin
their intended group before adopting a compatibility-mode default. Document
the broad UC scope and obtain explicit administrator confirmation for
Marketplace audience changes; Marketplace never defaults to `users`.

Reconciliation establishes required grants, not a general cleanup of all ACLs.
Warn about detected legacy broad permissions and document administrator review
and revocation; never silently remove unrelated permissions or memberships.
Changing the prefix/catalog after provisioning requires a separate storage
migration and is rejected by setup.

## Validation and Documentation

Unit and contract tests cover prefix derivation, identifier validation,
deployment/settings/legacy precedence, audience principal mapping, independent
admin access, manifest bindings, bundle grants, and preservation of
unrelated ACL entries. Each required permission has a missing and an unknown
failure case, a successful retry, and a ready-to-missing recheck.

Integration acceptance uses distinct installer, app SP, runner SP, authorized
non-admin author and unauthorized user identities. Use existing factory-managed
fixtures and injected clients; do not put live API calls in unit tests.

For both DAB and the Marketplace-equivalent custom-template path, verify:

- Fresh install with no pre-existing Studio schemas or volume.
- Scoped audience; DAB compatibility-mode audience separately.
- Admin installation, custom-admin bootstrap, and an admin-triggered run.
- Author OBO browsing, preview, profiler and dry-run, including denied source
  access and denied user SQL/warehouse prerequisites.
- Runner wheel installation, actual `run_as`, Delta output writes,
  oversized staged-config reads/cleanup, temporary-view reads and cleanup.
- Scheduled source access checks remain effective.
- Genie and configured dashboard consumer access without internal-table access.
- Missing each required grant prevents false readiness and yields a recoverable
  administrator action.
- Restart/retry, legacy install upgrade, warehouse rebind and shared-resource
  ACL preservation.

Custom templates exercise resource binding and application setup, not the
Marketplace listing/distribution flow. A real Marketplace installation remains
a separate release acceptance check.

Update `app/DEPLOYMENT.md`, `app/DEVELOPMENT.md`, `app/README.md`, and published
Studio installation and governance pages under `docs/dqx/docs/studio/`.
Document the permission matrix, minimal setup steps, role assignment, broad
mode, independent UC/Lakebase authority, app-sharing recovery, source-data
boundaries, and upgrade-safe overrides. Update `app/AGENTS.md` so it no longer
describes required audience grants as best effort. Tag published feature
documentation with the actual shipping version rather than guessing one.

Implementation and an implementation plan begin only after review of this
written design. The draft PR must remain clearly design-only until then.

## Platform References

- [DAB resources and schema ownership limitations](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
- [App resource warehouse permission values](https://docs.databricks.com/api/apps/v1/create-app)
- [App sharing permissions](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/permissions)
- [User authorization and OAuth scope boundaries](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
