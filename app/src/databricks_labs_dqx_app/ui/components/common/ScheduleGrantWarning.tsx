/** Yellow warning card shown above the schedule settings when the current user
 *  cannot grant the scheduler read access to the table(s) a schedule would run
 *  against (Task 12). Scheduled runs read the source table as a service account,
 *  so setting up a schedule requires the MANAGE privilege (or ownership) to
 *  grant that account SELECT. When the user lacks it we hard-block the save and
 *  render this card naming the users/groups that DO hold MANAGE, asking the user
 *  to have one of them set the schedule up instead.
 *
 *  When Studio's service principals can't use the table's catalog or schema,
 *  nobody can schedule it until those grants exist, so a separate card lists
 *  the missing grants (by service principal name) and the GRANT statements.
 *
 *  Tables whose access couldn't be checked (the SQL warehouse didn't answer, or
 *  the permissions couldn't be read) get a separate neutral notice with a retry
 *  instead — that is not a denial.
 *
 *  Rendered via the `ScheduleEditor` `banner` slot so it sits above the first
 *  schedule setting on both the table page and the collection page.
 */
import { useTranslation } from "react-i18next";
import { AlertTriangle, Info } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { SchedulePreflightTableOut } from "@/lib/api";
import type { ScheduleGrantPreflight } from "@/components/schedules/useScheduleGrantPreflight";

type Entity = "table" | "collection";

interface Props {
  /** Which entity is being scheduled — drives the description wording. */
  entity: Entity;
  /** Preflight result: blocked tables (with MANAGE holders) and unverified ones. */
  preflight: ScheduleGrantPreflight;
}

/** Label for a MANAGE holder's principal type.
 *  Service principals hold grants under a bare application id, so they are
 *  labelled as such rather than lumped in with groups — the card asks the user
 *  to contact a holder, and an SP cannot act on that request. */
function holderTypeLabel(type: string, t: (key: string) => string): string {
  if (type === "service_principal")
    return t("schedule.grantWarning.typeServicePrincipal");
  if (type === "group") return t("schedule.grantWarning.typeGroup");
  return t("schedule.grantWarning.typeUser");
}

function Holders({
  holders,
}: {
  holders: SchedulePreflightTableOut["manage_holders"];
}) {
  const { t } = useTranslation();
  const list = holders ?? [];
  if (list.length === 0) {
    return (
      <p className="text-xs text-amber-700 dark:text-amber-300/90">
        {t("schedule.grantWarning.noHolders")}
      </p>
    );
  }
  return (
    <div className="flex flex-wrap gap-1.5">
      {list.map((h) => (
        <Badge
          key={`${h.type}:${h.principal}`}
          variant="outline"
          className="border-amber-500/50 bg-amber-500/10 text-amber-800 dark:text-amber-200"
        >
          {h.display_name || h.principal}
          <span className="ml-1 text-[10px] uppercase tracking-wide text-amber-600/80 dark:text-amber-300/70">
            {holderTypeLabel(h.type, t)}
          </span>
        </Badge>
      ))}
    </div>
  );
}

export function ScheduleGrantWarning({ entity, preflight }: Props) {
  const warehouseTables = preflight.unverifiedTables.filter(
    (tbl) => tbl.unverified_reason !== "permissions",
  );
  const permissionTables = preflight.unverifiedTables.filter(
    (tbl) => tbl.unverified_reason === "permissions",
  );
  return (
    <div className="space-y-3">
      {preflight.missingGrantTables.length > 0 && (
        <MissingGrantsNotice entity={entity} preflight={preflight} />
      )}
      {preflight.blockedTables.length > 0 && (
        <BlockedNotice
          entity={entity}
          blockedTables={preflight.blockedTables}
        />
      )}
      {warehouseTables.length > 0 && (
        <UnverifiedNotice
          entity={entity}
          preflight={preflight}
          tables={warehouseTables}
          reason="warehouse"
        />
      )}
      {permissionTables.length > 0 && (
        <UnverifiedNotice
          entity={entity}
          preflight={preflight}
          tables={permissionTables}
          reason="permissions"
        />
      )}
    </div>
  );
}

/** Amber card for tables whose catalog or schema Studio's service principals
 *  can't use yet. Lists each missing grant by service principal name, then the
 *  GRANT statements (which need the application id) for an owner to run. */
function MissingGrantsNotice({ entity, preflight }: Props) {
  const { t } = useTranslation();
  const grants = preflight.missingGrantTables.flatMap(
    (tbl) => tbl.missing_grants ?? [],
  );
  const unique = grants.filter(
    (g, index) =>
      grants.findIndex((other) => other.statement === g.statement) === index,
  );
  return (
    <div
      role="alert"
      className="rounded-lg border border-amber-500/50 bg-amber-500/10 p-4 space-y-3 text-amber-900 dark:text-amber-100"
    >
      <div className="flex items-start gap-2">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
        <div className="space-y-1">
          <h4 className="text-sm font-medium">
            {t("schedule.grantWarning.title")}
          </h4>
          <p className="text-xs text-amber-800 dark:text-amber-200/90">
            {entity === "table"
              ? t("schedule.grantWarning.missingGrantsDescriptionTable")
              : t("schedule.grantWarning.missingGrantsDescriptionCollection")}
          </p>
        </div>
      </div>
      <div className="space-y-2 pl-6">
        <ul className="list-disc pl-4 text-xs">
          {unique.map((g) => (
            <li key={g.statement} className="break-all">
              {t("schedule.grantWarning.missingGrant", {
                name: g.display_name,
                privilege: g.privilege.replace(/_/g, " "),
                securable: g.securable,
              })}
            </li>
          ))}
        </ul>
        <p className="text-xs font-medium">
          {t("schedule.grantWarning.missingGrantsInstructions")}
        </p>
        <pre className="overflow-x-auto whitespace-pre-wrap break-words rounded-md border border-amber-500/40 bg-background/60 p-3 font-mono text-xs leading-relaxed">
          {unique.map((g) => g.statement).join("\n")}
        </pre>
        <Button
          size="sm"
          variant="outline"
          onClick={preflight.retry}
          disabled={preflight.isFetching}
        >
          {t("schedule.grantWarning.retry")}
        </Button>
      </div>
    </div>
  );
}

/** Neutral notice for tables whose access couldn't be checked: the SQL warehouse
 *  didn't answer (usually still starting), or the permissions couldn't be read.
 *  Not a denial, so it offers a retry instead of naming MANAGE holders. */
function UnverifiedNotice({
  entity,
  preflight,
  tables,
  reason,
}: Props & {
  tables: SchedulePreflightTableOut[];
  reason: "warehouse" | "permissions";
}) {
  const { t } = useTranslation();
  const single = entity === "table";
  const copy =
    reason === "permissions"
      ? {
          title: t("schedule.grantWarning.unverifiedPermissionsTitle"),
          description: single
            ? t("schedule.grantWarning.unverifiedPermissionsDescriptionTable")
            : t(
                "schedule.grantWarning.unverifiedPermissionsDescriptionCollection",
              ),
        }
      : {
          title: t("schedule.grantWarning.unverifiedTitle"),
          description: single
            ? t("schedule.grantWarning.unverifiedDescriptionTable")
            : t("schedule.grantWarning.unverifiedDescriptionCollection"),
        };
  return (
    <div role="status" className="rounded-lg border bg-muted/40 p-4 space-y-3">
      <div className="flex items-start gap-2">
        <Info className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
        <div className="space-y-1">
          <h4 className="text-sm font-medium">{copy.title}</h4>
          <p className="text-xs text-muted-foreground">{copy.description}</p>
          {!single && (
            <ul className="list-disc pl-4 text-xs text-muted-foreground">
              {tables.map((tbl) => (
                <li key={tbl.fqn} className="break-all">
                  {tbl.fqn}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
      <div className="pl-6">
        <Button
          size="sm"
          variant="outline"
          onClick={preflight.retry}
          disabled={preflight.isFetching}
        >
          {t("schedule.grantWarning.retry")}
        </Button>
      </div>
    </div>
  );
}

function BlockedNotice({
  entity,
  blockedTables,
}: {
  entity: Entity;
  blockedTables: SchedulePreflightTableOut[];
}) {
  const { t } = useTranslation();
  const single = entity === "table";

  return (
    <div
      role="alert"
      className="rounded-lg border border-amber-500/50 bg-amber-500/10 p-4 space-y-3 text-amber-900 dark:text-amber-100"
    >
      <div className="flex items-start gap-2">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
        <div className="space-y-1">
          <h4 className="text-sm font-medium">
            {t("schedule.grantWarning.title")}
          </h4>
          <p className="text-xs text-amber-800 dark:text-amber-200/90">
            {single
              ? t("schedule.grantWarning.descriptionTable")
              : t("schedule.grantWarning.descriptionCollection")}
          </p>
        </div>
      </div>

      {single ? (
        <div className="space-y-1.5 pl-6">
          <p className="text-xs font-medium">
            {t("schedule.grantWarning.holdersLabel")}
          </p>
          <Holders holders={blockedTables[0]?.manage_holders} />
        </div>
      ) : (
        <div className="space-y-3 pl-6">
          {blockedTables.map((tbl) => (
            <div key={tbl.fqn} className="space-y-1.5">
              <p className="text-xs font-medium break-all">
                {t("schedule.grantWarning.holdersLabelForTable", {
                  fqn: tbl.fqn,
                })}
              </p>
              <Holders holders={tbl.manage_holders} />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
