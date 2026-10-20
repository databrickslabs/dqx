/** Yellow warning card shown above the schedule settings when the current user
 *  cannot grant the scheduler read access to the table(s) a schedule would run
 *  against (Task 12). Scheduled runs read the source table as a service principal,
 *  so setting up a schedule requires the MANAGE privilege (or ownership) to
 *  grant that account SELECT. When the user lacks it we hard-block the save and
 *  render this card naming the users/groups that DO hold MANAGE, asking the user
 *  to have one of them set the schedule up instead.
 *
 *  When Studio's service principals can't use a table's catalog or schema,
 *  nobody can schedule it until those grants exist, so a separate card lists
 *  the missing grants (by service principal name) and the GRANT statements.
 *
 *  Tables whose access couldn't be checked get a separate neutral notice with a
 *  retry instead — that is not a denial. Its wording follows the reason: the SQL
 *  warehouse didn't answer, the permissions couldn't be read, or Studio's setup
 *  is incomplete.
 *
 *  Rendered via the `ScheduleEditor` `banner` slot so it sits above the first
 *  schedule setting on both the table page and the collection page.
 */
import { useTranslation } from "react-i18next";
import { AlertTriangle, Info } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { SqlCodeBlock } from "@/components/common/SqlCodeBlock";
import { GrantOutcomeNotice } from "@/components/setup/GrantOutcomeNotice";
import type { SchedulePreflightTableOut } from "@/lib/api";
import { grantStatementsSql } from "@/lib/setup-grants";
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
  if (type === "service_principal") return t("schedule.grantWarning.typeServicePrincipal");
  if (type === "group") return t("schedule.grantWarning.typeGroup");
  return t("schedule.grantWarning.typeUser");
}

function Holders({ holders }: { holders: SchedulePreflightTableOut["manage_holders"] }) {
  const { t } = useTranslation();
  const list = holders ?? [];
  if (list.length === 0) {
    return <p className="text-xs text-amber-700 dark:text-amber-300/90">{t("schedule.grantWarning.noHolders")}</p>;
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

type UnverifiedReason = "warehouse" | "permissions" | "configuration";

const UNVERIFIED_REASONS: UnverifiedReason[] = ["configuration", "permissions", "warehouse"];

export function ScheduleGrantWarning({ entity, preflight }: Props) {
  const reasonOf = (tbl: SchedulePreflightTableOut): UnverifiedReason => tbl.unverified_reason ?? "warehouse";
  return (
    <div className="space-y-3">
      {preflight.missingGrantTables.length > 0 && <MissingGrantsNotice entity={entity} preflight={preflight} />}
      {preflight.blockedTables.length > 0 && <BlockedNotice entity={entity} blockedTables={preflight.blockedTables} />}
      {UNVERIFIED_REASONS.map((reason) => {
        const tables = preflight.unverifiedTables.filter((tbl) => reasonOf(tbl) === reason);
        return (
          tables.length > 0 && (
            <UnverifiedNotice key={reason} entity={entity} preflight={preflight} tables={tables} reason={reason} />
          )
        );
      })}
    </div>
  );
}

/** One line per missing grant, named by service principal display name. */
function MissingGrantList({ grants }: { grants: NonNullable<SchedulePreflightTableOut["missing_grants"]> }) {
  const { t } = useTranslation();
  return (
    <ul className="list-disc pl-4 text-xs">
      {grants.map((g) => (
        <li key={g.statement} className="break-all">
          {t("schedule.grantWarning.missingGrant", {
            name: g.display_name,
            privilege: g.privilege.replace(/_/g, " "),
            securable: g.securable,
          })}
        </li>
      ))}
    </ul>
  );
}

/** Amber card for tables whose catalog or schema Studio's service principals
 *  can't use yet. Lists the missing grants (per table for a collection), then
 *  every distinct GRANT statement once. The user can copy them for an owner,
 *  or run them with their own Databricks access. */
function MissingGrantsNotice({ entity, preflight }: Props) {
  const { t } = useTranslation();
  const single = entity === "table";
  const tables = preflight.missingGrantTables;
  const statements = [...new Set(tables.flatMap((tbl) => (tbl.missing_grants ?? []).map((g) => g.statement)))];
  return (
    <div
      role="alert"
      className="rounded-lg border border-amber-500/50 bg-amber-500/10 p-4 space-y-3 text-amber-900 dark:text-amber-100"
    >
      <div className="flex items-start gap-2">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
        <div className="space-y-1">
          <h4 className="text-sm font-medium">{t("schedule.grantWarning.missingGrantsTitle")}</h4>
          <p className="text-xs text-amber-800 dark:text-amber-200/90">
            {single
              ? t("schedule.grantWarning.missingGrantsDescriptionTable")
              : t("schedule.grantWarning.missingGrantsDescriptionCollection")}
          </p>
        </div>
      </div>
      <div className="space-y-2 pl-6">
        {single ? (
          <MissingGrantList grants={tables[0]?.missing_grants ?? []} />
        ) : (
          tables.map((tbl) => (
            <div key={tbl.fqn} className="space-y-1">
              <p className="text-xs font-medium break-all">
                {t("schedule.grantWarning.missingGrantsForTable", { fqn: tbl.fqn })}
              </p>
              <MissingGrantList grants={tbl.missing_grants ?? []} />
            </div>
          ))
        )}
        <p className="text-xs font-medium">{t("schedule.grantWarning.missingGrantsInstructions")}</p>
        <div className="text-foreground">
          <SqlCodeBlock
            sql={grantStatementsSql(statements)}
            onRun={preflight.applyGrants}
            runLabel={t("schedule.grantWarning.apply.run")}
            isRunning={preflight.isApplying}
            runDisabled={preflight.isFetching}
            result={
              (preflight.recheckFailed || preflight.applyOutcome) && (
                <div className="space-y-2">
                  {preflight.recheckFailed && (
                    <p role="status" className="rounded-md border bg-muted/40 p-3 text-xs text-muted-foreground">
                      {t("schedule.grantWarning.apply.recheckFailed", preflight.recheckFailed)}
                    </p>
                  )}
                  {preflight.applyOutcome && (
                    <GrantOutcomeNotice
                      outcome={preflight.applyOutcome}
                      summaryKey="schedule.grantWarning.apply.partial"
                      messageKey={(code) => `schedule.grantWarning.apply.errors.${code}`}
                    />
                  )}
                </div>
              )
            }
          />
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={preflight.retry}
          disabled={preflight.isFetching || preflight.isApplying}
        >
          {t("schedule.grantWarning.retry")}
        </Button>
      </div>
    </div>
  );
}

/** Neutral notice for tables whose access couldn't be checked: the SQL warehouse
 *  didn't answer (usually still starting), the permissions couldn't be read, or
 *  Studio's setup is incomplete. Not a denial, so it offers a retry instead of
 *  naming MANAGE holders. */
function UnverifiedNotice({
  entity,
  preflight,
  tables,
  reason,
}: Props & { tables: SchedulePreflightTableOut[]; reason: UnverifiedReason }) {
  const { t } = useTranslation();
  const single = entity === "table";
  const copy = {
    warehouse: {
      title: t("schedule.grantWarning.unverifiedTitle"),
      table: t("schedule.grantWarning.unverifiedDescriptionTable"),
      collection: t("schedule.grantWarning.unverifiedDescriptionCollection"),
    },
    permissions: {
      title: t("schedule.grantWarning.unverifiedPermissionsTitle"),
      table: t("schedule.grantWarning.unverifiedPermissionsDescriptionTable"),
      collection: t("schedule.grantWarning.unverifiedPermissionsDescriptionCollection"),
    },
    configuration: {
      title: t("schedule.grantWarning.unverifiedConfigurationTitle"),
      table: t("schedule.grantWarning.unverifiedConfigurationDescription"),
      collection: t("schedule.grantWarning.unverifiedConfigurationDescription"),
    },
  }[reason];
  return (
    <div role="status" className="rounded-lg border bg-muted/40 p-4 space-y-3">
      <div className="flex items-start gap-2">
        <Info className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
        <div className="space-y-1">
          <h4 className="text-sm font-medium">{copy.title}</h4>
          <p className="text-xs text-muted-foreground">{single ? copy.table : copy.collection}</p>
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
        <Button size="sm" variant="outline" onClick={preflight.retry} disabled={preflight.isFetching}>
          {t("schedule.grantWarning.retry")}
        </Button>
      </div>
    </div>
  );
}

function BlockedNotice({ entity, blockedTables }: { entity: Entity; blockedTables: SchedulePreflightTableOut[] }) {
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
          <h4 className="text-sm font-medium">{t("schedule.grantWarning.title")}</h4>
          <p className="text-xs text-amber-800 dark:text-amber-200/90">
            {single ? t("schedule.grantWarning.descriptionTable") : t("schedule.grantWarning.descriptionCollection")}
          </p>
        </div>
      </div>

      {single ? (
        <div className="space-y-1.5 pl-6">
          <p className="text-xs font-medium">{t("schedule.grantWarning.holdersLabel")}</p>
          <Holders holders={blockedTables[0]?.manage_holders} />
        </div>
      ) : (
        <div className="space-y-3 pl-6">
          {blockedTables.map((tbl) => (
            <div key={tbl.fqn} className="space-y-1.5">
              <p className="text-xs font-medium break-all">
                {t("schedule.grantWarning.holdersLabelForTable", { fqn: tbl.fqn })}
              </p>
              <Holders holders={tbl.manage_holders} />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
