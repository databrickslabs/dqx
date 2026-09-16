/** Yellow warning card shown above the schedule settings when the current user
 *  cannot grant the scheduler read access to the table(s) a schedule would run
 *  against (Task 12). Scheduled runs read the source table as a service account,
 *  so setting up a schedule requires the MANAGE privilege (or ownership) to
 *  grant that account SELECT. When the user lacks it we hard-block the save and
 *  render this card naming the users/groups that DO hold MANAGE, asking the user
 *  to have one of them set the schedule up instead.
 *
 *  Rendered via the `ScheduleEditor` `banner` slot so it sits above the first
 *  schedule setting on both the table page and the collection page.
 */
import { useTranslation } from "react-i18next";
import { AlertTriangle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { SchedulePreflightTableOut } from "@/lib/api";

interface Props {
  /** Which entity is being scheduled — drives the description wording. */
  entity: "table" | "collection";
  /** The tables the caller cannot grant on (each with its MANAGE holders). */
  blockedTables: SchedulePreflightTableOut[];
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
          {h.principal}
          <span className="ml-1 text-[10px] uppercase tracking-wide text-amber-600/80 dark:text-amber-300/70">
            {holderTypeLabel(h.type, t)}
          </span>
        </Badge>
      ))}
    </div>
  );
}

export function ScheduleGrantWarning({ entity, blockedTables }: Props) {
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
