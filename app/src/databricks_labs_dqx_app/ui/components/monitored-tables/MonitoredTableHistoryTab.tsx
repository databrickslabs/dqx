import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Loader2 } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import { useListMonitoredTableVersions, type MonitoredTableVersionOut } from "@/lib/api";

/**
 * History tab for the monitored-table detail page — ported from dqlake's
 * `BindingHistoryTab` (right-aligned next to Schedule in the tab strip).
 *
 * dqlake renders a full audit feed (CREATE/UPDATE/PUBLISH entries with
 * per-field diffs); DQX tracks frozen `dq_monitored_table_versions`
 * snapshots only (one per table approval, re-frozen in place on rule-level
 * changes), so the feed here is the version lineage: one card per approved
 * version, newest first, with the actor, relative timestamp, a re-freeze
 * marker when the snapshot was rewritten in place, and a dqlake-style
 * "Show snapshot" toggle exposing the frozen applied-rules metadata
 * (`state_json`) — the analogue of dqlake's "Show full code".
 */
function VersionEntryCard({ entry }: { entry: MonitoredTableVersionOut }) {
  const { t } = useTranslation();
  const [showSnapshot, setShowSnapshot] = useState(false);
  const stateJson = (entry.state_json ?? {}) as Record<string, unknown>;
  const hasSnapshot = Object.keys(stateJson).length > 0;

  return (
    <Card>
      <CardContent className="p-4 text-sm space-y-2">
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-center gap-2">
            {/* Approval is the only event that mints a version — dqlake's
                PUBLISH bucket color (sky) is the closest semantic match. */}
            <span className="font-mono text-xs uppercase tracking-[0.12em] font-semibold text-sky-600 dark:text-sky-400">
              {t("monitoredTables.historyActionApproved")}
            </span>
            <span className="text-[10px] font-mono text-muted-foreground uppercase tracking-wide">
              {t("monitoredTables.versionBadge", { version: entry.version })}
            </span>
          </div>
          <span className="text-[11px] font-mono text-muted-foreground shrink-0">
            <RelativeTimeCell iso={entry.created_at} />
          </span>
        </div>
        {entry.created_by && (
          <div className="font-mono text-xs text-muted-foreground">{entry.created_by}</div>
        )}
        {entry.refrozen_at && (
          <div className="text-xs text-amber-600 dark:text-amber-400 flex items-center gap-1.5">
            <span>{t("monitoredTables.historyRefrozenLabel")}</span>
            <span className="font-mono text-[11px]">
              <RelativeTimeCell iso={entry.refrozen_at} />
            </span>
          </div>
        )}
        {hasSnapshot && (
          <button
            type="button"
            onClick={() => setShowSnapshot((cur) => !cur)}
            className="text-[11px] font-mono text-muted-foreground hover:text-foreground underline-offset-2 hover:underline"
          >
            {showSnapshot
              ? t("monitoredTables.historyHideSnapshot")
              : t("monitoredTables.historyShowSnapshot")}
          </button>
        )}
        {showSnapshot && hasSnapshot && (
          <pre className="font-mono text-xs leading-relaxed rounded border bg-muted/30 p-2 whitespace-pre-wrap break-all overflow-auto max-h-96">
            {JSON.stringify(stateJson, null, 2)}
          </pre>
        )}
      </CardContent>
    </Card>
  );
}

export function MonitoredTableHistoryTab({ bindingId }: { bindingId: string }) {
  const { t } = useTranslation();
  const versionsQuery = useListMonitoredTableVersions(bindingId);
  // Newest first — the backend orders by version DESC.
  const versions = versionsQuery.data?.data ?? [];

  if (versionsQuery.isLoading) {
    return (
      <p className="text-xs text-muted-foreground flex items-center gap-2 pt-4">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        {t("common.loading")}
      </p>
    );
  }

  if (versions.length === 0) {
    return <p className="text-sm text-muted-foreground pt-4">{t("monitoredTables.historyEmpty")}</p>;
  }

  return (
    <div className="space-y-3 max-w-3xl pt-4">
      {versions.map((v) => (
        <VersionEntryCard key={v.version} entry={v} />
      ))}
    </div>
  );
}
