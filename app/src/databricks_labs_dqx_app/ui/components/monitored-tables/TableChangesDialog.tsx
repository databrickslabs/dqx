import { useTranslation } from "react-i18next";
import {
  useGetMonitoredTableChanges,
  useListMonitoredTableHistory,
  useListMonitoredTableVersions,
} from "@/lib/api";
import {
  ChangesDialog,
  ChangesPanel,
  LIVE_STALE_TIME,
  sideParams,
  useChangesSides,
  useRefetchOnRevision,
  type ChangeItem,
  type ChangesViewProps,
} from "@/components/changes/ChangesDialog";

/**
 * View changes for a monitored table: each applied rule is one block, compared
 * by the server between the approved version and the proposed rules, or
 * between any two approved versions. Shown in a dialog or in the History tab.
 */
export interface TableChangesTarget {
  bindingId: string;
  name: string;
  version: number;
}

function useTableChanges(
  bindingId: string,
  enabled: boolean,
  historyTab = false,
  revision?: string,
): ChangesViewProps {
  const { t } = useTranslation();
  // The server picks the default comparison (pending changes when there are any).
  const defaults = useGetMonitoredTableChanges(bindingId, undefined, {
    query: { enabled, staleTime: LIVE_STALE_TIME },
  });
  const info = defaults.data?.data;
  const approved = info?.approved_version ?? 0;
  const hasPending = !!info?.has_pending_changes;
  const sides = useChangesSides(bindingId, hasPending && !historyTab, approved);
  const params = sideParams(sides.base, sides.target);
  const query = useGetMonitoredTableChanges(bindingId, params, {
    query: {
      // A version-0 target is nothing approved yet: there's nothing to compare until a draft is picked.
      enabled: enabled && !!info && !(sides.target.kind === "version" && sides.target.version === 0),
      staleTime: LIVE_STALE_TIME,
    },
  });
  const versions = useListMonitoredTableVersions(bindingId, { query: { enabled, staleTime: LIVE_STALE_TIME } });
  // Nothing on the target side (nothing approved yet, no draft picked) is nothing to compare.
  const shown = sides.target.kind === "version" && sides.target.version === 0 ? undefined : query.data?.data;
  const reviewEvents = useListMonitoredTableHistory(bindingId, {
    query: { enabled, staleTime: LIVE_STALE_TIME },
  });
  useRefetchOnRevision(revision, () => {
    void defaults.refetch();
    if (info) void query.refetch();
    void versions.refetch();
    void reviewEvents.refetch();
  });

  const items: ChangeItem[] = (shown?.changes ?? []).map((c) => ({
    key: c.key,
    name: c.rule_name,
    status: c.status,
    before: c.before ?? [],
    after: c.after ?? [],
  }));
  return {
    sides,
    hasPending,
    approvedVersion: approved,
    versions: (versions.data?.data ?? []).map((v) => ({
      version: v.version,
      createdBy: v.created_by,
      createdAt: v.created_at,
      note: v.refrozen_at ? t("monitoredTables.historyRefrozenLabel") : null,
    })),
    events: (reviewEvents.data?.data ?? []).map((e) => ({
      action: e.action,
      actor: e.actor,
      at: e.created_at,
      note: e.rationale,
      version: e.version,
      historyId: e.history_id,
      hasSnapshot: e.has_snapshot,
    })),
    items,
    loading: defaults.isLoading || query.isLoading,
    timelineLoading: versions.isLoading || reviewEvents.isLoading,
    error: defaults.isError || query.isError,
    neverApprovedText: t("tableChanges.neverApproved"),
    noChangesText: t("tableChanges.noChanges"),
  };
}

export function TableChangesDialog({ target, onClose }: { target: TableChangesTarget | null; onClose: () => void }) {
  const view = useTableChanges(target?.bindingId ?? "", !!target);
  return <ChangesDialog open={!!target} onClose={onClose} subject={target?.name ?? ""} {...view} />;
}

/**
 * The table page's History tab: the version timeline beside the changes.
 * *revision* (status, version, last edit) re-reads them when the table changes.
 */
export function TableChangesPanel({ bindingId, revision }: { bindingId: string; revision?: string }) {
  const view = useTableChanges(bindingId, true, true, revision);
  return <ChangesPanel {...view} />;
}
