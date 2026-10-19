import { useTranslation } from "react-i18next";
import { useGetMonitoredTableChanges, useListMonitoredTableVersions } from "@/lib/api";
import {
  ChangesDialog,
  ChangesPanel,
  useChangesSides,
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

function useTableChanges(bindingId: string, enabled: boolean): ChangesViewProps {
  const { t } = useTranslation();
  // The server picks the default comparison (pending changes when there are any).
  const defaults = useGetMonitoredTableChanges(bindingId, undefined, { query: { enabled } });
  const info = defaults.data?.data;
  const approved = info?.approved_version ?? 0;
  const hasPending = !!info?.has_pending_changes;
  const sides = useChangesSides(bindingId, hasPending, approved);
  const params =
    sides.mode === "pending"
      ? { base: approved }
      : { base: sides.base, target: sides.target > 0 ? sides.target : undefined };
  const query = useGetMonitoredTableChanges(bindingId, params, {
    query: { enabled: enabled && !!info && (sides.mode === "pending" || sides.target > 0) },
  });
  const versions = useListMonitoredTableVersions(bindingId, { query: { enabled } });

  const items: ChangeItem[] = (query.data?.data?.changes ?? []).map((c) => ({
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
    items,
    loading: defaults.isLoading || query.isLoading,
    error: defaults.isError || query.isError,
    neverApprovedText: t("tableChanges.neverApproved"),
    noChangesText: t("tableChanges.noChanges"),
  };
}

export function TableChangesDialog({ target, onClose }: { target: TableChangesTarget | null; onClose: () => void }) {
  const view = useTableChanges(target?.bindingId ?? "", !!target);
  return <ChangesDialog open={!!target} onClose={onClose} subject={target?.name ?? ""} {...view} />;
}

/** The table page's History tab: the version timeline beside the changes. */
export function TableChangesPanel({ bindingId }: { bindingId: string }) {
  const view = useTableChanges(bindingId, true);
  return <ChangesPanel resetKey={bindingId} {...view} />;
}
