import { useTranslation } from "react-i18next";
import { useGetMonitoredTableChanges, useListMonitoredTableVersions } from "@/lib/api";
import { ChangesDialog, useChangesSides, type ChangeItem } from "@/components/changes/ChangesDialog";

/**
 * View changes for a monitored table: each applied rule is one block, compared
 * by the server between the approved version and the proposed rules, or
 * between any two approved versions.
 */
export interface TableChangesTarget {
  bindingId: string;
  name: string;
  version: number;
}

export function TableChangesDialog({ target, onClose }: { target: TableChangesTarget | null; onClose: () => void }) {
  const { t } = useTranslation();
  const bindingId = target?.bindingId ?? "";

  // The server picks the default comparison (pending changes when there are any).
  const defaults = useGetMonitoredTableChanges(bindingId, undefined, { query: { enabled: !!target } });
  const info = defaults.data?.data;
  const approved = info?.approved_version ?? 0;
  const hasPending = !!info?.has_pending_changes;
  const sides = useChangesSides(bindingId, hasPending, approved);
  const params =
    sides.mode === "pending"
      ? { base: approved }
      : { base: sides.base, target: sides.target > 0 ? sides.target : undefined };
  const query = useGetMonitoredTableChanges(bindingId, params, {
    query: { enabled: !!target && !!info && (sides.mode === "pending" || sides.target > 0) },
  });
  const versions = useListMonitoredTableVersions(bindingId, { query: { enabled: !!target && sides.mode === "history" } });

  const items: ChangeItem[] = (query.data?.data?.changes ?? []).map((c) => ({
    key: c.key,
    name: c.rule_name,
    status: c.status,
    before: c.before ?? [],
    after: c.after ?? [],
  }));

  return (
    <ChangesDialog
      open={!!target}
      onClose={onClose}
      subject={target?.name ?? ""}
      sides={sides}
      hasPending={hasPending}
      approvedVersion={approved}
      versions={(versions.data?.data ?? []).map((v) => v.version)}
      items={items}
      loading={defaults.isLoading || query.isLoading}
      error={defaults.isError || query.isError}
      neverApprovedText={t("tableChanges.neverApproved")}
      noChangesText={t("tableChanges.noChanges")}
      footer={approved > 0 ? t("tableChanges.runsUse", { version: approved }) : t("tableChanges.neverApprovedShort")}
    />
  );
}
