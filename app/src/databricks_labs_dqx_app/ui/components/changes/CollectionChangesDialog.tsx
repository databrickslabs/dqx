import { useTranslation } from "react-i18next";
import { useGetDataProductChanges } from "@/lib/api";
import { ChangesDialog, useChangesSides, type ChangeItem } from "@/components/changes/ChangesDialog";

/**
 * View changes for a collection: its settings (name, description, owner) and
 * each member table with its version pin, compared by the server between the
 * approved version and the current members, or between any two approved
 * versions.
 */
export interface CollectionChangesTarget {
  productId: string;
  name: string;
}

const SETTINGS_KEY = "settings";

export function CollectionChangesDialog({
  target,
  onClose,
}: {
  target: CollectionChangesTarget | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const productId = target?.productId ?? "";

  // The server picks the default comparison (pending changes when there are any).
  const defaults = useGetDataProductChanges(productId, undefined, { query: { enabled: !!target } });
  const info = defaults.data?.data;
  const approved = info?.approved_version ?? 0;
  const hasPending = !!info?.has_pending_changes;
  const sides = useChangesSides(productId, hasPending, approved);
  const params =
    sides.mode === "pending"
      ? { base: approved }
      : { base: sides.base, target: sides.target > 0 ? sides.target : undefined };
  const query = useGetDataProductChanges(productId, params, {
    query: { enabled: !!target && !!info && (sides.mode === "pending" || sides.target > 0) },
  });

  const data = query.data?.data;
  const items: ChangeItem[] = (data?.changes ?? []).map((c) => ({
    key: c.key,
    name: c.name,
    status: c.status,
    before: c.before ?? [],
    after: c.after ?? [],
  }));
  const unrecorded = data && !data.base_recorded;
  const note = unrecorded
    ? t(data.target_version == null ? "tableChanges.notRecordedPending" : "tableChanges.notRecordedHistory", {
        version: data.base_version,
      })
    : null;

  return (
    <ChangesDialog
      open={!!target}
      onClose={onClose}
      subject={target?.name ?? ""}
      sides={sides}
      hasPending={hasPending}
      approvedVersion={approved}
      versions={(info?.versions ?? []).map((v) => v.version)}
      items={items}
      labelFor={(item) => (item.key === SETTINGS_KEY ? t("tableChanges.collectionSettings") : item.name)}
      note={note}
      loading={defaults.isLoading || query.isLoading}
      error={defaults.isError || query.isError}
      neverApprovedText={t("tableChanges.collectionNeverApproved")}
      noChangesText={t("tableChanges.noChangesCollection")}
      footer={
        approved > 0 ? t("tableChanges.collectionRunsUse", { version: approved }) : t("tableChanges.neverApprovedShort")
      }
    />
  );
}
