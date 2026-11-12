import { useTranslation } from "react-i18next";
import { useGetDataProductChanges, useListDataProductHistory } from "@/lib/api";
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
 * View changes for a collection: its settings (name, description, owner) and
 * each member table with its version pin, compared by the server between the
 * approved version and the current members, or between any two approved
 * versions. Shown in a dialog or in the History tab.
 */
export interface CollectionChangesTarget {
  productId: string;
  name: string;
}

const SETTINGS_KEY = "settings";

function useCollectionChanges(
  productId: string,
  enabled: boolean,
  historyTab = false,
  revision?: string,
): ChangesViewProps {
  const { t } = useTranslation();
  // The server picks the default comparison (pending changes when there are any).
  const defaults = useGetDataProductChanges(productId, undefined, { query: { enabled, staleTime: LIVE_STALE_TIME } });
  const info = defaults.data?.data;
  const approved = info?.approved_version ?? 0;
  const hasPending = !!info?.has_pending_changes;
  const sides = useChangesSides(productId, hasPending && !historyTab, approved);
  const params = sideParams(sides.base, sides.target);
  const query = useGetDataProductChanges(productId, params, {
    query: {
      // A version-0 target is nothing approved yet: there's nothing to compare until a draft is picked.
      enabled: enabled && !!info && !(sides.target.kind === "version" && sides.target.version === 0),
      staleTime: LIVE_STALE_TIME,
    },
  });

  // Nothing on the target side (nothing approved yet, no draft picked) is nothing to compare.
  const shown = sides.target.kind === "version" && sides.target.version === 0 ? undefined : query.data?.data;
  const reviewEvents = useListDataProductHistory(productId, {
    query: { enabled, staleTime: LIVE_STALE_TIME },
  });
  useRefetchOnRevision(revision, () => {
    void defaults.refetch();
    if (info) void query.refetch();
    void reviewEvents.refetch();
  });
  const data = query.data?.data;
  const changes = shown?.changes ?? [];
  const items: ChangeItem[] = changes.map((c) => ({
    key: c.key,
    name: c.name,
    status: c.status,
    before: c.before ?? [],
    after: c.after ?? [],
  }));
  const note =
    data && !data.base_recorded
      ? t(data.target_version == null ? "tableChanges.notRecordedPending" : "tableChanges.notRecordedHistory", {
          version: data.base_version,
        })
      : null;
  return {
    sides,
    hasPending,
    approvedVersion: approved,
    versions: (info?.versions ?? []).map((v) => ({
      version: v.version,
      createdBy: v.created_by,
      createdAt: v.created_at,
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
    labelFor: (item) => (item.key === SETTINGS_KEY ? t("tableChanges.collectionSettings") : item.name),
    note,
    loading: defaults.isLoading || query.isLoading,
    timelineLoading: defaults.isLoading || reviewEvents.isLoading,
    error: defaults.isError || query.isError,
    neverApprovedText: t("tableChanges.collectionNeverApproved"),
    noChangesText: t("tableChanges.noChanges"),
  };
}

export function CollectionChangesDialog({
  target,
  onClose,
}: {
  target: CollectionChangesTarget | null;
  onClose: () => void;
}) {
  const view = useCollectionChanges(target?.productId ?? "", !!target);
  return <ChangesDialog open={!!target} onClose={onClose} subject={target?.name ?? ""} {...view} />;
}

/** The collection page's History tab: the version timeline beside the changes. */
export function CollectionChangesPanel({ productId, revision }: { productId: string; revision?: string }) {
  const view = useCollectionChanges(productId, true, true, revision);
  return <ChangesPanel {...view} />;
}
