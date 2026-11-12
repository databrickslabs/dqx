import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import {
  useGetRegistryRuleSnapshot,
  useListRegistryRuleHistory,
  useListRegistryRuleVersions,
  type RegistryRuleVersionOut,
} from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import { buildDqxCheckJson, severityValueCriticality, type MaterializableRule } from "@/lib/registry-rule-conversion";
import {
  ChangesDialog,
  ChangesPanel,
  LIVE_STALE_TIME,
  useChangesSides,
  useRefetchOnRevision,
  type ChangeItem,
  type ChangesViewProps,
} from "@/components/changes/ChangesDialog";
import type { SideRef } from "@/lib/history-points";
import { itemStatus } from "@/lib/yaml-diff";

/**
 * View changes for a registry rule: the rule's full check (function,
 * arguments, criticality, name, message, tags) compared between its approved
 * version and the proposed edit, or between any two approved versions. Shown
 * in a dialog or in the rule's History tab.
 */
export interface RuleChangesTarget {
  ruleId: string;
  name: string;
  /** The rule's approved version (0 = never approved). */
  version: number;
  /** The live, proposed rule. */
  proposed: MaterializableRule;
  /** The rule's status, version and last edit; the History tab re-reads when it changes. */
  revision?: string;
}

function useRuleChanges(target: RuleChangesTarget | null, historyTab = false): ChangesViewProps {
  const { t } = useTranslation();
  const ruleId = target?.ruleId ?? "";
  const ruleVersions = useListRegistryRuleVersions(ruleId, {
    query: { enabled: !!target, staleTime: LIVE_STALE_TIME },
  });
  const { data, isLoading, isError } = ruleVersions;
  const enabled = !!target;
  const reviewEvents = useListRegistryRuleHistory(ruleId, {
    query: { enabled, staleTime: LIVE_STALE_TIME },
  });
  useRefetchOnRevision(target?.revision, () => {
    void ruleVersions.refetch();
    void reviewEvents.refetch();
  });
  // The rendered criticality honours the admin-edited severity -> criticality
  // mapping, matching the "View JSON" dialog and the materializer.
  const { data: labelDefsData } = useLabelDefinitions();
  const severityCriticality = useMemo(() => severityValueCriticality(labelDefsData?.definitions), [labelDefsData]);
  const approved = target?.version ?? 0;

  const checks = useMemo(() => {
    // Each version renders from its own frozen mode, polarity, definition and
    // tags, so a past version shows the check as it was approved.
    const byVersion = new Map<number, Record<string, unknown>>();
    for (const v of (data?.data ?? []) as RegistryRuleVersionOut[]) {
      byVersion.set(
        v.version,
        buildDqxCheckJson(
          { mode: v.mode ?? null, polarity: v.polarity ?? null, definition: v.definition, user_metadata: v.user_metadata },
          severityCriticality,
          target?.proposed.mode ?? undefined,
        ),
      );
    }
    return byVersion;
  }, [data, severityCriticality, target]);
  const live = useMemo(
    () => (target ? buildDqxCheckJson(target.proposed, severityCriticality) : null),
    [target, severityCriticality],
  );

  const versionSide = (version: number | null): unknown[] => {
    if (version === null) return live ? [live] : [];
    const check = version > 0 ? checks.get(version) : undefined;
    return check ? [check] : [];
  };
  const hasPending = approved === 0 || itemStatus(versionSide(approved), versionSide(null)) !== "unchanged";
  const sides = useChangesSides(ruleId, hasPending && !historyTab, approved);
  // A step side is the rule as that step left it, rendered the same way as a version.
  const snapshotQuery = (ref: SideRef) =>
    ({
      query: { enabled: enabled && ref.kind === "step", staleTime: LIVE_STALE_TIME },
    }) as const;
  const baseSnapshot = useGetRegistryRuleSnapshot(
    ruleId,
    sides.base.kind === "step" ? sides.base.historyId : 0,
    snapshotQuery(sides.base),
  );
  const targetSnapshot = useGetRegistryRuleSnapshot(
    ruleId,
    sides.target.kind === "step" ? sides.target.historyId : 0,
    snapshotQuery(sides.target),
  );
  useRefetchOnRevision(target?.revision, () => {
    if (sides.base.kind === "step") void baseSnapshot.refetch();
    if (sides.target.kind === "step") void targetSnapshot.refetch();
  });
  const side = (ref: SideRef, snapshot: Record<string, unknown> | null | undefined): unknown[] => {
    if (ref.kind === "proposed") return versionSide(null);
    if (ref.kind === "version") return versionSide(ref.version);
    return snapshot
      ? [
          buildDqxCheckJson(
            snapshot as unknown as Parameters<typeof buildDqxCheckJson>[0],
            severityCriticality,
            target?.proposed.mode ?? undefined,
          ),
        ]
      : [];
  };
  const before = side(sides.base, baseSnapshot.data?.data?.snapshot);
  const after = side(sides.target, targetSnapshot.data?.data?.snapshot);
  const item: ChangeItem = { key: ruleId, name: target?.name ?? "", status: itemStatus(before, after), before, after };

  const versions = ((data?.data ?? []) as RegistryRuleVersionOut[]).map((v) => ({
    version: v.version,
    createdBy: v.created_by,
    createdAt: v.created_at,
  }));
  return {
    sides,
    hasPending,
    approvedVersion: approved,
    versions,
    events: (reviewEvents.data?.data ?? []).map((e) => ({
      action: e.action,
      actor: e.actor,
      at: e.created_at,
      note: e.rationale,
      version: e.version,
      historyId: e.history_id,
      hasSnapshot: e.has_snapshot,
    })),
    // Nothing on either side (e.g. the Created step) is nothing to show.
    items: target && (before.length > 0 || after.length > 0) ? [item] : [],
    single: true,
    loading: isLoading || baseSnapshot.isLoading || targetSnapshot.isLoading,
    timelineLoading: isLoading || reviewEvents.isLoading,
    error: isError || baseSnapshot.isError || targetSnapshot.isError,
    neverApprovedText: t("tableChanges.ruleNeverApproved"),
    noChangesText: t("tableChanges.noChanges"),
  };
}

export function RuleChangesDialog({ target, onClose }: { target: RuleChangesTarget | null; onClose: () => void }) {
  const view = useRuleChanges(target);
  return <ChangesDialog open={!!target} onClose={onClose} subject={target?.name ?? ""} {...view} />;
}

/** The rule's History tab: the version timeline beside the changes. */
export function RuleChangesPanel({ target }: { target: RuleChangesTarget }) {
  const view = useRuleChanges(target, true);
  return <ChangesPanel {...view} />;
}
