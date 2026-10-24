import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useListRegistryRuleReviewEvents, useListRegistryRuleVersions, type RegistryRuleVersionOut } from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import { buildDqxCheckJson, severityValueCriticality, type MaterializableRule } from "@/lib/registry-rule-conversion";
import {
  ChangesDialog,
  ChangesPanel,
  useChangesSides,
  type ChangeItem,
  type ChangesViewProps,
} from "@/components/changes/ChangesDialog";
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
}

function useRuleChanges(target: RuleChangesTarget | null, withEvents = false): ChangesViewProps {
  const { t } = useTranslation();
  const ruleId = target?.ruleId ?? "";
  const { data, isLoading, isError } = useListRegistryRuleVersions(ruleId, { query: { enabled: !!target } });
  const enabled = !!target;
  const reviewEvents = useListRegistryRuleReviewEvents(ruleId, { query: { enabled: enabled && withEvents } });
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

  const side = (version: number | null): unknown[] => {
    if (version === null) return live ? [live] : [];
    const check = version > 0 ? checks.get(version) : undefined;
    return check ? [check] : [];
  };
  const hasPending = approved === 0 || itemStatus(side(approved), side(null)) !== "unchanged";
  const sides = useChangesSides(ruleId, hasPending, approved);
  const [base, to] = sides.mode === "pending" ? [approved, null] : [sides.base, sides.target];
  const before = side(base);
  const after = side(to);
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
    })),
    items: target ? [item] : [],
    single: true,
    loading: isLoading,
    error: isError,
    neverApprovedText: t("tableChanges.ruleNeverApproved"),
    noChangesText: t("tableChanges.noChangesItem"),
  };
}

export function RuleChangesDialog({ target, onClose }: { target: RuleChangesTarget | null; onClose: () => void }) {
  const view = useRuleChanges(target);
  return <ChangesDialog open={!!target} onClose={onClose} subject={target?.name ?? ""} {...view} />;
}

/** The rule's History tab: the version timeline beside the changes. */
export function RuleChangesPanel({ target }: { target: RuleChangesTarget }) {
  const view = useRuleChanges(target, true);
  return <ChangesPanel resetKey={target.ruleId} {...view} />;
}
