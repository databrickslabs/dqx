// AddRulesDialog — apply published registry rule(s) to a monitored table.
// Single step: pick one or more published (approved) registry rules from a
// searchable table and click Add. Column mapping no longer happens here —
// each applied rule is staged with an empty `column_mapping` (allowed by
// the backend precisely so it can be completed later) and the caller
// auto-expands the rule's card in the by-rule lens so mapping happens there
// via RuleConfigCard's inline "+ Apply to another column" affordance.
// Aggregate/dataset-level rules with no slots need no mapping at all, so
// they're applied with a single empty mapping group and are immediately
// complete.

import { useState } from "react";
import { useTranslation, Trans } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Loader2 } from "lucide-react";
import { useApplyRuleToTable, type RegistryRuleOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { RulesPicker } from "./RulesPicker";
import type { ColumnRef } from "./RulesByColumn";
import { extractApiError } from "./shared";

interface AddRulesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  publishedRules: RegistryRuleOut[];
  labelDefinitions: LabelDefinition[];
  /** Fired after every selected rule has been applied (staged), with the
   *  rule ids that were just added — the caller switches to the by-rule
   *  lens and auto-expands those cards. */
  onApplied: (ruleIds: string[]) => void;
  /**
   * When opened from the by-column lens's per-column "+ Add rule" CTA, this
   * carries the clicked column's name so the dialog can show a hint about
   * which column the user is adding a rule for. Column mapping itself now
   * happens in the by-rule card, not here.
   */
  initialColumn?: ColumnRef | null;
}

export function AddRulesDialog({
  open,
  onOpenChange,
  bindingId,
  publishedRules,
  labelDefinitions,
  onApplied,
  initialColumn = null,
}: AddRulesDialogProps) {
  const { t } = useTranslation();
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const applyMutation = useApplyRuleToTable();
  const [applying, setApplying] = useState(false);

  const reset = () => setSelectedIds(new Set());

  const toggleRule = (rule: RegistryRuleOut) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(rule.rule_id)) next.delete(rule.rule_id);
      else next.add(rule.rule_id);
      return next;
    });
  };

  const handleClose = (next: boolean) => {
    if (!next) reset();
    onOpenChange(next);
  };

  const selectedRules = publishedRules.filter((r) => selectedIds.has(r.rule_id));

  const handleAdd = async () => {
    if (selectedRules.length === 0) return;
    setApplying(true);
    const results = await Promise.allSettled(
      selectedRules.map((rule) => {
        const hasSlots = (rule.definition.slots ?? []).length > 0;
        // Rules with no slots need no mapping and can be fully applied
        // immediately with a single empty mapping group. Slotted rules are
        // staged with an empty column_mapping — the backend now allows
        // this — so the by-rule card can complete the mapping afterward.
        const column_mapping = hasSlots ? [] : [{}];
        return applyMutation.mutateAsync({ bindingId, data: { rule_id: rule.rule_id, column_mapping } });
      }),
    );
    setApplying(false);

    const appliedIds = selectedRules.filter((_, i) => results[i]?.status === "fulfilled").map((r) => r.rule_id);
    const failedCount = results.filter((r) => r.status === "rejected").length;

    if (appliedIds.length > 0) {
      toast.success(t("monitoredTables.toastAppliedCount", { count: appliedIds.length }));
      onApplied(appliedIds);
    }
    if (failedCount > 0) {
      const firstFailure = results.find((r): r is PromiseRejectedResult => r.status === "rejected");
      toast.error(extractApiError(firstFailure?.reason, t("monitoredTables.toastApplyFailed")), { duration: 6000 });
    }
    if (appliedIds.length > 0) {
      handleClose(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      {/* Widened for the mini Rules-Registry table's rule-selection view —
          matches dqlake's AddRulesDialog sizing, which is wide enough to
          show a real table view instead of a search-box list. */}
      <DialogContent className="!max-w-[min(92vw,900px)] w-[min(92vw,900px)]">
        <DialogHeader>
          <DialogTitle>{t("monitoredTables.addRuleDialogTitle")}</DialogTitle>
          <DialogDescription>{t("monitoredTables.addRuleDialogDescription")}</DialogDescription>
        </DialogHeader>

        {/* DialogContent's own p-6 already insets the whole body, but the
            picker's table fills 100% of that body width — this extra px-2
            gives the table itself a little more breathing room from the
            dialog edges than the rest of the (narrower) dialog content. */}
        <div className="space-y-3 px-2">
          {initialColumn && (
            <p className="text-xs text-muted-foreground">
              {t("monitoredTables.addRuleForColumnHint", { column: initialColumn.name })}
            </p>
          )}
          {/* A compact Rules-Registry table (checkbox rows, sortable +
              toggleable columns) rather than a plain search box — ported
              from dqlake's RulesPicker/AddRulesDialog so picking rules to
              apply means scanning a real table view. */}
          <RulesPicker
            rules={publishedRules}
            labelDefinitions={labelDefinitions}
            selectedIds={selectedIds}
            onToggle={toggleRule}
          />
          <p className="text-xs text-muted-foreground">
            <Trans
              i18nKey="monitoredTables.createRuleInlineHint"
              components={{
                link: (
                  <Link
                    to="/registry-rules/new"
                    className="underline underline-offset-2 hover:text-foreground"
                    onClick={() => handleClose(false)}
                  />
                ),
              }}
            />
          </p>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => handleClose(false)}>
            {t("common.cancel")}
          </Button>
          <Button onClick={handleAdd} disabled={selectedIds.size === 0 || applying} className="gap-2">
            {applying && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {applying
              ? t("monitoredTables.applying")
              : t("monitoredTables.addSelectedRulesButton", { count: selectedIds.size })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
