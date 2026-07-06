// AddRulesDialog — stage published registry rule(s) onto a monitored
// table's LOCAL editor state. Single step: pick one or more published
// (approved) registry rules from a searchable table and click Add. Column
// mapping no longer happens here — each rule is staged with an empty
// `column_mapping` and the caller auto-expands the rule's card in the
// by-rule lens so mapping happens there via RuleConfigCard's inline
// "+ Apply to another column" affordance. Aggregate/dataset-level rules
// with no slots need no mapping at all, so they're staged with a single
// empty mapping group and are immediately complete.
//
// Nothing here writes to the network — every add is a pure local-state
// append (see `newStagedRow`); persistence happens once, in a batch, when
// the caller hits Save-as-draft/Publish on the tab (P16-F staged editor).

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
import type { RegistryRuleOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { RulesPicker } from "./RulesPicker";
import type { ColumnRef } from "./RulesByColumn";
import { newStagedRow } from "./shared";

interface AddRulesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  publishedRules: RegistryRuleOut[];
  labelDefinitions: LabelDefinition[];
  /** Fired synchronously with the rule ids just staged — the caller appends
   *  a new local row per rule (via `onAdd`) and switches to the by-rule
   *  lens, auto-expanding those cards. */
  onApplied: (ruleIds: string[]) => void;
  /** Appends one locally-staged row per selected rule to the tab's staged
   *  row list. Pure local-state mutation — no network call. */
  onAdd: (rows: ReturnType<typeof newStagedRow>[]) => void;
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
  onAdd,
  initialColumn = null,
}: AddRulesDialogProps) {
  const { t } = useTranslation();
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

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

  const handleAdd = () => {
    if (selectedRules.length === 0) return;
    const rows = selectedRules.map((rule) => {
      const hasSlots = (rule.definition.slots ?? []).length > 0;
      // Rules with no slots need no mapping and are staged immediately
      // complete with a single empty mapping group. Slotted rules are
      // staged with an empty column_mapping so the by-rule card can
      // complete the mapping afterward.
      const columnMapping = hasSlots ? [] : [{}];
      return newStagedRow(bindingId, rule, columnMapping);
    });
    onAdd(rows);
    toast.success(t("monitoredTables.toastAppliedCount", { count: rows.length }));
    onApplied(selectedRules.map((r) => r.rule_id));
    handleClose(false);
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
          <Button onClick={handleAdd} disabled={selectedIds.size === 0} className="gap-2">
            {t("monitoredTables.addSelectedRulesButton", { count: selectedIds.size })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
