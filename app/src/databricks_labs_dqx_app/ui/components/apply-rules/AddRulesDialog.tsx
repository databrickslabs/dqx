// AddRulesDialog — apply a PUBLISHED registry rule to a monitored table.
// Step 1 lists published (approved) registry rules, searchable by name/id.
// Step 2 maps every one of the selected rule's {{slots}} to a real table
// column via a family-filtered ColumnPicker. Submitting stages the
// application on the binding (useApplyRuleToTable) — it does not touch the
// live checks until the table is published.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
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
import {
  useApplyRuleToTable,
  useGetTableColumns,
  getListRegistryRulesQueryKey,
  type ColumnOut,
  type RegistryRuleOut,
  type RuleSlot,
} from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { RegistryRuleFormDialog } from "@/components/RegistryRuleFormDialog";
import { columnsForSlot } from "./ColumnPicker";
import { RuleMappingCard } from "./RuleMappingCard";
import { RulesPicker } from "./RulesPicker";
import type { ColumnRef } from "./RulesByColumn";
import { RESERVED_NAME_KEY, extractApiError, getTag } from "./shared";

interface AddRulesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  tableFqn: string;
  publishedRules: RegistryRuleOut[];
  labelDefinitions: LabelDefinition[];
  onApplied: () => void;
  /**
   * When opened from the by-column lens's per-column "+ Add rule" CTA, this
   * carries the clicked column's name/family so it can be preselected for
   * every slot whose family matches once a rule is picked — this is the
   * fix for the by-column "Add rule" flow not doing anything useful.
   */
  initialColumn?: ColumnRef | null;
  /**
   * When set, the dialog skips the rule-selection step and goes straight to
   * the mapping step for this rule — used by the "+ Apply to another
   * column" affordance on an already-applied rule's card, which stages a
   * brand-new mapping group (and therefore its own applied-check entry) for
   * a rule that's already applied to the table.
   */
  presetRule?: RegistryRuleOut | null;
  /**
   * Column names `presetRule` is already mapped to (across its other
   * mapping groups) — passed through to the mapping step's column pickers
   * so the "+ Apply to another column" flow can't re-pick a column the
   * rule already covers. See `getUsedColumnsForRule`. Ignored outside the
   * preset flow.
   */
  presetExcludeColumns?: string[];
}

export function AddRulesDialog({
  open,
  onOpenChange,
  bindingId,
  tableFqn,
  publishedRules,
  labelDefinitions,
  onApplied,
  initialColumn = null,
  presetRule = null,
  presetExcludeColumns,
}: AddRulesDialogProps) {
  const { t } = useTranslation();
  const [selectedRule, setSelectedRule] = useState<RegistryRuleOut | null>(null);
  const [mapping, setMapping] = useState<Record<string, string | string[]>>({});
  const [createOpen, setCreateOpen] = useState(false);
  const queryClient = useQueryClient();

  // Preset mode ("+ Apply to another column" on an existing rule card):
  // jump straight to the mapping step with a fresh (empty) mapping every
  // time the dialog opens, skipping the rule-selection list entirely.
  useEffect(() => {
    if (open && presetRule) {
      setSelectedRule(presetRule);
      setMapping({});
    }
  }, [open, presetRule]);

  const parts = tableFqn.split(".");
  const columnsQuery = useGetTableColumns(parts[0] ?? "", parts[1] ?? "", parts[2] ?? "", {
    query: { enabled: open && parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  const applyMutation = useApplyRuleToTable();

  const reset = () => {
    setSelectedRule(null);
    setMapping({});
  };

  const selectRule = (rule: RegistryRuleOut) => {
    setSelectedRule(rule);
    if (!initialColumn) {
      setMapping({});
      return;
    }
    // Preselect every slot whose family matches the column that the
    // by-column "+ Add rule" CTA was clicked from, so the picker opens
    // already pointed at that column instead of forcing the user to
    // re-find it.
    const initial: Record<string, string | string[]> = {};
    for (const slot of rule.definition.slots ?? []) {
      const matches = columnsForSlot(columns, slot).some((c) => c.name === initialColumn.name);
      if (!matches) continue;
      initial[slot.name] = slot.cardinality === "many" ? [initialColumn.name] : initialColumn.name;
    }
    setMapping(initial);
  };

  const handleClose = (next: boolean) => {
    if (!next) reset();
    onOpenChange(next);
  };

  const slots: RuleSlot[] = selectedRule?.definition.slots ?? [];
  const mappingComplete = slots.every((slot) => {
    const v = mapping[slot.name];
    if (slot.cardinality === "many") return Array.isArray(v) && v.length > 0;
    return typeof v === "string" && v.length > 0;
  });

  const handleApply = () => {
    if (!selectedRule || !mappingComplete) return;
    const group: Record<string, string> = {};
    for (const slot of slots) {
      const v = mapping[slot.name];
      group[slot.name] = Array.isArray(v) ? v.join(",") : (v as string);
    }
    applyMutation.mutate(
      { bindingId, data: { rule_id: selectedRule.rule_id, column_mapping: [group] } },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastApplied"));
          onApplied();
          handleClose(false);
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.toastApplyFailed")), { duration: 6000 }),
      },
    );
  };

  const openCreateRule = () => {
    onOpenChange(false);
    setCreateOpen(true);
  };

  const handleCreateSaved = () => {
    queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() });
  };

  return (
    <>
      <Dialog open={open} onOpenChange={handleClose}>
        {/* Widened for the mini Rules-Registry table's rule-selection step
            (Step 1) — matches dqlake's AddRulesDialog sizing, which is wide
            enough to show a real table view instead of a search-box list.
            Step 2's mapping card is comfortable at this width too. */}
        <DialogContent className="!max-w-[min(92vw,900px)] w-[min(92vw,900px)]">
          <DialogHeader>
            <DialogTitle>
              {presetRule
                ? t("monitoredTables.addMappingDialogTitle")
                : t("monitoredTables.addRuleDialogTitle")}
            </DialogTitle>
            <DialogDescription>
              {presetRule
                ? t("monitoredTables.addMappingDialogDescription", {
                    rule: getTag(presetRule, RESERVED_NAME_KEY) || presetRule.rule_id,
                  })
                : t("monitoredTables.addRuleDialogDescription")}
            </DialogDescription>
          </DialogHeader>

          {!selectedRule ? (
            <div className="space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {t("monitoredTables.stepSelectRule")}
              </p>
              {initialColumn && (
                <p className="text-xs text-muted-foreground">
                  {t("monitoredTables.addRuleForColumnHint", { column: initialColumn.name })}
                </p>
              )}
              {/* A compact Rules-Registry table (checkbox rows, sortable +
                  toggleable columns) rather than a plain search box —
                  ported from dqlake's RulesPicker/AddRulesDialog so picking
                  a rule to apply means scanning a real table view. */}
              <RulesPicker
                rules={publishedRules}
                labelDefinitions={labelDefinitions}
                onSelect={selectRule}
              />
              <Button variant="outline" size="sm" className="gap-2 w-full" onClick={openCreateRule}>
                {t("monitoredTables.createNewRuleButton")}
              </Button>
              <p className="text-xs text-muted-foreground">{t("monitoredTables.createNewRuleHint")}</p>
            </div>
          ) : (
            <div className="space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {t("monitoredTables.stepMapColumns")}
              </p>
              {/* Same card used by the by-rule lens's applied-rule list
                  (RuleConfigCard), with live column pickers standing in for
                  MappingChips since the rule isn't applied yet — keeps the
                  pick->map flow visually consistent with the main Apply
                  Rules page instead of a bespoke form. */}
              <RuleMappingCard
                rule={selectedRule}
                columns={columns}
                mapping={mapping}
                onChange={(slotName, value) => setMapping((m) => ({ ...m, [slotName]: value }))}
                labelDefinitions={labelDefinitions}
                excludeColumns={presetRule ? presetExcludeColumns : undefined}
              />
              {!mappingComplete && (
                <p className="text-xs text-amber-600">{t("monitoredTables.mappingIncomplete")}</p>
              )}
            </div>
          )}

          <DialogFooter>
            {selectedRule && !presetRule && (
              <Button variant="outline" onClick={() => setSelectedRule(null)}>
                {t("monitoredTables.backButton")}
              </Button>
            )}
            <Button variant="outline" onClick={() => handleClose(false)}>
              {t("common.cancel")}
            </Button>
            {selectedRule && (
              <Button onClick={handleApply} disabled={!mappingComplete || applyMutation.isPending} className="gap-2">
                {applyMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                {applyMutation.isPending ? t("monitoredTables.applying") : t("monitoredTables.applyButton")}
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <RegistryRuleFormDialog
        open={createOpen}
        onOpenChange={(next) => {
          setCreateOpen(next);
          if (!next) onOpenChange(true);
        }}
        editingRule={null}
        viewingRule={null}
        labelDefinitions={labelDefinitions}
        onSaved={handleCreateSaved}
      />
    </>
  );
}
