// AddRulesDialog — apply a PUBLISHED registry rule to a monitored table.
// Step 1 lists published (approved) registry rules, searchable by name/id.
// Step 2 maps every one of the selected rule's {{slots}} to a real table
// column via a family-filtered ColumnPicker. Submitting stages the
// application on the binding (useApplyRuleToTable) — it does not touch the
// live checks until the table is published.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Loader2, Search } from "lucide-react";
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
import { HelpTooltip } from "@/components/HelpTooltip";
import { MultiColumnPicker, SingleColumnPicker } from "./ColumnPicker";
import { RESERVED_DIMENSION_KEY, RESERVED_NAME_KEY, RESERVED_SEVERITY_KEY, TagBadge, colorFor, extractApiError, getTag } from "./shared";

interface AddRulesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  tableFqn: string;
  publishedRules: RegistryRuleOut[];
  labelDefinitions: LabelDefinition[];
  onApplied: () => void;
}

export function AddRulesDialog({
  open,
  onOpenChange,
  bindingId,
  tableFqn,
  publishedRules,
  labelDefinitions,
  onApplied,
}: AddRulesDialogProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const [selectedRule, setSelectedRule] = useState<RegistryRuleOut | null>(null);
  const [mapping, setMapping] = useState<Record<string, string | string[]>>({});
  const [createOpen, setCreateOpen] = useState(false);
  const queryClient = useQueryClient();

  const parts = tableFqn.split(".");
  const columnsQuery = useGetTableColumns(parts[0] ?? "", parts[1] ?? "", parts[2] ?? "", {
    query: { enabled: open && parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  const filteredRules = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return publishedRules;
    return publishedRules.filter((r) => {
      const name = getTag(r, RESERVED_NAME_KEY).toLowerCase();
      return name.includes(q) || r.rule_id.toLowerCase().includes(q);
    });
  }, [publishedRules, search]);

  const applyMutation = useApplyRuleToTable();

  const reset = () => {
    setSelectedRule(null);
    setMapping({});
    setSearch("");
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
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>{t("monitoredTables.addRuleDialogTitle")}</DialogTitle>
            <DialogDescription>{t("monitoredTables.addRuleDialogDescription")}</DialogDescription>
          </DialogHeader>

          {!selectedRule ? (
            <div className="space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {t("monitoredTables.stepSelectRule")}
              </p>
              <div className="relative">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder={t("monitoredTables.searchRulesPlaceholder")}
                  className="pl-7 h-8 text-xs"
                />
              </div>
              <div className="max-h-72 overflow-y-auto border rounded-md divide-y">
                {filteredRules.length === 0 ? (
                  <p className="text-sm text-muted-foreground text-center py-6">
                    {t("monitoredTables.noPublishedRules")}
                  </p>
                ) : (
                  filteredRules.map((rule) => {
                    const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;
                    const dimension = getTag(rule, RESERVED_DIMENSION_KEY);
                    const severity = getTag(rule, RESERVED_SEVERITY_KEY);
                    return (
                      <button
                        key={rule.rule_id}
                        type="button"
                        onClick={() => setSelectedRule(rule)}
                        className="w-full text-left p-3 hover:bg-muted/40 transition-colors"
                      >
                        <p className="text-sm font-medium">{name}</p>
                        <div className="flex flex-wrap gap-1 mt-1">
                          <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
                          <TagBadge label={severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />
                        </div>
                      </button>
                    );
                  })
                )}
              </div>
              <Button variant="outline" size="sm" className="gap-2 w-full" onClick={openCreateRule}>
                {t("monitoredTables.createNewRuleButton")}
              </Button>
              <p className="text-xs text-muted-foreground">{t("monitoredTables.createNewRuleHint")}</p>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="flex items-center gap-1.5">
                <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {t("monitoredTables.stepMapColumns")}
                </p>
                <HelpTooltip text={t("monitoredTables.mapColumnsTooltip")} />
              </div>
              <p className="text-sm font-medium">{getTag(selectedRule, RESERVED_NAME_KEY) || selectedRule.rule_id}</p>
              <div className="space-y-3">
                {slots.map((slot) => {
                  if (slot.cardinality === "many") {
                    const selected = (mapping[slot.name] as string[] | undefined) ?? [];
                    return (
                      <div key={slot.name} className="space-y-1.5">
                        <p className="text-sm font-medium">
                          {t("monitoredTables.slotColumnsLabel", { slot: slot.name })}
                        </p>
                        <MultiColumnPicker
                          slot={slot}
                          columns={columns}
                          value={selected}
                          onChange={(next) => setMapping((m) => ({ ...m, [slot.name]: next }))}
                        />
                      </div>
                    );
                  }
                  return (
                    <div key={slot.name} className="space-y-1.5">
                      <p className="text-sm font-medium">
                        {t("monitoredTables.slotColumnLabel", { slot: slot.name })}
                      </p>
                      <SingleColumnPicker
                        slot={slot}
                        columns={columns}
                        value={mapping[slot.name] as string | undefined}
                        onChange={(v) => setMapping((m) => ({ ...m, [slot.name]: v }))}
                      />
                    </div>
                  );
                })}
              </div>
              {!mappingComplete && (
                <p className="text-xs text-amber-600">{t("monitoredTables.mappingIncomplete")}</p>
              )}
            </div>
          )}

          <DialogFooter>
            {selectedRule && (
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
