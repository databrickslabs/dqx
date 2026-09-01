import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Loader2 } from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { useTableScopePicker, TableScopePickerFields } from "@/components/monitored-tables/TableScopePicker";
import {
  useBulkRegisterMonitoredTables,
  useApplyRuleToTable,
  getGetTableColumnsQueryOptions,
  getListMonitoredTablesQueryOptions,
  type RegistryRuleOut,
} from "@/lib/api";
import { buildSlotMapping } from "@/lib/slot-mapping";
import { invalidateResultsAfterRuleApplicationChange } from "@/lib/results-invalidation";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

/** Splits a 3-part "catalog.schema.table" FQN into its parts. */
function splitFqn(fqn: string): [string, string, string] {
  const parts = fqn.split(".");
  return [parts[0] ?? "", parts[1] ?? "", parts[2] ?? ""];
}

interface ApplyRuleModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  rule: RegistryRuleOut;
  onApplied: () => void;
}

/**
 * Light wrapper over the shared {@link useTableScopePicker} table picker
 * (Rules Registry Phase 7D-d): pick tables, bulk-register/monitor them,
 * then apply this rule to each via a best-effort name-matched
 * slot->column mapping ({@link buildSlotMapping}). Tables where a slot
 * can't be auto-mapped are skipped and counted in the summary toast —
 * this modal deliberately does not offer a manual per-table mapping UI;
 * finishing those is done from each table's own Apply Rules tab.
 */
export function ApplyRuleModal({ open, onOpenChange, rule, onApplied }: ApplyRuleModalProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [pendingFqns, setPendingFqns] = useState<string[] | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const picker = useTableScopePicker(open);
  const { effectiveFqns, reset: resetPicker } = picker;

  const bulkMutation = useBulkRegisterMonitoredTables();
  const applyMutation = useApplyRuleToTable();

  const handleClose = useCallback(
    (next: boolean) => {
      if (!next && !isProcessing) {
        resetPicker();
        setPendingFqns(null);
      }
      onOpenChange(next);
    },
    [isProcessing, onOpenChange, resetPicker],
  );

  const runApply = useCallback(
    async (fqns: string[]) => {
      setIsProcessing(true);
      try {
        const bulkResp = await bulkMutation.mutateAsync({ data: { table_fqns: fqns } });
        const monitoredFqns = [
          ...(bulkResp.data.registered ?? []),
          ...(bulkResp.data.skipped_existing ?? []),
        ];
        if (monitoredFqns.length === 0) {
          toast.error(t("monitoredTables.toastRegisterFailed"), { duration: 6000 });
          return;
        }

        const allMonitored = await queryClient.fetchQuery(getListMonitoredTablesQueryOptions({}));
        const bindingIdByFqn = new Map(
          (allMonitored.data ?? []).map((summary) => [summary.table.table_fqn, summary.table.binding_id]),
        );

        const slots = rule.definition.slots ?? [];
        let applied = 0;
        let needsMapping = 0;

        for (const fqn of monitoredFqns) {
          const bindingId = bindingIdByFqn.get(fqn);
          if (!bindingId) {
            needsMapping += 1;
            continue;
          }
          const [catalog, schema, table] = splitFqn(fqn);
          try {
            const columnsResp = await queryClient.fetchQuery(
              getGetTableColumnsQueryOptions(catalog, schema, table),
            );
            const mapping = buildSlotMapping(slots, columnsResp.data ?? []);
            if (mapping === null) {
              needsMapping += 1;
              continue;
            }
            await applyMutation.mutateAsync({
              bindingId,
              data: { rule_id: rule.rule_id, column_mapping: [mapping] },
            });
            applied += 1;
          } catch {
            needsMapping += 1;
          }
        }

        toast.success(t("rulesRegistry.applyModalToastSummary", { applied, needsMapping }), {
          duration: 6000,
        });
        if (applied > 0) {
          // The rule's applications changed, which moves the dq-score /
          // dq-results aggregates — notably this rule's `applied_to_count`,
          // which gates its Results tab. Those queries never refetch on
          // their own (staleTime Infinity), so invalidate them here or the
          // tab stays stale-disabled until a full reload.
          invalidateResultsAfterRuleApplicationChange(queryClient);
        }
        onApplied();
        handleClose(false);
      } catch (err) {
        toast.error(extractApiError(err, t("monitoredTables.toastRegisterFailed")), { duration: 6000 });
      } finally {
        setIsProcessing(false);
      }
    },
    [bulkMutation, applyMutation, queryClient, rule, t, onApplied, handleClose],
  );

  const handleApplyClick = () => {
    if (effectiveFqns.length === 0) return;
    if (effectiveFqns.length > 10) {
      setPendingFqns(effectiveFqns);
      return;
    }
    void runApply(effectiveFqns);
  };

  return (
    <>
      <Dialog open={open} onOpenChange={handleClose}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>{t("rulesRegistry.applyModalTitle")}</DialogTitle>
            <DialogDescription>{t("rulesRegistry.applyModalDescription")}</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <TableScopePickerFields state={picker} />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => handleClose(false)} disabled={isProcessing}>
              {t("common.cancel")}
            </Button>
            <Button
              onClick={handleApplyClick}
              disabled={effectiveFqns.length === 0 || isProcessing}
              className="gap-2"
            >
              {isProcessing && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("rulesRegistry.applyModalApplyButton")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog open={pendingFqns !== null} onOpenChange={(next) => !next && setPendingFqns(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("monitoredTables.wizard.confirmTitle", { count: pendingFqns?.length ?? 0 })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.wizard.confirmDescription", { count: pendingFqns?.length ?? 0 })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const fqns = pendingFqns ?? [];
                setPendingFqns(null);
                void runApply(fqns);
              }}
            >
              {t("monitoredTables.wizard.confirmAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
