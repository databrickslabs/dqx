import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
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
  getListMonitoredTablesQueryKey,
  useBulkRegisterMonitoredTables,
  type BulkRegisterMonitoredTablesOut,
} from "@/lib/api";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

function buildSummaryToast(
  t: (key: string, opts?: Record<string, unknown>) => string,
  summary: BulkRegisterMonitoredTablesOut,
): string {
  const registered = summary.registered?.length ?? 0;
  const skipped = summary.skipped_existing?.length ?? 0;
  const invalid = summary.invalid?.length ?? 0;
  return t("monitoredTables.wizard.toastSummary", { registered, skipped, invalid });
}

export interface AddMonitoredTableModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

/**
 * Dialog-based "monitor table(s)" picker — dqlake-style modal (see
 * `components/rules/TablePickerModal.tsx` in dqlake) instead of a dedicated
 * page. Keeps the multiselect catalog/schema/table scoping and the >10
 * bulk-register confirmation the old `/monitored-tables/new` page had; the
 * steward field is intentionally omitted here (it's set later, from the
 * table detail page) per the current design.
 */
export function AddMonitoredTableModal({ open, onOpenChange }: AddMonitoredTableModalProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  const [pendingFqns, setPendingFqns] = useState<string[] | null>(null);
  const picker = useTableScopePicker(open);
  const { effectiveFqns } = picker;

  const bulkMutation = useBulkRegisterMonitoredTables();

  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListMonitoredTablesQueryKey() }),
    [queryClient],
  );

  const closeAndReset = useCallback(() => {
    onOpenChange(false);
    picker.reset();
  }, [onOpenChange, picker]);

  const submitBulk = useCallback(
    (fqns: string[]) => {
      bulkMutation.mutate(
        { data: { table_fqns: fqns } },
        {
          onSuccess: (resp) => {
            toast.success(buildSummaryToast(t, resp.data));
            invalidate();
            closeAndReset();
          },
          onError: (err) => {
            toast.error(extractApiError(err, t("monitoredTables.toastRegisterFailed")), {
              duration: 6000,
            });
          },
        },
      );
    },
    [bulkMutation, t, invalidate, closeAndReset],
  );

  const handleCreate = () => {
    if (effectiveFqns.length === 0) return;
    if (effectiveFqns.length > 10) {
      setPendingFqns(effectiveFqns);
      return;
    }
    submitBulk(effectiveFqns);
  };

  const canSubmit = effectiveFqns.length > 0 && !bulkMutation.isPending;

  return (
    <>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          if (!next) closeAndReset();
        }}
      >
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>{t("monitoredTables.wizard.title")}</DialogTitle>
            <DialogDescription>{t("monitoredTables.wizard.description")}</DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <TableScopePickerFields state={picker} />
          </div>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={closeAndReset}>
              {t("common.cancel")}
            </Button>
            <Button onClick={handleCreate} disabled={!canSubmit} className="gap-2">
              {bulkMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("monitoredTables.wizard.nextButton")}
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
                submitBulk(fqns);
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
