import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ArrowLeft, Loader2 } from "lucide-react";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import { MonitoredTableTabsShell, type MonitoredTableTabKey } from "@/components/monitored-tables/MonitoredTableTabsShell";
import {
  getListMonitoredTablesQueryKey,
  useBulkRegisterMonitoredTables,
  type BulkRegisterMonitoredTablesOut,
} from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";

export const Route = createFileRoute("/_sidebar/monitored-tables/new")({
  component: NewMonitoredTablePage,
});

// Every tab except About is locked until the table(s) are registered — the
// wizard is multi-table (bulk register), so there is no single binding to
// drive Profile / Apply Rules / Results yet. Matches dqlake's
// `tables/new.tsx` locked-tab treatment.
const LOCKED_TABS = new Set<MonitoredTableTabKey>(["profile", "apply", "results"]);

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

function NewMonitoredTablePage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [steward, setSteward] = useState("");
  const [pendingFqns, setPendingFqns] = useState<string[] | null>(null);
  const picker = useTableScopePicker(true);
  const { effectiveFqns } = picker;

  const bulkMutation = useBulkRegisterMonitoredTables();

  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListMonitoredTablesQueryKey() }),
    [queryClient],
  );

  const submitBulk = useCallback(
    (fqns: string[]) => {
      bulkMutation.mutate(
        { data: { table_fqns: fqns, steward: steward.trim() || undefined } },
        {
          onSuccess: (resp) => {
            toast.success(buildSummaryToast(t, resp.data));
            invalidate();
            navigate({ to: "/monitored-tables" });
          },
          onError: (err) => {
            toast.error(extractApiError(err, t("monitoredTables.toastRegisterFailed")), {
              duration: 6000,
            });
          },
        },
      );
    },
    [bulkMutation, steward, t, invalidate, navigate],
  );

  const handleCreate = () => {
    if (effectiveFqns.length === 0) return;
    if (effectiveFqns.length > 10) {
      setPendingFqns(effectiveFqns);
      return;
    }
    submitBulk(effectiveFqns);
  };

  const canSubmit = effectiveFqns.length > 0 && !bulkMutation.isPending && perms.canCreateRules;

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[{ label: t("monitoredTables.title"), to: "/monitored-tables" }]}
          page={t("monitoredTables.wizard.untitledMonitor")}
        />

        <div className="border-b pb-4 space-y-1">
          <h1 className="text-xl text-muted-foreground italic">{t("monitoredTables.wizard.untitledMonitor")}</h1>
          <Link
            to="/monitored-tables"
            className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground"
          >
            <ArrowLeft className="h-4 w-4 mr-1" />
            {t("monitoredTables.backToList")}
          </Link>
        </div>

        <MonitoredTableTabsShell activeTab="about" onTabChange={() => {}} disabledTabs={LOCKED_TABS}>
          {{
            about: (
              <div className="space-y-6 py-4 max-w-2xl">
                <section className="space-y-3">
                  <h2 className="text-sm font-semibold">{t("monitoredTables.wizard.title")}</h2>
                  <p className="text-sm text-muted-foreground">{t("monitoredTables.wizard.description")}</p>
                  <TableScopePickerFields state={picker} />
                </section>

                <div className="space-y-1.5">
                  <Label htmlFor="mt-new-steward">{t("monitoredTables.stewardLabel")}</Label>
                  <Input
                    id="mt-new-steward"
                    value={steward}
                    onChange={(e) => setSteward(e.target.value)}
                    placeholder={t("monitoredTables.stewardPlaceholder")}
                  />
                  <p className="text-xs text-muted-foreground">{t("monitoredTables.stewardOptionalHint")}</p>
                </div>

                <div className="flex gap-2">
                  <Button onClick={handleCreate} disabled={!canSubmit} className="gap-2">
                    {bulkMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                    {t("monitoredTables.wizard.nextButton")}
                  </Button>
                  <Button type="button" variant="outline" asChild>
                    <Link to="/monitored-tables">{t("common.cancel")}</Link>
                  </Button>
                </div>
              </div>
            ),
          }}
        </MonitoredTableTabsShell>
      </div>

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
    </FadeIn>
  );
}
