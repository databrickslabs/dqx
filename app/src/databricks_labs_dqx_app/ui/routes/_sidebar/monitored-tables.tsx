import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo, useState, Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import {
  AlertCircle,
  Boxes,
  FileEdit,
  Loader2,
  Plus,
  RotateCcw,
  Search,
  ShieldCheck,
  Trash2,
} from "lucide-react";
import {
  useListMonitoredTables,
  getListMonitoredTablesQueryKey,
  useRegisterMonitoredTable,
  useDeleteMonitoredTable,
  type MonitoredTableSummaryOut,
} from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";
import { formatDateShort } from "@/lib/format-utils";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/_sidebar/monitored-tables")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} fallbackRender={MonitoredTablesError}>
          <Suspense fallback={<MonitoredTablesSkeleton />}>
            <MonitoredTablesPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function MonitoredTablesError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("common.loadFailed")}</p>
      <p className="text-muted-foreground/70 text-xs mb-3">{t("common.retryHint")}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

function MonitoredTablesSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-4 w-64" />
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  if (status === "published") {
    return (
      <Badge variant="outline" className="gap-1 text-[10px] border-emerald-500 text-emerald-600">
        <ShieldCheck className="h-2.5 w-2.5" />
        {t("monitoredTables.statusPublished")}
      </Badge>
    );
  }
  return (
    <Badge variant="secondary" className="gap-1 text-[10px]">
      <FileEdit className="h-2.5 w-2.5" />
      {t("monitoredTables.statusDraft")}
    </Badge>
  );
}

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

const ALL = "all";

function RegisterTableDialog({
  open,
  onOpenChange,
  onRegistered,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRegistered: () => void;
}) {
  const { t } = useTranslation();
  const [tableFqn, setTableFqn] = useState("");
  const [steward, setSteward] = useState("");
  const registerMutation = useRegisterMonitoredTable();

  const handleClose = (next: boolean) => {
    if (!next) {
      setTableFqn("");
      setSteward("");
    }
    onOpenChange(next);
  };

  const handleSubmit = () => {
    if (!tableFqn) return;
    registerMutation.mutate(
      { data: { table_fqn: tableFqn, steward: steward.trim() || undefined } },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastRegistered"));
          onRegistered();
          handleClose(false);
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.toastRegisterFailed")), {
            duration: 6000,
          });
        },
      },
    );
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{t("monitoredTables.registerDialogTitle")}</DialogTitle>
          <DialogDescription>{t("monitoredTables.registerDialogDescription")}</DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          <CatalogBrowser value={tableFqn} onChange={setTableFqn} />
          <div className="space-y-1.5">
            <Label htmlFor="mt-steward">{t("monitoredTables.stewardLabel")}</Label>
            <Input
              id="mt-steward"
              value={steward}
              onChange={(e) => setSteward(e.target.value)}
              placeholder={t("monitoredTables.stewardPlaceholder")}
            />
            <p className="text-xs text-muted-foreground">{t("monitoredTables.stewardOptionalHint")}</p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleClose(false)}>
            {t("common.cancel")}
          </Button>
          <Button onClick={handleSubmit} disabled={!tableFqn || registerMutation.isPending} className="gap-2">
            {registerMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {t("monitoredTables.registerSubmit")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function MonitoredTablesPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [statusFilter, setStatusFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState("");
  const [nameSearch, setNameSearch] = useState("");
  const [registerOpen, setRegisterOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<MonitoredTableSummaryOut | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);

  const queryParams = useMemo(
    () => ({
      status: statusFilter === ALL ? undefined : statusFilter,
      steward: stewardFilter.trim() || undefined,
      name: nameSearch.trim() || undefined,
    }),
    [statusFilter, stewardFilter, nameSearch],
  );

  const { data } = useListMonitoredTables(queryParams);
  const tables = useMemo(() => data?.data ?? [], [data]);

  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListMonitoredTablesQueryKey() }),
    [queryClient],
  );

  const deleteMutation = useDeleteMonitoredTable();

  const confirmDelete = () => {
    if (!deleteTarget?.table.binding_id) return;
    const bindingId = deleteTarget.table.binding_id;
    setDeleteTarget(null);
    setPendingId(bindingId);
    deleteMutation.mutate(
      { bindingId },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastDeleted"));
          invalidate();
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.toastDeleteFailed")), {
            duration: 6000,
          });
        },
        onSettled: () => setPendingId(null),
      },
    );
  };

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("monitoredTables.breadcrumb")} />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("monitoredTables.title")}</h1>
            <p className="text-sm text-muted-foreground mt-1">{t("monitoredTables.subtitle")}</p>
          </div>
          {perms.canCreateRules && (
            <Button onClick={() => setRegisterOpen(true)} className="gap-2">
              <Plus className="h-4 w-4" />
              {t("monitoredTables.monitorTable")}
            </Button>
          )}
        </div>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">{t("rulesRegistry.filtersTitle")}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            <div className="relative w-56">
              <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                placeholder={t("monitoredTables.searchPlaceholder")}
                value={nameSearch}
                onChange={(e) => setNameSearch(e.target.value)}
                className="h-8 text-xs pl-7"
              />
            </div>
            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="h-8 w-40 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL} className="text-xs">{t("monitoredTables.allStatuses")}</SelectItem>
                <SelectItem value="draft" className="text-xs">{t("monitoredTables.statusDraft")}</SelectItem>
                <SelectItem value="published" className="text-xs">{t("monitoredTables.statusPublished")}</SelectItem>
              </SelectContent>
            </Select>
            <Input
              placeholder={t("monitoredTables.stewardPlaceholder")}
              value={stewardFilter}
              onChange={(e) => setStewardFilter(e.target.value)}
              className="h-8 w-40 text-xs"
            />
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-0">
            {tables.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-center">
                <Boxes className="h-10 w-10 text-muted-foreground/30 mb-3" />
                <p className="text-sm text-muted-foreground">{t("monitoredTables.emptyState")}</p>
              </div>
            ) : (
              <div className="divide-y">
                <div className="hidden md:grid grid-cols-[1fr_auto_auto_auto_auto_auto] gap-3 px-4 py-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground bg-muted/30">
                  <span>{t("monitoredTables.colTable")}</span>
                  <span>{t("monitoredTables.colStatus")}</span>
                  <span>{t("monitoredTables.colAppliedRules")}</span>
                  <span>{t("monitoredTables.colLastProfiled")}</span>
                  <span>{t("monitoredTables.colSteward")}</span>
                  <span className="text-right">{t("monitoredTables.colActions")}</span>
                </div>
                {tables.map((summary) => {
                  const bindingId = summary.table.binding_id;
                  const busy = pendingId === bindingId;
                  return (
                    <div
                      key={bindingId}
                      className="grid grid-cols-1 md:grid-cols-[1fr_auto_auto_auto_auto_auto] gap-2 md:gap-3 items-center px-4 py-3 hover:bg-muted/20 cursor-pointer transition-colors"
                      onClick={() => navigate({ to: "/monitored-tables/$bindingId", params: { bindingId } })}
                    >
                      <div className="min-w-0">
                        <code className="text-sm font-mono truncate block">{summary.table.table_fqn}</code>
                      </div>
                      <div><StatusBadge status={summary.table.status} /></div>
                      <div className="text-xs text-muted-foreground tabular-nums">
                        {t("monitoredTables.appliedRuleCount", { count: summary.applied_rule_count })}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {summary.table.last_profiled_at
                          ? formatDateShort(summary.table.last_profiled_at)
                          : t("monitoredTables.neverProfiled")}
                      </div>
                      <div className="text-xs text-muted-foreground truncate max-w-[10rem]">
                        {summary.table.steward || "—"}
                      </div>
                      <div
                        className="flex items-center justify-end gap-1"
                        onClick={(e) => e.stopPropagation()}
                      >
                        {busy ? (
                          <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                        ) : (
                          perms.canCreateRules && (
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-destructive"
                              title={t("monitoredTables.actionDelete")}
                              onClick={() => setDeleteTarget(summary)}
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </Button>
                          )
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <RegisterTableDialog open={registerOpen} onOpenChange={setRegisterOpen} onRegistered={invalidate} />

      <AlertDialog open={deleteTarget !== null} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("monitoredTables.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.deleteConfirmDescription", { table: deleteTarget?.table.table_fqn ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmDelete}>
              {t("monitoredTables.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
