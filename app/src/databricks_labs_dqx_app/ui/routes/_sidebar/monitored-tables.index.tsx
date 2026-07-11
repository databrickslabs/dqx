import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo, useState, Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Pagination } from "@/components/Pagination";
import {
  MonitoredTablesTable,
  getMonitoredTablesSortValue,
  type MonitoredTablesSortKey,
} from "@/components/monitored-tables/MonitoredTablesTable";
import { AddMonitoredTableModal } from "@/components/monitored-tables/AddMonitoredTableModal";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
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
import { AlertCircle, CheckCircle2, Loader2, Plus, RotateCcw, Search, Table2, Trash2, XCircle } from "lucide-react";
import {
  useListMonitoredTables,
  useDeleteMonitoredTable,
  useApproveMonitoredTable,
  useRejectMonitoredTable,
  type MonitoredTableSummaryOut,
} from "@/lib/api";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { usePermissions } from "@/hooks/use-permissions";
import { FILTER_TRIGGER_CLASS } from "@/components/data-table/filter-bar";
import { SearchableSelect } from "@/components/data-table/SearchableSelect";
import { cn } from "@/lib/utils";

const PAGE_SIZE = 25;
const ALL = "all";

export const Route = createFileRoute("/_sidebar/monitored-tables/")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={MonitoredTablesError}>
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

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

/** Splits a `catalog.schema.table` FQN into its parts for the cascading
 *  catalog/schema filters below (options are derived from the loaded rows,
 *  mirroring dqlake's `BindingsTable` rather than a separate catalog API
 *  call). */
function splitFqn(fqn: string): { catalog: string; schema: string } {
  const parts = fqn.split(".");
  return { catalog: parts[0] ?? "", schema: parts[1] ?? "" };
}

function MonitoredTablesPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [statusFilter, setStatusFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [catalogFilter, setCatalogFilter] = useState<string>(ALL);
  const [schemaFilter, setSchemaFilter] = useState<string>(ALL);
  const [nameSearch, setNameSearch] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<MonitoredTableSummaryOut | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [addOpen, setAddOpen] = useState(false);

  const queryParams = useMemo(
    () => ({
      status: statusFilter === ALL ? undefined : statusFilter,
      steward: stewardFilter === ALL ? undefined : stewardFilter,
      catalog: catalogFilter === ALL ? undefined : catalogFilter,
      schema: schemaFilter === ALL ? undefined : schemaFilter,
      name: nameSearch.trim() || undefined,
    }),
    [statusFilter, stewardFilter, catalogFilter, schemaFilter, nameSearch],
  );

  const { data, isLoading, isError, refetch } = useListMonitoredTables(queryParams);
  const tables = useMemo(() => data?.data ?? [], [data]);

  // Cascading: schema options restricted to whatever catalog is selected —
  // derived from the (server-filtered) row set rather than a separate
  // catalog-browser API call, matching dqlake's BindingsTable approach.
  const { data: unfilteredData } = useListMonitoredTables({});
  const allTables = useMemo(() => unfilteredData?.data ?? [], [unfilteredData]);
  const catalogOptions = useMemo(
    () => Array.from(new Set(allTables.map((r) => splitFqn(r.table.table_fqn).catalog))).sort(),
    [allTables],
  );
  const schemaOptions = useMemo(() => {
    const rows = catalogFilter === ALL ? allTables : allTables.filter((r) => splitFqn(r.table.table_fqn).catalog === catalogFilter);
    return Array.from(new Set(rows.map((r) => splitFqn(r.table.table_fqn).schema))).sort();
  }, [allTables, catalogFilter]);
  const stewardOptions = useMemo(
    () =>
      Array.from(new Set(allTables.map((r) => r.table.steward).filter((s): s is string => !!s))).sort(),
    [allTables],
  );

  const [sortKey, setSortKey] = useState<MonitoredTablesSortKey | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const handleHeaderClick = useCallback(
    (key: MonitoredTablesSortKey) => {
      if (sortKey !== key) {
        setSortKey(key);
        setSortDir("asc");
        return;
      }
      if (sortDir === "asc") {
        setSortDir("desc");
        return;
      }
      setSortKey(null);
    },
    [sortKey, sortDir],
  );

  const sortedTables = useMemo(() => {
    if (!sortKey) return tables;
    const copy = [...tables];
    copy.sort((a, b) => {
      const av = getMonitoredTablesSortValue(sortKey, a);
      const bv = getMonitoredTablesSortValue(sortKey, b);
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return copy;
  }, [tables, sortKey, sortDir]);

  const pagedTables = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sortedTables.slice(start, start + PAGE_SIZE);
  }, [sortedTables, page]);

  const hasActiveFilters =
    statusFilter !== ALL ||
    stewardFilter !== ALL ||
    catalogFilter !== ALL ||
    schemaFilter !== ALL ||
    nameSearch.trim() !== "";

  const applyFilter = useCallback(
    <T,>(setter: (v: T) => void) =>
      (v: T) => {
        setter(v);
        setPage(1);
      },
    [],
  );

  const deleteMutation = useDeleteMonitoredTable();
  const approveMutation = useApproveMonitoredTable();
  const rejectMutation = useRejectMonitoredTable();
  const [rejectTarget, setRejectTarget] = useState<MonitoredTableSummaryOut | null>(null);

  // Shared runner for the row-level approve/reject actions — mirrors the
  // Rules Registry list page's `runAction` (registry-rules.index.tsx).
  const runRowAction = useCallback(
    (bindingId: string, mutate: () => Promise<unknown>, successMsg: string, errorMsg: string) => {
      if (pendingId) return;
      setPendingId(bindingId);
      mutate()
        .then(() => {
          toast.success(successMsg);
          invalidateAfterMonitoredTableChange(queryClient, bindingId);
        })
        .catch((err: unknown) => {
          toast.error(extractApiError(err, errorMsg), { duration: 6000 });
        })
        .finally(() => setPendingId(null));
    },
    [pendingId, queryClient],
  );

  const handleApprove = (summary: MonitoredTableSummaryOut) =>
    runRowAction(
      summary.table.binding_id,
      () => approveMutation.mutateAsync({ bindingId: summary.table.binding_id }),
      t("monitoredTables.toastApproved"),
      t("monitoredTables.toastApproveFailed"),
    );

  // Reject is destructive (moves the table out of the approval queue) so it
  // is gated behind a confirm dialog, mirroring the detail page's
  // `rejectConfirmOpen` — the actual mutation only fires from
  // `confirmReject` once the user confirms.
  const confirmReject = () => {
    if (!rejectTarget) return;
    const summary = rejectTarget;
    setRejectTarget(null);
    runRowAction(
      summary.table.binding_id,
      () => rejectMutation.mutateAsync({ bindingId: summary.table.binding_id }),
      t("monitoredTables.toastRejected"),
      t("monitoredTables.toastRejectFailed"),
    );
  };

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
          invalidateAfterMonitoredTableChange(queryClient, bindingId);
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
            <Button onClick={() => setAddOpen(true)} className="gap-2">
              <Plus className="h-4 w-4" />
              {t("monitoredTables.monitorTable")}
            </Button>
          )}
        </div>

        {/* Bare table — no Card wrapper, matching the Rules Registry list.
            The filter row is passed as `toolbarExtra` so it renders inline
            with the Edit Columns button rather than on its own row. */}
        <MonitoredTablesTable
          rows={pagedTables}
          sortKey={sortKey}
          sortDir={sortDir}
          onHeaderClick={handleHeaderClick}
          onRowClick={(summary) =>
            navigate({ to: "/monitored-tables/$bindingId", params: { bindingId: summary.table.binding_id } })
          }
          pendingBindingId={pendingId}
          toolbarExtra={
            <>
              <div className="relative w-56">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder={t("monitoredTables.searchTablesPlaceholder")}
                  value={nameSearch}
                  onChange={(e) => applyFilter(setNameSearch)(e.target.value)}
                  className="h-8 text-xs pl-7"
                />
              </div>
              <SearchableSelect
                value={catalogFilter}
                onChange={applyFilter(setCatalogFilter)}
                options={catalogOptions.map((c) => ({ value: c, label: c }))}
                allValue={ALL}
                allLabel={t("monitoredTables.allCatalogs")}
                searchPlaceholder={t("common.search")}
                emptyText={t("common.noMatches")}
                ariaLabel={t("monitoredTables.colCatalog")}
              />
              <SearchableSelect
                value={schemaFilter}
                onChange={applyFilter(setSchemaFilter)}
                options={schemaOptions.map((s) => ({ value: s, label: s }))}
                allValue={ALL}
                allLabel={t("monitoredTables.allSchemas")}
                searchPlaceholder={t("common.search")}
                emptyText={t("common.noMatches")}
                ariaLabel={t("monitoredTables.colSchema")}
              />
              <Select value={statusFilter} onValueChange={applyFilter(setStatusFilter)}>
                <SelectTrigger className={FILTER_TRIGGER_CLASS}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={ALL} className="text-xs">{t("monitoredTables.allStatuses")}</SelectItem>
                  <SelectItem value="draft" className="text-xs">{t("monitoredTables.statusDraft")}</SelectItem>
                  <SelectItem value="pending_approval" className="text-xs">{t("monitoredTables.statusPendingApproval")}</SelectItem>
                  <SelectItem value="rejected" className="text-xs">{t("monitoredTables.statusRejected")}</SelectItem>
                  <SelectItem value="approved" className="text-xs">{t("monitoredTables.statusApproved")}</SelectItem>
                </SelectContent>
              </Select>
              <SearchableSelect
                value={stewardFilter}
                onChange={applyFilter(setStewardFilter)}
                options={stewardOptions.map((s) => ({ value: s, label: s }))}
                allValue={ALL}
                allLabel={t("monitoredTables.allStewards")}
                searchPlaceholder={t("common.search")}
                emptyText={t("common.noMatches")}
                ariaLabel={t("monitoredTables.colSteward")}
              />
            </>
          }
          renderActions={
            // Actions column stays visible for anyone who can approve OR
            // create/delete — an approver-only user still needs to see the
            // approve/reject buttons even without create/delete rights, and
            // vice versa. Mirrors RulesTable's per-status action gating
            // (registry-rules.index.tsx#renderActionsCell).
            perms.canApproveRules || perms.canCreateRules
              ? (summary) => (
                  <div className="flex items-center justify-end gap-1">
                    {summary.table.status === "pending_approval" && perms.canApproveRules && (
                      <>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-emerald-600"
                              aria-label={t("monitoredTables.approveAction")}
                              onClick={() => handleApprove(summary)}
                            >
                              <CheckCircle2 className="h-3.5 w-3.5" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>{t("monitoredTables.approveAction")}</TooltipContent>
                        </Tooltip>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-destructive"
                              aria-label={t("monitoredTables.rejectAction")}
                              onClick={() => setRejectTarget(summary)}
                            >
                              <XCircle className="h-3.5 w-3.5" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>{t("monitoredTables.rejectAction")}</TooltipContent>
                        </Tooltip>
                      </>
                    )}
                    {perms.canCreateRules && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-destructive"
                            aria-label={t("monitoredTables.actionDelete")}
                            onClick={() => setDeleteTarget(summary)}
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("monitoredTables.actionDelete")}</TooltipContent>
                      </Tooltip>
                    )}
                  </div>
                )
              : undefined
          }
          emptyState={
            isLoading ? (
              <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" />
                {t("common.loading")}
              </div>
            ) : isError ? (
              <div className="flex flex-col items-center justify-center text-center">
                <AlertCircle className="h-10 w-10 text-destructive/30 mb-3" />
                <p className="text-sm text-muted-foreground mb-3">{t("common.loadFailed")}</p>
                <Button variant="outline" size="sm" onClick={() => refetch()} className="gap-2">
                  <RotateCcw className="h-3 w-3" />
                  {t("common.retry")}
                </Button>
              </div>
            ) : (
              <div className="flex flex-col items-center justify-center text-center">
                {/* h-12 w-12: an integer 2× of lucide's 24px grid renders
                    crisp 4px strokes; 40px (h-10) is a fractional 5/3 scale
                    whose 3.33px strokes anti-alias soft (P23 item 18). */}
                <Table2 className="h-12 w-12 text-muted-foreground/30 mb-3" />
                <p className="text-sm text-muted-foreground">
                  {hasActiveFilters
                    ? t("monitoredTables.emptyState")
                    : perms.canCreateRules
                      ? t("monitoredTables.emptyStateNoTablesCta")
                      : t("monitoredTables.emptyStateNoTables")}
                </p>
              </div>
            )
          }
        />

        {tables.length > 0 && (
          <Pagination page={page} totalItems={tables.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
        )}
      </div>

      {/* Reject is destructive — gate it behind a confirm dialog, mirroring
          the detail page's `rejectConfirmOpen` (monitored-tables.$bindingId.tsx),
          reusing the same copy/i18n keys. */}
      <AlertDialog open={rejectTarget !== null} onOpenChange={(open) => !open && setRejectTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("monitoredTables.rejectConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.rejectConfirmDescription", { table: rejectTarget?.table.table_fqn ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmReject}>
              {t("monitoredTables.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

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

      <AddMonitoredTableModal open={addOpen} onOpenChange={setAddOpen} />
    </FadeIn>
  );
}
