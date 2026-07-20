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
  getMonitoredTablesSortConfig,
  type MonitoredTablesSortKey,
  type MonitoredTablesTableSelection,
} from "@/components/monitored-tables/MonitoredTablesTable";
import { compareSortValues } from "@/components/data-table/sort";
import { AddMonitoredTableModal } from "@/components/monitored-tables/AddMonitoredTableModal";
import { ExportDialog } from "@/components/ExportDialog";
import { exportMonitoredTables } from "@/lib/api-custom";
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
import { AlertCircle, CheckCircle2, FileDown, GitCompare, Loader2, Play, Plus, RotateCcw, Search, Table2, Trash2, Undo2, XCircle } from "lucide-react";
import {
  useListMonitoredTables,
  useDeleteMonitoredTable,
  useApproveMonitoredTable,
  useRejectMonitoredTable,
  useRevertMonitoredTable,
  useRunMonitoredTable,
  type MonitoredTableSummaryOut,
} from "@/lib/api";
import {
  MonitoredTableDiffDialog,
  type MonitoredTableDiffTarget,
} from "@/components/drafts/ChangeDiffDialog";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { usePermissions } from "@/hooks/use-permissions";
import {
  DQ_SCORE_BUCKETS,
  DQ_SCORE_FILTER_ALL,
  FILTER_TRIGGER_CLASS,
  matchesDqScoreBucket,
} from "@/components/data-table/filter-bar";
import { SearchableSelect } from "@/components/data-table/SearchableSelect";
import { BulkActionBar } from "@/components/data-table/BulkActionBar";
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

  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [catalogFilter, setCatalogFilter] = useState<string>(ALL);
  const [schemaFilter, setSchemaFilter] = useState<string>(ALL);
  const [scoreFilter, setScoreFilter] = useState<string>(DQ_SCORE_FILTER_ALL);
  const [nameSearch, setNameSearch] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<MonitoredTableSummaryOut | null>(null);
  const [diffTarget, setDiffTarget] = useState<MonitoredTableDiffTarget | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [addOpen, setAddOpen] = useState(false);

  // ONE list fetch, unfiltered — every facet (catalog, schema, name, steward,
  // DQ-score) is applied CLIENT-SIDE over this single row set (T-perf/B2-3).
  // The page previously fired TWO `useListMonitoredTables` calls — a
  // server-filtered one for the rows plus an unfiltered one for the facet
  // options — which doubled the (already-warehouse-free) list-load cost. All
  // filter inputs are already on each fetched row, so no server round-trip is
  // needed per filter change; this also matches how the Rules Registry and
  // Table Spaces overviews filter (P25 items 91/92). The backend list route
  // still honors these query params, but the overview no longer uses them.
  const { data, isLoading, isError, refetch } = useListMonitoredTables({});
  const tables = useMemo(() => data?.data ?? [], [data]);

  const visibleTables = useMemo(() => {
    const needle = nameSearch.trim().toLowerCase();
    return tables.filter((r) => {
      const { catalog, schema } = splitFqn(r.table.table_fqn);
      return (
        (catalogFilter === ALL || catalog === catalogFilter) &&
        (schemaFilter === ALL || schema === schemaFilter) &&
        (!needle || r.table.table_fqn.toLowerCase().includes(needle)) &&
        (stewardFilter === ALL || (r.table.steward ?? "") === stewardFilter) &&
        matchesDqScoreBucket(r.score, scoreFilter)
      );
    });
  }, [tables, catalogFilter, schemaFilter, nameSearch, stewardFilter, scoreFilter]);

  // Facet options are derived from the full fetched row set — always the
  // complete catalog/steward lists regardless of the active filters, with the
  // schema list cascading to the selected catalog.
  const catalogOptions = useMemo(
    () => Array.from(new Set(tables.map((r) => splitFqn(r.table.table_fqn).catalog))).sort(),
    [tables],
  );
  const schemaOptions = useMemo(() => {
    const rows = catalogFilter === ALL ? tables : tables.filter((r) => splitFqn(r.table.table_fqn).catalog === catalogFilter);
    return Array.from(new Set(rows.map((r) => splitFqn(r.table.table_fqn).schema))).sort();
  }, [tables, catalogFilter]);
  const stewardOptions = useMemo(
    () =>
      Array.from(new Set(tables.map((r) => r.table.steward).filter((s): s is string => !!s))).sort(),
    [tables],
  );

  const [sortKey, setSortKey] = useState<MonitoredTablesSortKey | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const handleHeaderClick = useCallback(
    (key: MonitoredTablesSortKey) => {
      // First click uses the column's steward-first default direction (B2-92);
      // repeat clicks toggle to the opposite direction, then clear.
      const { dir } = getMonitoredTablesSortConfig(key);
      if (sortKey !== key) {
        setSortKey(key);
        setSortDir(dir);
        return;
      }
      if (sortDir === dir) {
        setSortDir(dir === "asc" ? "desc" : "asc");
        return;
      }
      setSortKey(null);
    },
    [sortKey, sortDir],
  );

  const sortedTables = useMemo(() => {
    if (!sortKey) return visibleTables;
    const { nullsFirst } = getMonitoredTablesSortConfig(sortKey);
    const copy = [...visibleTables];
    copy.sort((a, b) =>
      compareSortValues(
        getMonitoredTablesSortValue(sortKey, a),
        getMonitoredTablesSortValue(sortKey, b),
        sortDir,
        nullsFirst,
      ),
    );
    return copy;
  }, [visibleTables, sortKey, sortDir]);

  const pagedTables = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sortedTables.slice(start, start + PAGE_SIZE);
  }, [sortedTables, page]);

  const hasActiveFilters =
    stewardFilter !== ALL ||
    catalogFilter !== ALL ||
    schemaFilter !== ALL ||
    scoreFilter !== DQ_SCORE_FILTER_ALL ||
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
  const revertMutation = useRevertMonitoredTable();
  const runMutation = useRunMonitoredTable();
  const [rejectTarget, setRejectTarget] = useState<MonitoredTableSummaryOut | null>(null);

  // Bulk-selection state — mirrors registry-rules.index.tsx
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkBusy, setBulkBusy] = useState(false);
  const [bulkApproveOpen, setBulkApproveOpen] = useState(false);
  const [bulkRejectOpen, setBulkRejectOpen] = useState(false);
  const [bulkRunOpen, setBulkRunOpen] = useState(false);
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);

  /** Binding IDs eligible for selection: the same gating as the row-level
   *  action bar — runnable (version > 0, canRunRules), approvable /
   *  rejectable (pending + canApproveRules), or deletable (canCreateRules,
   *  not pending). */
  const selectableIds = useMemo(() => {
    const ids = new Set<string>();
    for (const r of sortedTables) {
      const bindingId = r.table.binding_id;
      const isPending = r.table.status === "pending_approval";
      if (
        (perms.canRunRules && (r.table.version ?? 0) > 0) ||
        (isPending && perms.canApproveRules) ||
        (perms.canCreateRules && !isPending)
      ) {
        ids.add(bindingId);
      }
    }
    return ids;
  }, [sortedTables, perms]);

  const selectedRows = useMemo(
    () => sortedTables.filter((r) => selectedIds.has(r.table.binding_id)),
    [sortedTables, selectedIds],
  );

  const toggleSelect = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const toggleSelectAll = useCallback(() => {
    setSelectedIds((prev) => {
      if (prev.size === selectableIds.size) return new Set();
      return new Set(selectableIds);
    });
  }, [selectableIds]);

  const tableSelection = useMemo<MonitoredTablesTableSelection>(
    () => ({
      selectedIds,
      selectableIds,
      onToggle: toggleSelect,
      onToggleAll: toggleSelectAll,
    }),
    [selectedIds, selectableIds, toggleSelect, toggleSelectAll],
  );

  /** Runs a bulk operation sequentially and shows success/partial toasts —
   *  mirrors registry-rules.index.tsx's `bulkAction`. */
  const bulkAction = useCallback(
    async (
      rowsToAct: MonitoredTableSummaryOut[],
      mutate: (bindingId: string) => Promise<unknown>,
      successMsg: string,
    ) => {
      if (bulkBusy || rowsToAct.length === 0) return;
      setBulkBusy(true);
      let ok = 0;
      let fail = 0;
      let lastDetail = "";
      for (const r of rowsToAct) {
        try {
          await mutate(r.table.binding_id);
          ok++;
          invalidateAfterMonitoredTableChange(queryClient, r.table.binding_id);
        } catch (err: unknown) {
          fail++;
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          if (detail) lastDetail = detail;
        }
      }
      setBulkBusy(false);
      setSelectedIds(new Set());
      if (fail === 0) {
        toast.success(t("monitoredTables.bulkSucceeded", { count: ok, msg: successMsg }));
      } else {
        toast.warning(
          t("monitoredTables.bulkPartial", {
            ok,
            fail,
            reason: lastDetail ? ` — ${lastDetail}` : "",
          }),
        );
      }
    },
    [bulkBusy, queryClient, t],
  );

  const confirmBulkRun = () => {
    setBulkRunOpen(false);
    const eligible = selectedRows.filter((r) => (r.table.version ?? 0) > 0 && perms.canRunRules);
    bulkAction(
      eligible,
      (bindingId) => runMutation.mutateAsync({ bindingId, data: { source: "approved" } }),
      t("monitoredTables.bulkRunStarted"),
    );
  };

  const confirmBulkApprove = () => {
    setBulkApproveOpen(false);
    const eligible = selectedRows.filter((r) => r.table.status === "pending_approval");
    bulkAction(
      eligible,
      (bindingId) => approveMutation.mutateAsync({ bindingId }),
      t("monitoredTables.bulkApproved"),
    );
  };

  const confirmBulkReject = () => {
    setBulkRejectOpen(false);
    const eligible = selectedRows.filter((r) => r.table.status === "pending_approval");
    bulkAction(
      eligible,
      (bindingId) => rejectMutation.mutateAsync({ bindingId }),
      t("monitoredTables.bulkRejected"),
    );
  };

  const confirmBulkDelete = () => {
    setBulkDeleteOpen(false);
    const eligible = selectedRows.filter(
      (r) => perms.canCreateRules && r.table.status !== "pending_approval",
    );
    bulkAction(
      eligible,
      (bindingId) => deleteMutation.mutateAsync({ bindingId }),
      t("monitoredTables.bulkDeleted"),
    );
  };

  // Canonical action order across all three overviews (bug-bash-v4 item 14):
  // Run → Approve → Reject → Export → Delete → Clear.
  const bulkToolbar = (
    <BulkActionBar
      count={selectedIds.size}
      label={t("monitoredTables.selectedCount", { count: selectedIds.size })}
      busy={bulkBusy}
      onClear={() => setSelectedIds(new Set())}
      clearLabel={t("monitoredTables.clearSelection")}
    >
      {perms.canRunRules && selectedRows.some((r) => (r.table.version ?? 0) > 0) && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs" onClick={() => setBulkRunOpen(true)}>
          <Play className="h-3 w-3" />
          {t("monitoredTables.bulkRun")}
        </Button>
      )}
      {perms.canApproveRules && selectedRows.some((r) => r.table.status === "pending_approval") && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-emerald-600" onClick={() => setBulkApproveOpen(true)}>
          <CheckCircle2 className="h-3 w-3" />
          {t("monitoredTables.bulkApprove")}
        </Button>
      )}
      {perms.canApproveRules && selectedRows.some((r) => r.table.status === "pending_approval") && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-destructive" onClick={() => setBulkRejectOpen(true)}>
          <XCircle className="h-3 w-3" />
          {t("monitoredTables.bulkReject")}
        </Button>
      )}
      {/* Export lives in the selection action bar (exports exactly the
          ticked rows via the binding_id[] filter), mirroring the Rules
          overview — not a per-row action. */}
      <Button
        size="sm"
        variant="outline"
        className="gap-1 h-7 text-xs"
        onClick={() => setExportOpen(true)}
      >
        <FileDown className="h-3 w-3" />
        {t("exportYaml.button")}
      </Button>
      {perms.canCreateRules && selectedRows.some((r) => r.table.status !== "pending_approval") && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-destructive" onClick={() => setBulkDeleteOpen(true)}>
          <Trash2 className="h-3 w-3" />
          {t("monitoredTables.bulkDelete")}
        </Button>
      )}
    </BulkActionBar>
  );

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

  // Revert (withdraw to draft) — the author's counterpart to submit. Unlike
  // reject it leaves no rejected trail, so it fires directly (no confirm).
  const handleRevert = (summary: MonitoredTableSummaryOut) =>
    runRowAction(
      summary.table.binding_id,
      () => revertMutation.mutateAsync({ bindingId: summary.table.binding_id }),
      t("monitoredTables.toastReverted"),
      t("monitoredTables.toastRevertFailed"),
    );

  // Run the approved snapshot (item 76). Gated to tables that have an
  // approved version (version > 0) below; `runRowAction` sets `pendingId`,
  // which swaps this row's action cell to a spinner until the submit
  // settles. A cross-session "already running" gate would need the same
  // per-row run-activity feed the far-left running cog needs — deferred
  // with it (see report), so the guard here is the in-flight pending state.
  const handleRun = (summary: MonitoredTableSummaryOut) =>
    runRowAction(
      summary.table.binding_id,
      () => runMutation.mutateAsync({ bindingId: summary.table.binding_id, data: { source: "approved" } }),
      t("monitoredTables.toastRunStarted"),
      t("monitoredTables.toastRunFailed"),
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
          <div className="flex items-center gap-2">
            {/* Export lives in the selection action bar (bulkToolbar) — select
                rows to export exactly those, mirroring the Rules overview. */}
            {perms.canCreateRules && (
              <Button onClick={() => setAddOpen(true)} className="gap-2">
                <Plus className="h-4 w-4" />
                {t("monitoredTables.monitorTable")}
              </Button>
            )}
          </div>
        </div>

        {/* Bare table — no Card wrapper, matching the Rules Registry list.
            The filter row is passed as `toolbarExtra` so it renders inline
            with the Edit Columns button rather than on its own row. */}
        {/* ExportDialog lives outside BulkActionBar so it stays mounted even
            when selection drops to 0 (BulkActionBar returns null when count≤0,
            which would abruptly unmount a dialog opened while deselecting). */}
        <ExportDialog
          open={exportOpen}
          onOpenChange={setExportOpen}
          fetchDqx={() => exportMonitoredTables({ format: "dqx", binding_id: [...selectedIds] })}
          fetchOdcs={() => exportMonitoredTables({ format: "odcs", binding_id: [...selectedIds] })}
        />
        <div className="relative">
          {bulkToolbar}
          <MonitoredTablesTable
          rows={pagedTables}
          sortKey={sortKey}
          sortDir={sortDir}
          onHeaderClick={handleHeaderClick}
          onRowClick={(summary) =>
            navigate({ to: "/monitored-tables/$bindingId", params: { bindingId: summary.table.binding_id } })
          }
          pendingBindingId={pendingId}
          selection={tableSelection}
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
              <Select value={scoreFilter} onValueChange={applyFilter(setScoreFilter)}>
                <SelectTrigger className={FILTER_TRIGGER_CLASS} aria-label={t("monitoredTables.colDqScore")}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DQ_SCORE_BUCKETS.map((b) => (
                    <SelectItem key={b.value} value={b.value} className="text-xs">
                      {t(b.labelKey)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </>
          }
          renderActions={
            // Per-status action gating: approve/reject gated on canApproveRules;
            // run gated on canRunRules; delete gated on canCreateRules and hidden
            // when the row has a pending approval. Mirrors registry-rules.index.tsx.
            (summary) => (
                  <div className="flex items-center justify-end gap-1">
                    {perms.canRunRules && (summary.table.version ?? 0) > 0 && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0"
                            aria-label={t("monitoredTables.runAction")}
                            onClick={() => handleRun(summary)}
                          >
                            <Play className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("monitoredTables.runAction")}</TooltipContent>
                      </Tooltip>
                    )}
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
                    {summary.table.status === "pending_approval" && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-purple-600 dark:text-purple-400"
                            aria-label={t("rulesDrafts.diff.viewChanges")}
                            onClick={() =>
                              setDiffTarget({
                                bindingId: summary.table.binding_id,
                                name: summary.table.table_fqn,
                                version: summary.table.version ?? 0,
                              })
                            }
                          >
                            <GitCompare className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("rulesDrafts.diff.viewChanges")}</TooltipContent>
                      </Tooltip>
                    )}
                    {/* Revert (withdraw to draft) — authors can pull a pending
                        submission back to keep editing. Mirrors the Rules
                        overview's Undo2 revoke action. */}
                    {perms.canCreateRules && summary.table.status === "pending_approval" && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-amber-600 dark:text-amber-400"
                            aria-label={t("monitoredTables.revertAction")}
                            onClick={() => handleRevert(summary)}
                          >
                            <Undo2 className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("monitoredTables.revertAction")}</TooltipContent>
                      </Tooltip>
                    )}
                    {perms.canCreateRules && summary.table.status !== "pending_approval" && (
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
          }
          emptyState={
            isLoading ? (
              <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" />
                {t("common.loading")}
              </div>
            ) : isError ? (
              <div className="flex flex-col items-center justify-center text-center">
                <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
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
        </div>

        {visibleTables.length > 0 && (
          <Pagination page={page} totalItems={visibleTables.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
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

      <MonitoredTableDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

      <AlertDialog open={bulkRunOpen} onOpenChange={setBulkRunOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("monitoredTables.bulkRunTitle", {
                count: selectedRows.filter((r) => (r.table.version ?? 0) > 0).length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("monitoredTables.bulkRunBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkRun}>{t("monitoredTables.runAction")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkApproveOpen} onOpenChange={setBulkApproveOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("monitoredTables.bulkApproveTitle", {
                count: selectedRows.filter((r) => r.table.status === "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("monitoredTables.bulkApproveBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkApprove}>{t("monitoredTables.approveAction")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkRejectOpen} onOpenChange={setBulkRejectOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("monitoredTables.bulkRejectTitle", {
                count: selectedRows.filter((r) => r.table.status === "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("monitoredTables.bulkRejectBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmBulkReject}>
              {t("monitoredTables.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkDeleteOpen} onOpenChange={setBulkDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("monitoredTables.bulkDeleteTitle", {
                count: selectedRows.filter((r) => r.table.status !== "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("monitoredTables.bulkDeleteBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmBulkDelete}>
              {t("monitoredTables.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
