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
  DataProductsTable,
  getDataProductsSortValue,
  getDataProductsSortConfig,
  type DataProductsSortKey,
  type DataProductsTableSelection,
} from "@/components/data-products/DataProductsTable";
import { compareSortValues } from "@/components/data-table/sort";
import { ExportDialog } from "@/components/ExportDialog";
import { exportDataProducts } from "@/lib/api-custom";
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
import { AlertCircle, Boxes, CheckCircle2, FileDown, GitCompare, Loader2, Play, Plus, RotateCcw, Search, Trash2, Undo2, XCircle } from "lucide-react";
import {
  useListDataProducts,
  useApproveDataProduct,
  useRejectDataProduct,
  useRevertDataProduct,
  useDeleteDataProduct,
  useRunDataProduct,
  getListDataProductsQueryKey,
  getGetDataProductQueryKey,
  type DataProductOut,
} from "@/lib/api";
import {
  TableSpaceDiffDialog,
  type TableSpaceDiffTarget,
} from "@/components/drafts/ChangeDiffDialog";
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

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

const PAGE_SIZE = 50;
const ALL = "all";

export const Route = createFileRoute("/_sidebar/table-spaces/")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={DataProductsError}>
          <Suspense fallback={<DataProductsSkeleton />}>
            <DataProductsPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function DataProductsError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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

function DataProductsSkeleton() {
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

function DataProductsPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // Run-completion invalidation (results-invalidation.ts) already refetches
  // this list when a run settles or a product changes, so the blanket
  // `refetchOnMount: "always"` is dropped (T-perf/B2-3): it forced a full
  // refetch — which fans out to member scores/timestamps — on every remount
  // (e.g. tabbing back), even when nothing changed. React Query's default
  // staleness handling plus the explicit invalidations keep a newly created
  // product appearing without the always-refetch cost.
  const { data, isLoading, isError, refetch } = useListDataProducts();
  const products = useMemo(() => data?.data ?? [], [data]);

  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [scoreFilter, setScoreFilter] = useState<string>(DQ_SCORE_FILTER_ALL);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  const stewardOptions = useMemo(
    () => Array.from(new Set(products.map((p) => p.steward).filter((s): s is string => !!s))).sort(),
    [products],
  );

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return products.filter((p) => {
      if (stewardFilter !== ALL && (p.steward ?? "") !== stewardFilter) return false;
      if (!matchesDqScoreBucket(p.score, scoreFilter)) return false;
      if (!q) return true;
      const hay = `${p.name} ${p.description ?? ""} ${p.steward ?? ""}`.toLowerCase();
      return hay.includes(q);
    });
  }, [products, stewardFilter, scoreFilter, search]);

  const [sortKey, setSortKey] = useState<DataProductsSortKey | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const handleHeaderClick = useCallback(
    (key: DataProductsSortKey) => {
      // First click uses the column's steward-first default direction (B2-92);
      // repeat clicks toggle to the opposite direction, then clear.
      const { dir } = getDataProductsSortConfig(key);
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

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    const { nullsFirst } = getDataProductsSortConfig(sortKey);
    const copy = [...filtered];
    copy.sort((a, b) =>
      compareSortValues(
        getDataProductsSortValue(sortKey, a),
        getDataProductsSortValue(sortKey, b),
        sortDir,
        nullsFirst,
      ),
    );
    return copy;
  }, [filtered, sortKey, sortDir]);

  const paged = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sorted.slice(start, start + PAGE_SIZE);
  }, [sorted, page]);

  const hasActiveFilters = stewardFilter !== ALL || scoreFilter !== DQ_SCORE_FILTER_ALL || search.trim() !== "";

  const applyFilter = useCallback(
    <T,>(setter: (v: T) => void) =>
      (v: T) => {
        setter(v);
        setPage(1);
      },
    [],
  );

  const openProduct = (product: DataProductOut) => {
    navigate({ to: "/table-spaces/$productId", params: { productId: product.product_id } });
  };

  const [pendingId, setPendingId] = useState<string | null>(null);
  const [rejectTarget, setRejectTarget] = useState<DataProductOut | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DataProductOut | null>(null);
  const [diffTarget, setDiffTarget] = useState<TableSpaceDiffTarget | null>(null);

  const approveMutation = useApproveDataProduct();
  const rejectMutation = useRejectDataProduct();
  const revertMutation = useRevertDataProduct();
  const deleteMutation = useDeleteDataProduct();
  const runMutation = useRunDataProduct();

  const invalidateAfterChange = useCallback(
    (productId: string) => {
      queryClient.invalidateQueries({ queryKey: getGetDataProductQueryKey(productId) });
      queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
    },
    [queryClient],
  );

  // Bulk-selection state — mirrors monitored-tables.index.tsx
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkBusy, setBulkBusy] = useState(false);
  const [bulkRunOpen, setBulkRunOpen] = useState(false);
  const [bulkApproveOpen, setBulkApproveOpen] = useState(false);
  const [bulkRejectOpen, setBulkRejectOpen] = useState(false);
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);

  /** Product IDs eligible for selection — same gating as the row-level bar. */
  const selectableIds = useMemo(() => {
    const ids = new Set<string>();
    for (const p of sorted) {
      const isPending = p.status === "pending_approval";
      const isModified = p.display_status === "modified";
      if (
        (perms.canRunRules && (p.runnable_count ?? 0) > 0) ||
        (isPending && perms.canApproveRules) ||
        (perms.canCreateRules && !isModified && !isPending)
      ) {
        ids.add(p.product_id);
      }
    }
    return ids;
  }, [sorted, perms]);

  const selectedRows = useMemo(
    () => sorted.filter((p) => selectedIds.has(p.product_id)),
    [sorted, selectedIds],
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

  const tableSelection = useMemo<DataProductsTableSelection>(
    () => ({
      selectedIds,
      selectableIds,
      onToggle: toggleSelect,
      onToggleAll: toggleSelectAll,
    }),
    [selectedIds, selectableIds, toggleSelect, toggleSelectAll],
  );

  /** Runs a bulk operation sequentially and shows success/partial toasts. */
  const bulkAction = useCallback(
    async (
      productsToAct: DataProductOut[],
      mutate: (productId: string) => Promise<unknown>,
      successMsg: string,
    ) => {
      if (bulkBusy || productsToAct.length === 0) return;
      setBulkBusy(true);
      let ok = 0;
      let fail = 0;
      let lastDetail = "";
      for (const p of productsToAct) {
        try {
          await mutate(p.product_id);
          ok++;
          invalidateAfterChange(p.product_id);
        } catch (err: unknown) {
          fail++;
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          if (detail) lastDetail = detail;
        }
      }
      setBulkBusy(false);
      setSelectedIds(new Set());
      if (fail === 0) {
        toast.success(t("dataProducts.bulkSucceeded", { count: ok, msg: successMsg }));
      } else {
        toast.warning(
          t("dataProducts.bulkPartial", {
            ok,
            fail,
            reason: lastDetail ? ` — ${lastDetail}` : "",
          }),
        );
      }
    },
    [bulkBusy, invalidateAfterChange, t],
  );

  const confirmBulkRun = () => {
    setBulkRunOpen(false);
    const eligible = selectedRows.filter((p) => (p.runnable_count ?? 0) > 0 && perms.canRunRules);
    bulkAction(
      eligible,
      (productId) => runMutation.mutateAsync({ productId, data: { source: "approved" } }),
      t("dataProducts.bulkRunStarted"),
    );
  };

  const confirmBulkApprove = () => {
    setBulkApproveOpen(false);
    const eligible = selectedRows.filter((p) => p.status === "pending_approval");
    bulkAction(
      eligible,
      (productId) => approveMutation.mutateAsync({ productId }),
      t("dataProducts.bulkApproved"),
    );
  };

  const confirmBulkReject = () => {
    setBulkRejectOpen(false);
    const eligible = selectedRows.filter((p) => p.status === "pending_approval");
    bulkAction(
      eligible,
      (productId) => rejectMutation.mutateAsync({ productId }),
      t("dataProducts.bulkRejected"),
    );
  };

  const confirmBulkDelete = () => {
    setBulkDeleteOpen(false);
    const eligible = selectedRows.filter(
      (p) => perms.canCreateRules && p.display_status !== "modified" && p.status !== "pending_approval",
    );
    bulkAction(
      eligible,
      (productId) => deleteMutation.mutateAsync({ productId }),
      t("dataProducts.bulkDeleted"),
    );
  };

  // Canonical action order across all three overviews (bug-bash-v4 item 14):
  // Run → Approve → Reject → Export → Delete → Clear.
  const bulkToolbar = (
    <BulkActionBar
      count={selectedIds.size}
      label={t("dataProducts.selectedCount", { count: selectedIds.size })}
      busy={bulkBusy}
      onClear={() => setSelectedIds(new Set())}
      clearLabel={t("dataProducts.clearSelection")}
    >
      {perms.canRunRules && selectedRows.some((p) => (p.runnable_count ?? 0) > 0) && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs" onClick={() => setBulkRunOpen(true)}>
          <Play className="h-3 w-3" />
          {t("dataProducts.bulkRun")}
        </Button>
      )}
      {perms.canApproveRules && selectedRows.some((p) => p.status === "pending_approval") && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-emerald-600" onClick={() => setBulkApproveOpen(true)}>
          <CheckCircle2 className="h-3 w-3" />
          {t("dataProducts.bulkApprove")}
        </Button>
      )}
      {perms.canApproveRules && selectedRows.some((p) => p.status === "pending_approval") && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-destructive" onClick={() => setBulkRejectOpen(true)}>
          <XCircle className="h-3 w-3" />
          {t("dataProducts.bulkReject")}
        </Button>
      )}
      {/* Export lives in the selection action bar (exports exactly the
          ticked rows via the product_id[] filter), mirroring the Rules
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
      <ExportDialog
        open={exportOpen}
        onOpenChange={setExportOpen}
        fetchDqx={() => exportDataProducts({ format: "dqx", product_id: [...selectedIds] })}
        fetchOdcs={() => exportDataProducts({ format: "odcs", product_id: [...selectedIds] })}
      />
      {perms.canCreateRules && selectedRows.some((p) => p.display_status !== "modified" && p.status !== "pending_approval") && (
        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-destructive" onClick={() => setBulkDeleteOpen(true)}>
          <Trash2 className="h-3 w-3" />
          {t("dataProducts.bulkDelete")}
        </Button>
      )}
    </BulkActionBar>
  );

  // Shared runner for the row-level approve/reject/delete actions — mirrors
  // the Monitored Tables list page's `runRowAction`.
  const runRowAction = useCallback(
    (productId: string, mutate: () => Promise<unknown>, successMsg: string, errorMsg: string) => {
      if (pendingId) return;
      setPendingId(productId);
      mutate()
        .then(() => {
          toast.success(successMsg);
          invalidateAfterChange(productId);
        })
        .catch((err: unknown) => {
          toast.error(extractApiError(err, errorMsg), { duration: 6000 });
        })
        .finally(() => setPendingId(null));
    },
    [pendingId, invalidateAfterChange],
  );

  const handleApprove = (product: DataProductOut) =>
    runRowAction(
      product.product_id,
      () => approveMutation.mutateAsync({ productId: product.product_id }),
      t("dataProducts.toastApproved"),
      t("dataProducts.toastApproveFailed"),
    );

  // Revert (withdraw to draft) — the author's counterpart to submit. Unlike
  // reject it leaves no rejected trail, so it fires directly (no confirm).
  const handleRevert = (product: DataProductOut) =>
    runRowAction(
      product.product_id,
      () => revertMutation.mutateAsync({ productId: product.product_id }),
      t("dataProducts.toastReverted"),
      t("dataProducts.toastRevertFailed"),
    );

  // Run the approved snapshot (item 76). Gated to spaces with at least one
  // runnable member (`runnable_count > 0`) below. Not routed through
  // `runRowAction` because the run endpoint returns 200 even when every
  // member failed to launch (empty `submitted`, run set rolled back) — that
  // must surface as a failure, mirroring the detail header's `handleRun`.
  // `pendingId` swaps the row's action cell to a spinner while in flight; a
  // cross-session "already running" gate would need the per-row run-activity
  // feed the far-left running cog needs, deferred with it (see report).
  const handleRun = (product: DataProductOut) => {
    if (pendingId) return;
    setPendingId(product.product_id);
    runMutation
      .mutateAsync({ productId: product.product_id, data: { source: "approved" } })
      .then((resp) => {
        if ((resp.data.submitted?.length ?? 0) === 0) {
          toast.error(t("dataProducts.toastRunNoneStarted"), { duration: 6000 });
          return;
        }
        toast.success(t("dataProducts.toastRunStarted"));
        invalidateAfterChange(product.product_id);
      })
      .catch((err: unknown) => {
        toast.error(extractApiError(err, t("dataProducts.toastRunFailed")), { duration: 6000 });
      })
      .finally(() => setPendingId(null));
  };

  // Reject is destructive (sends the space back to draft) so it is gated
  // behind a confirm dialog, mirroring the detail header's reject button —
  // the actual mutation only fires from `confirmReject` once confirmed.
  const confirmReject = () => {
    if (!rejectTarget) return;
    const product = rejectTarget;
    setRejectTarget(null);
    runRowAction(
      product.product_id,
      () => rejectMutation.mutateAsync({ productId: product.product_id }),
      t("dataProducts.toastRejected"),
      t("dataProducts.toastRejectFailed"),
    );
  };

  const confirmDelete = () => {
    if (!deleteTarget) return;
    const product = deleteTarget;
    setDeleteTarget(null);
    runRowAction(
      product.product_id,
      () => deleteMutation.mutateAsync({ productId: product.product_id }),
      t("dataProducts.toastDeleted"),
      t("dataProducts.toastDeleteFailed"),
    );
  };

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("dataProducts.title")} />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("dataProducts.title")}</h1>
            <p className="text-sm text-muted-foreground mt-1">{t("dataProducts.subtitle")}</p>
          </div>
          <div className="flex items-center gap-2">
            {/* Export lives in the selection action bar (bulkToolbar) — select
                rows to export exactly those, mirroring the Rules overview. */}
            {perms.canCreateRules && (
              <Button onClick={() => navigate({ to: "/table-spaces/new" })} className="gap-2">
                <Plus className="h-4 w-4" />
                {t("dataProducts.newProduct")}
              </Button>
            )}
          </div>
        </div>

        <div className="relative">
          {bulkToolbar}
          <DataProductsTable
          rows={paged}
          sortKey={sortKey}
          sortDir={sortDir}
          onHeaderClick={handleHeaderClick}
          onRowClick={openProduct}
          pendingProductId={pendingId}
          selection={tableSelection}
          renderActions={
            // Per-status action gating: approve/reject gated on canApproveRules;
            // run gated on canRunRules; delete gated on canCreateRules and hidden
            // when the row has pending changes (modified or pending_approval).
            (product) => (
                  <div className="flex items-center justify-end gap-1">
                    {perms.canRunRules && (product.runnable_count ?? 0) > 0 && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0"
                            aria-label={t("dataProducts.runAction")}
                            onClick={() => handleRun(product)}
                          >
                            <Play className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("dataProducts.runAction")}</TooltipContent>
                      </Tooltip>
                    )}
                    {product.status === "pending_approval" && perms.canApproveRules && (
                      <>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-emerald-600"
                              aria-label={t("dataProducts.approveAction")}
                              onClick={() => handleApprove(product)}
                            >
                              <CheckCircle2 className="h-3.5 w-3.5" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>{t("dataProducts.approveAction")}</TooltipContent>
                        </Tooltip>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-destructive"
                              aria-label={t("dataProducts.rejectAction")}
                              onClick={() => setRejectTarget(product)}
                            >
                              <XCircle className="h-3.5 w-3.5" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>{t("dataProducts.rejectAction")}</TooltipContent>
                        </Tooltip>
                      </>
                    )}
                    {(product.display_status === "modified" || product.status === "pending_approval") && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-purple-600 dark:text-purple-400"
                            aria-label={t("rulesDrafts.diff.viewChanges")}
                            onClick={() =>
                              setDiffTarget({
                                productId: product.product_id,
                                name: product.name,
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
                    {perms.canCreateRules && product.status === "pending_approval" && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-amber-600 dark:text-amber-400"
                            aria-label={t("dataProducts.revertAction")}
                            onClick={() => handleRevert(product)}
                          >
                            <Undo2 className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("dataProducts.revertAction")}</TooltipContent>
                      </Tooltip>
                    )}
                    {perms.canCreateRules && product.display_status !== "modified" && product.status !== "pending_approval" && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-destructive"
                            aria-label={t("dataProducts.actionDelete")}
                            onClick={() => setDeleteTarget(product)}
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("dataProducts.actionDelete")}</TooltipContent>
                      </Tooltip>
                    )}
                  </div>
                )
          }
          toolbarExtra={
            <>
              <div className="relative w-56">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder={t("dataProducts.searchPlaceholder")}
                  value={search}
                  onChange={(e) => applyFilter(setSearch)(e.target.value)}
                  className="h-8 text-xs pl-7"
                />
              </div>
              <SearchableSelect
                value={stewardFilter}
                onChange={applyFilter(setStewardFilter)}
                options={stewardOptions.map((s) => ({ value: s, label: s }))}
                allValue={ALL}
                allLabel={t("dataProducts.allStewards")}
                searchPlaceholder={t("common.search")}
                emptyText={t("common.noMatches")}
                ariaLabel={t("dataProducts.colSteward")}
              />
              <Select value={scoreFilter} onValueChange={applyFilter(setScoreFilter)}>
                <SelectTrigger className={FILTER_TRIGGER_CLASS} aria-label={t("dataProducts.colDqScore")}>
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
                {/* Boxes — the same glyph as the Table Spaces sidebar nav, so
                    the empty state reads as the same feature. Boxes is built
                    from isometric diagonals at fractional coordinates that
                    anti-alias softly at 48px / very-low opacity, so we render
                    it a touch smaller (h-10) and at /50 opacity where its
                    strokes stay crisp rather than blurring. */}
                <Boxes className="h-10 w-10 text-muted-foreground/50 mb-3" />
                <p className="text-sm text-muted-foreground">
                  {hasActiveFilters
                    ? t("dataProducts.emptyState")
                    : perms.canCreateRules
                      ? t("dataProducts.emptyStateNoProductsCta")
                      : t("dataProducts.emptyStateNoProducts")}
                </p>
              </div>
            )
          }
          />
        </div>

        {filtered.length > 0 && (
          <Pagination page={page} totalItems={filtered.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
        )}
      </div>

      {/* Reject is destructive — gate it behind a confirm dialog, mirroring
          the Monitored Tables list page's reject confirm (P21-A lesson). */}
      <AlertDialog open={rejectTarget !== null} onOpenChange={(open) => !open && setRejectTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.rejectConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("dataProducts.rejectConfirmDescription", { name: rejectTarget?.name ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmReject}>
              {t("dataProducts.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={deleteTarget !== null} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.deleteConfirmDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmDelete}>
              {t("dataProducts.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <TableSpaceDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

      <AlertDialog open={bulkRunOpen} onOpenChange={setBulkRunOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("dataProducts.bulkRunTitle", {
                count: selectedRows.filter((p) => (p.runnable_count ?? 0) > 0).length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.bulkRunBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkRun}>{t("dataProducts.runAction")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkApproveOpen} onOpenChange={setBulkApproveOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("dataProducts.bulkApproveTitle", {
                count: selectedRows.filter((p) => p.status === "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.bulkApproveBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkApprove}>{t("dataProducts.approveAction")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkRejectOpen} onOpenChange={setBulkRejectOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("dataProducts.bulkRejectTitle", {
                count: selectedRows.filter((p) => p.status === "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.bulkRejectBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmBulkReject}>
              {t("dataProducts.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkDeleteOpen} onOpenChange={setBulkDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("dataProducts.bulkDeleteTitle", {
                count: selectedRows.filter((p) => p.display_status !== "modified" && p.status !== "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.bulkDeleteBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmBulkDelete}>
              {t("dataProducts.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
