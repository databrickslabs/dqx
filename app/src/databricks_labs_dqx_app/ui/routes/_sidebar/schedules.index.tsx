import { useCallback, useMemo, useState, type ReactNode } from "react";
import { createFileRoute, Link, Navigate, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { AlertCircle, CalendarClock, History, Loader2, Pause, Play, Plus, RotateCcw, Search, Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { FadeIn } from "@/components/anim/FadeIn";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { Pagination } from "@/components/Pagination";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
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
import { BulkActionBar } from "@/components/data-table/BulkActionBar";
import { FILTER_TRIGGER_CLASS } from "@/components/data-table/filter-bar";
import { filterLayoutMenuConfig, useFilterLayout } from "@/components/data-table/filter-layout";
import { SearchableSelect } from "@/components/data-table/SearchableSelect";
import { compareSortValues, type SortDirection } from "@/components/data-table/sort";
import {
  EMPTY_SCHEDULE_FILTERS,
  SCHEDULE_FILTER_ALL,
  SCHEDULE_FILTER_DEFS,
  SCHEDULE_FILTER_ORDER,
  getScheduleSortValue,
  hasActiveScheduleFilters,
  matchesScheduleFilters,
  resetScheduleFilter,
  scheduleOwners,
  scheduleRowKey,
  tablesScheduledByCollections,
  type ScheduleFilterKey,
  type ScheduleFilters,
  type ScheduleSortKey,
} from "@/components/schedules/schedule-list";
import { SchedulesTable, getScheduleSortConfig, type SchedulesTableSelection } from "@/components/schedules/SchedulesTable";
import { usePermissions } from "@/hooks/use-permissions";
import {
  deleteSchedule,
  getListDataProductsQueryKey,
  getListScheduleOverviewQueryKey,
  getListSchedulesQueryKey,
  saveSchedule,
  setSchedulePaused,
  updateDataProduct,
  updateMonitoredTableSchedule,
  useListDataProducts,
  useListScheduleOverview,
  useListSchedules,
  type ScheduleOverviewOut,
} from "@/lib/api";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/_sidebar/schedules/")({
  component: SchedulesPage,
});

const PAGE_SIZE = 50;
const LS_KEY_FILTERS = "dqx.schedules.filters.v1";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

function SchedulesPage() {
  const permissions = usePermissions();
  if (!permissions.canRunRules) return <Navigate to="/results" replace />;
  return <SchedulesContent isAdmin={permissions.isAdmin} canCreate={permissions.canCreateRules} />;
}

/** Target of a delete confirmation: one row (row action) or the selection. */
type DeleteTarget = { kind: "row"; row: ScheduleOverviewOut } | { kind: "bulk"; rows: ScheduleOverviewOut[] };

function SchedulesContent({ isAdmin, canCreate }: { isAdmin: boolean; canCreate: boolean }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data, isPending, isError, refetch } = useListScheduleOverview();
  const { data: configsData } = useListSchedules();
  const { data: productsData } = useListDataProducts();

  const rows = useMemo(() => data?.data ?? [], [data]);
  const owners = useMemo(() => scheduleOwners(rows), [rows]);
  const duplicateTargets = useMemo(() => tablesScheduledByCollections(productsData?.data ?? []), [productsData]);

  const [filters, setFilters] = useState<ScheduleFilters>(EMPTY_SCHEDULE_FILTERS);
  const [page, setPage] = useState(1);
  const setFilter = useCallback(<K extends keyof ScheduleFilters>(key: K, value: ScheduleFilters[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
    setPage(1);
  }, []);

  // Which filter pills show, and in what order (Edit Columns > Filters).
  // Hiding a pill clears its value so it can't keep narrowing the rows.
  const filterLayout = useFilterLayout<ScheduleFilterKey>({
    storageKey: LS_KEY_FILTERS,
    defaultOrder: SCHEDULE_FILTER_ORDER,
    filters: SCHEDULE_FILTER_DEFS,
    onHide: (key) => {
      setFilters((prev) => resetScheduleFilter(prev, key));
      setPage(1);
    },
  });

  const filtered = useMemo(() => rows.filter((row) => matchesScheduleFilters(row, filters)), [rows, filters]);

  const [sortKey, setSortKey] = useState<ScheduleSortKey | null>(null);
  const [sortDir, setSortDir] = useState<SortDirection>("asc");
  const handleHeaderClick = useCallback(
    (key: ScheduleSortKey) => {
      // First click uses the column's default direction; repeat clicks toggle
      // to the opposite direction, then clear — same as the other overviews.
      const { dir } = getScheduleSortConfig(key);
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
    const { nullsFirst } = getScheduleSortConfig(sortKey);
    return [...filtered].sort((a, b) =>
      compareSortValues(getScheduleSortValue(sortKey, a), getScheduleSortValue(sortKey, b), sortDir, nullsFirst),
    );
  }, [filtered, sortKey, sortDir]);

  const paged = useMemo(() => sorted.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE), [sorted, page]);

  const refreshAfterChange = useCallback(
    (changed: readonly ScheduleOverviewOut[]) => {
      void queryClient.invalidateQueries({ queryKey: getListScheduleOverviewQueryKey() });
      if (changed.some((row) => row.source_type === "scope")) {
        void queryClient.invalidateQueries({ queryKey: getListSchedulesQueryKey() });
      }
      if (changed.some((row) => row.source_type === "collection")) {
        void queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
      }
      for (const row of changed) {
        if (row.source_type === "table") invalidateAfterMonitoredTableChange(queryClient, row.source_id);
      }
    },
    [queryClient],
  );

  // -- Mutations (plain request functions so bulk actions can await them) ---
  const setPaused = useCallback(
    async (row: ScheduleOverviewOut, paused: boolean): Promise<void> => {
      if (row.source_type === "scope") {
        const entry = (configsData?.data ?? []).find((config) => config.schedule_name === row.source_id);
        if (!entry) throw new Error(t("schedules.scopeMissing"));
        await saveSchedule({ schedule_name: entry.schedule_name, config: { ...entry.config, paused } });
        return;
      }
      await setSchedulePaused(row.source_type, row.source_id, { paused });
    },
    [configsData, t],
  );

  const removeSchedule = useCallback(async (row: ScheduleOverviewOut): Promise<void> => {
    const cleared = {
      schedule_cron: null,
      schedule_tz: null,
      schedule_kind: row.schedule_kind ?? "profiling_and_dq",
      schedule_sample_size: null,
    };
    if (row.source_type === "scope") await deleteSchedule(row.source_id);
    else if (row.source_type === "table") await updateMonitoredTableSchedule(row.source_id, cleared);
    else await updateDataProduct(row.source_id, cleared);
  }, []);

  // -- Row actions ------------------------------------------------------------
  const [pendingKey, setPendingKey] = useState<string | null>(null);
  const runRowAction = useCallback(
    (row: ScheduleOverviewOut, action: () => Promise<void>, successMsg: string) => {
      if (pendingKey) return;
      setPendingKey(scheduleRowKey(row));
      action()
        .then(() => {
          toast.success(successMsg);
          refreshAfterChange([row]);
        })
        .catch((err: unknown) => toast.error(extractApiError(err, t("schedules.actionFailed")), { duration: 6000 }))
        .finally(() => setPendingKey(null));
    },
    [pendingKey, refreshAfterChange, t],
  );

  // -- Bulk selection (admins only: pause/resume/delete are admin actions) ----
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  const [bulkBusy, setBulkBusy] = useState(false);
  const selectableKeys = useMemo(() => new Set(isAdmin ? sorted.map(scheduleRowKey) : []), [isAdmin, sorted]);
  const selectedRows = useMemo(() => sorted.filter((row) => selectedKeys.has(scheduleRowKey(row))), [sorted, selectedKeys]);

  const toggleSelect = useCallback((key: string) => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);
  const toggleSelectAll = useCallback(() => {
    setSelectedKeys((prev) => (prev.size === selectableKeys.size ? new Set() : new Set(selectableKeys)));
  }, [selectableKeys]);
  const selection = useMemo<SchedulesTableSelection | undefined>(
    () =>
      isAdmin ? { selectedKeys, selectableKeys, onToggle: toggleSelect, onToggleAll: toggleSelectAll } : undefined,
    [isAdmin, selectedKeys, selectableKeys, toggleSelect, toggleSelectAll],
  );

  /** Runs a bulk operation sequentially, then reports success or a partial count. */
  const bulkAction = useCallback(
    async (targets: ScheduleOverviewOut[], action: (row: ScheduleOverviewOut) => Promise<void>, successMsg: string) => {
      if (bulkBusy || targets.length === 0) return;
      setBulkBusy(true);
      let ok = 0;
      let lastDetail = "";
      const done: ScheduleOverviewOut[] = [];
      for (const row of targets) {
        try {
          await action(row);
          ok++;
          done.push(row);
        } catch (err: unknown) {
          lastDetail = extractApiError(err, lastDetail);
        }
      }
      setBulkBusy(false);
      setSelectedKeys(new Set());
      refreshAfterChange(done);
      const fail = targets.length - ok;
      if (fail === 0) toast.success(t("schedules.bulkSucceeded", { count: ok, msg: successMsg }));
      else toast.warning(t("schedules.bulkPartial", { ok, fail, reason: lastDetail ? ` — ${lastDetail}` : "" }));
    },
    [bulkBusy, refreshAfterChange, t],
  );

  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const confirmDelete = () => {
    if (!deleteTarget) return;
    const target = deleteTarget;
    setDeleteTarget(null);
    if (target.kind === "row") {
      runRowAction(target.row, () => removeSchedule(target.row), t("schedules.removed"));
    } else {
      void bulkAction(target.rows, removeSchedule, t("schedules.bulkRemoved"));
    }
  };

  const pausable = selectedRows.filter((row) => !row.paused);
  const resumable = selectedRows.filter((row) => row.paused);
  const bulkToolbar = (
    <BulkActionBar
      count={selectedKeys.size}
      label={t("schedules.selectedCount", { count: selectedKeys.size })}
      busy={bulkBusy}
      onClear={() => setSelectedKeys(new Set())}
      clearLabel={t("schedules.clearSelection")}
    >
      {pausable.length > 0 && (
        <Button
          size="sm"
          variant="outline"
          className="h-7 gap-1 text-xs"
          onClick={() => void bulkAction(pausable, (row) => setPaused(row, true), t("schedules.bulkPaused"))}
        >
          <Pause className="h-3 w-3" />
          {t("schedules.pause")}
        </Button>
      )}
      {resumable.length > 0 && (
        <Button
          size="sm"
          variant="outline"
          className="h-7 gap-1 text-xs"
          onClick={() => void bulkAction(resumable, (row) => setPaused(row, false), t("schedules.bulkResumed"))}
        >
          <Play className="h-3 w-3" />
          {t("schedules.resume")}
        </Button>
      )}
      <Button
        size="sm"
        variant="outline"
        className="h-7 gap-1 text-xs text-destructive"
        onClick={() => setDeleteTarget({ kind: "bulk", rows: selectedRows })}
      >
        <Trash2 className="h-3 w-3" />
        {t("schedules.deleteSchedule")}
      </Button>
    </BulkActionBar>
  );

  const openRow = (row: ScheduleOverviewOut) => {
    if (row.source_type === "table") {
      void navigate({ to: "/monitored-tables/$bindingId", params: { bindingId: row.source_id }, search: { tab: "schedule" } });
    } else if (row.source_type === "collection") {
      void navigate({ to: "/collections/$productId", params: { productId: row.source_id }, search: { tab: "scheduling" } });
    }
  };

  const renderActions = (row: ScheduleOverviewOut) => (
    <div className="flex items-center justify-end gap-1">
      <ViewRunsButton row={row} />
      {isAdmin && (
        <>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 w-7 p-0"
                aria-label={row.paused ? t("schedules.resume") : t("schedules.pause")}
                onClick={() =>
                  runRowAction(row, () => setPaused(row, !row.paused), row.paused ? t("schedules.resumed") : t("schedules.pausedToast"))
                }
              >
                {row.paused ? <Play className="h-3.5 w-3.5" /> : <Pause className="h-3.5 w-3.5" />}
              </Button>
            </TooltipTrigger>
            <TooltipContent>{row.paused ? t("schedules.resume") : t("schedules.pause")}</TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 w-7 p-0 text-destructive"
                aria-label={t("schedules.deleteSchedule")}
                onClick={() => setDeleteTarget({ kind: "row", row })}
              >
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>{t("schedules.deleteSchedule")}</TooltipContent>
          </Tooltip>
        </>
      )}
    </div>
  );

  const hasFilters = hasActiveScheduleFilters(filters);
  const emptyState = isPending ? (
    <div className="flex items-center justify-center gap-2 py-4 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin" />
      {t("common.loading")}
    </div>
  ) : isError ? (
    <div className="flex flex-col items-center justify-center text-center">
      <AlertCircle className="mb-3 h-12 w-12 text-destructive/30" />
      <p className="mb-3 text-sm text-muted-foreground">{t("common.loadFailed")}</p>
      <Button variant="outline" size="sm" onClick={() => void refetch()} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  ) : (
    <div className="flex flex-col items-center justify-center text-center">
      <CalendarClock className="mb-3 h-12 w-12 text-muted-foreground/30" />
      <p className="text-sm text-muted-foreground">
        {hasFilters ? t("schedules.empty") : canCreate ? t("schedules.emptyNoSchedulesCta") : t("schedules.emptyNoSchedules")}
      </p>
    </div>
  );

  const filterControls: Record<ScheduleFilterKey, ReactNode> = {
    search: (
      <div key="search" className="relative w-56">
        <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          placeholder={t("schedules.search")}
          value={filters.search}
          onChange={(e) => setFilter("search", e.target.value)}
          className="h-8 pl-7 text-xs"
        />
      </div>
    ),
    type: (
      <Select key="type" value={filters.type} onValueChange={(value) => setFilter("type", value as ScheduleFilters["type"])}>
        <SelectTrigger className={FILTER_TRIGGER_CLASS} aria-label={t("schedules.filterType")}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={SCHEDULE_FILTER_ALL} className="text-xs">
            {t("schedules.allTypes")}
          </SelectItem>
          <SelectItem value="table" className="text-xs">
            {t("schedules.typeTable")}
          </SelectItem>
          <SelectItem value="collection" className="text-xs">
            {t("schedules.typeCollection")}
          </SelectItem>
          <SelectItem value="scope" className="text-xs">
            {t("schedules.typeScope")}
          </SelectItem>
        </SelectContent>
      </Select>
    ),
    status: (
      <Select
        key="status"
        value={filters.status}
        onValueChange={(value) => setFilter("status", value as ScheduleFilters["status"])}
      >
        <SelectTrigger className={FILTER_TRIGGER_CLASS} aria-label={t("schedules.filterStatus")}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={SCHEDULE_FILTER_ALL} className="text-xs">
            {t("schedules.allStatuses")}
          </SelectItem>
          <SelectItem value="active" className="text-xs">
            {t("schedules.active")}
          </SelectItem>
          <SelectItem value="paused" className="text-xs">
            {t("schedules.paused")}
          </SelectItem>
          <SelectItem value="failing" className="text-xs">
            {t("schedules.failing")}
          </SelectItem>
        </SelectContent>
      </Select>
    ),
    owner: (
      <SearchableSelect
        key="owner"
        value={filters.owner}
        onChange={(value) => setFilter("owner", value)}
        options={owners.map((owner) => ({ value: owner, label: owner }))}
        allValue={SCHEDULE_FILTER_ALL}
        allLabel={t("schedules.allOwners")}
        searchPlaceholder={t("common.search")}
        emptyText={t("common.noMatches")}
        ariaLabel={t("schedules.filterOwner")}
      />
    ),
  };

  const deleteCount = deleteTarget?.kind === "bulk" ? deleteTarget.rows.length : 1;

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("schedules.title")} />
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("schedules.title")}</h1>
            <p className="mt-1 text-sm text-muted-foreground">{t("schedules.subtitle")}</p>
          </div>
          <div className="flex items-center gap-2">
            {canCreate && (
              <Button onClick={() => void navigate({ to: "/schedules/new" })} className="gap-2">
                <Plus className="h-4 w-4" />
                {t("schedules.newSchedule")}
              </Button>
            )}
          </div>
        </div>

        <div className="relative">
          {bulkToolbar}
          <SchedulesTable
            rows={paged}
            sortKey={sortKey}
            sortDir={sortDir}
            onHeaderClick={handleHeaderClick}
            onRowClick={openRow}
            renderActions={renderActions}
            pendingKey={pendingKey}
            duplicateTargets={duplicateTargets}
            selection={selection}
            emptyState={emptyState}
            filterMenu={filterLayoutMenuConfig(filterLayout, (key) => t(SCHEDULE_FILTER_DEFS[key].labelKey))}
            toolbarExtra={filterLayout.visibleKeys.map((key) => filterControls[key])}
          />
        </div>

        {filtered.length > 0 && (
          <Pagination page={page} totalItems={filtered.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
        )}
      </div>

      <AlertDialog open={deleteTarget !== null} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("schedules.deleteConfirmTitle", { count: deleteCount })}</AlertDialogTitle>
            <AlertDialogDescription>
              {deleteTarget?.kind === "row"
                ? t("schedules.removeConfirm", { name: deleteTarget.row.name })
                : t("schedules.deleteConfirmBulk", { count: deleteCount })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmDelete}>
              {t("schedules.deleteSchedule")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}

/** Opens Runs History narrowed to this schedule: the table's runs, the
 *  collection's member-table runs, or (named scopes, whose targets are
 *  resolved at run time) every run. */
function ViewRunsButton({ row }: { row: ScheduleOverviewOut }) {
  const { t } = useTranslation();
  const search =
    row.source_type === "table"
      ? { tableFqn: row.target }
      : row.source_type === "collection"
        ? { productId: row.source_id }
        : {};
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button asChild variant="ghost" size="sm" className="h-7 w-7 p-0">
          <Link to="/runs-history" search={search} aria-label={t("schedules.viewRuns")}>
            <History className="h-3.5 w-3.5" />
          </Link>
        </Button>
      </TooltipTrigger>
      <TooltipContent>{t("schedules.viewRuns")}</TooltipContent>
    </Tooltip>
  );
}
