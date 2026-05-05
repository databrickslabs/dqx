import { createFileRoute } from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  useListRules,
  type User as UserType,
} from "@/lib/api";
import { isAxiosError } from "axios";
import {
  useListValidationRuns,
  type ValidationRunSummaryOut,
} from "@/lib/api-custom";
import { useGetDryRunResults, getDryRunStatus, type DryRunResultsOut } from "@/lib/api";
import { DryRunResults } from "@/components/DryRunResults";
import { CommentThread } from "@/components/CommentThread";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  AlertCircle,
  RotateCcw,
  Loader2,
  History,
  CheckCircle2,
  XCircle,
  Clock,
  User,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { useState, useCallback, useEffect, Suspense, useMemo, type ReactNode } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { FadeIn } from "@/components/anim/FadeIn";
import { ArrowDown, ArrowUp, ArrowUpDown, CircleStop, ShieldAlert } from "lucide-react";
import { parseFqn, formatDateTime as formatDate, getUserMetadata, labelToken } from "@/lib/format-utils";
import { LabelFilter, labelsMatchFilter } from "@/components/Labels";

export const Route = createFileRoute("/_sidebar/runs-history")({
  component: RunsHistoryPage,
});

const _SQL_CHECK_PREFIX = "__sql_check__/";

function cleanFqn(fqn: string) {
  return fqn.startsWith(_SQL_CHECK_PREFIX) ? fqn.slice(_SQL_CHECK_PREFIX.length) : fqn;
}

function statusBadge(status: string | null) {
  switch (status) {
    case "SUCCESS":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          Success
        </Badge>
      );
    case "FAILED":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          Failed
        </Badge>
      );
    case "RUNNING":
      return (
        <Badge variant="outline" className="gap-1 border-blue-500 text-blue-600">
          <Loader2 className="h-3 w-3 animate-spin" />
          Running
        </Badge>
      );
    case "CANCELED":
      return (
        <Badge variant="outline" className="gap-1 border-gray-400 text-gray-500">
          <CircleStop className="h-3 w-3" />
          Canceled
        </Badge>
      );
    default:
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3" />
          {status ?? "Pending"}
        </Badge>
      );
  }
}

type SortDir = "asc" | "desc";

function SortableHeader<K extends string>({
  label,
  sortKey,
  active,
  direction,
  onSort,
  children,
  align = "left",
}: {
  label: string;
  sortKey: K;
  active: boolean;
  direction: SortDir;
  onSort: (key: K) => void;
  children?: ReactNode;
  align?: "left" | "right";
}) {
  const Icon = active ? (direction === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      className={`flex items-center gap-1 hover:text-foreground transition-colors ${align === "right" ? "ml-auto" : ""}`}
      onClick={() => onSort(sortKey)}
    >
      {children}
      {label}
      <Icon className={`h-3 w-3 ${active ? "text-foreground" : "text-muted-foreground/50"}`} />
    </button>
  );
}

function useSort<K extends string>(defaultKey: K, defaultDir: SortDir = "desc") {
  const [sort, setSort] = useState<{ key: K; dir: SortDir }>({ key: defaultKey, dir: defaultDir });
  const handleSort = useCallback((key: K) => {
    setSort((prev) => {
      if (prev.key === key) return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
      return { key, dir: key === ("run_date" as string) ? "desc" : "asc" };
    });
  }, []);
  return { sortKey: sort.key, sortDir: sort.dir, handleSort };
}

function RunsHistoryPage() {
  return (
    <div className="flex flex-col h-full">
      <PageBreadcrumb items={[]} page="Runs History" />
      <div className="flex-1 overflow-hidden mt-4">
        <QueryErrorResetBoundary>
          {({ reset }) => (
            <ErrorBoundary onReset={reset} fallbackRender={RunHistoryError}>
              <Suspense fallback={<RunHistorySkeleton />}>
                <RunHistoryContent />
              </Suspense>
            </ErrorBoundary>
          )}
        </QueryErrorResetBoundary>
      </div>
    </div>
  );
}

type RunsSortKey = "table" | "type" | "status" | "requested_by" | "total" | "valid" | "invalid" | "run_date";

function RunHistoryContent() {
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  const { sortKey: rSortKey, sortDir: rSortDir, handleSort: handleRunsSort } = useSort<RunsSortKey>("run_date");

  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [tableSearch, setTableSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [runTypeFilter, setRunTypeFilter] = useState("all");
  const [invalidOnly, setInvalidOnly] = useState(false);
  const [myRunsOnly, setMyRunsOnly] = useState(false);
  const [labelFilter, setLabelFilter] = useState<Set<string>>(new Set());
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  const { data: runsResp, isLoading, error, refetch } = useListValidationRuns();
  const { data: rulesResp } = useListRules({ status: "approved" }, { query: {} });

  const rawRuns: ValidationRunSummaryOut[] = Array.isArray(runsResp?.data) ? runsResp.data : [];
  const runningRuns = rawRuns.filter((r) => r.status === "RUNNING");

  useEffect(() => {
    if (runningRuns.length === 0) return;
    const id = setInterval(async () => {
      for (const run of runningRuns) {
        try {
          await getDryRunStatus(run.run_id);
        } catch { /* status check is best-effort */ }
      }
      refetch();
    }, 8000);
    return () => clearInterval(id);
  }, [runningRuns.length, refetch]); // eslint-disable-line react-hooks/exhaustive-deps

  const rulesByTable = useMemo(() => {
    const map = new Map<string, Record<string, unknown>[]>();
    const rules = Array.isArray(rulesResp?.data) ? rulesResp.data : [];
    for (const rule of rules) {
      const existing = map.get(rule.table_fqn);
      if (existing) {
        existing.push(...rule.checks);
      } else {
        map.set(rule.table_fqn, [...rule.checks]);
      }
    }
    return map;
  }, [rulesResp]);

  const allRuns: ValidationRunSummaryOut[] = useMemo(() => {
    const raw = Array.isArray(runsResp?.data) ? runsResp.data : [];
    return raw.map((run) => {
      if (run.checks && run.checks.length > 0) return run;
      const fallback = rulesByTable.get(run.source_table_fqn);
      if (fallback && fallback.length > 0) {
        return { ...run, checks: fallback };
      }
      return run;
    });
  }, [runsResp, rulesByTable]);

  const [tableFilter, setTableFilter] = useState("all");

  const { catalogs, schemasByCatalog, tablesByCatalogSchema } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    const tableMap = new Map<string, Set<string>>();
    for (const run of allRuns) {
      if (run.source_table_fqn.startsWith(_SQL_CHECK_PREFIX)) {
        catalogSet.add("Cross-table rules");
        continue;
      }
      const { catalog, schema, table } = parseFqn(run.source_table_fqn);
      if (catalog) {
        catalogSet.add(catalog);
        if (!schemaMap.has(catalog)) schemaMap.set(catalog, new Set());
        if (schema) schemaMap.get(catalog)!.add(schema);
      }
      if (catalog && schema && table) {
        const key = `${catalog}.${schema}`;
        if (!tableMap.has(key)) tableMap.set(key, new Set());
        tableMap.get(key)!.add(table);
      }
    }
    return {
      catalogs: Array.from(catalogSet).sort(),
      schemasByCatalog: Object.fromEntries(
        Array.from(schemaMap.entries()).map(([cat, schemas]) => [cat, Array.from(schemas).sort()]),
      ),
      tablesByCatalogSchema: Object.fromEntries(
        Array.from(tableMap.entries()).map(([key, tables]) => [key, Array.from(tables).sort()]),
      ),
    };
  }, [allRuns]);

  const runs = useMemo(() => {
    const filtered = allRuns.filter((run) => {
      const isSqlCheck = run.source_table_fqn.startsWith(_SQL_CHECK_PREFIX);
      if (catalogFilter !== "all") {
        if (catalogFilter === "Cross-table rules") {
          if (!isSqlCheck) return false;
        } else {
          if (isSqlCheck) return false;
          const { catalog } = parseFqn(run.source_table_fqn);
          if (catalog !== catalogFilter) return false;
        }
      }
      if (!isSqlCheck) {
        const { schema, table } = parseFqn(run.source_table_fqn);
        if (schemaFilter !== "all" && schema !== schemaFilter) return false;
        if (tableFilter !== "all" && table !== tableFilter) return false;
        if (tableSearch && !table.toLowerCase().includes(tableSearch.toLowerCase()) && !run.source_table_fqn.toLowerCase().includes(tableSearch.toLowerCase())) return false;
      } else if (tableSearch) {
        const name = run.source_table_fqn.slice(_SQL_CHECK_PREFIX.length);
        if (!name.toLowerCase().includes(tableSearch.toLowerCase())) return false;
      }
      if (statusFilter !== "all" && run.status !== statusFilter) return false;
      if (runTypeFilter !== "all" && (run.run_type ?? "dryrun") !== runTypeFilter) return false;
      if (invalidOnly && !(run.invalid_rows != null && run.invalid_rows > 0)) return false;
      if (myRunsOnly && currentUserEmail && run.requesting_user !== currentUserEmail) return false;
      if (labelFilter.size > 0) {
        // Match the label filter against any check captured on the run; an
        // empty checks list means we can't match → exclude under filter.
        const checks = run.checks ?? [];
        const matched = checks.some((c) =>
          labelsMatchFilter(getUserMetadata(c as Record<string, unknown>), labelFilter),
        );
        if (!matched) return false;
      }
      return true;
    });

    const dir = rSortDir === "asc" ? 1 : -1;
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (rSortKey) {
        case "table":
          cmp = cleanFqn(a.source_table_fqn).localeCompare(cleanFqn(b.source_table_fqn));
          break;
        case "type":
          cmp = (a.run_type ?? "dryrun").localeCompare(b.run_type ?? "dryrun");
          break;
        case "status":
          cmp = (a.status ?? "").localeCompare(b.status ?? "");
          break;
        case "requested_by":
          cmp = (a.requesting_user ?? "").localeCompare(b.requesting_user ?? "");
          break;
        case "total":
          cmp = (a.total_rows ?? 0) - (b.total_rows ?? 0);
          break;
        case "valid":
          cmp = (a.valid_rows ?? 0) - (b.valid_rows ?? 0);
          break;
        case "invalid":
          cmp = (a.invalid_rows ?? 0) - (b.invalid_rows ?? 0);
          break;
        case "run_date":
          cmp = (a.created_at ?? "").localeCompare(b.created_at ?? "");
          break;
      }
      return cmp * dir;
    });
  }, [allRuns, catalogFilter, schemaFilter, tableFilter, tableSearch, statusFilter, runTypeFilter, invalidOnly, myRunsOnly, currentUserEmail, rSortKey, rSortDir, labelFilter]);

  // Distinct labels seen across all runs' checks. Drives the LabelFilter
  // dropdown content for this page.
  const availableLabels = useMemo(() => {
    const seen = new Set<string>();
    const out: { key: string; value: string }[] = [];
    for (const run of allRuns) {
      for (const check of run.checks ?? []) {
        const md = getUserMetadata(check as Record<string, unknown>);
        for (const [key, value] of Object.entries(md)) {
          const tok = labelToken(key, value);
          if (!seen.has(tok)) {
            seen.add(tok);
            out.push({ key, value });
          }
        }
      }
    }
    return out;
  }, [allRuns]);

  const availableSchemas = catalogFilter !== "all" ? schemasByCatalog[catalogFilter] || [] : [];
  const availableTables = (catalogFilter !== "all" && schemaFilter !== "all")
    ? tablesByCatalogSchema[`${catalogFilter}.${schemaFilter}`] || []
    : [];

  const handleCatalogChange = (value: string) => {
    setCatalogFilter(value);
    setSchemaFilter("all");
    setTableFilter("all");
  };

  const handleSchemaChange = (value: string) => {
    setSchemaFilter(value);
    setTableFilter("all");
  };

  const hasActiveFilters = catalogFilter !== "all" || schemaFilter !== "all" || tableFilter !== "all" || tableSearch !== "" || statusFilter !== "all" || runTypeFilter !== "all" || invalidOnly || myRunsOnly || labelFilter.size > 0;

  const PAGE_SIZE = 25;
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(runs.length / PAGE_SIZE));
  const pagedRuns = useMemo(
    () => runs.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE),
    [runs, currentPage],
  );

  useEffect(() => { setCurrentPage(1); }, [catalogFilter, schemaFilter, tableFilter, tableSearch, statusFilter, runTypeFilter, invalidOnly, myRunsOnly, rSortKey, rSortDir]);

  return (
    <div className="space-y-4 h-full overflow-y-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Runs History</h1>
          <p className="text-muted-foreground text-sm">
            View past rule validation results.
          </p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()} className="gap-1.5 text-xs">
          <RotateCcw className="h-3.5 w-3.5" />
          Refresh
        </Button>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2 text-base">
                <History className="h-4 w-4" />
                Validation runs
              </CardTitle>
              <CardDescription>
                {isLoading
                  ? "Loading..."
                  : `${runs.length} run${runs.length !== 1 ? "s" : ""}${
                      runs.length !== allRuns.length ? ` (filtered from ${allRuns.length})` : ""
                    }`}
              </CardDescription>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap pt-2">
            <Select value={catalogFilter} onValueChange={handleCatalogChange}>
              <SelectTrigger className="w-[160px]">
                <SelectValue placeholder="All Catalogs" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Catalogs</SelectItem>
                {catalogs.map((cat) => (
                  <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={schemaFilter} onValueChange={handleSchemaChange} disabled={catalogFilter === "all"}>
              <SelectTrigger className="w-[160px]">
                <SelectValue placeholder="All Schemas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Schemas</SelectItem>
                {availableSchemas.map((sch) => (
                  <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={tableFilter} onValueChange={setTableFilter} disabled={schemaFilter === "all"}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="All Tables" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Tables</SelectItem>
                {availableTables.map((tbl) => (
                  <SelectItem key={tbl} value={tbl}>{tbl}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-[140px]">
                <SelectValue placeholder="All Statuses" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Statuses</SelectItem>
                <SelectItem value="SUCCESS">Success</SelectItem>
                <SelectItem value="FAILED">Failed</SelectItem>
                <SelectItem value="RUNNING">Running</SelectItem>
              </SelectContent>
            </Select>

            <Select value={runTypeFilter} onValueChange={setRunTypeFilter}>
              <SelectTrigger className="w-[140px]">
                <SelectValue placeholder="All Types" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Types</SelectItem>
                <SelectItem value="dryrun">Manual</SelectItem>
                <SelectItem value="scheduled">Scheduled</SelectItem>
              </SelectContent>
            </Select>

            <Button
              variant={invalidOnly ? "default" : "outline"}
              size="sm"
              className="h-9 gap-1.5 text-xs"
              onClick={() => setInvalidOnly((prev) => !prev)}
            >
              <AlertCircle className="h-3.5 w-3.5" />
              Has invalid
            </Button>

            <Button
              variant={myRunsOnly ? "default" : "outline"}
              size="sm"
              className="h-9 gap-1.5 text-xs"
              onClick={() => setMyRunsOnly((prev) => !prev)}
            >
              <User className="h-3.5 w-3.5" />
              My runs
            </Button>

            <LabelFilter
              available={availableLabels}
              selected={labelFilter}
              onChange={setLabelFilter}
            />

            {hasActiveFilters && (
              <Button
                variant="ghost"
                size="sm"
                className="h-9 text-xs"
                onClick={() => {
                  setCatalogFilter("all");
                  setSchemaFilter("all");
                  setTableFilter("all");
                  setTableSearch("");
                  setStatusFilter("all");
                  setRunTypeFilter("all");
                  setInvalidOnly(false);
                  setMyRunsOnly(false);
                  setLabelFilter(new Set());
                }}
              >
                Clear filters
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {isLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} className="h-14 w-full" />)}
            </div>
          )}

          {error && (
            isAxiosError(error) && error.response?.status === 403 ? (
              <div className="flex flex-col items-center justify-center py-16 text-center">
                <ShieldAlert className="h-12 w-12 text-destructive/30 mb-3" />
                <p className="text-destructive text-sm mb-1">Insufficient permissions</p>
                <p className="text-muted-foreground/70 text-xs">
                  You don't have permission to view run history. Please contact your administrator.
                </p>
              </div>
            ) : (
              <p className="text-destructive text-sm">Failed to load run history: {(error as Error).message}</p>
            )
          )}

          {!isLoading && !error && runs.length > 0 && (
            <FadeIn duration={0.3}>
              <div className="border rounded-lg overflow-x-auto">
                <table className="w-full text-sm min-w-[900px]">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="w-8 p-3"></th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label="Table" sortKey="table" active={rSortKey === "table"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label="Type" sortKey="type" active={rSortKey === "type"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label="Status" sortKey="status" active={rSortKey === "status"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-left p-3 font-medium">Rules</th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label="Requested by" sortKey="requested_by" active={rSortKey === "requested_by"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label="Total" sortKey="total" active={rSortKey === "total"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label="Valid" sortKey="valid" active={rSortKey === "valid"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label="Invalid" sortKey="invalid" active={rSortKey === "invalid"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label="Run date" sortKey="run_date" active={rSortKey === "run_date"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {pagedRuns.map((run) => {
                      const invalidPct =
                        run.total_rows && run.invalid_rows
                          ? ((run.invalid_rows / run.total_rows) * 100).toFixed(1)
                          : null;
                      const isExpanded = expandedRunId === run.run_id;
                      return (
                        <RunHistoryRow
                          key={run.run_id}
                          run={run}
                          invalidPct={invalidPct}
                          isExpanded={isExpanded}
                          onToggle={() => setExpandedRunId(isExpanded ? null : run.run_id)}
                        />
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {totalPages > 1 && (
                <div className="flex items-center justify-between pt-3">
                  <p className="text-xs text-muted-foreground">
                    Showing {(currentPage - 1) * PAGE_SIZE + 1}–{Math.min(currentPage * PAGE_SIZE, runs.length)} of {runs.length}
                  </p>
                  <div className="flex items-center gap-1">
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 w-8 p-0"
                      disabled={currentPage <= 1}
                      onClick={() => setCurrentPage((p) => p - 1)}
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </Button>
                    {Array.from({ length: totalPages }, (_, i) => i + 1)
                      .filter((p) => p === 1 || p === totalPages || Math.abs(p - currentPage) <= 2)
                      .reduce<(number | "...")[]>((acc, p, i, arr) => {
                        if (i > 0 && p - arr[i - 1] > 1) acc.push("...");
                        acc.push(p);
                        return acc;
                      }, [])
                      .map((p, i) =>
                        p === "..." ? (
                          <span key={`ellipsis-${i}`} className="px-1 text-xs text-muted-foreground">...</span>
                        ) : (
                          <Button
                            key={p}
                            variant={p === currentPage ? "default" : "outline"}
                            size="sm"
                            className="h-8 w-8 p-0 text-xs"
                            onClick={() => setCurrentPage(p)}
                          >
                            {p}
                          </Button>
                        ),
                      )}
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 w-8 p-0"
                      disabled={currentPage >= totalPages}
                      onClick={() => setCurrentPage((p) => p + 1)}
                    >
                      <ChevronRight className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              )}
            </FadeIn>
          )}

          {!isLoading && !error && runs.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-center">
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
                <History className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-muted-foreground">
                {hasActiveFilters ? "No matching runs" : "No runs yet"}
              </h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                {hasActiveFilters
                  ? "Try adjusting your filters to find runs."
                  : "Execute approved rules from the Run Rules page to see results here."}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function RunHistoryRow({
  run,
  invalidPct,
  isExpanded,
  onToggle,
}: {
  run: ValidationRunSummaryOut;
  invalidPct: string | null;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const { data: resultsResp, isLoading: isLoadingResults } = useGetDryRunResults(
    run.run_id,
    { query: { enabled: isExpanded && run.status === "SUCCESS" } },
  );
  const results: DryRunResultsOut | null = isExpanded && resultsResp?.data ? resultsResp.data : null;

  return (
    <>
      <tr
        className={cn(
          "border-b hover:bg-muted/30 transition-colors cursor-pointer",
          isExpanded && "bg-muted/20",
        )}
        onClick={onToggle}
      >
        <td className="p-3 w-8">
          {isExpanded ? (
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-4 w-4 text-muted-foreground" />
          )}
        </td>
        <td className="p-3 font-mono text-xs">{cleanFqn(run.source_table_fqn)}</td>
        <td className="p-3">
          <Badge variant={(run.run_type ?? "dryrun") === "scheduled" ? "default" : "outline"} className="text-[10px]">
            {(run.run_type ?? "dryrun") === "scheduled" ? "Scheduled" : "Manual"}
          </Badge>
        </td>
        <td className="p-3">
          <div className="flex flex-col gap-0.5">
            {statusBadge(run.status)}
            {run.status === "CANCELED" && run.canceled_by && (
              <span className="text-[10px] text-muted-foreground" title={`Canceled by ${run.canceled_by}`}>
                by {run.canceled_by.split("@")[0]}
              </span>
            )}
          </div>
        </td>
        <td className="p-3">
          {run.checks && run.checks.length > 0 ? (() => {
            const labels = run.checks.map((c: Record<string, unknown>) => {
              const check = c.check as Record<string, unknown> | undefined;
              const fn = check?.function as string | undefined;
              const args = check?.arguments as Record<string, unknown> | undefined;
              if (!fn) return c.name as string ?? "check";
              const col = args?.column as string | undefined;
              return col ? `${fn}(${col})` : fn;
            });
            const MAX_SHOWN = 3;
            const shown = labels.slice(0, MAX_SHOWN);
            const remaining = labels.length - MAX_SHOWN;
            return (
              <div className="flex flex-col gap-0.5 max-w-[240px]" title={labels.join("\n")}>
                {shown.map((label, i) => (
                  <span key={i} className="font-mono text-[11px] text-muted-foreground truncate">{label}</span>
                ))}
                {remaining > 0 && (
                  <span className="text-[10px] text-muted-foreground/70">+{remaining} more</span>
                )}
              </div>
            );
          })() : (
            <span className="text-xs text-muted-foreground">—</span>
          )}
        </td>
        <td className="p-3 text-xs text-muted-foreground truncate max-w-[180px]" title={run.requesting_user ?? ""}>
          {run.requesting_user ?? "—"}
        </td>
        <td className="p-3 text-right tabular-nums">{run.total_rows?.toLocaleString() ?? "—"}</td>
        <td className="p-3 text-right tabular-nums text-green-600">
          {run.valid_rows?.toLocaleString() ?? "—"}
        </td>
        <td className="p-3 text-right tabular-nums">
          {run.invalid_rows != null ? (
            <span className={run.invalid_rows > 0 ? "text-red-600 font-medium" : ""}>
              {run.invalid_rows.toLocaleString()}
              {invalidPct && run.invalid_rows > 0 && (
                <span className="text-muted-foreground font-normal ml-1">({invalidPct}%)</span>
              )}
            </span>
          ) : (
            "—"
          )}
        </td>
        <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={run.created_at ?? ""}>
          {formatDate(run.created_at)}
        </td>
      </tr>
      {isExpanded && (
        <tr>
          <td colSpan={10} className="p-0">
            <div className="border-t bg-muted/10 p-4 space-y-4">
              {run.status === "FAILED" && run.error_message && (
                <div className="flex items-start gap-2 rounded-md border border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950/30 p-3">
                  <AlertCircle className="h-4 w-4 text-red-600 shrink-0 mt-0.5" />
                  <div className="text-sm">
                    <p className="font-medium text-red-700 dark:text-red-400">Run failed</p>
                    <p className="text-red-600 dark:text-red-300 mt-0.5 whitespace-pre-wrap break-words font-mono text-xs">{run.error_message}</p>
                  </div>
                </div>
              )}
              {run.status !== "SUCCESS" && !run.error_message && run.status !== "CANCELED" && (
                <div className="text-sm text-muted-foreground">
                  Detailed results are available for completed (SUCCESS) runs.
                </div>
              )}
              {run.status === "CANCELED" && !run.error_message && (
                <div className="text-sm text-muted-foreground">
                  This run was canceled.
                </div>
              )}
              {run.status === "SUCCESS" && isLoadingResults && (
                <div className="flex items-center gap-2 text-muted-foreground text-sm py-4">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading results...
                </div>
              )}
              {results && <DryRunResults result={results} />}
              <CommentThread entityType="run" entityId={run.run_id} />
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function RunHistorySkeleton() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-7 w-32" />
          <Skeleton className="h-4 w-64" />
        </div>
        <Skeleton className="h-9 w-28" />
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

function RunHistoryError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">Failed to load run history</p>
      <p className="text-muted-foreground/70 text-xs mb-3">
        The validation runs table may not exist yet. Run some rules first.
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        Retry
      </Button>
    </div>
  );
}
