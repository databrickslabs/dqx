import { useMemo, useState } from "react";
import type * as React from "react";
import { createFileRoute, Link, Navigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  CalendarClock,
  History,
  MoreHorizontal,
  Pause,
  Play,
  Search,
  Trash2,
} from "lucide-react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { FadeIn } from "@/components/anim/FadeIn";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useDeleteSchedule,
  useListDataProducts,
  useListSchedules,
  useSaveSchedule,
  useSetSchedulePaused,
  useListScheduleOverview,
  useUpdateDataProduct,
  useUpdateMonitoredTableSchedule,
  type ScheduleOverviewOut,
} from "@/lib/api";
import { cronHint } from "@/lib/cron";

export const Route = createFileRoute("/_sidebar/schedules")({
  component: SchedulesPage,
});

function formatDate(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function SchedulesPage() {
  const permissions = usePermissions();
  if (!permissions.canRunRules) return <Navigate to="/results" replace />;
  return <SchedulesContent isAdmin={permissions.isAdmin} />;
}

function SchedulesContent({ isAdmin }: { isAdmin: boolean }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { data, isPending, isError } = useListScheduleOverview();
  const { data: configsData } = useListSchedules();
  const { data: productsData } = useListDataProducts();
  const [search, setSearch] = useState("");
  const [type, setType] = useState("all");
  const [status, setStatus] = useState("all");
  const [owner, setOwner] = useState("all");

  const rows = data?.data ?? [];
  const owners = useMemo(
    () =>
      Array.from(
        new Set(
          rows
            .map((row) => row.owner)
            .filter((value): value is string => !!value),
        ),
      ).sort(),
    [rows],
  );
  const scheduledByCollection = useMemo(
    () =>
      new Set(
        (productsData?.data ?? [])
          .filter((product) => !!product.schedule_cron)
          .flatMap((product) =>
            (product.members ?? []).map((member) => member.table_fqn),
          ),
      ),
    [productsData],
  );
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return rows.filter((row) => {
      if (type !== "all" && row.source_type !== type) return false;
      if (owner !== "all" && row.owner !== owner) return false;
      if (status === "paused" && !row.paused) return false;
      if (status === "active" && (!row.enabled || row.paused)) return false;
      if (
        status === "failing" &&
        !["failed", "partial_failure"].includes(row.run_status ?? "")
      )
        return false;
      return (
        !q ||
        [row.name, row.target, row.owner, row.updated_by].some((value) =>
          value?.toLowerCase().includes(q),
        )
      );
    });
  }, [rows, search, type, status, owner]);

  const saveScope = useSaveSchedule({
    mutation: {
      onSuccess: () => {
        toast.success(t("schedules.saved"));
        void queryClient.invalidateQueries({ queryKey: ["/api/v1/schedules"] });
        void queryClient.invalidateQueries({
          queryKey: ["/api/v1/schedules/overview"],
        });
      },
    },
  });
  const refreshOverview = () =>
    void queryClient.invalidateQueries({
      queryKey: ["/api/v1/schedules/overview"],
    });
  const removeTable = useUpdateMonitoredTableSchedule({
    mutation: {
      onSuccess: () => {
        toast.success(t("schedules.removed"));
        refreshOverview();
      },
    },
  });
  const removeCollection = useUpdateDataProduct({
    mutation: {
      onSuccess: () => {
        toast.success(t("schedules.removed"));
        refreshOverview();
      },
    },
  });
  const setEntityPaused = useSetSchedulePaused({
    mutation: {
      onSuccess: () => {
        toast.success(t("schedules.saved"));
        refreshOverview();
      },
    },
  });
  const deleteScope = useDeleteSchedule({
    mutation: {
      onSuccess: () => {
        toast.success(t("schedules.removed"));
        void queryClient.invalidateQueries({ queryKey: ["/api/v1/schedules"] });
        void queryClient.invalidateQueries({
          queryKey: ["/api/v1/schedules/overview"],
        });
      },
    },
  });

  const toggleScope = (row: ScheduleOverviewOut) => {
    const entry = (configsData?.data ?? []).find(
      (config) => config.schedule_name === row.source_id,
    );
    if (!entry) return;
    saveScope.mutate({
      data: {
        schedule_name: entry.schedule_name,
        config: { ...entry.config, paused: !row.paused },
      },
    });
  };

  const removeSchedule = (row: ScheduleOverviewOut) => {
    if (!window.confirm(t("schedules.removeConfirm", { name: row.name })))
      return;
    if (row.source_type === "scope") {
      deleteScope.mutate({ name: row.source_id });
    } else if (row.source_type === "table") {
      removeTable.mutate({
        bindingId: row.source_id,
        data: {
          schedule_cron: null,
          schedule_tz: null,
          schedule_kind: row.schedule_kind ?? "profiling_and_dq",
          schedule_sample_size: null,
        },
      });
    } else {
      removeCollection.mutate({
        productId: row.source_id,
        data: {
          schedule_cron: null,
          schedule_tz: null,
          schedule_kind: row.schedule_kind ?? "profiling_and_dq",
          schedule_sample_size: null,
        },
      });
    }
  };
  const toggleSchedule = (row: ScheduleOverviewOut) => {
    if (row.source_type === "scope") {
      toggleScope(row);
      return;
    }
    setEntityPaused.mutate({
      sourceType: row.source_type,
      sourceId: row.source_id,
      data: { paused: !row.paused },
    });
  };

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("schedules.title")} />
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">
              {t("schedules.title")}
            </h1>
            <p className="mt-1 text-sm text-muted-foreground">
              {t("schedules.subtitle")}
            </p>
          </div>
          <Badge
            variant="outline"
            className="gap-1.5 border-blue-500/30 bg-blue-50/60 text-blue-700 dark:bg-blue-950/20 dark:text-blue-300"
          >
            <CalendarClock className="h-3.5 w-3.5" />
            {t("schedules.count", { count: rows.length })}
          </Badge>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="relative min-w-60 flex-1">
            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder={t("schedules.search")}
              className="pl-8"
            />
          </div>
          <FilterSelect
            value={type}
            onChange={setType}
            ariaLabel={t("schedules.filterType")}
          >
            <option value="all">{t("schedules.allTypes")}</option>
            <option value="table">{t("schedules.typeTable")}</option>
            <option value="collection">{t("schedules.typeCollection")}</option>
            <option value="scope">{t("schedules.typeScope")}</option>
          </FilterSelect>
          <FilterSelect
            value={status}
            onChange={setStatus}
            ariaLabel={t("schedules.filterStatus")}
          >
            <option value="all">{t("schedules.allStatuses")}</option>
            <option value="active">{t("schedules.active")}</option>
            <option value="paused">{t("schedules.paused")}</option>
            <option value="failing">{t("schedules.failing")}</option>
          </FilterSelect>
          <FilterSelect
            value={owner}
            onChange={setOwner}
            ariaLabel={t("schedules.filterOwner")}
          >
            <option value="all">{t("schedules.allOwners")}</option>
            {owners.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </FilterSelect>
        </div>

        <div className="overflow-hidden rounded-lg border bg-card">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t("schedules.name")}</TableHead>
                <TableHead>{t("schedules.type")}</TableHead>
                <TableHead>{t("schedules.cadence")}</TableHead>
                <TableHead>{t("schedules.owner")}</TableHead>
                <TableHead>{t("schedules.health")}</TableHead>
                <TableHead>{t("schedules.nextRun")}</TableHead>
                <TableHead>{t("schedules.lastRun")}</TableHead>
                <TableHead className="text-right">
                  {t("schedules.actions")}
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isPending && <MessageRow text={t("common.loading")} />}
              {isError && <MessageRow text={t("common.loadFailed")} />}
              {!isPending && !isError && filtered.length === 0 && (
                <MessageRow text={t("schedules.empty")} />
              )}
              {filtered.map((row) => {
                const duplicate =
                  row.source_type === "table" &&
                  scheduledByCollection.has(row.target);
                return (
                  <TableRow key={`${row.source_type}:${row.source_id}`}>
                    <TableCell>
                      <ScheduleName row={row} />
                      <div className="flex max-w-72 items-center gap-1 text-xs text-muted-foreground">
                        <span className="truncate">{row.target}</span>
                        {duplicate && (
                          <AlertTriangle
                            className="h-3.5 w-3.5 shrink-0 text-amber-500"
                            aria-label={t("schedules.duplicateWarning")}
                          />
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <SourceBadge type={row.source_type} />
                    </TableCell>
                    <TableCell className="text-sm">
                      {row.cron
                        ? cronHint(row.cron, row.timezone, t)
                        : (row.frequency ?? "—")}
                    </TableCell>
                    <TableCell className="max-w-44 truncate text-sm">
                      {row.owner ?? "—"}
                    </TableCell>
                    <TableCell>
                      <HealthBadge row={row} />
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-xs">
                      {formatDate(row.next_run_at)}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-xs">
                      {formatDate(row.last_run_at)}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center justify-end gap-1">
                        <ViewRunsButton row={row} />
                        {isAdmin && (
                          <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                aria-label={t("schedules.actions")}
                              >
                                <MoreHorizontal className="h-4 w-4" />
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              <DropdownMenuItem
                                onClick={() => toggleSchedule(row)}
                                className="gap-2"
                              >
                                {row.paused ? (
                                  <Play className="h-3.5 w-3.5" />
                                ) : (
                                  <Pause className="h-3.5 w-3.5" />
                                )}
                                {row.paused
                                  ? t("schedules.resume")
                                  : t("schedules.pause")}
                              </DropdownMenuItem>
                              <DropdownMenuSeparator />
                              <DropdownMenuItem
                                variant="destructive"
                                onClick={() => removeSchedule(row)}
                                className="gap-2"
                              >
                                <Trash2 className="h-3.5 w-3.5" />
                                {t("schedules.deleteSchedule")}
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      </div>
    </FadeIn>
  );
}

function ScheduleName({ row }: { row: ScheduleOverviewOut }) {
  const className = "font-medium text-primary hover:underline";
  if (row.source_type === "table") {
    return (
      <Link
        className={className}
        to="/monitored-tables/$bindingId"
        params={{ bindingId: row.source_id }}
        search={{ tab: "schedule" }}
      >
        {row.name}
      </Link>
    );
  }
  if (row.source_type === "collection") {
    return (
      <Link
        className={className}
        to="/collections/$productId"
        params={{ productId: row.source_id }}
        search={{ tab: "scheduling" }}
      >
        {row.name}
      </Link>
    );
  }
  return <div className="font-medium">{row.name}</div>;
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
    <Button
      asChild
      variant="ghost"
      size="icon"
      className="h-8 w-8"
      title={t("schedules.viewRuns")}
    >
      <Link
        to="/runs-history"
        search={search}
        aria-label={t("schedules.viewRuns")}
      >
        <History className="h-4 w-4" />
      </Link>
    </Button>
  );
}

function FilterSelect({
  value,
  onChange,
  ariaLabel,
  children,
}: {
  value: string;
  onChange: (value: string) => void;
  ariaLabel: string;
  children: React.ReactNode;
}) {
  return (
    <select
      value={value}
      onChange={(event) => onChange(event.target.value)}
      aria-label={ariaLabel}
      className="h-9 rounded-md border bg-background px-3 text-sm shadow-xs"
    >
      {children}
    </select>
  );
}

function MessageRow({ text }: { text: string }) {
  return (
    <TableRow>
      <TableCell
        colSpan={8}
        className="h-28 text-center text-sm text-muted-foreground"
      >
        {text}
      </TableCell>
    </TableRow>
  );
}

function SourceBadge({ type }: { type: ScheduleOverviewOut["source_type"] }) {
  const { t } = useTranslation();
  return (
    <Badge variant="outline">
      {t(`schedules.type${type[0].toUpperCase()}${type.slice(1)}`)}
    </Badge>
  );
}

function HealthBadge({ row }: { row: ScheduleOverviewOut }) {
  const { t } = useTranslation();
  if (row.paused || !row.enabled)
    return <Badge variant="secondary">{t("schedules.paused")}</Badge>;
  if (row.run_status === "failed")
    return <Badge variant="destructive">{t("schedules.failed")}</Badge>;
  if (row.run_status === "partial_failure")
    return (
      <Badge variant="outline" className="border-amber-500/40 text-amber-700">
        {t("schedules.partial")}
      </Badge>
    );
  return (
    <Badge variant="outline" className="border-emerald-500/40 text-emerald-700">
      {t("schedules.active")}
    </Badge>
  );
}
