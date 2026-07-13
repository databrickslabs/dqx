/**
 * ProductRunsTab — the data product's Runs tab: a list of run sets (one row
 * per trigger, either a manual "Run now"/"Run draft" click or a scheduled
 * tick), each expandable into its per-table member rows.
 *
 * Visually ported from dqlake's `components/products/ProductRunsTab.tsx`
 * (outer table shell, chevron-expand row, pagination) and
 * `components/runs/RunSubtasks.tsx` (nested row indent/shading pattern for
 * the expanded detail). The DATA model differs from dqlake's: dqlake's rows
 * are individual Databricks job runs with a job-task subtree fetched via the
 * Jobs API; ours are `dq_run_sets` rows (Task 3) whose members are DQX
 * validation runs against frozen/draft rule snapshots, fetched via
 * `useListRunSets`/`useGetRunSet` — never the Jobs API.
 */
import { Fragment, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import { Link } from "@tanstack/react-router";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ChevronDown, ChevronLeft, ChevronRight, ExternalLink, Loader2 } from "lucide-react";
import { useGetRunSet, type DataProductOut, type RunSetSummaryOut } from "@/lib/api";
import { useProductRunSets } from "@/hooks/use-product-run-sets";
import { formatDateTime } from "@/lib/format-utils";

const PAGE_SIZE = 25;

function statusVariant(status: string | null | undefined): "default" | "destructive" | "secondary" {
  const s = (status ?? "").toUpperCase();
  if (s === "SUCCESS") return "default";
  if (s === "FAILED" || s === "CANCELED" || s === "CANCELLED") return "destructive";
  return "secondary";
}

/** Aggregate run-set status labels — the backend returns lowercase values
 *  (design spec §4.2: running > failed > canceled > success). */
function aggregateStatusLabel(t: TFunction, status: string): string {
  switch (status) {
    case "running":
      return t("dataProducts.runsStatusRunning");
    case "failed":
      return t("dataProducts.runsStatusFailed");
    case "canceled":
      return t("dataProducts.runsStatusCanceled");
    case "success":
      return t("dataProducts.runsStatusSuccess");
    default:
      return status;
  }
}

/** Per-member status labels — sourced from `dq_validation_runs.status`,
 *  which is uppercase (RUNNING/SUCCESS/FAILED/CANCELED). */
function memberStatusLabel(t: TFunction, status: string | null | undefined): string {
  switch ((status ?? "").toUpperCase()) {
    case "SUCCESS":
      return t("dataProducts.runsStatusSuccess");
    case "FAILED":
      return t("dataProducts.runsStatusFailed");
    case "RUNNING":
      return t("dataProducts.runsStatusRunning");
    case "CANCELED":
    case "CANCELLED":
      return t("dataProducts.runsStatusCanceled");
    default:
      return status ?? t("dataProducts.runsStatusUnknown");
  }
}

function formatDuration(ms: number): string {
  if (ms < 0) return "0s";
  const secs = ms / 1000;
  if (secs < 60) return `${secs.toFixed(0)}s`;
  const mins = Math.floor(secs / 60);
  const rem = Math.round(secs % 60);
  return `${mins}m ${rem}s`;
}

export function ProductRunsTab({ product }: { product: DataProductOut }) {
  const { t } = useTranslation();
  const { runSets, hasActive } = useProductRunSets(product.product_id);

  // Tick once a second so a RUNNING run set's live duration updates
  // visibly, only while something is actually active.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!hasActive) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [hasActive]);

  const [page, setPage] = useState(0);
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());
  const pageCount = Math.max(1, Math.ceil(runSets.length / PAGE_SIZE));
  const pageRows = runSets.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const toggle = (runSetId: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(runSetId)) next.delete(runSetId);
      else next.add(runSetId);
      return next;
    });

  if (runSets.length === 0) {
    return (
      <p className="text-sm text-muted-foreground py-6 text-center max-w-3xl">
        {(product.runnable_count ?? 0) === 0
          ? t("dataProducts.runsEmptyNotRunnable")
          : t("dataProducts.runsEmpty")}
      </p>
    );
  }

  return (
    <div className="space-y-3 max-w-3xl">
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[36px]" />
              <TableHead className="w-[90px]">{t("dataProducts.runsColSource")}</TableHead>
              <TableHead className="w-[100px]">{t("dataProducts.runsColTrigger")}</TableHead>
              <TableHead>{t("dataProducts.runsColStarted")}</TableHead>
              <TableHead className="text-right w-[100px]">{t("dataProducts.runsColDuration")}</TableHead>
              <TableHead className="text-right w-[80px]">{t("dataProducts.runsColMembers")}</TableHead>
              <TableHead className="w-[110px]">{t("dataProducts.runsColStatus")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageRows.map((rs) => {
              const isOpen = expanded.has(rs.run_set_id);
              const startedMs = rs.created_at ? Date.parse(rs.created_at) : null;
              // Run sets carry no completion timestamp (design spec §4.2 —
              // aggregation is status-only), so a live duration is only
              // meaningful while the set is still running; once settled we
              // don't have enough data to show an accurate elapsed time.
              const duration =
                rs.status === "running" && startedMs != null ? formatDuration(now - startedMs) : "—";
              return (
                <Fragment key={rs.run_set_id}>
                  <TableRow>
                    <TableCell className="pr-0">
                      <button
                        type="button"
                        onClick={() => toggle(rs.run_set_id)}
                        className="inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:bg-muted hover:text-foreground"
                        aria-label={isOpen ? t("dataProducts.runsCollapseAria") : t("dataProducts.runsExpandAria")}
                        aria-expanded={isOpen}
                      >
                        {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                      </button>
                    </TableCell>
                    <TableCell>
                      <Badge variant={rs.source === "approved" ? "default" : "outline"} className="text-[10px]">
                        {rs.source === "approved" ? t("dataProducts.runsSourceApproved") : t("dataProducts.runsSourceDraft")}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {rs.trigger === "scheduled" ? t("dataProducts.runsTriggerScheduled") : t("dataProducts.runsTriggerManual")}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {rs.created_at ? formatDateTime(rs.created_at) : "—"}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-right tabular-nums">{duration}</TableCell>
                    <TableCell className="text-right tabular-nums text-xs">{rs.member_count}</TableCell>
                    <TableCell>
                      <Badge variant={statusVariant(rs.status)}>{aggregateStatusLabel(t, rs.status)}</Badge>
                    </TableCell>
                  </TableRow>
                  {isOpen && <RunSetMembers runSetId={rs.run_set_id} row={rs} />}
                </Fragment>
              );
            })}
          </TableBody>
        </Table>
      </div>

      {pageCount > 1 && (
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>
            {t("dataProducts.runsShowingRange", {
              from: page * PAGE_SIZE + 1,
              to: Math.min((page + 1) * PAGE_SIZE, runSets.length),
              total: runSets.length,
            })}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="sm"
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              aria-label={t("dataProducts.runsPrevPageAria")}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="font-mono tabular-nums">
              {page + 1} / {pageCount}
            </span>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= pageCount - 1}
              onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
              aria-label={t("dataProducts.runsNextPageAria")}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}

/** Lazily fetches one run set's members on first expand, then keeps polling
 *  every 4s while it's still running (page-visible only) so member statuses
 *  tick without the user re-opening the row. */
function RunSetMembers({ runSetId, row }: { runSetId: string; row: RunSetSummaryOut }) {
  const { t } = useTranslation();
  const { data, isLoading, isError } = useGetRunSet(runSetId, {
    query: {
      select: (d) => d.data,
      refetchInterval: () => (row.status === "running" && document.visibilityState === "visible" ? 4000 : false),
      refetchIntervalInBackground: false,
    },
  });

  if (isLoading) {
    return (
      <TableRow className="border-0 bg-muted/30 hover:bg-muted/30">
        <TableCell colSpan={7} className="py-2 text-xs text-muted-foreground">
          <span className="inline-flex items-center gap-2">
            <Loader2 className="h-3 w-3 animate-spin" />
            {t("dataProducts.runsLoadingMembers")}
          </span>
        </TableCell>
      </TableRow>
    );
  }

  const members = data?.members ?? [];
  if (isError || members.length === 0) {
    return (
      <TableRow className="border-0 bg-muted/30 hover:bg-muted/30">
        <TableCell colSpan={7} className="py-2 text-xs text-muted-foreground">
          {t("dataProducts.runsNoMembers")}
        </TableCell>
      </TableRow>
    );
  }

  return (
    <>
      {members.map((m) => (
        <TableRow key={m.run_id} className="border-0 bg-muted/30 hover:bg-muted/30">
          {/* Spacer under the parent's chevron column. */}
          <TableCell className="py-1.5" />
          {/* Table FQN + version badge, indented under the parent's
              source/trigger/started columns (RunSubtasks' depth-indent
              pattern, applied here at a fixed depth of one level). */}
          <TableCell colSpan={3} className="py-1.5">
            <span className="inline-flex items-center gap-1.5 truncate" style={{ paddingLeft: "0.75rem" }}>
              <span className="font-mono text-xs truncate">{m.table_fqn ?? m.binding_id}</span>
              <Badge variant="outline" className="font-mono text-[10px] shrink-0">
                {m.binding_version != null
                  ? t("dataProducts.versionBadge", { version: m.binding_version })
                  : t("dataProducts.runsDraftBadge")}
              </Badge>
            </span>
          </TableCell>
          {/* Status, under the parent's duration column. */}
          <TableCell className="py-1.5">
            <Badge variant={statusVariant(m.status)}>{memberStatusLabel(t, m.status)}</Badge>
          </TableCell>
          {/* Deep link to the filtered Runs History row, under the parent's
              members+status columns. */}
          <TableCell colSpan={2} className="py-1.5 text-right">
            <Link
              to="/runs-history"
              search={{ runSetId }}
              className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground hover:underline"
            >
              {t("dataProducts.runsViewInHistory")}
              <ExternalLink className="h-3 w-3 opacity-60" />
            </Link>
          </TableCell>
        </TableRow>
      ))}
    </>
  );
}
