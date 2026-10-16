import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertCircle, ChevronDown, ChevronRight } from "lucide-react";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useGetMonitoredTableChanges, useListMonitoredTableVersions, type RuleChangeOut } from "@/lib/api";
import { diffLines, inlineChange, toYaml, unifiedRows, type DiffCell, type DiffRow } from "@/lib/yaml-diff";

/**
 * View changes for a monitored table, git-style: each applied rule is one
 * block — added, removed, modified (only the changed lines, with the changed
 * words marked) or unchanged (collapsed) — under a summary of the counts.
 *
 * Opens on "Pending changes" (the approved version against the proposed
 * rules) when the table has changes not yet approved, else on "Version
 * history", where any two approved versions can be compared.
 */
export interface TableChangesTarget {
  bindingId: string;
  name: string;
  version: number;
}

type Mode = "pending" | "history";
type Layout = "unified" | "split";
type Status = "added" | "removed" | "modified" | "unchanged";

const STATUS_STYLE: Record<Status, { box: string; head: string; badge: string }> = {
  removed: {
    box: "border-red-300 dark:border-red-900",
    head: "bg-red-50 dark:bg-red-950/40",
    badge: "bg-red-600 text-white dark:bg-red-900 dark:text-red-100",
  },
  added: {
    box: "border-green-300 dark:border-green-900",
    head: "bg-green-50 dark:bg-green-950/40",
    badge: "bg-green-700 text-white dark:bg-green-900 dark:text-green-100",
  },
  modified: {
    box: "border-amber-300 dark:border-amber-900",
    head: "bg-amber-50 dark:bg-amber-950/40",
    badge: "bg-amber-600 text-white dark:bg-amber-900 dark:text-amber-100",
  },
  unchanged: { box: "border-border", head: "bg-muted/40", badge: "bg-muted text-muted-foreground" },
};

const LINE_STYLE: Record<DiffCell["tag"], string> = {
  removed: "bg-red-50 text-red-900 dark:bg-red-950/50 dark:text-red-200",
  added: "bg-green-50 text-green-900 dark:bg-green-950/50 dark:text-green-200",
  equal: "",
};
const WORD_STYLE: Record<DiffCell["tag"], string> = {
  removed: "rounded-sm bg-red-200 dark:bg-red-800/70",
  added: "rounded-sm bg-green-200 dark:bg-green-800/70",
  equal: "",
};

function ruleYaml(checks: RuleChangeOut["before"]): string {
  return checks && checks.length ? toYaml(checks.length === 1 ? checks[0] : checks).trim() : "";
}

function Line({ cell, other }: { cell: DiffCell | null; other?: DiffCell | null }) {
  if (!cell) return <div className="h-5 bg-muted/30" />;
  const gutter = cell.tag === "removed" ? "−" : cell.tag === "added" ? "+" : " ";
  const parts = other && other.tag !== "equal" && cell.tag !== "equal" ? inlineChange(cell.text, other.text) : null;
  return (
    <div className={cn("grid grid-cols-[1.25rem_minmax(0,1fr)] whitespace-pre-wrap break-all px-2", LINE_STYLE[cell.tag])}>
      <span aria-hidden="true" className="select-none opacity-70">
        {gutter}
      </span>
      {parts ? (
        <span>
          {parts.before}
          <mark className={cn("text-inherit", WORD_STYLE[cell.tag])}>{parts.changed}</mark>
          {parts.after}
        </span>
      ) : (
        <span>{cell.text}</span>
      )}
    </div>
  );
}

function RuleDiff({ change, layout }: { change: RuleChangeOut; layout: Layout }) {
  const rows: DiffRow[] = useMemo(
    () => diffLines(ruleYaml(change.before), ruleYaml(change.after)),
    [change.before, change.after],
  );
  if (layout === "split" && change.status === "modified") {
    return (
      <div className="grid grid-cols-2 font-mono text-xs leading-5">
        <div className="border-r">
          {rows.map((r, i) => (
            <Line key={i} cell={r.left} other={r.right} />
          ))}
        </div>
        <div>
          {rows.map((r, i) => (
            <Line key={i} cell={r.right} other={r.left} />
          ))}
        </div>
      </div>
    );
  }
  // Unified: pair each removed line with the added line opposite it for word marks.
  const pairs = new Map<DiffCell, DiffCell | null>();
  for (const r of rows) {
    if (r.left && r.right && r.left.tag !== "equal") {
      pairs.set(r.left, r.right);
      pairs.set(r.right, r.left);
    }
  }
  return (
    <div className="font-mono text-xs leading-5">
      {unifiedRows(rows).map((cell, i) => (
        <Line key={i} cell={cell} other={pairs.get(cell) ?? null} />
      ))}
    </div>
  );
}

function RuleBlock({ change, layout, defaultOpen }: { change: RuleChangeOut; layout: Layout; defaultOpen: boolean }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(defaultOpen);
  const status = change.status as Status;
  const style = STATUS_STYLE[status] ?? STATUS_STYLE.unchanged;
  const counts = useMemo(() => {
    const rows = diffLines(ruleYaml(change.before), ruleYaml(change.after));
    return {
      added: rows.filter((r) => r.right?.tag === "added").length,
      removed: rows.filter((r) => r.left?.tag === "removed").length,
    };
  }, [change.before, change.after]);
  return (
    <div className={cn("rounded-lg border overflow-hidden", style.box)}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className={cn("flex w-full items-center gap-2 px-3 py-2 text-left", style.head)}
      >
        {open ? <ChevronDown className="h-4 w-4 shrink-0" /> : <ChevronRight className="h-4 w-4 shrink-0" />}
        <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide", style.badge)}>
          {t(`tableChanges.status.${status}`)}
        </span>
        <span className="truncate text-sm font-medium">{change.rule_name || change.key}</span>
        <span className="ml-auto shrink-0 font-mono text-xs">
          {counts.added > 0 && <span className="text-green-700 dark:text-green-400">+{counts.added} </span>}
          {counts.removed > 0 && <span className="text-red-700 dark:text-red-400">−{counts.removed}</span>}
        </span>
      </button>
      {open && <RuleDiff change={change} layout={layout} />}
    </div>
  );
}

export function TableChangesDialog({ target, onClose }: { target: TableChangesTarget | null; onClose: () => void }) {
  const { t } = useTranslation();
  const bindingId = target?.bindingId ?? "";
  const [mode, setMode] = useState<Mode | null>(null);
  const [layout, setLayout] = useState<Layout>("unified");
  const [showUnchanged, setShowUnchanged] = useState(false);
  const [base, setBase] = useState<number | null>(null);
  const [to, setTo] = useState<number | null>(null);

  useEffect(() => {
    setMode(null);
    setBase(null);
    setTo(null);
    setShowUnchanged(false);
  }, [bindingId]);

  // The server picks the default comparison (pending changes when there are any).
  const defaults = useGetMonitoredTableChanges(bindingId, undefined, { query: { enabled: !!target } });
  const info = defaults.data?.data;
  const effectiveMode: Mode = mode ?? (info?.has_pending_changes ? "pending" : "history");
  const approved = info?.approved_version ?? 0;
  const historyTo = to ?? approved;
  const historyBase = base ?? Math.max(historyTo - 1, 0);
  const params =
    effectiveMode === "pending" ? { base: approved } : { base: historyBase, target: historyTo > 0 ? historyTo : undefined };
  const query = useGetMonitoredTableChanges(bindingId, params, {
    query: { enabled: !!target && !!info && (effectiveMode === "pending" || historyTo > 0) },
  });
  const versions = useListMonitoredTableVersions(bindingId, { query: { enabled: !!target && effectiveMode === "history" } });
  const versionNumbers = (versions.data?.data ?? []).map((v) => v.version).sort((a, b) => b - a);

  const data = query.data?.data;
  const changes = data?.changes ?? [];
  const visible = changes.filter((c) => showUnchanged || c.status !== "unchanged");
  const counts = data?.counts ?? {};
  const loading = defaults.isLoading || query.isLoading;
  const error = defaults.isError || query.isError;
  const noHistory = effectiveMode === "history" && approved === 0;

  return (
    <Dialog open={!!target} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-5xl max-h-[90vh] flex flex-col gap-3">
        <DialogHeader>
          <DialogTitle>{t(effectiveMode === "pending" ? "tableChanges.pendingTitle" : "tableChanges.historyTitle")}</DialogTitle>
          <DialogDescription className="font-mono text-xs break-all">{target?.name ?? ""}</DialogDescription>
        </DialogHeader>

        <div className="flex flex-wrap items-center gap-2">
          <div role="tablist" aria-label={t("tableChanges.compare")} className="flex rounded-lg border p-0.5">
            {(["pending", "history"] as Mode[]).map((m) => (
              <button
                key={m}
                type="button"
                role="tab"
                aria-selected={effectiveMode === m}
                disabled={m === "pending" && !info?.has_pending_changes}
                onClick={() => setMode(m)}
                className={cn(
                  "rounded-md px-3 py-1.5 text-sm font-medium disabled:opacity-40",
                  effectiveMode === m ? "bg-foreground text-background" : "text-muted-foreground",
                )}
              >
                {t(m === "pending" ? "tableChanges.pendingTab" : "tableChanges.historyTab")}
              </button>
            ))}
          </div>
          {effectiveMode === "pending" ? (
            <span className="font-mono text-xs text-muted-foreground">
              {t("tableChanges.pendingCompare", { version: approved })}
            </span>
          ) : (
            versionNumbers.length > 0 && (
              <div className="flex items-center gap-2 text-sm">
                <label htmlFor="tc-base">{t("tableChanges.compare")}</label>
                <select
                  id="tc-base"
                  value={historyBase}
                  onChange={(e) => setBase(Number(e.target.value))}
                  className="rounded-md border bg-background px-2 py-1 font-mono text-xs"
                >
                  <option value={0}>{t("tableChanges.nothing")}</option>
                  {versionNumbers
                    .filter((v) => v < historyTo)
                    .map((v) => (
                      <option key={v} value={v}>
                        v{v}
                      </option>
                    ))}
                </select>
                <label htmlFor="tc-to">{t("tableChanges.to")}</label>
                <select
                  id="tc-to"
                  value={historyTo}
                  onChange={(e) => {
                    const next = Number(e.target.value);
                    setTo(next);
                    if (historyBase >= next) setBase(next - 1);
                  }}
                  className="rounded-md border bg-background px-2 py-1 font-mono text-xs"
                >
                  {versionNumbers.map((v) => (
                    <option key={v} value={v}>
                      v{v}
                      {v === approved ? ` · ${t("tableChanges.runsNow")}` : ""}
                    </option>
                  ))}
                </select>
              </div>
            )
          )}
        </div>

        {loading ? (
          <div className="space-y-2">
            <Skeleton className="h-6 w-64" />
            <Skeleton className="h-40 w-full" />
          </div>
        ) : error ? (
          <p className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4 shrink-0" />
            {t("tableChanges.loadFailed")}
          </p>
        ) : noHistory ? (
          <p className="text-sm text-muted-foreground">{t("tableChanges.neverApproved")}</p>
        ) : (
          <>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex flex-wrap gap-2 text-xs">
                <span className="rounded-full border px-2.5 py-1 text-red-700 dark:text-red-400">
                  {t("tableChanges.countRemoved", { count: counts.removed ?? 0 })}
                </span>
                <span className="rounded-full border px-2.5 py-1 text-green-700 dark:text-green-400">
                  {t("tableChanges.countAdded", { count: counts.added ?? 0 })}
                </span>
                <span className="rounded-full border px-2.5 py-1 text-amber-700 dark:text-amber-400">
                  {t("tableChanges.countModified", { count: counts.modified ?? 0 })}
                </span>
                <span className="rounded-full border px-2.5 py-1 text-muted-foreground">
                  {t("tableChanges.countUnchanged", { count: counts.unchanged ?? 0 })}
                </span>
              </div>
              <div className="flex items-center gap-3 text-sm">
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={showUnchanged} onChange={(e) => setShowUnchanged(e.target.checked)} />
                  {t("tableChanges.showUnchanged")}
                </label>
                <div role="radiogroup" aria-label={t("tableChanges.layout")} className="flex overflow-hidden rounded-md border">
                  {(["unified", "split"] as Layout[]).map((l) => (
                    <button
                      key={l}
                      type="button"
                      role="radio"
                      aria-checked={layout === l}
                      onClick={() => setLayout(l)}
                      className={cn("px-2.5 py-1 text-xs", layout === l ? "bg-muted font-medium" : "text-muted-foreground")}
                    >
                      {t(l === "unified" ? "tableChanges.unified" : "tableChanges.split")}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
              {visible.length === 0 ? (
                <p className="text-sm text-muted-foreground">{t("tableChanges.noChanges")}</p>
              ) : (
                visible.map((c, i) => (
                  <RuleBlock key={`${effectiveMode}-${c.key}`} change={c} layout={layout} defaultOpen={i < 3 && c.status !== "unchanged"} />
                ))
              )}
            </div>
          </>
        )}

        <div className="flex items-center justify-between gap-3 border-t pt-3">
          <p className="text-xs text-muted-foreground">
            {approved > 0 ? t("tableChanges.runsUse", { version: approved }) : t("tableChanges.neverApprovedShort")}
          </p>
          <Button variant="outline" onClick={onClose}>
            {t("common.close", "Close")}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
