import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertCircle, ChevronDown, ChevronRight, Info } from "lucide-react";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { diffLines, inlineChange, sideYaml, unifiedRows, type DiffCell, type DiffRow } from "@/lib/yaml-diff";

/**
 * View changes, shared by tables, rules and collections, git-style:
 * each item (an applied rule, a rule, a collection member or its settings) is
 * one block — added, removed, modified (only the changed lines, with the
 * changed words marked) or unchanged (collapsed) — under a summary of counts.
 *
 * It opens on "Pending changes" (the approved version against the proposed
 * state) when there are changes not yet approved, else on "Version history",
 * where any two approved versions can be compared. Each caller loads its own
 * sides and passes them in, and shows them in a dialog (:func:`ChangesDialog`)
 * or in its History tab (:func:`ChangesPanel`, with the version timeline).
 */
export type ChangesMode = "pending" | "history";
type Layout = "unified" | "split";
type Status = "added" | "removed" | "modified" | "unchanged";

export interface ChangeItem {
  key: string;
  name: string;
  status: string;
  before: unknown[];
  after: unknown[];
}

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

/**
 * The compare state of a dialog: the tab, and the two versions picked under
 * Version history (defaulting to the approved version and the one before it).
 * Resets when *resetKey* (the object shown) changes.
 */
export function useChangesSides(resetKey: string, hasPending: boolean, approved: number) {
  const [mode, setMode] = useState<ChangesMode | null>(null);
  const [base, setBase] = useState<number | null>(null);
  const [to, setTo] = useState<number | null>(null);
  useEffect(() => {
    setMode(null);
    setBase(null);
    setTo(null);
  }, [resetKey]);
  const target = to ?? approved;
  const historyBase = base ?? Math.max(target - 1, 0);
  return {
    mode: mode ?? (hasPending ? "pending" : "history"),
    setMode,
    base: historyBase,
    setBase,
    target,
    setTarget: (next: number) => {
      setTo(next);
      if (historyBase >= next) setBase(next - 1);
    },
    /** Show one version against the version before it. */
    selectVersion: (version: number) => {
      setMode("history");
      setTo(version);
      setBase(version - 1);
    },
  };
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

function ItemDiff({ item, layout }: { item: ChangeItem; layout: Layout }) {
  const rows: DiffRow[] = useMemo(() => diffLines(sideYaml(item.before), sideYaml(item.after)), [item.before, item.after]);
  if (layout === "split" && item.status === "modified") {
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

function ItemBlock({
  item,
  label,
  layout,
  defaultOpen,
}: {
  item: ChangeItem;
  label: string;
  layout: Layout;
  defaultOpen: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(defaultOpen);
  const status = item.status as Status;
  const style = STATUS_STYLE[status] ?? STATUS_STYLE.unchanged;
  const counts = useMemo(() => {
    const rows = diffLines(sideYaml(item.before), sideYaml(item.after));
    return {
      added: rows.filter((r) => r.right?.tag === "added").length,
      removed: rows.filter((r) => r.left?.tag === "removed").length,
    };
  }, [item.before, item.after]);
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
        <span className="truncate text-sm font-medium">{label}</span>
        <span className="ml-auto shrink-0 font-mono text-xs">
          {counts.added > 0 && <span className="text-green-700 dark:text-green-400">+{counts.added} </span>}
          {counts.removed > 0 && <span className="text-red-700 dark:text-red-400">−{counts.removed}</span>}
        </span>
      </button>
      {open && <ItemDiff item={item} layout={layout} />}
    </div>
  );
}

/** An approved version, for the picker and the History timeline. */
export interface ChangesVersion {
  version: number;
  createdBy?: string | null;
  createdAt?: string | null;
  /** A note under the version in the timeline (e.g. re-frozen in place). */
  note?: string | null;
}

export interface ChangesViewProps {
  sides: ReturnType<typeof useChangesSides>;
  hasPending: boolean;
  approvedVersion: number;
  /** Approved versions, for the picker and the History timeline. */
  versions: ChangesVersion[];
  items: ChangeItem[];
  /** Hide the per-status counts and the unchanged toggle (a single item). */
  single?: boolean;
  labelFor?: (item: ChangeItem) => string;
  note?: string | null;
  loading: boolean;
  error: boolean;
  /** Shown under Version history when nothing has been approved yet. */
  neverApprovedText: string;
  noChangesText: string;
}

function ModeTabs({ sides, hasPending }: Pick<ChangesViewProps, "sides" | "hasPending">) {
  const { t } = useTranslation();
  return (
    <div role="tablist" aria-label={t("tableChanges.compare")} className="flex rounded-lg border p-0.5">
      {(["pending", "history"] as ChangesMode[]).map((m) => (
        <button
          key={m}
          type="button"
          role="tab"
          aria-selected={sides.mode === m}
          disabled={m === "pending" && !hasPending}
          onClick={() => sides.setMode(m)}
          className={cn(
            "rounded-md px-3 py-1.5 text-sm font-medium disabled:opacity-40",
            sides.mode === m ? "bg-foreground text-background" : "text-muted-foreground",
          )}
        >
          {t(m === "pending" ? "tableChanges.pendingTab" : "tableChanges.historyTab")}
        </button>
      ))}
    </div>
  );
}

/** The comparison: what is compared, the counts and toggles, and the items. */
function ChangesBody({
  sides,
  approvedVersion: approved,
  versions,
  items,
  single = false,
  labelFor,
  note,
  loading,
  error,
  neverApprovedText,
  noChangesText,
  resetKey,
  scroll,
}: ChangesViewProps & { resetKey: string; scroll: boolean }) {
  const { t } = useTranslation();
  const [layout, setLayout] = useState<Layout>("unified");
  const [showUnchanged, setShowUnchanged] = useState(false);
  useEffect(() => setShowUnchanged(false), [resetKey]);
  const { mode } = sides;
  const versionNumbers = versions.map((v) => v.version).sort((a, b) => b - a);
  const visible = items.filter((c) => single || showUnchanged || c.status !== "unchanged");
  const counts = useMemo(() => {
    const out: Record<Status, number> = { added: 0, removed: 0, modified: 0, unchanged: 0 };
    for (const item of items) out[item.status as Status] = (out[item.status as Status] ?? 0) + 1;
    return out;
  }, [items]);
  const noHistory = mode === "history" && approved === 0;

  return (
    <div className={cn("flex min-h-0 flex-col gap-3", scroll && "flex-1")}>
      {mode === "pending" ? (
        <span className="font-mono text-xs text-muted-foreground">
          {approved > 0 ? t("tableChanges.pendingCompare", { version: approved }) : t("tableChanges.pendingCompareFirst")}
        </span>
      ) : (
        versionNumbers.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <label htmlFor={`changes-base-${resetKey}`}>{t("tableChanges.compare")}</label>
            <select
              id={`changes-base-${resetKey}`}
              value={sides.base}
              onChange={(e) => sides.setBase(Number(e.target.value))}
              className="rounded-md border bg-background px-2 py-1 font-mono text-xs"
            >
              <option value={0}>{t("tableChanges.nothing")}</option>
              {versionNumbers
                .filter((v) => v < sides.target)
                .map((v) => (
                  <option key={v} value={v}>
                    v{v}
                  </option>
                ))}
            </select>
            <label htmlFor={`changes-to-${resetKey}`}>{t("tableChanges.to")}</label>
            <select
              id={`changes-to-${resetKey}`}
              value={sides.target}
              onChange={(e) => sides.setTarget(Number(e.target.value))}
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
        <p className="text-sm text-muted-foreground">{neverApprovedText}</p>
      ) : (
        <>
          {note && (
            <div className="flex items-start gap-2 rounded-lg border border-amber-300 bg-amber-50 p-3 text-xs text-amber-800 dark:border-amber-700 dark:bg-amber-950/30 dark:text-amber-300">
              <Info className="mt-0.5 h-4 w-4 shrink-0" />
              <span>{note}</span>
            </div>
          )}
          <div className="flex flex-wrap items-center justify-between gap-2">
            {single ? (
              <span />
            ) : (
              <div className="flex flex-wrap gap-2 text-xs">
                <span className="rounded-full border px-2.5 py-1 text-red-700 dark:text-red-400">
                  {t("tableChanges.countRemoved", { count: counts.removed })}
                </span>
                <span className="rounded-full border px-2.5 py-1 text-green-700 dark:text-green-400">
                  {t("tableChanges.countAdded", { count: counts.added })}
                </span>
                <span className="rounded-full border px-2.5 py-1 text-amber-700 dark:text-amber-400">
                  {t("tableChanges.countModified", { count: counts.modified })}
                </span>
                <span className="rounded-full border px-2.5 py-1 text-muted-foreground">
                  {t("tableChanges.countUnchanged", { count: counts.unchanged })}
                </span>
              </div>
            )}
            <div className="flex items-center gap-3 text-sm">
              {!single && (
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={showUnchanged} onChange={(e) => setShowUnchanged(e.target.checked)} />
                  {t("tableChanges.showUnchanged")}
                </label>
              )}
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

          <div className={cn("space-y-2", scroll && "min-h-0 flex-1 overflow-y-auto pr-1")}>
            {visible.length === 0 ? (
              <p className="text-sm text-muted-foreground">{noChangesText}</p>
            ) : (
              visible.map((c, i) => (
                <ItemBlock
                  key={`${mode}-${sides.base}-${sides.target}-${c.key}`}
                  item={c}
                  label={labelFor?.(c) || c.name || c.key}
                  layout={layout}
                  defaultOpen={single || (i < 3 && c.status !== "unchanged")}
                />
              ))
            )}
          </div>
        </>
      )}
    </div>
  );
}

export function ChangesDialog({
  open,
  onClose,
  subject,
  ...view
}: ChangesViewProps & { open: boolean; onClose: () => void; subject: string }) {
  const { t } = useTranslation();
  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-5xl max-h-[90vh] flex flex-col gap-3">
        <DialogHeader>
          <DialogTitle>{t(view.sides.mode === "pending" ? "tableChanges.pendingTitle" : "tableChanges.historyTitle")}</DialogTitle>
          <DialogDescription className="font-mono text-xs break-all">{subject}</DialogDescription>
        </DialogHeader>
        <ModeTabs sides={view.sides} hasPending={view.hasPending} />
        <ChangesBody {...view} resetKey={subject} scroll />
        <div className="flex justify-end border-t pt-3">
          <Button variant="outline" onClick={onClose}>
            {t("common.close", "Close")}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

/**
 * View changes inside a History tab: the version timeline (pending changes on
 * top, then each approved version with who approved it and when) beside the
 * comparison. Picking a version shows it against the version before it.
 */
export function ChangesPanel({ resetKey, ...view }: ChangesViewProps & { resetKey: string }) {
  const { t } = useTranslation();
  const { sides, hasPending, approvedVersion: approved } = view;
  const versions = [...view.versions].sort((a, b) => b.version - a.version);
  const entry = (selected: boolean) =>
    cn(
      "flex w-full flex-col gap-0.5 rounded-lg border px-3 py-2 text-left text-xs",
      selected ? "border-foreground/40 bg-muted" : "border-transparent hover:bg-muted/50",
    );
  return (
    <div className="grid gap-4 pt-4 md:grid-cols-[16rem_minmax(0,1fr)]">
      <nav aria-label={t("tableChanges.historyTitle")} className="space-y-1">
        {hasPending && (
          <button
            type="button"
            aria-current={sides.mode === "pending"}
            onClick={() => sides.setMode("pending")}
            className={entry(sides.mode === "pending")}
          >
            <span className="font-medium text-amber-700 dark:text-amber-400">{t("tableChanges.pendingTab")}</span>
            <span className="text-muted-foreground">{t("tableChanges.notApprovedYet")}</span>
          </button>
        )}
        {versions.length === 0 && !hasPending && (
          <p className="px-3 py-2 text-xs text-muted-foreground">{view.neverApprovedText}</p>
        )}
        {versions.map((v) => {
          const selected = sides.mode === "history" && sides.target === v.version;
          return (
            <button
              key={v.version}
              type="button"
              aria-current={selected}
              onClick={() => sides.selectVersion(v.version)}
              className={entry(selected)}
            >
              <span className="flex items-center justify-between gap-2">
                <span className="font-mono font-medium">v{v.version}</span>
                {v.version === approved && (
                  <span className="text-[10px] uppercase tracking-wide text-green-700 dark:text-green-400">
                    {t("tableChanges.runsNow")}
                  </span>
                )}
              </span>
              {v.createdBy && <span className="truncate font-mono text-muted-foreground">{v.createdBy}</span>}
              {v.createdAt && (
                <span className="text-muted-foreground">
                  <RelativeTimeCell iso={v.createdAt} />
                </span>
              )}
              {v.note && <span className="text-amber-700 dark:text-amber-400">{v.note}</span>}
            </button>
          );
        })}
      </nav>
      <div className="min-w-0 space-y-3">
        <ModeTabs sides={sides} hasPending={hasPending} />
        <ChangesBody {...view} resetKey={resetKey} scroll={false} />
      </div>
    </div>
  );
}
