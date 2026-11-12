import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertCircle, ChevronRight, Info, Loader2 } from "lucide-react";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { MultiSelectPopover } from "@/components/monitored-tables/MultiSelectPopover";
import { formatDateTimeCompact } from "@/lib/format-utils";
import {
  NOTHING,
  PROPOSED_REF,
  historyPoints,
  pickerPoints,
  parseRefKey,
  pointTime,
  refKey,
  sameRef,
  selectionFor,
  versionRef,
  type SideRef,
} from "@/lib/history-points";
import { cn } from "@/lib/utils";
import { inlineChange, itemRows, unifiedRows, type DiffCell, type DiffRow } from "@/lib/yaml-diff";

/**
 * View changes, shared by tables, rules and collections, git-style: the object
 * is one file, and each item in it (an applied rule, a collection member or its
 * settings) is diffed against the same item on the other side — added, removed,
 * modified (with the changed words marked) or unchanged (folded) — under a
 * summary of counts, in a unified or split layout.
 *
 * Two pickers choose what is compared. In the dialog (:func:`ChangesDialog`)
 * "To" can also be the proposed changes, and it opens on them (against the
 * approved version) when there are any; otherwise, and in a History tab
 * (:func:`ChangesPanel`, beside the version timeline), any two approved
 * versions are compared. Each caller loads its own sides and passes them in.
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
 * What is compared: a *base* and a *target* side, each an approved version
 * (0 = nothing), a history step's saved state, or (target only) the proposed
 * changes. It opens on the proposed changes against the approved version when
 * there are any, else on the approved version against the one before it.
 * *picked* is the timeline entry the sides were picked from, if any. Resets
 * when *resetKey* (the object shown) changes.
 */
export function useChangesSides(resetKey: string, hasPending: boolean, approved: number) {
  const [chosen, setChosen] = useState<{ base: SideRef; target: SideRef; picked: string | null } | null>(null);
  // Reset only when the object shown changes: on mount there's nothing to reset,
  // and resetting then would undo a selection made as the History tab opens.
  const shownKey = useRef(resetKey);
  useEffect(() => {
    if (shownKey.current === resetKey) return;
    shownKey.current = resetKey;
    setChosen(null);
  }, [resetKey]);
  const target = chosen?.target ?? (hasPending ? PROPOSED_REF : versionRef(approved));
  const base =
    chosen?.base ?? (target.kind === "proposed" ? versionRef(approved) : versionRef(Math.max(approved - 1, 0)));
  const mode: ChangesMode = target.kind === "proposed" ? "pending" : "history";
  return {
    mode,
    base,
    target,
    picked: chosen?.picked ?? null,
    /** Nothing has been picked yet: the sides are the defaults above. */
    isDefault: chosen === null,
    setBase: (next: SideRef) => setChosen({ base: next, target, picked: null }),
    setTarget: (next: SideRef) => setChosen({ base, target: next, picked: null }),
    /** Compare *nextBase* with *nextTarget*, as picked from timeline entry *picked*. */
    select: (nextBase: SideRef, nextTarget: SideRef, picked: string | null = null) =>
      setChosen({ base: nextBase, target: nextTarget, picked }),
  };
}

/** A comparison's sides as the changes endpoints' query parameters. */
export function sideParams(base: SideRef, target: SideRef) {
  return {
    ...(base.kind === "step" ? { base_step: base.historyId } : { base: base.kind === "version" ? base.version : 0 }),
    ...(target.kind === "step"
      ? { target_step: target.historyId }
      : target.kind === "version" && target.version > 0
        ? { target: target.version }
        : {}),
  };
}

/**
 * Keeps View changes and a History tab current. Their reads skip the app's
 * five-minute query cache (pass this as the queries' *staleTime*), and are read
 * again whenever *revision* — the object's status, version and last edit —
 * changes, e.g. after approving from the banner while the History tab is open.
 */
export const LIVE_STALE_TIME = 0;

export function useRefetchOnRevision(revision: string | undefined, refetch: () => void) {
  const refetchRef = useRef(refetch);
  refetchRef.current = refetch;
  const seen = useRef(revision);
  useEffect(() => {
    if (seen.current === revision) return;
    seen.current = revision;
    refetchRef.current();
  }, [revision]);
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

/** A folded run of untouched lines inside a modified item (when only changes are shown). */
function LineGap({ count }: { count: number }) {
  const { t } = useTranslation();
  return (
    <div className="select-none bg-muted/30 px-2 pl-7 text-muted-foreground">
      {t("tableChanges.unchangedLines", { count })}
    </div>
  );
}

type Shown<T> = { kind: "line"; value: T } | { kind: "gap"; count: number };

/** *values* with each run of untouched ones folded into a gap when *changesOnly*. */
function withGaps<T>(values: T[], untouched: (value: T) => boolean, changesOnly: boolean): Shown<T>[] {
  if (!changesOnly) return values.map((value) => ({ kind: "line", value }));
  const out: Shown<T>[] = [];
  for (const value of values) {
    const last = out[out.length - 1];
    if (!untouched(value)) out.push({ kind: "line", value });
    else if (last?.kind === "gap") last.count += 1;
    else out.push({ kind: "gap", count: 1 });
  }
  return out;
}

function ItemDiff({ item, layout, changesOnly }: { item: ChangeItem; layout: Layout; changesOnly: boolean }) {
  const rows: DiffRow[] = useMemo(() => itemRows(item.before, item.after), [item.before, item.after]);
  // Only a modified item has untouched lines worth folding away.
  const fold = changesOnly && item.status === "modified";
  // Split, as in a git diff: before on the left, after on the right. An added
  // item's left side and a removed item's right side are empty.
  if (layout === "split") {
    const shown = withGaps(rows, (r) => r.left?.tag === "equal" && r.right?.tag === "equal", fold);
    return (
      <div className="font-mono text-xs leading-5">
        {shown.map((entry, i) =>
          entry.kind === "gap" ? (
            <LineGap key={i} count={entry.count} />
          ) : (
            <div key={i} className="grid grid-cols-2">
              <div className="border-r">
                <Line cell={entry.value.left} other={entry.value.right} />
              </div>
              <Line cell={entry.value.right} other={entry.value.left} />
            </div>
          ),
        )}
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
  const shown = withGaps(unifiedRows(rows), (cell) => cell.tag === "equal", fold);
  return (
    <div className="font-mono text-xs leading-5">
      {shown.map((entry, i) =>
        entry.kind === "gap" ? (
          <LineGap key={i} count={entry.count} />
        ) : (
          <Line key={i} cell={entry.value} other={pairs.get(entry.value) ?? null} />
        ),
      )}
    </div>
  );
}

/**
 * Expands and collapses its content smoothly (animating the height), and keeps
 * collapsed content out of the tab order. No motion when the user prefers less.
 */
function Reveal({ open, children }: { open: boolean; children: React.ReactNode }) {
  return (
    <div
      className={cn(
        "grid transition-[grid-template-rows] duration-200 ease-out motion-reduce:transition-none",
        open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
      )}
    >
      <div className="min-h-0 overflow-hidden" inert={!open}>
        {children}
      </div>
    </div>
  );
}

function Chevron({ open }: { open: boolean }) {
  return (
    <ChevronRight
      aria-hidden="true"
      className={cn(
        "h-4 w-4 shrink-0 transition-transform duration-200 motion-reduce:transition-none",
        open && "rotate-90",
      )}
    />
  );
}

/**
 * One item (an applied rule, a rule, a collection member or its settings)
 * inside the file: a header row with its status, then its lines. Clicking the
 * header folds the item away.
 */
function FileSection({
  item,
  label,
  layout,
  changesOnly,
  showHeader,
  defaultOpen = true,
}: {
  item: ChangeItem;
  label: string;
  layout: Layout;
  changesOnly: boolean;
  showHeader: boolean;
  defaultOpen?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(defaultOpen);
  const status = item.status as Status;
  const style = STATUS_STYLE[status] ?? STATUS_STYLE.unchanged;
  const counts = useMemo(() => {
    const rows = itemRows(item.before, item.after);
    return {
      added: rows.filter((r) => r.right?.tag === "added").length,
      removed: rows.filter((r) => r.left?.tag === "removed").length,
    };
  }, [item.before, item.after]);
  if (!showHeader) return <ItemDiff item={item} layout={layout} changesOnly={changesOnly} />;
  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className={cn("flex w-full items-center gap-2 border-b px-3 py-1.5 text-left", style.head)}
      >
        <Chevron open={open} />
        <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide", style.badge)}>
          {t(`tableChanges.status.${status}`)}
        </span>
        <span className="truncate text-sm font-medium">{label}</span>
        <span className="ml-auto shrink-0 font-mono text-xs">
          {counts.added > 0 && <span className="text-green-700 dark:text-green-400">+{counts.added} </span>}
          {counts.removed > 0 && <span className="text-red-700 dark:text-red-400">−{counts.removed}</span>}
        </span>
      </button>
      <Reveal open={open}>
        <ItemDiff item={item} layout={layout} changesOnly={changesOnly} />
      </Reveal>
    </div>
  );
}

/**
 * The whole object (a table, rule or collection) as one file, git-style. Each
 * item is diffed against the same item on the other side (matched by its key,
 * not by line position), so a removed or reordered item stays one run of lines
 * instead of scattering through its neighbours. Unchanged items start folded.
 */
function FileDiff({
  items,
  single,
  labelFor,
  layout,
  changesOnly,
}: {
  items: ChangeItem[];
  single: boolean;
  labelFor: (item: ChangeItem) => string;
  layout: Layout;
  changesOnly: boolean;
}) {
  if (items.length === 0) return null;
  return (
    <div className="overflow-hidden rounded-lg border">
      {items.map((item) => (
        <FileSection
          key={item.key}
          item={item}
          label={labelFor(item)}
          layout={layout}
          changesOnly={changesOnly}
          showHeader={!single}
          defaultOpen={single || item.status !== "unchanged"}
        />
      ))}
    </div>
  );
}

const STATUS_ORDER: Status[] = ["added", "modified", "removed", "unchanged"];

/** Items in status order (added, modified, removed, unchanged), keeping their order within a status. */
function orderByStatus(items: ChangeItem[]): ChangeItem[] {
  return STATUS_ORDER.flatMap((status) => items.filter((item) => item.status === status));
}

const COUNT_STYLE: Record<Status, { text: string; label: string }> = {
  added: { text: "text-green-700 dark:text-green-400", label: "tableChanges.countAdded" },
  modified: { text: "text-amber-700 dark:text-amber-400", label: "tableChanges.countModified" },
  removed: { text: "text-red-700 dark:text-red-400", label: "tableChanges.countRemoved" },
  unchanged: { text: "text-muted-foreground", label: "tableChanges.countUnchanged" },
};

/** An approved version, for the pickers and the History timeline. */
export interface ChangesVersion {
  version: number;
  createdBy?: string | null;
  createdAt?: string | null;
  /** A note under the version in the timeline (e.g. re-frozen in place). */
  note?: string | null;
}

/** One history step (created, edited, submitted, approved, rejected, withdrawn), for the History timeline. */
export interface ChangesEvent {
  action: string;
  actor?: string | null;
  at?: string | null;
  note?: string | null;
  /** The version an approval created. */
  version?: number | null;
  /** The step's id. */
  historyId?: number | null;
  /** The step saved the object's state, so it can be compared (a saved draft, when it's an edit). */
  hasSnapshot?: boolean;
}

export interface ChangesViewProps {
  sides: ReturnType<typeof useChangesSides>;
  hasPending: boolean;
  approvedVersion: number;
  /** Approved versions, for the pickers and the History timeline. */
  versions: ChangesVersion[];
  /** The history: the saved drafts in the pickers, and the History timeline. */
  events?: ChangesEvent[];
  items: ChangeItem[];
  /** Hide the per-status counts (a single item). */
  single?: boolean;
  labelFor?: (item: ChangeItem) => string;
  note?: string | null;
  loading: boolean;
  error: boolean;
  /** The versions or the history steps are still loading (the History timeline waits for both). */
  timelineLoading?: boolean;
  /** Shown when nothing has been approved yet and no draft is picked. */
  neverApprovedText: string;
  noChangesText: string;
}

/** The comparable states (versions and saved drafts), newest first. */
function usePoints(versions: ChangesVersion[], events: ChangesEvent[] | undefined) {
  return useMemo(
    () =>
      historyPoints(
        versions,
        (events ?? []).map((e) => ({ action: e.action, at: e.at, historyId: e.historyId, hasSnapshot: e.hasSnapshot })),
      ),
    [versions, events],
  );
}

/** The comparison: what is compared and how it's shown, the counts (which filter it), and the file. */
function ChangesBody({
  sides,
  versions,
  events,
  items,
  single = false,
  labelFor,
  note,
  loading,
  error,
  neverApprovedText,
  noChangesText,
  hasPending,
  allowProposed,
  scroll,
}: ChangesViewProps & { allowProposed: boolean; scroll: boolean }) {
  const { t } = useTranslation();
  const [layout, setLayout] = useState<Layout>("unified");
  const [changesOnly, setChangesOnly] = useState(true);
  // Statuses switched off with their pill; unchanged items start off.
  const [hidden, setHidden] = useState<Set<Status>>(() => new Set<Status>(["unchanged"]));
  const offerProposed = allowProposed && hasPending;
  const points = usePoints(versions, events);
  const toggleStatus = (status: Status) =>
    setHidden((current) => {
      const next = new Set(current);
      if (next.has(status)) next.delete(status);
      else next.add(status);
      return next;
    });
  const pointLabel = (ref: SideRef): string => {
    if (ref.kind === "proposed") return t("tableChanges.proposed");
    if (ref.kind === "version") return ref.version === 0 ? t("tableChanges.nothing") : `v${ref.version}`;
    const point = points.find((p) => sameRef(p.ref, ref));
    return t("tableChanges.draftAt", { at: formatDateTimeCompact(point?.atIso) });
  };
  const option = (ref: SideRef) => ({ value: refKey(ref), label: pointLabel(ref) });
  const targetAt = pointTime(sides.target, points);
  const offered = pickerPoints(points);
  const baseOptions = [...offered.filter((p) => p.at < targetAt).map((p) => option(p.ref)), option(NOTHING)];
  const targetOptions = [...(offerProposed ? [option(PROPOSED_REF)] : []), ...offered.map((p) => option(p.ref))];
  // The current sides are always options, so the pickers show their labels (e.g. "Nothing").
  if (!targetOptions.some((o) => o.value === refKey(sides.target))) targetOptions.push(option(sides.target));
  if (!baseOptions.some((o) => o.value === refKey(sides.base))) baseOptions.push(option(sides.base));

  const changed = items.filter((c) => c.status !== "unchanged");
  // One item on its own (a rule) has no status pills: "Only show changes" hides it when it's unchanged.
  const shown = single
    ? changesOnly
      ? changed
      : items
    : orderByStatus(items).filter((c) => !hidden.has(c.status as Status));
  const label = (c: ChangeItem) => labelFor?.(c) || c.name || c.key;
  const counts = useMemo(() => {
    const out: Record<Status, number> = { added: 0, removed: 0, modified: 0, unchanged: 0 };
    for (const item of items) out[item.status as Status] = (out[item.status as Status] ?? 0) + 1;
    return out;
  }, [items]);
  const noHistory = sides.target.kind === "version" && sides.target.version === 0 && points.length === 0;
  const showPickers = points.length > 0 || offerProposed;

  return (
    <div className={cn("flex min-h-0 flex-col gap-3", scroll && "flex-1")}>
      {/* One toolbar, bottom-aligned: what is compared on the left, how it's shown on the right. */}
      <div className="flex flex-wrap items-end justify-between gap-3 text-sm">
        {showPickers ? (
          <div className="flex flex-wrap items-end gap-3">
            <div className="w-56">
              <MultiSelectPopover
                single
                label={t("tableChanges.compare")}
                placeholder={pointLabel(sides.base)}
                searchPlaceholder={t("tableChanges.searchVersions")}
                emptyText={t("tableChanges.noVersionMatch")}
                options={baseOptions}
                selected={[refKey(sides.base)]}
                onChange={(values) => values[0] != null && sides.setBase(parseRefKey(values[0]))}
              />
            </div>
            <div className="w-56">
              <MultiSelectPopover
                single
                label={t("tableChanges.compareTo")}
                placeholder={pointLabel(sides.target)}
                searchPlaceholder={t("tableChanges.searchVersions")}
                emptyText={t("tableChanges.noVersionMatch")}
                options={targetOptions}
                selected={[refKey(sides.target)]}
                onChange={(values) => {
                  const next = values[0];
                  if (next == null) return;
                  const target = parseRefKey(next);
                  // Keep the base older than the new target.
                  const base =
                    pointTime(sides.base, points) < pointTime(target, points)
                      ? sides.base
                      : (offered.find((p) => p.at < pointTime(target, points))?.ref ?? NOTHING);
                  sides.select(base, target);
                }}
              />
            </div>
          </div>
        ) : (
          <span />
        )}
        <div className="flex h-9 items-center gap-3">
          <label className="flex cursor-pointer select-none items-center gap-2 text-xs">
            <Checkbox checked={changesOnly} onCheckedChange={(checked) => setChangesOnly(checked === true)} />
            {t("tableChanges.onlyChanges")}
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
          {!single && (
            <div role="group" aria-label={t("tableChanges.filterByStatus")} className="flex flex-wrap gap-2 text-xs">
              {STATUS_ORDER.map((status) => {
                const style = COUNT_STYLE[status];
                const on = !hidden.has(status);
                return (
                  <button
                    key={status}
                    type="button"
                    aria-pressed={on}
                    disabled={counts[status] === 0}
                    onClick={() => toggleStatus(status)}
                    className={cn(
                      "select-none rounded-full border px-2.5 py-1 transition-opacity disabled:cursor-default",
                      style.text,
                      on && counts[status] > 0 ? "opacity-100" : "opacity-40",
                    )}
                  >
                    {t(style.label, { count: counts[status] })}
                  </button>
                );
              })}
            </div>
          )}

          <div className={cn("space-y-2", scroll && "min-h-0 flex-1 overflow-y-auto pr-1")}>
            {changed.length === 0 && <p className="text-sm text-muted-foreground">{noChangesText}</p>}
            <FileDiff
              key={`${refKey(sides.base)}-${refKey(sides.target)}`}
              items={shown}
              single={single}
              labelFor={label}
              layout={layout}
              changesOnly={changesOnly}
            />
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
        <ChangesBody {...view} allowProposed scroll />
        <div className="flex justify-end border-t pt-3">
          <Button variant="outline" onClick={onClose}>
            {t("common.close", "Close")}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

type TimelineEntry =
  | { kind: "version"; at?: string | null; version: ChangesVersion; approval?: ChangesEvent }
  | { kind: "event"; at?: string | null; event: ChangesEvent };

const EVENT_STYLE: Record<string, string> = {
  created: "text-muted-foreground",
  edited: "text-muted-foreground",
  submitted: "text-sky-700 dark:text-sky-400",
  approved: "text-green-700 dark:text-green-400",
  rejected: "text-red-700 dark:text-red-400",
  withdrawn: "text-muted-foreground",
};

function timeOf(at: string | null | undefined): number {
  const ms = at ? Date.parse(at) : Number.NaN;
  return Number.isNaN(ms) ? Number.NEGATIVE_INFINITY : ms;
}

/**
 * A History tab: the timeline (each approved version with who approved it,
 * when and their note, between the other steps — created, edited, submitted,
 * rejected, withdrawn — newest first) beside the comparison. Picking an entry
 * sets the comparison (see ``selectionFor``); the pickers compare any two saved
 * states. Pending changes are left to View changes.
 */
export function ChangesPanel(view: ChangesViewProps) {
  const { t } = useTranslation();
  const { sides } = view;
  const events = view.events ?? [];
  const points = usePoints(view.versions, view.events);
  // An approval's note belongs on the version it created; every other step is
  // its own line in the timeline.
  const approvals = new Map(
    events.filter((e) => e.action === "approved" && e.version != null).map((e) => [e.version as number, e]),
  );
  const steps = events.filter(
    (e) => !(e.action === "approved" && e.version != null && view.versions.some((v) => v.version === e.version)),
  );
  const entries: TimelineEntry[] = [
    ...view.versions.map((v) => ({ kind: "version" as const, at: v.createdAt, version: v, approval: approvals.get(v.version) })),
    ...steps.map((e) => ({ kind: "event" as const, at: e.at, event: e })),
  ].sort((x, y) => timeOf(y.at) - timeOf(x.at) || 0);
  const entryKey = (item: TimelineEntry, i: number) =>
    item.kind === "version" ? `v${item.version.version}` : `e${item.event.historyId ?? `i${i}`}`;
  const selectionOf = (item: TimelineEntry) =>
    item.kind === "version"
      ? selectionFor({ kind: "version", version: item.version.version }, points)
      : selectionFor(
          {
            kind: "step",
            action: item.event.action,
            at: item.event.at,
            historyId: item.event.historyId,
            hasSnapshot: item.event.hasSnapshot,
          },
          points,
        );
  // A History tab opens on the object as it is now (its newest saved state) against nothing.
  const newest = pickerPoints(points)[0]?.ref ?? null;
  const newestKey = newest ? refKey(newest) : null;
  const openRef = useRef<(() => void) | null>(null);
  openRef.current = sides.isDefault && newest ? () => sides.select(NOTHING, newest) : null;
  useEffect(() => {
    openRef.current?.();
  }, [newestKey]);
  // The picked entry is highlighted; with none picked, the entry the target side is.
  const isSelected = (item: TimelineEntry, key: string) => {
    if (sides.picked) return sides.picked === key;
    if (item.kind === "version") return sameRef(sides.target, versionRef(item.version.version));
    return sides.target.kind === "step" && sides.target.historyId === item.event.historyId;
  };
  // The tab shows once everything it opens on has loaded (the versions, the
  // steps and the first comparison), rather than filling in piece by piece.
  const [settled, setSettled] = useState(false);
  const ready = view.error || (!view.timelineLoading && !view.loading && !(sides.isDefault && newest));
  useEffect(() => {
    if (ready) setSettled(true);
  }, [ready]);
  const entryClass = (selected: boolean) =>
    cn(
      "flex w-full select-none flex-col gap-0.5 rounded-lg border px-3 py-2 text-left text-xs transition-colors",
      selected ? "border-foreground/40 bg-muted" : "border-transparent hover:bg-muted/50",
      "disabled:cursor-default disabled:hover:bg-transparent",
    );
  if (!settled) {
    return (
      <div className="flex justify-center py-16" role="status" aria-label={t("tableChanges.loading")}>
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }
  return (
    <div className="grid gap-4 pt-4 md:grid-cols-[16rem_minmax(0,1fr)]">
      {/* The timeline keeps its own scrollbar, so a long history doesn't push the comparison away. */}
      <nav
        aria-label={t("tableChanges.historyTitle")}
        className="dq-scroll-visible space-y-1 md:max-h-[36rem] md:overflow-y-auto md:pr-1"
      >
        {entries.length === 0 && (
          <p className="px-3 py-2 text-xs text-muted-foreground">{view.neverApprovedText}</p>
        )}
        {entries.map((item, i) => {
          const key = entryKey(item, i);
          const selection = selectionOf(item);
          const pick = () => selection && sides.select(selection.base, selection.target, key);
          if (item.kind === "event") {
            const e = item.event;
            return (
              <button
                key={key}
                type="button"
                aria-current={isSelected(item, key)}
                disabled={!selection}
                onClick={pick}
                className={entryClass(isSelected(item, key))}
              >
                <span className={cn("font-medium", EVENT_STYLE[e.action] ?? "text-muted-foreground")}>
                  {t(`tableChanges.event.${e.action}`, e.action)}
                  {e.version != null ? ` (v${e.version})` : ""}
                </span>
                {e.actor && <span className="truncate font-mono text-muted-foreground">{e.actor}</span>}
                {e.at && (
                  <span className="text-muted-foreground">
                    <RelativeTimeCell iso={e.at} />
                  </span>
                )}
                {e.note && <span className="italic text-muted-foreground">“{e.note}”</span>}
              </button>
            );
          }
          const v = item.version;
          const actor = item.approval?.actor ?? v.createdBy;
          return (
            <button
              key={key}
              type="button"
              aria-current={isSelected(item, key)}
              onClick={pick}
              className={entryClass(isSelected(item, key))}
            >
              <span className={cn("font-medium", EVENT_STYLE.approved)}>
                {t("tableChanges.approvedVersion", { version: v.version })}
              </span>
              {actor && <span className="truncate font-mono text-muted-foreground">{actor}</span>}
              {v.createdAt && (
                <span className="text-muted-foreground">
                  <RelativeTimeCell iso={v.createdAt} />
                </span>
              )}
              {item.approval?.note && <span className="italic text-muted-foreground">“{item.approval.note}”</span>}
              {v.note && <span className="text-amber-700 dark:text-amber-400">{v.note}</span>}
            </button>
          );
        })}
      </nav>
      <div className="min-w-0 space-y-3">
        <ChangesBody {...view} allowProposed={false} scroll={false} />
      </div>
    </div>
  );
}
