import { useMemo, useRef, type UIEvent } from "react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import { diffLines, toYaml, type DiffCell, type DiffRow } from "@/lib/yaml-diff";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertCircle, Info } from "lucide-react";
import {
  useListRegistryRuleVersions,
  useGetDataProductReviewChanges,
  useGetRuleHistory,
} from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import {
  buildDqxCheckJson,
  severityValueCriticality,
  type MaterializableRule,
} from "@/lib/registry-rule-conversion";

/**
 * Shared previous-vs-proposed change-diff popout for the Drafts & Review
 * approval cards (Stream J item 13e). Registry rules diff against their
 * currently-published version snapshot; per-table rule drafts diff the two most
 * recent audit-log entries; monitored tables use the per-rule TableChangesDialog.
 * Table Spaces have no per-version snapshot store, so their popout shows the current proposed definition with an explicit note
 * rather than fabricating a diff.
 *
 * All four surfaces render through :func:`ChangeDiffDialog`, keeping the
 * popout visually consistent. YAML is used for the payload panes so any
 * check/definition shape renders legibly without a bespoke renderer per type.
 */

function DiffLine({ cell }: { cell: DiffCell | null }) {
  if (!cell) {
    // Filler row opposite an addition/removal — keeps the panes aligned.
    return <div className="w-full h-[1.125rem] bg-muted/40 dark:bg-muted/20 border-l-2 border-transparent" />;
  }
  const gutter = cell.tag === "removed" ? "-" : cell.tag === "added" ? "+" : " ";
  return (
    <div
      className={cn(
        "w-full h-[1.125rem] leading-[1.125rem] whitespace-pre border-l-2",
        cell.tag === "removed" && "bg-red-500/20 border-red-500/60",
        cell.tag === "added" && "bg-emerald-500/20 border-emerald-500/60",
        cell.tag === "equal" && "border-transparent",
      )}
    >
      <span
        className={cn(
          "inline-block w-4 pl-1 select-none",
          cell.tag === "removed" && "text-destructive",
          cell.tag === "added" && "text-emerald-600 dark:text-emerald-400",
          cell.tag === "equal" && "text-muted-foreground/40",
        )}
      >
        {gutter}
      </span>
      <span className="pr-3">{cell.text || " "}</span>
    </div>
  );
}

function SplitDiffPane({
  label,
  tone,
  rows,
  side,
  scrollRef,
  onScroll,
}: {
  label: string;
  tone: "previous" | "proposed";
  rows: DiffRow[];
  side: "left" | "right";
  scrollRef: React.RefObject<HTMLDivElement | null>;
  onScroll: (e: UIEvent<HTMLDivElement>) => void;
}) {
  return (
    <div className="border rounded-lg overflow-hidden flex flex-col min-w-0">
      <div
        className={cn(
          "px-3 py-1.5 text-xs font-medium border-b",
          tone === "proposed"
            ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300"
            : "bg-muted text-muted-foreground",
        )}
      >
        {label}
      </div>
      <div ref={scrollRef} onScroll={onScroll} className="overflow-auto max-h-[52vh] text-xs font-mono">
        <div className="min-w-max">
          {rows.map((row, idx) => (
            <DiffLine key={idx} cell={side === "left" ? row.left : row.right} />
          ))}
        </div>
      </div>
    </div>
  );
}

/**
 * Git-style split diff: the previous YAML on the left with removed lines in
 * red, the proposed YAML on the right with added lines in green, aligned
 * line-for-line. The two panes scroll vertically (and horizontally) in
 * lockstep via shared refs with a re-entrancy guard.
 */
function SplitDiff({
  previous,
  proposed,
  previousLabel,
  proposedLabel,
}: {
  previous: unknown;
  proposed: unknown;
  previousLabel: string;
  proposedLabel: string;
}) {
  const leftRef = useRef<HTMLDivElement>(null);
  const rightRef = useRef<HTMLDivElement>(null);
  const syncing = useRef(false);

  const rows = useMemo(
    () => diffLines(toYaml(previous).trim(), toYaml(proposed).trim()),
    [previous, proposed],
  );

  const makeScrollHandler = (from: "left" | "right") => (e: UIEvent<HTMLDivElement>) => {
    // Guard against the feedback loop: assigning scrollTop below fires the
    // other pane's onScroll, which would otherwise scroll us back.
    if (syncing.current) {
      syncing.current = false;
      return;
    }
    const source = e.currentTarget;
    const target = (from === "left" ? rightRef : leftRef).current;
    if (!target) return;
    syncing.current = true;
    target.scrollTop = source.scrollTop;
    target.scrollLeft = source.scrollLeft;
  };

  return (
    <div className="grid gap-3 md:grid-cols-2">
      <SplitDiffPane
        label={previousLabel}
        tone="previous"
        rows={rows}
        side="left"
        scrollRef={leftRef}
        onScroll={makeScrollHandler("left")}
      />
      <SplitDiffPane
        label={proposedLabel}
        tone="proposed"
        rows={rows}
        side="right"
        scrollRef={rightRef}
        onScroll={makeScrollHandler("right")}
      />
    </div>
  );
}

/** Single plain pane — used when there is no previous snapshot to diff against. */
function PlainPane({ label, value, emptyText }: { label: string; value: unknown; emptyText: string }) {
  const body = toYaml(value).trim();
  return (
    <div className="border rounded-lg overflow-hidden flex flex-col min-w-0">
      <div className="px-3 py-1.5 text-xs font-medium border-b bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300">
        {label}
      </div>
      {body ? (
        <pre className="p-3 text-xs font-mono overflow-auto max-h-[52vh] whitespace-pre">{body}</pre>
      ) : (
        <p className="p-3 text-xs italic text-muted-foreground/70">{emptyText}</p>
      )}
    </div>
  );
}

interface ChangeDiffDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  itemName: string;
  previous: unknown | null;
  proposed: unknown;
  previousLabel?: string;
  proposedLabel?: string;
  note?: string | null;
  loading?: boolean;
  error?: boolean;
}

export function ChangeDiffDialog({
  open,
  onOpenChange,
  title,
  itemName,
  previous,
  proposed,
  previousLabel,
  proposedLabel,
  note,
  loading,
  error,
}: ChangeDiffDialogProps) {
  const { t } = useTranslation();
  const hasPrevious = previous !== null && previous !== undefined;
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription className="font-mono text-xs break-all">{itemName}</DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="space-y-2">
            <Skeleton className="h-6 w-40" />
            <Skeleton className="h-48 w-full" />
          </div>
        ) : error ? (
          <p className="text-destructive text-sm flex items-center gap-2">
            <AlertCircle className="h-4 w-4 shrink-0" />
            {t("rulesDrafts.diff.loadFailed")}
          </p>
        ) : (
          <div className="space-y-3">
            {note && (
              <div className="flex items-start gap-2 rounded-lg border border-amber-300 bg-amber-50 p-3 text-xs text-amber-800 dark:bg-amber-950/30 dark:border-amber-700 dark:text-amber-300">
                <Info className="h-4 w-4 shrink-0 mt-0.5" />
                <span>{note}</span>
              </div>
            )}
            {hasPrevious ? (
              <SplitDiff
                previous={previous}
                proposed={proposed}
                previousLabel={previousLabel ?? t("rulesDrafts.diff.previous")}
                proposedLabel={proposedLabel ?? t("rulesDrafts.diff.proposed")}
              />
            ) : (
              <PlainPane
                label={proposedLabel ?? t("rulesDrafts.diff.proposed")}
                value={proposed}
                emptyText={t("rulesDrafts.diff.empty")}
              />
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

// ---------------------------------------------------------------------------
// Registry rule — diff current definition against the published vN snapshot
// ---------------------------------------------------------------------------

export interface RegistryDiffTarget {
  ruleId: string;
  name: string;
  version: number;
  /**
   * The LIVE (proposed) rule, carrying everything :func:`buildDqxCheckJson`
   * needs to render its fully-materialized check dict — mode, polarity,
   * structured definition, and tag user_metadata. Rendering the materialized
   * check (not the trimmed structured `definition`) makes the rule diff show
   * every field — function, arguments, criticality, name, message, tags —
   * exactly like the monitored-table diff does (bug-bash-v4 item 43).
   */
  proposed: MaterializableRule;
}

export function RegistryRuleDiffDialog({
  target,
  onClose,
}: {
  target: RegistryDiffTarget | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const enabled = !!target;
  const { data, isLoading, isError } = useListRegistryRuleVersions(target?.ruleId ?? "", {
    query: { enabled },
  });
  // The rendered `criticality` honours the admin-edited severity -> criticality
  // mapping (config page), matching the "View JSON" dialog and the materializer.
  const { data: labelDefsData } = useLabelDefinitions();
  const severityCriticality = useMemo(
    () => severityValueCriticality(labelDefsData?.definitions),
    [labelDefsData],
  );
  const versions = data?.data ?? [];
  const published = useMemo(
    () => (target ? versions.find((v) => v.version === target.version) : undefined),
    [versions, target],
  );
  const hasPrior = !!target && target.version > 0 && !!published;

  // Materialize BOTH sides into the full DQX check dict so every field renders.
  // The proposed side uses the live rule; the previous side uses the FROZEN
  // vN snapshot (its own frozen mode/polarity/definition/tags) so a past
  // version's diff reflects that version's frozen JSON, not live metadata.
  const proposedCheck = useMemo(
    () => (target ? buildDqxCheckJson(target.proposed, severityCriticality) : {}),
    [target, severityCriticality],
  );
  const previousCheck = useMemo(
    () =>
      hasPrior
        ? buildDqxCheckJson(
            {
              mode: published!.mode ?? null,
              polarity: published!.polarity ?? null,
              definition: published!.definition,
              user_metadata: published!.user_metadata,
            },
            severityCriticality,
            target?.proposed.mode ?? undefined,
          )
        : null,
    [hasPrior, published, severityCriticality, target],
  );

  return (
    <ChangeDiffDialog
      open={enabled}
      onOpenChange={(o) => !o && onClose()}
      title={t("rulesDrafts.diff.title")}
      itemName={target?.name ?? ""}
      previous={previousCheck}
      proposed={proposedCheck}
      previousLabel={hasPrior ? t("rulesDrafts.diff.versionLabel", { version: target!.version }) : undefined}
      proposedLabel={t("rulesDrafts.diff.proposedDefinition")}
      note={target && !hasPrior ? t("rulesDrafts.diff.rrNoPrior") : null}
      loading={enabled && isLoading}
      error={isError}
    />
  );
}

// ---------------------------------------------------------------------------
// Table Space — no per-version snapshot store; show current proposed members
// ---------------------------------------------------------------------------

export interface TableSpaceDiffTarget {
  productId: string;
  name: string;
}

export function TableSpaceDiffDialog({
  target,
  onClose,
}: {
  target: TableSpaceDiffTarget | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const enabled = !!target;
  const { data, isLoading, isError } = useGetDataProductReviewChanges(target?.productId ?? "", {
    query: { enabled },
  });
  const changes = data?.data;
  const proposed = useMemo(
    () =>
      (changes?.members ?? []).map((m) => ({
        table: m.table_fqn,
        pinned_version: m.pinned_version ?? null,
        checks: m.checks ?? [],
      })),
    [changes],
  );
  return (
    <ChangeDiffDialog
      open={enabled}
      onOpenChange={(o) => !o && onClose()}
      title={t("rulesDrafts.diff.title")}
      itemName={target?.name ?? ""}
      previous={null}
      proposed={proposed}
      proposedLabel={t("rulesDrafts.diff.proposedMembers")}
      note={t("rulesDrafts.diff.tsNoSnapshot")}
      loading={enabled && isLoading}
      error={isError}
    />
  );
}

// ---------------------------------------------------------------------------
// Per-table rule draft — diff the two most recent audit-log check payloads
// ---------------------------------------------------------------------------

export interface RuleHistoryDiffTarget {
  ruleId: string;
  name: string;
}

export function RuleHistoryDiffDialog({
  target,
  onClose,
}: {
  target: RuleHistoryDiffTarget | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const enabled = !!target;
  const { data, isLoading, isError } = useGetRuleHistory(target?.ruleId ?? "", {
    query: { enabled },
  });
  const entries = useMemo(() => (data?.data ?? []).filter((e) => e.check), [data]);
  const proposed = entries[0]?.check ?? {};
  const hasPrior = entries.length > 1;
  const previous = hasPrior ? entries[1]!.check ?? null : null;
  return (
    <ChangeDiffDialog
      open={enabled}
      onOpenChange={(o) => !o && onClose()}
      title={t("rulesDrafts.diff.title")}
      itemName={target?.name ?? ""}
      previous={previous}
      proposed={proposed}
      note={target && !hasPrior ? t("rulesDrafts.diff.ruleNoPrior") : null}
      loading={enabled && isLoading}
      error={isError}
    />
  );
}
