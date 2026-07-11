import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import yaml from "js-yaml";
import { cn } from "@/lib/utils";
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
  useGetMonitoredTableVersionChecks,
  useGetDataProductReviewChanges,
  useGetRuleHistory,
} from "@/lib/api";

/**
 * Shared previous-vs-proposed change-diff popout for the Drafts & Review
 * approval cards (Stream J item 13e). Registry rules diff against their
 * currently-published version snapshot; monitored tables diff the two most
 * recent frozen version snapshots; per-table rule drafts diff the two most
 * recent audit-log entries. Table Spaces have no per-version snapshot store,
 * so their popout shows the current proposed definition with an explicit note
 * rather than fabricating a diff.
 *
 * All four surfaces render through :func:`ChangeDiffDialog`, keeping the
 * popout visually consistent. YAML is used for the payload panes so any
 * check/definition shape renders legibly without a bespoke renderer per type.
 */

function toYaml(value: unknown): string {
  if (value === null || value === undefined) return "";
  try {
    return yaml.dump(value, { sortKeys: false, noRefs: true, lineWidth: 100 });
  } catch {
    return String(value);
  }
}

function DiffPane({
  label,
  tone,
  value,
  emptyText,
}: {
  label: string;
  tone: "previous" | "proposed";
  value: unknown;
  emptyText: string;
}) {
  const body = toYaml(value).trim();
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
            <div className={hasPrevious ? "grid gap-3 md:grid-cols-2" : ""}>
              {hasPrevious && (
                <DiffPane
                  label={previousLabel ?? t("rulesDrafts.diff.previous")}
                  tone="previous"
                  value={previous}
                  emptyText={t("rulesDrafts.diff.empty")}
                />
              )}
              <DiffPane
                label={proposedLabel ?? t("rulesDrafts.diff.proposed")}
                tone="proposed"
                value={proposed}
                emptyText={t("rulesDrafts.diff.empty")}
              />
            </div>
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
  definition: unknown;
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
  const versions = data?.data ?? [];
  const published = useMemo(
    () => (target ? versions.find((v) => v.version === target.version) : undefined),
    [versions, target],
  );
  const hasPrior = !!target && target.version > 0 && !!published;
  return (
    <ChangeDiffDialog
      open={enabled}
      onOpenChange={(o) => !o && onClose()}
      title={t("rulesDrafts.diff.title")}
      itemName={target?.name ?? ""}
      previous={hasPrior ? published!.definition : null}
      proposed={target?.definition ?? {}}
      previousLabel={hasPrior ? t("rulesDrafts.diff.versionLabel", { version: target!.version }) : undefined}
      proposedLabel={t("rulesDrafts.diff.proposedDefinition")}
      note={target && !hasPrior ? t("rulesDrafts.diff.rrNoPrior") : null}
      loading={enabled && isLoading}
      error={isError}
    />
  );
}

// ---------------------------------------------------------------------------
// Monitored table — diff the two most recent frozen version snapshots
// ---------------------------------------------------------------------------

export interface MonitoredTableDiffTarget {
  bindingId: string;
  name: string;
  version: number;
}

export function MonitoredTableDiffDialog({
  target,
  onClose,
}: {
  target: MonitoredTableDiffTarget | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const version = target?.version ?? 0;
  const proposedEnabled = !!target && version > 0;
  const previousEnabled = !!target && version > 1;

  const proposedQuery = useGetMonitoredTableVersionChecks(target?.bindingId ?? "", version, {
    query: { enabled: proposedEnabled },
  });
  const previousQuery = useGetMonitoredTableVersionChecks(target?.bindingId ?? "", Math.max(version - 1, 0), {
    query: { enabled: previousEnabled },
  });

  const proposed = proposedQuery.data?.data?.checks ?? [];
  const previous = previousEnabled ? previousQuery.data?.data?.checks ?? [] : null;
  const loading = proposedEnabled && (proposedQuery.isLoading || (previousEnabled && previousQuery.isLoading));
  const error = proposedQuery.isError || (previousEnabled && previousQuery.isError);

  // No approved snapshot yet (version 0): nothing frozen to show for a
  // first-approval binding — the pending rule set is not frozen until approval.
  const note = !target
    ? null
    : version === 0
      ? t("rulesDrafts.diff.mtFirstApproval")
      : !previousEnabled
        ? t("rulesDrafts.diff.mtNoPrior")
        : null;

  return (
    <ChangeDiffDialog
      open={!!target}
      onOpenChange={(o) => !o && onClose()}
      title={t("rulesDrafts.diff.title")}
      itemName={target?.name ?? ""}
      previous={previous}
      proposed={proposed}
      previousLabel={previousEnabled ? t("rulesDrafts.diff.versionLabel", { version: version - 1 }) : undefined}
      proposedLabel={version > 0 ? t("rulesDrafts.diff.versionLabel", { version }) : t("rulesDrafts.diff.proposed")}
      note={note}
      loading={loading}
      error={error}
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
