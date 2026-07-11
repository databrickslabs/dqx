import { useTranslation } from "react-i18next";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { ChevronDown, History } from "lucide-react";
import { cn } from "@/lib/utils";

export type Run = {
  run_id: string;
  run_ts?: string | null;
  pass_rate?: number | null;
};

/** Human-readable run label, e.g. "11 Jun 09:51 · 95%". */
export function formatRun(run: Run): string {
  let label = run.run_id;
  if (run.run_ts) {
    const d = new Date(run.run_ts);
    if (!Number.isNaN(d.getTime())) {
      label = d.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
  }
  if (run.pass_rate != null) {
    label += ` · ${Math.round(run.pass_rate * 100)}%`;
  }
  return label;
}

/**
 * Run selector. `runs` is newest-first, so `runs[0]` is the latest run and
 * value `null` is the "latest" selection (the backend defaults to the latest
 * run when no run_id is pinned). Each run maps to its run_id. No data
 * fetching — callers pass the runs in.
 *
 * The latest run is a SINGLE entry: the menu always shows literal dates (the
 * latest run's date included), but selecting the latest run reports `null`
 * (the latest path) and the closed trigger then reads "Latest". This avoids
 * listing both a "Latest" item and the latest run's date as two entries that
 * resolve to the same run.
 *
 * Styled to match the profiler's version picker (ProfileHistoryPicker): an
 * outline DropdownMenu trigger with a History icon + chevron, rather than a
 * filled Radix Select.
 */
export function RunPicker({
  runs,
  value,
  onChange,
}: {
  runs: Run[];
  value: string | null;
  onChange: (runId: string | null) => void;
}) {
  const { t } = useTranslation();
  const latestRunId = runs[0]?.run_id ?? null;
  // `null` (no pinned run) AND an explicit pin of the latest run are the same
  // selection — both resolve to the latest run, and both read "Latest".
  const isLatestSelected = value == null || value === latestRunId;
  const selected = value ? runs.find((r) => r.run_id === value) : undefined;
  const triggerLabel =
    !selected || isLatestSelected ? t("resultsUi.latestRun") : formatRun(selected);

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className="h-8 gap-1.5 text-xs mr-2"
          aria-label={t("resultsUi.runAria")}
        >
          <History className="h-3.5 w-3.5" />
          <span>{triggerLabel}</span>
          <ChevronDown className="h-3 w-3 opacity-60" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-[14rem]">
        {runs.map((run, i) => {
          const isLatest = i === 0;
          // Selecting the latest run routes through the `null` ("latest") path
          // so the trigger reads "Latest" and the data uses the latest branch.
          const isSelected = isLatest ? isLatestSelected : run.run_id === value;
          return (
            <DropdownMenuItem
              key={run.run_id}
              onSelect={() => onChange(isLatest ? null : run.run_id)}
              className={cn("font-mono text-xs", isSelected && "bg-muted")}
            >
              {formatRun(run)}
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
