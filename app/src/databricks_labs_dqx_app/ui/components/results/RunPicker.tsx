import { useTranslation } from "react-i18next";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Button } from "@/components/ui/button";
import { ChevronDown, History } from "lucide-react";
import { cn } from "@/lib/utils";
import { BreachIcon } from "./BreachIcon";

export type Run = {
  run_id: string;
  run_ts?: string | null;
  pass_rate?: number | null;
  /** Run provenance ('draft' | 'published'); only present in Published+Draft
   *  mode. When 'draft' the label is tagged with `draftMarker`. */
  run_mode?: string | null;
  /** True when this run has at least one threshold breach. */
  breached?: boolean;
  /** Criticality of the worst breach: "error" | "warn" | null.
   *  The wider string type matches the API payload; BreachIcon handles
   *  non-"error"/"warn" values by rendering nothing. */
  breach_criticality?: string | null;
};

/**
 * Human-readable run label, e.g. "11 Jun 09:51 · 95%". When the run is a draft
 * run (`run_mode === "draft"`) and a `draftMarker` is supplied, it is appended
 * (e.g. "11 Jun 09:51 · 95% (draft)") so draft-dated runs are distinguishable
 * in Published+Draft mode. Published runs are never tagged.
 */
export function formatRun(run: Run, draftMarker?: string): string {
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
  if (draftMarker && run.run_mode === "draft") {
    label += ` ${draftMarker}`;
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
  breachEnabled = true,
}: {
  runs: Run[];
  value: string | null;
  onChange: (runId: string | null) => void;
  /** When false, breach warning icons are hidden (pass-threshold feature is
   *  disabled globally). Defaults to true (fail-open). */
  breachEnabled?: boolean;
}) {
  const { t } = useTranslation();
  const draftMarker = t("resultsUi.draftRunMarker");
  const latestRunId = runs[0]?.run_id ?? null;
  // `null` (no pinned run) AND an explicit pin of the latest run are the same
  // selection — both resolve to the latest run, and both read "Latest".
  const isLatestSelected = value == null || value === latestRunId;
  const selected = value ? runs.find((r) => r.run_id === value) : undefined;
  const triggerLabel =
    !selected || isLatestSelected ? t("resultsUi.latestRun") : formatRun(selected, draftMarker);

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
        <TooltipProvider delayDuration={200}>
          {runs.map((run, i) => {
            const isLatest = i === 0;
            // Selecting the latest run routes through the `null` ("latest") path
            // so the trigger reads "Latest" and the data uses the latest branch.
            const isSelected = isLatest ? isLatestSelected : run.run_id === value;
            return (
              <DropdownMenuItem
                key={run.run_id}
                onSelect={() => onChange(isLatest ? null : run.run_id)}
                className={cn("font-mono text-xs gap-1.5", isSelected && "bg-muted")}
              >
                <span className="flex-1">{formatRun(run, draftMarker)}</span>
                {breachEnabled && run.breached && (
                  <BreachIcon criticality={run.breach_criticality} />
                )}
              </DropdownMenuItem>
            );
          })}
        </TooltipProvider>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
