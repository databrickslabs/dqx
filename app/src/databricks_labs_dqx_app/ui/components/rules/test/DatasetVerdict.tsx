// One verdict for a dataset-level rule's test run.
//
// A dataset-level query returns a single row whose `condition` decides the whole
// table, so a one-cell grid says nothing a pass/fail summary doesn't say better.
// Any other columns the query returns (counts, ratios) are listed under it.
import { CheckCircle2, AlertTriangle, XCircle } from "lucide-react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";

/** Columns the verdict already explains, so they aren't repeated as values. */
const VERDICT_COLUMNS = new Set(["condition"]);

interface Props {
  /** Every column the query returned, in order. */
  columns: string[];
  /** The returned rows: each row's cells by column, plus its verdict. */
  rows: { cells: Record<string, unknown>; passed: boolean | null }[];
}

export function DatasetVerdict({ columns, rows }: Props) {
  const { t } = useTranslation();
  if (rows.length !== 1) {
    return (
      <div
        role="status"
        className="flex items-start gap-2 rounded-lg border border-amber-500/50 bg-amber-500/10 p-4 text-sm text-amber-900 dark:text-amber-100"
      >
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
        <p>{t("ruleTest.datasetVerdictRowCount", { count: rows.length })}</p>
      </div>
    );
  }
  const [row] = rows;
  const passed = row.passed !== false;
  const values = columns.filter((c) => !VERDICT_COLUMNS.has(c.toLowerCase()));
  const Icon = passed ? CheckCircle2 : XCircle;
  return (
    <div
      role="status"
      className={cn(
        "rounded-lg border p-4 space-y-3",
        passed ? "border-green-500/40 bg-green-500/10" : "border-red-500/40 bg-red-500/10",
      )}
    >
      <div className="flex items-center gap-2">
        <Icon className={cn("h-5 w-5 shrink-0", passed ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400")} />
        <div>
          <p className="text-sm font-medium">{passed ? t("ruleTest.datasetVerdictPass") : t("ruleTest.datasetVerdictFail")}</p>
          <p className="text-xs text-muted-foreground">{t("ruleTest.datasetVerdictDescription")}</p>
        </div>
      </div>
      {values.length > 0 && (
        <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 pl-7 text-xs">
          {values.map((c) => (
            <div key={c} className="contents">
              <dt className="font-mono text-muted-foreground">{c}</dt>
              <dd className="font-mono break-all">{row.cells[c] == null ? "null" : String(row.cells[c])}</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}
