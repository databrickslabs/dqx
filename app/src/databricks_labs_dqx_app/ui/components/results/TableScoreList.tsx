import { useTranslation } from "react-i18next";
import type { TableScoreOut } from "@/lib/api";
import { formatScorePercent, scoreBandClass } from "@/lib/results-display";

/**
 * Per-table score breakdown list shared by every Results surface (global
 * Results page, Table Space Results tab, registry-rule Results tab): one row
 * per table with its FQN and either the latest score percentage (colored by
 * scoreBandClass) or a "No runs yet" placeholder when the table has no
 * scored run. Pure presentation — empty states and totals stay with the
 * callers.
 */
export function TableScoreList({ tables }: { tables: TableScoreOut[] }) {
  const { t } = useTranslation();
  return (
    <ul className="flex flex-col gap-2">
      {tables.map((table) => {
        const score = table.score ?? null;
        return (
          <li key={table.source_table_fqn} className="flex items-center justify-between gap-3 text-sm">
            <span className="font-mono truncate">{table.source_table_fqn}</span>
            {score === null ? (
              <span className="text-muted-foreground shrink-0">{t("results.noRunsYet")}</span>
            ) : (
              <span className={`tabular-nums font-medium shrink-0 ${scoreBandClass(score)}`}>
                {formatScorePercent(score)}
              </span>
            )}
          </li>
        );
      })}
    </ul>
  );
}
