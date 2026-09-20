import { useTranslation } from "react-i18next";
import { Check, AlertCircle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { PreviewDryRunOut, PreviewRowResult } from "@/lib/api-custom";

export function PreviewDryRunResultPanel({ result }: { result: PreviewDryRunOut }) {
  const { t } = useTranslation();
  const passRate =
    result.total_rows > 0
      ? Math.round((result.pass_rows / result.total_rows) * 100)
      : 100;

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-4 flex-wrap text-sm">
        <span className="font-semibold">{t("common.previewValidation", "Sample validation")}</span>
        <span className="flex items-center gap-1 text-green-600 dark:text-green-400">
          <Check className="h-3.5 w-3.5" />
          {result.pass_rows} {t("common.passed", "passed")}
        </span>
        {result.error_rows > 0 && (
          <span className="flex items-center gap-1 text-red-600 dark:text-red-400">
            <AlertCircle className="h-3.5 w-3.5" />
            {result.error_rows} {t("common.errors", "errors")}
          </span>
        )}
        {result.warning_rows > 0 && (
          <span className="flex items-center gap-1 text-amber-600 dark:text-amber-400">
            <AlertCircle className="h-3.5 w-3.5" />
            {result.warning_rows} {t("common.warnings", "warnings")}
          </span>
        )}
        <span className="ml-auto text-xs text-muted-foreground font-mono">{passRate}% pass rate</span>
      </div>

      <div className="overflow-x-auto border rounded-md">
        <table className="w-full text-xs">
          <thead className="border-b bg-muted/40">
            <tr>
              <th className="px-2 py-2 text-left font-semibold text-[10px] uppercase tracking-wide text-muted-foreground w-8">
                #
              </th>
              {result.rows[0] &&
                Object.keys(result.rows[0].row).map((col) => (
                  <th
                    key={col}
                    className="px-3 py-2 text-left font-semibold text-[10px] uppercase tracking-wide text-muted-foreground whitespace-nowrap font-mono"
                  >
                    {col}
                  </th>
                ))}
              <th className="px-3 py-2 text-left font-semibold text-[10px] uppercase tracking-wide text-muted-foreground">
                {t("common.status", "Status")}
              </th>
            </tr>
          </thead>
          <tbody>
            {result.rows.map((r: PreviewRowResult, i: number) => {
              const hasError = r.errors.length > 0;
              const hasWarning = r.warnings.length > 0;
              return (
                <tr
                  key={i}
                  className={`border-b last:border-0 ${
                    hasError
                      ? "bg-red-50/60 dark:bg-red-950/20"
                      : hasWarning
                        ? "bg-amber-50/60 dark:bg-amber-950/20"
                        : "hover:bg-muted/20"
                  } transition-colors`}
                >
                  <td className="px-2 py-2 text-muted-foreground/50 font-mono text-[10px]">{i + 1}</td>
                  {Object.values(r.row).map((v, vi) => (
                    <td key={vi} className="px-3 py-2 font-mono text-[11px] whitespace-nowrap text-muted-foreground">
                      {v == null ? (
                        <span className="italic text-muted-foreground/40">null</span>
                      ) : (
                        String(v)
                      )}
                    </td>
                  ))}
                  <td className="px-3 py-2 whitespace-nowrap">
                    {hasError ? (
                      <div className="space-y-0.5">
                        {r.errors.map((e) => (
                          <Badge key={e} variant="destructive" className="text-[9px] font-normal block w-fit">
                            {e}
                          </Badge>
                        ))}
                      </div>
                    ) : hasWarning ? (
                      <div className="space-y-0.5">
                        {r.warnings.map((w) => (
                          <Badge
                            key={w}
                            variant="outline"
                            className="text-[9px] font-normal border-amber-400 text-amber-700 dark:text-amber-400 block w-fit"
                          >
                            {w}
                          </Badge>
                        ))}
                      </div>
                    ) : (
                      <Badge
                        variant="outline"
                        className="text-[9px] font-normal text-green-700 dark:text-green-400 border-green-400"
                      >
                        <Check className="h-2.5 w-2.5 mr-1" />
                        {t("common.pass", "Pass")}
                      </Badge>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
