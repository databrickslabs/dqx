import { useTranslation } from "react-i18next";

/**
 * Pure presentational summary of a DQ results overview: an aggregate
 * pass rate plus a "failed of total" line. No data fetching — callers
 * pass the values in.
 */
export function ScoreSummary({
  passRate,
  failedChecks,
  totalChecks,
}: {
  passRate: number | null;
  failedChecks: number;
  totalChecks: number;
}) {
  const { t } = useTranslation();
  const passRateLabel =
    passRate == null ? "—" : `${(passRate * 100).toFixed(1)}%`;

  const failedLabel =
    totalChecks > 0
      ? t("resultsUi.failedOfChecks", { failed: failedChecks, total: totalChecks })
      : t("resultsUi.failedChecks", { failed: failedChecks });

  return (
    <div className="space-y-1">
      <div className="text-3xl font-semibold tabular-nums">{passRateLabel}</div>
      <div className="text-sm text-muted-foreground">{failedLabel}</div>
    </div>
  );
}
