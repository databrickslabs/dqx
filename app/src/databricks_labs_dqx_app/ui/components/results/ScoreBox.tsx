import { useTranslation } from "react-i18next";
import { Card, CardContent } from "@/components/ui/card";
import { formatScorePercent, scoreBandClass } from "@/lib/results-display";

interface ScoreBoxProps {
  /** Row-weighted DQ score in 0..1, or null when no scored run exists yet. */
  score: number | null;
  /** Caption above the score, e.g. "Table score" — already translated by the caller. */
  label: string;
  totalTests: number;
  failedTests: number;
}

/**
 * Compact score card: big percentage colored by severity band, with a
 * "N of M tests failed" subline. All display logic lives in
 * lib/results-display.ts (unit-tested there).
 */
export function ScoreBox({ score, label, totalTests, failedTests }: ScoreBoxProps) {
  const { t } = useTranslation();
  return (
    <Card>
      <CardContent className="flex flex-col items-center gap-1 py-6">
        <span className="text-sm text-muted-foreground">{label}</span>
        {score === null ? (
          <span className="text-lg text-muted-foreground">{t("results.noRunsYet")}</span>
        ) : (
          <>
            <span className={`text-4xl font-semibold tabular-nums ${scoreBandClass(score)}`}>
              {formatScorePercent(score)}
            </span>
            <span className="text-sm text-muted-foreground">
              {t("results.testsFailedOfTotal", { failed: failedTests, total: totalTests })}
            </span>
          </>
        )}
      </CardContent>
    </Card>
  );
}
