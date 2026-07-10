import { useTranslation } from "react-i18next";
import { ScoreBox } from "@/components/results/ScoreBox";
import { TableScoreList } from "@/components/results/TableScoreList";
import { useGetProductScoreSuspense } from "@/lib/api";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { sumMemberTestCounts } from "@/lib/results-display";

/**
 * Results tab for the table-space (data product) detail page: one ScoreBox
 * with the product's average score (unweighted mean of the member tables'
 * latest scores, computed server-side) over a per-table breakdown list.
 * Members without a scored run show a "No runs yet" placeholder instead of
 * a percentage and are excluded from the mean by the backend.
 *
 * The score query never refetches on its own (RESULTS_QUERY_OPTIONS) — it
 * is refreshed by `invalidateResultsAfterRunCompletion`, fired from the
 * product run-set poll (hooks/use-product-run-sets.ts) and Runs History's
 * RUNNING-run poll. All display logic lives in lib/results-display.ts
 * (unit-tested there); this file stays a thin JSX shell.
 */
export function ProductResultsTab({ productId }: { productId: string }) {
  const { t } = useTranslation();
  const { data } = useGetProductScoreSuspense(productId, {
    query: { select: (d) => d.data, ...RESULTS_QUERY_OPTIONS },
  });

  const members = data.member_table_scores ?? [];
  const { totalTests, failedTests } = sumMemberTestCounts(members);

  return (
    <div className="flex flex-col gap-4 max-w-5xl">
      <ScoreBox
        passRate={data.score ?? null}
        label={t("dataProducts.resultsAverageScoreLabel")}
        totalTests={totalTests}
        failedTests={failedTests}
      />
      <div className="space-y-2">
        <h3 className="text-sm font-medium">{t("dataProducts.resultsBreakdownTitle")}</h3>
        {members.length === 0 ? (
          <p className="text-sm text-muted-foreground">{t("dataProducts.resultsNoMembers")}</p>
        ) : (
          <TableScoreList tables={members} />
        )}
      </div>
    </div>
  );
}
