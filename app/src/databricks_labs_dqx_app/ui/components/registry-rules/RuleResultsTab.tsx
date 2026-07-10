import { Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { AlertCircle, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ScoreBox } from "@/components/results/ScoreBox";
import { TableScoreList } from "@/components/results/TableScoreList";
import { useGetRuleScoreSuspense } from "@/lib/api";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { ruleResultsState, sumMemberTestCounts } from "@/lib/results-display";

/**
 * Results tab content for the registry-rule detail page: the rule's overall
 * score (row-weighted across every applied table the viewer can access,
 * computed server-side) in a ScoreBox, over a per-table breakdown list —
 * same layout as the Table Space Results tab (ProductResultsTab).
 *
 * Data-state semantics live in `ruleResultsState` (lib/results-display.ts,
 * unit-tested there): `applied_to_count` counts ALL current applications
 * (viewer-independent) while `per_table` holds only the viewer-accessible
 * ones, so "applied to N tables you can't see" renders an explanatory empty
 * state rather than the misleading "not applied". The "not-applied" state
 * normally never renders here — the tab trigger is disabled with a tooltip
 * (RegistryRuleFormDialog) — but a `?tab=results` deep link can still land
 * on it, so it shows the same explanation inline.
 *
 * The score query never refetches on its own (RESULTS_QUERY_OPTIONS); it is
 * refreshed by `invalidateResultsAfterRunCompletion`, fired from the Runs
 * History RUNNING-run poll and the product run-set poll.
 */
function RuleResultsContent({ ruleId }: { ruleId: string }) {
  const { t } = useTranslation();
  const { data } = useGetRuleScoreSuspense(ruleId, {
    query: { select: (d) => d.data, ...RESULTS_QUERY_OPTIONS },
  });

  const state = ruleResultsState(data);
  const appliedCount = data.applied_to_count ?? 0;

  if (state === "not-applied") {
    return <p className="text-sm text-muted-foreground">{t("rulesRegistry.resultsNotApplied")}</p>;
  }

  if (state === "no-access") {
    return (
      <p className="text-sm text-muted-foreground">
        {t("rulesRegistry.resultsNoAccessibleTables", { count: appliedCount })}
      </p>
    );
  }

  const perTable = data.per_table ?? [];
  const { totalTests, failedTests } = sumMemberTestCounts(perTable);

  return (
    <div className="flex flex-col gap-4 max-w-5xl">
      <ScoreBox
        passRate={data.overall_score ?? null}
        // Deliberately the viewer-ACCESSIBLE table count (per_table.length),
        // not applied_to_count: overall_score and the breakdown below only
        // cover the tables the viewer can see, so the label must count the
        // same set (applied_to_count may be higher — see ruleResultsState).
        label={t("rulesRegistry.resultsOverallScoreLabel", { count: perTable.length })}
        totalTests={totalTests}
        failedTests={failedTests}
      />
      <div className="space-y-2">
        <h3 className="text-sm font-medium">{t("rulesRegistry.resultsBreakdownTitle")}</h3>
        <TableScoreList tables={perTable} />
      </div>
    </div>
  );
}

function RuleResultsError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("common.loadFailed")}</p>
      <p className="text-muted-foreground/70 text-xs mb-3">{t("common.retryHint")}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

/**
 * Boundary wrapper so a score fetch failure retries tab-locally without
 * unmounting the surrounding rule form — same shape as the sibling tabs'
 * host page boundary (registry-rules.$ruleId.tsx).
 */
export function RuleResultsTab({ ruleId }: { ruleId: string }) {
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={RuleResultsError}>
          <Suspense fallback={<Skeleton className="h-48 w-full" />}>
            <RuleResultsContent ruleId={ruleId} />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}
