import { Suspense } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { AlertCircle, RotateCcw } from "lucide-react";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ScoreBox } from "@/components/results/ScoreBox";
import { TableScoreList } from "@/components/results/TableScoreList";
import { useGetGlobalScoreSuspense } from "@/lib/api";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { globalResultsState, sumMemberTestCounts } from "@/lib/results-display";

export const Route = createFileRoute("/_sidebar/results")({
  component: GlobalResultsPage,
});

/**
 * Global (org-wide) Results page: the cross-table DQ score over every table
 * tracked in dq_metrics, in a ScoreBox over a per-table breakdown list —
 * same layout as the Table Space and rule Results tabs (ProductResultsTab /
 * RuleResultsTab). The backend filters the table list to the viewer's
 * accessible catalogs, so tables the viewer can't see are silently absent
 * (never an error) and the empty state covers both "nothing tracked yet"
 * and "nothing you can see" (see globalResultsState).
 *
 * The score query never refetches on its own (RESULTS_QUERY_OPTIONS) — it
 * is refreshed by `invalidateResultsAfterRunCompletion`, which invalidates
 * every /api/v1/dq-score/* query by path prefix, fired from the Runs
 * History RUNNING-run poll and the product run-set poll.
 */
function GlobalResultsPage() {
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={GlobalResultsError}>
          <Suspense fallback={<GlobalResultsSkeleton />}>
            <GlobalResultsContent />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}

function GlobalResultsContent() {
  const { t } = useTranslation();
  const { data } = useGetGlobalScoreSuspense({
    query: { select: (d) => d.data, ...RESULTS_QUERY_OPTIONS },
  });

  const tables = data.tables ?? [];
  const { totalTests, failedTests } = sumMemberTestCounts(tables);

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("globalResults.breadcrumb")} />

        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("globalResults.title")}</h1>
          <p className="text-sm text-muted-foreground mt-1">{t("globalResults.subtitle")}</p>
        </div>

        {globalResultsState(data) === "empty" ? (
          <p className="text-sm text-muted-foreground">{t("globalResults.noTables")}</p>
        ) : (
          <div className="flex flex-col gap-4 max-w-5xl">
            <ScoreBox
              score={data.overall_score ?? null}
              label={t("globalResults.orgWideScoreLabel", { count: tables.length })}
              totalTests={totalTests}
              failedTests={failedTests}
            />
            <div className="space-y-2">
              <h3 className="text-sm font-medium">{t("globalResults.breakdownTitle")}</h3>
              {/* Rows are deliberately NOT links to the monitored-table detail
                  page: TableScoreOut carries only the source table FQN (no
                  binding_id), so FQN->binding navigation would need an extra
                  list-all-bindings lookup on every visit — and a table can
                  appear in dq_metrics after its binding was deleted, so some
                  rows would have no destination at all. */}
              <TableScoreList tables={tables} />
            </div>
          </div>
        )}
      </div>
    </FadeIn>
  );
}

function GlobalResultsError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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

function GlobalResultsSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-4 w-64" />
      </div>
      <Skeleton className="h-40 w-full max-w-5xl" />
    </div>
  );
}
