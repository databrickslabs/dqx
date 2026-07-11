import { Suspense, useState } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { AlertCircle, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useGetRuleResults, useGetRuleScoreSuspense } from "@/lib/api";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { ruleResultsState } from "@/lib/results-display";
import { includeDraftsParam } from "@/components/results/RunModeSelect";
import {
  MultiTableResultsSection,
  type UseEntityResults,
} from "@/components/results/MultiTableResults";

/**
 * Results tab content for the registry-rule detail page: the full multi-table
 * results composition (dqlake's product-tab layout, shared with the Table
 * Space Results tab and the global Results page via
 * `components/results/MultiTableResults.tsx`) locked to THIS rule — the
 * `/api/v1/dq-results/rule/{rule_id}` endpoint scopes every trend/breakdown
 * row to the rule's checks by frozen registry_rule_id provenance, across the
 * applied tables the viewer can access (adaptation #2).
 *
 * Rule-locked deviations from the product/global consumers:
 * - The "By rule" breakdown is hidden (`hideRuleBreakdown`): the rule facet
 *   is locked server-side, so the box would always show this one rule.
 * - NO run picker: the rule's applied tables have per-table run_ids, not a
 *   rule-level batch — the same reason the product tab pinned its picker to
 *   "Latest" and the global page omitted it (see the run-picker adaptation
 *   note in ProductResultsTab); "latest" is the only coherent semantic, so
 *   no `runPickerSlot` is passed.
 * - The Average line is the server's as-of series over the rule's
 *   accessible applied tables, plotted as-is (no membership gate).
 *
 * Data-state gating (unchanged from Task 11) lives in `ruleResultsState`
 * (lib/results-display.ts, unit-tested there): `applied_to_count` counts ALL
 * current applications (viewer-independent) while `per_table` holds only the
 * viewer-accessible ones, so "applied to N tables you can't see" renders an
 * explanatory empty state rather than the misleading "not applied". The
 * "not-applied" state normally never renders here — the tab trigger is
 * disabled with a tooltip (RegistryRuleFormDialog, reading the same cheap P1
 * `useGetRuleScore`) — but a `?tab=results` deep link can still land on it,
 * so it shows the same explanation inline. Only the has-data body renders
 * the shared composition.
 *
 * The queries never refetch on their own (RESULTS_QUERY_OPTIONS); they are
 * refreshed by `invalidateResultsAfterRunCompletion` (run-completion polls)
 * and `invalidateResultsAfterRuleApplicationChange` (apply/unapply edits),
 * both in `lib/results-invalidation.ts`.
 */
function RuleResultsContent({ ruleId }: { ruleId: string }) {
  const { t } = useTranslation();
  // Run mode ("Published only" vs "Published + Draft"), owned per surface —
  // wired into the rule-score query here AND (via the composition props)
  // into every rule-results trend/breakdown/failed-rows query.
  const [includeDrafts, setIncludeDrafts] = useState(false);
  const { data } = useGetRuleScoreSuspense(
    ruleId,
    { include_drafts: includeDraftsParam(includeDrafts) },
    {
      query: { select: (d) => d.data, ...RESULTS_QUERY_OPTIONS },
    },
  );

  const state = ruleResultsState(data);
  const appliedCount = data.applied_to_count ?? 0;

  // The composition's data source: the rule-scoped results endpoint. The
  // closure is re-created per render, but the composition calls it
  // unconditionally a fixed number of times, so the rules of hooks hold
  // (same pattern as the product tab).
  const useEntityResults: UseEntityResults = (params, queryOptions) =>
    useGetRuleResults(ruleId, params, { query: queryOptions });

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

  return (
    <div className="max-w-5xl">
      <MultiTableResultsSection
        useEntityResults={useEntityResults}
        // Counts the viewer-ACCESSIBLE tables (the composition passes the
        // base by-table row count): the score and breakdowns only cover the
        // applied tables the viewer can see, so the label must count the
        // same set (applied_to_count may be higher — see ruleResultsState).
        scoreLabel={(count) => t("rulesRegistry.resultsOverallScoreLabel", { count })}
        hideRuleBreakdown
        includeDrafts={includeDrafts}
        onIncludeDraftsChange={setIncludeDrafts}
        // Rule results are published-only: drafts are per-authoring-surface,
        // not a coherent cross-table universe for a single rule.
        hideRunMode
      />
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
