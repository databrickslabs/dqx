import { Suspense, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { AlertCircle, RotateCcw } from "lucide-react";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useGetGlobalResults, type EntityResultsOut } from "@/lib/api";
import { AskGenieButton } from "@/components/results/AskGenieButton";
import {
  BY_TABLE_PREVERIFY_LIMIT,
  preverifyRowEntitlements,
} from "@/lib/entitlement-preverify";
import {
  MultiTableResultsSection,
  type UseEntityResults,
} from "@/components/results/MultiTableResults";

export const Route = createFileRoute("/_sidebar/results")({
  component: GlobalResultsPage,
});

/**
 * Global (org-wide) Results page: the FULL multi-table results composition
 * (dqlake's product-tab layout, shared with the Table Space Results tab via
 * `components/results/MultiTableResults.tsx`) over EVERY table tracked in
 * dq_metrics that the viewer can access — average score, over-time trends,
 * count charts, dimension/severity/rule/table/column breakdowns with facet
 * drilldown, and per-table invalid samples (fetched through the OBO-gated
 * failed-rows endpoint on By-table selection). The backend filters the
 * table universe to the viewer's accessible catalogs, so tables the viewer
 * can't see are silently absent (never an error).
 *
 * NO RUN PICKER on this page: the product tab already had to restrict its
 * picker to "Latest" because per-table run_ids are not product-level batches
 * (see the run-picker adaptation note in ProductResultsTab) — across the
 * global all-tables universe a run_id is even less batch-like (it identifies
 * one run of ONE table among all tables in the org), so no run selection can
 * scope this view coherently and the picker is omitted entirely. The
 * breakdown queries never pass a run_id (shared breakdownParams); "latest"
 * is the only semantic, exactly like the product tab's pinned picker.
 *
 * The queries never refetch on their own (RESULTS_QUERY_OPTIONS inside the
 * composition) — they are refreshed by `invalidateResultsAfterRunCompletion`,
 * which invalidates every /api/v1/dq-results/* query by path prefix, fired
 * from the Runs History RUNNING-run poll and the product run-set poll.
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

// The composition's data source: the global (all accessible tables) results
// endpoint — same axes/filter params as the product endpoint, no path
// parameter. Module-scope so the hook wrapper is a stable identity.
const useGlobalEntityResults: UseEntityResults = (params, queryOptions) =>
  useGetGlobalResults(params, { query: queryOptions });

// Pre-verify row entitlements for the on-screen tables (P4.3): when the
// base by-table rows land, fire-and-forget verify-entitlements for the
// first screenful of FQNs (the row labels ARE the table FQNs) so the
// Genie failing-rows view is already open before anyone asks. Module-scope
// for a stable identity — the composition's effect keys on it.
function preverifyByTable(byTable: NonNullable<EntityResultsOut["by_table"]>): void {
  preverifyRowEntitlements(
    byTable.map((g) => g.label),
    BY_TABLE_PREVERIFY_LIMIT,
  );
}

function GlobalResultsContent() {
  const { t } = useTranslation();
  // Run mode ("Published only" vs "Published + Draft"), owned per surface.
  const [includeDrafts, setIncludeDrafts] = useState(false);
  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("globalResults.breadcrumb")} />

        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("globalResults.title")}</h1>
          <p className="text-sm text-muted-foreground mt-1">{t("globalResults.subtitle")}</p>
        </div>

        <div className="max-w-5xl">
          <MultiTableResultsSection
            useEntityResults={useGlobalEntityResults}
            // Org-wide label carrying the ACCESSIBLE table count (the base
            // by-table rows are already catalog-filtered to the viewer —
            // Phase 1's global-score semantics).
            scoreLabel={(count) => t("globalResults.orgWideScoreLabel", { count })}
            includeDrafts={includeDrafts}
            onIncludeDraftsChange={setIncludeDrafts}
            onBaseByTable={preverifyByTable}
            // No runPickerSlot: the picker is omitted (see the module
            // comment); the Average line is the server's as-of series over
            // the accessible tables.
          />
        </div>

        {/* Genie chat launcher (P3.10, restoring dqlake's ResultsOverview
            standalone button): unscoped conversation over all tables — no
            contextKind, so the default (table-worded) suggested questions
            show and no subject preamble is injected. */}
        <AskGenieButton context="all tables" />
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
