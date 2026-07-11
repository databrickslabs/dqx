import { useState } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import {
  useGetProductResults,
  useGetProductResultsRuns,
  useGetDataProductSuspense,
  type DataProductOut,
} from "@/lib/api";
import selector from "@/lib/selector";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { GenieChatProvider } from "@/components/results/AskGenieButton";
import { RunPicker, type Run } from "@/components/results/RunPicker";
import { includeDraftsParam } from "@/components/results/RunModeSelect";
import {
  MultiTableResultsSection,
  type UseEntityResults,
} from "@/components/results/MultiTableResults";

// Ported from dqlake's `components/products/ProductResultsTab.tsx` (the spec —
// keep structure/section order/interactions aligned with it). The tab body is
// the shared multi-table composition in
// `components/results/MultiTableResults.tsx` (extracted verbatim from this
// file so the global Results page and the rule Results view reuse it instead
// of near-copying it); this file keeps only the product-specific pieces: the
// run history + latest-only picker, the Genie member scope, and the
// "Average score" label. The sanctioned dqlake deviations (hooks re-pointed
// to `/api/v1/dq-results/*`, t() strings, Genie stripped, NO idle polling —
// RESULTS_QUERY_OPTIONS + run-completion invalidation only, dqlake's
// resultsPolling / useDataProductRunsActivity / useDataProductMemberActivity
// 5s/15s refetch loops deliberately not ported) live in the shared
// composition. The RunInProgressBanner is omitted with them (same as the
// monitored-table tab): deriving run activity here would mean reintroducing
// an idle poll.
//
// RUN-PICKER SEMANTIC ADAPTATION (contract difference disclosed by P2.1):
// dqlake's product runs endpoint returns batch-keyed ProductRunsOut rows
// (product_run_id) and the picker pins every query "as of" that batch
// (`as_of_batch` carry-forward: trends show history up to the batch, scores
// carry each member's latest run at that instant). Our
// `/dq-results/product/{id}/runs` returns per-member-table run_id rollups
// (RunsOut — one row per underlying run, GROUP BY run_id), so a run_id here
// identifies ONE member table's run, NOT a product-level batch. Filtering a
// multi-table view by such a run_id silently collapses every breakdown
// (By dimension/severity/rule/table) to that single member table — a
// run-selection artifact that masquerades as a permission problem. The
// picker is therefore restricted to "Latest" (productRunPickerRuns) and the
// breakdown queries NEVER pass a run_id (breakdownParams); "Latest" keeps
// dqlake's behaviour exactly (score = last point of the product trend;
// breakdowns unscoped). The backend's run_id filter stays in place
// underneath — a product-level batch id landing in dq_metrics (which needs
// a change to how product runs are submitted; backlog, not now) would
// restore full dqlake picker parity.

// Re-exported from the shared composition so this file's public surface (and
// its bun tests) survive the extraction unchanged. `productBreakdownParams`
// keeps its historical name: it is the shared never-run-scoped breakdown
// params builder.
export {
  DULL_TABLE_COLORS,
  buildTableColorMap,
  computeOverallPoints,
  friendlyTableName,
  breakdownParams as productBreakdownParams,
} from "@/components/results/MultiTableResults";

/** Runs offered by the product tab's RunPicker: the LATEST entry only. Our
 *  run history rows are per-member-table run_id rollups, not product-level
 *  batches, so a non-latest selection cannot scope a multi-table view
 *  coherently (see the run-picker adaptation note in the module comment). */
export function productRunPickerRuns(runs: Run[]): Run[] {
  return runs.slice(0, 1);
}

/**
 * Results tab for a table space (data product). Reads the MV-backed
 * product-scoped DQ results endpoint and renders the shared multi-table
 * composition (average score, over-time trends, count charts, pass-rate
 * breakdowns, per-table invalid samples). The page shell renders immediately
 * and each widget loads independently.
 */
export function ProductResultsTab({ productId }: { productId: string }) {
  const { t } = useTranslation();
  return (
    <div className="space-y-6 pt-4 max-w-5xl">
      <QueryErrorResetBoundary>
        {({ reset }) => (
          <ErrorBoundary
            onReset={reset}
            fallbackRender={({ resetErrorBoundary }) => (
              <div className="rounded-md border border-dashed p-6 text-center space-y-2">
                <p className="text-sm">{t("resultsUi.loadFailed")}</p>
                <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
                  {t("resultsUi.tryAgain")}
                </Button>
              </div>
            )}
          >
            <ResultsBody productId={productId} />
          </ErrorBoundary>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}

function ResultsBody({ productId }: { productId: string }) {
  const { t } = useTranslation();

  // Run mode ("Published only" vs "Published + Draft") for THIS surface —
  // owned here because the runs list below is a surface-level dq-results
  // query outside the shared composition; the dropdown itself renders
  // inside the composition (next to the run picker).
  const [includeDrafts, setIncludeDrafts] = useState(false);

  // The product run history for the run picker (newest-first). Our rows are
  // already RunPicker-shaped (run_id/run_ts/pass_rate). NON-suspense so the
  // picker fills in once available without blocking the rest of the shell.
  const runsQuery = useGetProductResultsRuns(
    productId,
    { include_drafts: includeDraftsParam(includeDrafts) },
    {
      query: { ...RESULTS_QUERY_OPTIONS },
    },
  );
  const runs: Run[] = (runsQuery.data?.data?.rows ?? []).filter(
    (r): r is typeof r & { run_id: string } => typeof r.run_id === "string",
  );

  // The product (with members) supplies the member FQNs for the Genie
  // chat scope below (the Average line itself is server-computed from the
  // as-of view; no client-side member universe is needed).
  const { data: product } = useGetDataProductSuspense(productId, selector<DataProductOut>());
  const memberFqns = Array.from(
    new Set((product.members ?? []).map((m) => m.table_fqn)),
  );

  // The composition's data source: the product-scoped results endpoint. The
  // closure is re-created per render, but the composition calls it
  // unconditionally a fixed number of times, so the rules of hooks hold.
  const useEntityResults: UseEntityResults = (params, queryOptions) =>
    useGetProductResults(productId, params, { query: queryOptions });

  return (
    // Genie chat scope (P3.10, restoring dqlake's ProductResultsTab wrapper):
    // conversation keyed to this product; sent questions carry the product
    // name PLUS its member-table FQNs — no product-level object exists in the
    // aggregates-only Genie space, so the P3.9 space instructions route
    // product questions on that table list (our adaptation).
    <GenieChatProvider
      context={`data product ${productId}`}
      contextKind="product"
      contextSubject={product.name}
      contextTables={memberFqns}
    >
      <MultiTableResultsSection
        useEntityResults={useEntityResults}
        scoreLabel={() => t("resultsUi.averageScoreLabel")}
        includeDrafts={includeDrafts}
        onIncludeDraftsChange={setIncludeDrafts}
        // Fixed to "Latest": only the newest entry is offered, and selecting it
        // resolves to the latest path anyway, so onChange is a no-op (see the
        // run-picker adaptation note in the module comment).
        runPickerSlot={
          <RunPicker runs={productRunPickerRuns(runs)} value={null} onChange={() => {}} />
        }
      />
    </GenieChatProvider>
  );
}
