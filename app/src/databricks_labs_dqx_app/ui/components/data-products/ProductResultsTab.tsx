import { useState } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { FadeIn } from "@/components/anim/FadeIn";
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
import { RunInProgressBanner } from "@/components/results/RunInProgressBanner";
import { includeDraftsParam } from "@/components/results/RunModeSelect";
import { useProductRunSets } from "@/hooks/use-product-run-sets";
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
// composition. The RunInProgressBanner IS shown here, driven by the shared
// `useProductRunSets(productId).hasActive` signal (the same cached query the
// product header and Runs tab already consume — it polls ONLY while a run
// set is running, never idle). It is the banner form, not a true X-of-Y
// progress bar: the run-set summary carries no per-member status to count.
//
// RUN-PICKER (B2-18): the picker now offers product-level RUN BATCHES and
// pins every query AS-OF the chosen batch (dqlake parity). The backend rolls
// `/dq-results/product/{id}/runs` up per run set (COALESCE(run_set_id,
// run_id)) so each picker entry is ONE product "Run now" — its run_id is the
// batch's representative (latest member) run, and passing it as `as_of_batch`
// caps the trends/breakdowns to that batch's instant (the backend resolves
// the run_id to its batch and truncates history there; concurrent member runs
// are consolidated onto the batch instant, so no breakdown collapses to a
// single member as the old per-run_id filter did). The former Latest-only
// restriction is therefore lifted.

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

/** Runs offered by the product tab's RunPicker. The backend now rolls the
 *  product runs up per RUN BATCH (one entry per product "Run now"), so every
 *  entry is a coherent product-level batch that can scope the view AS-OF its
 *  instant — the whole newest-first list is offered (B2-18). Kept as a named
 *  pass-through so the picker's run universe stays a single documented seam. */
export function productRunPickerRuns(runs: Run[]): Run[] {
  return runs;
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
    // B2-9: results-page entrance animation (see BindingResultsTab) — the tab
    // body remounts on tab switch, so the Results tab fades in each time.
    <FadeIn className="space-y-6 pt-4 max-w-5xl">
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
    </FadeIn>
  );
}

function ResultsBody({ productId }: { productId: string }) {
  const { t } = useTranslation();

  // The product (with members) supplies the member FQNs for the Genie chat
  // scope below, and its version tells us whether it has ever been approved.
  const { data: product } = useGetDataProductSuspense(productId, selector<DataProductOut>());

  // Run mode ("Published only" vs "Published + Draft") for THIS surface —
  // owned here because the runs list below is a surface-level dq-results
  // query outside the shared composition; the dropdown itself renders
  // inside the composition (next to the run picker). A never-approved space
  // (version 0) only has draft runs, which the published-only default would
  // hide entirely, so seed the picker to "Published + Draft" in that case so
  // those runs are visible without the user switching (still toggleable).
  const [includeDrafts, setIncludeDrafts] = useState(product.version === 0);

  // The pinned run batch (a run_id from the batch-keyed picker), or null for
  // "Latest". Threaded into the composition as `asOfBatch` so every axis
  // truncates to that batch's instant (B2-18).
  const [selectedBatch, setSelectedBatch] = useState<string | null>(null);

  // Run-in-progress signal: the shared run-set activity query (deduped with
  // the product header and Runs tab — same params, one cached poll that only
  // ticks while a run set is running). Drives the banner below.
  const { hasActive } = useProductRunSets(productId);

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

  // The product's members supply the member FQNs for the Genie chat scope
  // below (the Average line itself is server-computed from the as-of view;
  // no client-side member universe is needed).
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
      <div className="space-y-6">
        <RunInProgressBanner show={hasActive}>
          {t("resultsUi.runInProgressBanner")}
        </RunInProgressBanner>
        <MultiTableResultsSection
          useEntityResults={useEntityResults}
          scoreLabel={() => t("resultsUi.averageScoreLabel")}
          includeDrafts={includeDrafts}
          onIncludeDraftsChange={setIncludeDrafts}
          // The picker offers product-level run batches; selecting one pins
          // the view AS-OF that batch (asOfBatch below). null = Latest.
          runPickerSlot={
            <RunPicker
              runs={productRunPickerRuns(runs)}
              value={selectedBatch}
              onChange={setSelectedBatch}
            />
          }
          asOfBatch={selectedBatch}
          // B2-8: the selected batch's representative run (its latest member
          // run) drives the review-status card between the score and the
          // trend; falls back to the newest batch when nothing is pinned.
          reviewStatusRunId={selectedBatch ?? runs[0]?.run_id}
        />
      </div>
    </GenieChatProvider>
  );
}
