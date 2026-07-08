import { createFileRoute, Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Input } from "@/components/ui/input";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { HelpTooltip } from "@/components/HelpTooltip";
import {
  AlertCircle,
  AlertTriangle,
  ArrowLeft,
  BarChart3,
  CheckCircle2,
  ChevronDown,
  ClipboardList,
  Clock,
  Columns3,
  Database,
  Info,
  KeyRound,
  Loader2,
  Play,
  Plus,
  RefreshCw,
  RotateCcw,
  Send,
  Sparkles,
  UploadCloud,
  X,
  XCircle,
} from "lucide-react";
import {
  useGetMonitoredTableSuspense,
  useGetMonitoredTableProfile,
  getGetMonitoredTableProfileQueryKey,
  useSubmitProfileRun,
  getProfileRunStatus,
  useSubmitMonitoredTable,
  useApproveMonitoredTable,
  useRejectMonitoredTable,
  useSaveAppliedRules,
  useListRegistryRules,
  useGetTableColumns,
  useGetRules,
  useListMonitoredTableVersions,
  useRunMonitoredTable,
  useSuggestRulesForTable,
  usePreviewTableData,
  useQueryTableData,
  type AppliedRuleOut,
  type ColumnOut,
  type MonitoredTableOut,
  type MonitoredTableVersionOut,
  type RegistryRuleOut,
  type TableDataOut,
} from "@/lib/api";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { StatusBadge } from "@/components/RegistryRuleBadges";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { useLabelDefinitions } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { useJobPolling } from "@/hooks/use-job-polling";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import { formatDateShort } from "@/lib/format-utils";
import { cn } from "@/lib/utils";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_BUTTON_BG } from "@/lib/ai-style";
import { AddRulesDialog } from "@/components/apply-rules/AddRulesDialog";
import { AiSuggestionDialog, type SuggestRulesState } from "@/components/apply-rules/AiSuggestionDialog";
import { RuleConfigCard, computeStatus } from "@/components/apply-rules/RuleConfigCard";
import { RulesByColumn, type ColumnRef } from "@/components/apply-rules/RulesByColumn";
import {
  RESERVED_SEVERITY_KEY,
  buildDesiredApplications,
  desiredApplicationsKey,
  extractApiError,
  groupAppliedRulesByRuleId,
  mergeRuleRowGroup,
  nextLocalRowId,
  normalizeStagedRows,
} from "@/components/apply-rules/shared";
import { orderSeverityValuesForDisplay } from "@/components/RegistryRuleBadges";
import { ProfileColumnList } from "@/components/bindings/ProfileColumnList";
import { MonitoredTableSchedulingTab } from "@/components/monitored-tables/MonitoredTableSchedulingTab";

const DETAIL_TAB_KEYS = ["about", "view-data", "profile", "apply-rules", "results", "schedule"] as const;
type DetailTab = (typeof DETAIL_TAB_KEYS)[number];

/** Client-side deadline for the AI suggest-rules request (prefetch + manual
 *  Refresh) — bounds a hung/blocked request so the dialog can't spin forever
 *  with Refresh disabled; see `runSuggest` in `ApplyRulesTab`. */
const SUGGEST_RULES_TIMEOUT_MS = 30_000;

export const Route = createFileRoute("/_sidebar/monitored-tables/$bindingId")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={DetailError}>
          <Suspense fallback={<DetailSkeleton />}>
            <MonitoredTableDetailPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function DetailError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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
 * Subtle vertical rule grouping the binding-detail tab strip into three
 * clusters, matching dqlake's `BindingTabsShell` groups (`[About] | [Profile,
 * Apply Rules] | [Results]`, dqlake's Sharing tab omitted since DQX has no
 * equivalent): About on its own, then Profile + Apply Rules together, then
 * Results — one divider between About/Profile and another between Apply
 * Rules/Results.
 *
 * The TabsList background is `bg-muted`, which in the dark theme resolves to
 * the *exact same* oklch value as the `border` token — so `bg-border` was
 * fully invisible in dark mode (and barely visible in light mode, a ~2.5%
 * lightness gap). Use `muted-foreground` instead: it's calibrated to
 * contrast against `muted` in both themes, matching dqlake's separator
 * (a `text-muted-foreground/40` pipe glyph) far more closely than `border`.
 */
function TabGroupDivider() {
  return <div aria-hidden="true" className="mx-1 self-stretch w-px my-1.5 bg-muted-foreground/40" />;
}

function DetailSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-64" />
      </div>
      <Skeleton className="h-96 w-full" />
    </div>
  );
}

function MonitoredTableDetailPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { bindingId } = useParams({ from: "/_sidebar/monitored-tables/$bindingId" });
  const { tab } = useSearch({ from: "/_sidebar/monitored-tables/$bindingId" });
  // The URL is the source of truth for the active tab so browser back/forward
  // moves between tabs — mirrors the Rules Registry detail page and dqlake's
  // rule editor. Push (not replace) a history entry per switch.
  const activeTab: DetailTab =
    tab && (DETAIL_TAB_KEYS as readonly string[]).includes(tab) ? (tab as DetailTab) : "about";
  const handleTabChange = useCallback(
    (next: string) => {
      navigate({
        to: "/monitored-tables/$bindingId",
        params: { bindingId },
        search: (prev) => ({ ...prev, tab: next }),
      });
    },
    [navigate, bindingId],
  );

  const { data } = useGetMonitoredTableSuspense(bindingId);
  const detail = data.data;
  const table = detail.table;
  const appliedRules = useMemo(() => detail.applied_rules ?? [], [detail.applied_rules]);
  // Header titles on the bare table name (item 9) — the full FQN stays
  // available via the breadcrumb and the header tooltip.
  const tableName = useMemo(() => {
    const parts = table.table_fqn.split(".");
    return parts[parts.length - 1] || table.table_fqn;
  }, [table.table_fqn]);

  // Applying/submitting/approving/rejecting rules on this binding changes
  // its own detail state as well as server-derived rules/checks counts read
  // by the Monitored Tables list (`checksCount`/`rulesCount` columns) and
  // any Data Product whose member counts derive from this binding's
  // summary. Those live in separate queries that a plain detail refetch
  // wouldn't touch, so without this the counts stay stale until a manual
  // reload (A2). Shared with the overview page's row-level approve/reject —
  // see `lib/monitored-table-invalidation.ts`.
  const invalidateLifecycleQueries = useCallback(
    () => invalidateAfterMonitoredTableChange(queryClient, bindingId),
    [queryClient, bindingId],
  );

  // ---------------------------------------------------------------------
  // Staged editor (P16-F) — every add/mapping-edit/severity-override/pin/
  // removal on the Apply Rules tab mutates `stagedRows` ONLY (no network).
  // `baseline` is the last-known-persisted state (seeded from the server on
  // mount/binding-switch and re-seeded from the `saveAppliedRules` mutation
  // response on a successful Save/Publish — NOT from a background refetch
  // of `appliedRules`, which could otherwise race with in-flight edits).
  // `isDirty` diffs the two via a stable, order-independent key so the
  // Save-as-draft/Publish buttons and the unsaved-changes nav guard only
  // engage when there's something to persist.
  // ---------------------------------------------------------------------
  // About-tab schema row → Apply Rules "by column" deep link (item 1):
  // reuses the same jump+expand state handoff already wired between the
  // by-rule and by-column lenses (P19-F item 24). Cleared once ApplyRulesTab
  // consumes it so re-entering the tab later doesn't re-trigger the jump.
  const [pendingColumnJump, setPendingColumnJump] = useState<string | null>(null);
  const handleColumnDeepLink = useCallback(
    (columnName: string) => {
      setPendingColumnJump(columnName);
      handleTabChange("apply-rules");
    },
    [handleTabChange],
  );

  const [stagedRows, setStagedRows] = useState<AppliedRuleOut[]>(() => normalizeStagedRows(appliedRules));
  const [baseline, setBaseline] = useState<AppliedRuleOut[]>(() => normalizeStagedRows(appliedRules));
  const seededBindingIdRef = useRef(bindingId);
  useEffect(() => {
    if (seededBindingIdRef.current !== bindingId) {
      seededBindingIdRef.current = bindingId;
      setStagedRows(normalizeStagedRows(appliedRules));
      setBaseline(normalizeStagedRows(appliedRules));
    }
  }, [bindingId, appliedRules]);

  const isDirty = desiredApplicationsKey(stagedRows) !== desiredApplicationsKey(baseline);
  // Bypasses the nav guard right after a successful save/submit/approve so an
  // in-flight invalidate+refetch (which briefly leaves the page mid-settle)
  // can't fire a spurious "unsaved changes" prompt when the user JUST saved
  // (B10). Set on each lifecycle success; cleared once the editor is clean
  // again so a genuine later edit still engages the guard.
  const justSavedRef = useRef(false);
  useEffect(() => {
    if (!isDirty) justSavedRef.current = false;
  }, [isDirty]);
  const { blocker } = useUnsavedGuard({ hasUnsavedChanges: isDirty, bypassRef: justSavedRef });

  const saveMutation = useSaveAppliedRules();
  const submitMutation = useSubmitMonitoredTable();
  const approveMutation = useApproveMonitoredTable();
  const rejectMutation = useRejectMonitoredTable();
  const [rejectConfirmOpen, setRejectConfirmOpen] = useState(false);

  const persistStagedRows = useCallback(
    () => saveMutation.mutateAsync({ bindingId, data: { applications: buildDesiredApplications(stagedRows) } }),
    [saveMutation, bindingId, stagedRows],
  );

  // Persist staged applied-rule edits. Returns true on success. Shared by the
  // Save-as-draft button and the "Run draft" flow (item 15), which must SAVE
  // any pending edits before running so the draft run reflects them.
  const saveDraft = useCallback(async (): Promise<boolean> => {
    try {
      const resp = await persistStagedRows();
      const normalized = normalizeStagedRows(resp.data);
      setStagedRows(normalized);
      setBaseline(normalized);
      justSavedRef.current = true;
      invalidateLifecycleQueries();
      return true;
    } catch (err: unknown) {
      toast.error(extractApiError(err, t("monitoredTables.toastSaveFailed")), { duration: 6000 });
      return false;
    }
  }, [persistStagedRows, invalidateLifecycleQueries, t]);

  const handleSaveAsDraft = () => {
    void saveDraft().then((ok) => {
      if (ok) toast.success(t("monitoredTables.toastSavedDraft"));
    });
  };

  // Submit for review: persist any staged edits first (so the materializer
  // renders the latest applied rules), then move the binding's checks into
  // the pending_approval queue via the submit endpoint. Preserves P16-F's
  // staged-save-first sequencing and clears the dirty/nav-guard state.
  const handleSubmit = () => {
    const persistFirst = isDirty
      ? persistStagedRows().then((resp) => {
          const normalized = normalizeStagedRows(resp.data);
          setStagedRows(normalized);
          setBaseline(normalized);
        })
      : Promise.resolve();
    persistFirst
      .then(() => submitMutation.mutateAsync({ bindingId }))
      .then(() => {
        justSavedRef.current = true;
        toast.success(t("monitoredTables.toastSubmitted"));
        invalidateLifecycleQueries();
      })
      .catch((err: unknown) => {
        toast.error(extractApiError(err, t("monitoredTables.toastSubmitFailed")), { duration: 6000 });
      });
  };

  const handleApprove = () => {
    approveMutation.mutateAsync({ bindingId }).then(
      () => {
        toast.success(t("monitoredTables.toastApproved"));
        invalidateLifecycleQueries();
      },
      (err: unknown) => {
        toast.error(extractApiError(err, t("monitoredTables.toastApproveFailed")), { duration: 6000 });
      },
    );
  };

  const handleReject = () => {
    rejectMutation.mutateAsync({ bindingId }).then(
      () => {
        toast.success(t("monitoredTables.toastRejected"));
        invalidateLifecycleQueries();
      },
      (err: unknown) => {
        toast.error(extractApiError(err, t("monitoredTables.toastRejectFailed")), { duration: 6000 });
      },
    );
  };

  const lifecycleBusy =
    saveMutation.isPending ||
    submitMutation.isPending ||
    approveMutation.isPending ||
    rejectMutation.isPending;

  // Nothing to resubmit: Save-as-draft is already disabled (no staged
  // edits) and this version is already approved, so "Submit for review"
  // would just be a no-op re-approval request (item 11).
  const submitDisabledNoChanges = !isDirty && table.status === "approved";

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[{ label: t("monitoredTables.title"), to: "/monitored-tables" }]}
          page={table.table_fqn}
        />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              {/* Header shows only the table name — the full catalog.schema.table
                  FQN is still available via the breadcrumb above and this
                  tooltip, matching dqlake's binding header (which titles on
                  `binding.table_name` alone; item 9). */}
              <TooltipProvider delayDuration={200}>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <h1 className="text-2xl font-semibold tracking-tight leading-none font-mono cursor-default">
                      {tableName}
                    </h1>
                  </TooltipTrigger>
                  <TooltipContent side="bottom" className="font-mono">
                    {table.table_fqn}
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
              <StatusBadge status={table.status} />
              <VersionBadge table={table} />
            </div>
            {table.steward && (
              <p className="text-sm text-muted-foreground mt-1">{t("monitoredTables.colSteward")}: {table.steward}</p>
            )}
          </div>
          <div className="flex items-center gap-2">
            {table.status === "pending_approval" && perms.canApproveRules && (
              <>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={lifecycleBusy}
                  onClick={handleApprove}
                  className="gap-1.5 text-emerald-600 border-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950"
                >
                  {approveMutation.isPending ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <CheckCircle2 className="h-3.5 w-3.5" />
                  )}
                  {t("monitoredTables.approveAction")}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={lifecycleBusy}
                  onClick={() => setRejectConfirmOpen(true)}
                  className="gap-1.5 text-red-600 border-red-400 hover:bg-red-50 dark:hover:bg-red-950"
                >
                  {rejectMutation.isPending ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <XCircle className="h-3.5 w-3.5" />
                  )}
                  {t("monitoredTables.rejectAction")}
                </Button>
              </>
            )}
            {perms.canCreateRules && (
              <>
                <Button
                  variant="outline"
                  onClick={handleSaveAsDraft}
                  disabled={!isDirty || lifecycleBusy}
                  className="gap-2"
                >
                  {saveMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
                  {t("monitoredTables.saveAsDraftButton")}
                </Button>
                <TooltipProvider delayDuration={200}>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className={cn(submitDisabledNoChanges && "cursor-not-allowed")}>
                        <Button
                          onClick={handleSubmit}
                          disabled={lifecycleBusy || submitDisabledNoChanges}
                          className="gap-2"
                        >
                          {saveMutation.isPending || submitMutation.isPending ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <UploadCloud className="h-4 w-4" />
                          )}
                          {submitMutation.isPending ? t("monitoredTables.submitting") : t("monitoredTables.submitButton")}
                        </Button>
                      </span>
                    </TooltipTrigger>
                    {submitDisabledNoChanges && (
                      <TooltipContent side="bottom">{t("monitoredTables.submitDisabledNoChangesHint")}</TooltipContent>
                    )}
                  </Tooltip>
                </TooltipProvider>
              </>
            )}
            {/* Run now sits to the right of Submit for review — matches
                dqlake's binding header ordering (Save, Publish, then Run;
                item 12). */}
            {perms.canRunRules && (
              <RunTableAction
                bindingId={bindingId}
                table={table}
                isDirty={isDirty}
                onSaveDraft={saveDraft}
              />
            )}
          </div>
        </div>

        {table.status === "pending_approval" && (
          <div className="flex items-start gap-3 p-3 rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-700">
            <Clock className="h-4 w-4 text-amber-600 shrink-0 mt-0.5" />
            <div className="space-y-1">
              <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                {t("monitoredTables.pendingBannerTitle")}
              </p>
              <p className="text-sm text-amber-800/90 dark:text-amber-300/90">
                {t("monitoredTables.pendingBannerBody")}
              </p>
              {!perms.canApproveRules && (
                <p className="text-xs text-amber-700/80 dark:text-amber-300/70 italic">
                  {t("monitoredTables.awaitingApproval")}
                </p>
              )}
            </div>
          </div>
        )}

        <AlertDialog open={rejectConfirmOpen} onOpenChange={setRejectConfirmOpen}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>{t("monitoredTables.rejectConfirmTitle")}</AlertDialogTitle>
              <AlertDialogDescription>
                {t("monitoredTables.rejectConfirmDescription", { table: table.table_fqn })}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
              <AlertDialogAction
                className="bg-destructive text-white hover:bg-destructive/90"
                onClick={() => {
                  setRejectConfirmOpen(false);
                  handleReject();
                }}
              >
                {t("monitoredTables.rejectAction")}
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>

        <Tabs value={activeTab} onValueChange={handleTabChange}>
          <TabsList>
            <TabsTrigger value="about" className="gap-1.5">
              <Info className="h-3.5 w-3.5" />
              {t("monitoredTables.tabAbout")}
            </TabsTrigger>
            <TabsTrigger value="view-data" className="gap-1.5">
              <Database className="h-3.5 w-3.5" />
              {t("monitoredTables.tabViewData")}
            </TabsTrigger>
            <TabGroupDivider />
            <TabsTrigger value="profile" className="gap-1.5">
              <BarChart3 className="h-3.5 w-3.5" />
              {t("monitoredTables.tabProfile")}
            </TabsTrigger>
            <TabsTrigger value="apply-rules" className="gap-1.5">
              <Columns3 className="h-3.5 w-3.5" />
              {t("monitoredTables.tabApplyRules")}
            </TabsTrigger>
            <TabGroupDivider />
            <TabsTrigger value="results" className="gap-1.5">
              <ClipboardList className="h-3.5 w-3.5" />
              {t("monitoredTables.tabResults")}
            </TabsTrigger>
            <TabsTrigger value="schedule" className="gap-1.5">
              <Clock className="h-3.5 w-3.5" />
              {t("monitoredTables.tabSchedule")}
            </TabsTrigger>
          </TabsList>

          <TabsContent value="about">
            <AboutTab table={table} onColumnClick={handleColumnDeepLink} />
          </TabsContent>

          <TabsContent value="view-data">
            <ViewDataTab tableFqn={table.table_fqn} />
          </TabsContent>

          <TabsContent value="profile">
            <ProfileTab bindingId={bindingId} tableFqn={table.table_fqn} />
          </TabsContent>

          <TabsContent value="apply-rules">
            <ApplyRulesTab
              bindingId={bindingId}
              tableFqn={table.table_fqn}
              stagedRows={stagedRows}
              setStagedRows={setStagedRows}
              canEdit={perms.canCreateRules}
              initialJumpColumn={pendingColumnJump}
              onJumpColumnConsumed={() => setPendingColumnJump(null)}
            />
          </TabsContent>

          <TabsContent value="results">
            <ResultsTab tableFqn={table.table_fqn} status={table.status} />
          </TabsContent>

          <TabsContent value="schedule">
            <MonitoredTableSchedulingTab table={table} canEdit={perms.canCreateRules} />
          </TabsContent>
        </Tabs>
      </div>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("common.unsavedChanges")}</AlertDialogTitle>
            <AlertDialogDescription>{t("monitoredTables.unsavedChangesDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>{t("common.stayOnPage")}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={() => blocker.proceed?.()}
            >
              {t("monitoredTables.discardAndLeave")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}

// ---------------------------------------------------------------------------
// Header: version badge + Run split-action
// ---------------------------------------------------------------------------

/** `vN` when the table's latest freeze is currently approved; "Modified
 *  since vN" when a later draft/pending edit sits on top of an older
 *  approved snapshot; nothing at v0 (never approved). */
function VersionBadge({ table }: { table: MonitoredTableOut }) {
  const { t } = useTranslation();
  const version = table.version ?? 0;
  if (version === 0) return null;
  if (table.status === "approved") {
    return (
      <Badge variant="secondary" className="font-mono text-[10px]">
        {t("monitoredTables.versionBadge", { version })}
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
      {t("monitoredTables.modifiedSinceVersion", { version })}
    </Badge>
  );
}

/** Split-button Run action, RUNNER-gated (`usePermissions().canRunRules`,
 *  checked by the caller). Primary click runs the latest approved snapshot
 *  ("Run now (vN)"), disabled with a tooltip at v0. The attached dropdown
 *  offers "Run draft" at the TOP (item 15) followed by each approved version.
 *  "Run draft" is only enabled when the binding is a draft OR has pending
 *  applied-rule edits; in the latter case those edits are SAVED first (so the
 *  draft run reflects them), and a save failure is surfaced without running.
 *  Otherwise it is disabled with a tooltip, matching the app's split-button
 *  convention. */
function RunTableAction({
  bindingId,
  table,
  isDirty,
  onSaveDraft,
}: {
  bindingId: string;
  table: MonitoredTableOut;
  isDirty: boolean;
  onSaveDraft: () => Promise<boolean>;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const versionsQuery = useListMonitoredTableVersions(bindingId);
  const versions = versionsQuery.data?.data ?? [];
  const runMutation = useRunMonitoredTable();
  const hasApproved = (table.version ?? 0) > 0;
  // Run draft is available only when there is a draft to run: either the
  // binding itself is in draft, or there are unsaved applied-rule edits that
  // Save-as-draft would persist (item 15).
  const canRunDraft = table.status === "draft" || isDirty;
  // Spans the whole save-then-run sequence in `handleRunDraft`, not just
  // `runMutation.isPending`. `onSaveDraft` runs against the caller's own save
  // mutation (not visible here), so without this a fast double-click could
  // fire a second save-then-run while the first save is still in flight.
  const [runDraftBusy, setRunDraftBusy] = useState(false);

  const handleRun = (source: "approved" | "draft", version?: number) => {
    runMutation.mutate(
      { bindingId, data: { source, version } },
      {
        onSuccess: (resp) => {
          toast.success(t("monitoredTables.toastRunStarted"), {
            action: {
              label: t("monitoredTables.toastRunStartedViewAction"),
              onClick: () =>
                void navigate({ to: "/runs-history", search: { runSetId: resp.data.run_set_id } }),
            },
          });
        },
        onError: (err: unknown) => {
          toast.error(extractApiError(err, t("monitoredTables.toastRunFailed")), { duration: 6000 });
        },
      },
    );
  };

  // Save any pending edits first (so the draft run reflects them), then run.
  // A save failure is surfaced by `onSaveDraft` and we do NOT run.
  const handleRunDraft = async () => {
    setRunDraftBusy(true);
    try {
      if (isDirty) {
        const ok = await onSaveDraft();
        if (!ok) return;
      }
      handleRun("draft");
    } finally {
      setRunDraftBusy(false);
    }
  };

  return (
    <div className="inline-flex" role="group" aria-label={t("monitoredTables.runActionGroupAria")}>
      <TooltipProvider delayDuration={200}>
        <Tooltip>
          <TooltipTrigger asChild>
            <span className={cn(!hasApproved && "cursor-not-allowed")}>
              <Button
                onClick={() => handleRun("approved")}
                disabled={!hasApproved || runMutation.isPending}
                className="gap-2 rounded-r-none"
              >
                {runMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {hasApproved
                  ? t("monitoredTables.runNowButton", { version: table.version })
                  : t("monitoredTables.runNowButtonNoVersion")}
              </Button>
            </span>
          </TooltipTrigger>
          {!hasApproved && (
            <TooltipContent side="bottom">{t("monitoredTables.runNowDisabledHint")}</TooltipContent>
          )}
        </Tooltip>
      </TooltipProvider>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            className="rounded-l-none border-l border-primary-foreground/20 px-2"
            disabled={runMutation.isPending}
            aria-label={t("monitoredTables.runMenuAria")}
          >
            <ChevronDown className="h-4 w-4" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          {/* Run draft at the TOP (item 15), disabled+tooltip when unavailable. */}
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className={cn(!canRunDraft && "cursor-not-allowed")}>
                  <DropdownMenuItem
                    disabled={!canRunDraft || runMutation.isPending || runDraftBusy}
                    onSelect={(e) => {
                      e.preventDefault();
                      void handleRunDraft();
                    }}
                  >
                    {t("monitoredTables.runDraftAction")}
                  </DropdownMenuItem>
                </span>
              </TooltipTrigger>
              {!canRunDraft && (
                <TooltipContent side="left">{t("monitoredTables.runDraftDisabledHint")}</TooltipContent>
              )}
            </Tooltip>
          </TooltipProvider>
          {versions.length > 0 && <DropdownMenuSeparator />}
          {versions.map((v: MonitoredTableVersionOut) => (
            <DropdownMenuItem key={v.version} onSelect={() => handleRun("approved", v.version)}>
              {t("monitoredTables.runVersionOption", { version: v.version })}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

// ---------------------------------------------------------------------------
// About tab
// ---------------------------------------------------------------------------

function AboutTab({
  table,
  onColumnClick,
}: {
  table: MonitoredTableOut;
  /** Deep-links to the Apply Rules "by column" lens, expanded to that
   *  column (item 1) — reuses the jump+expand handoff already wired
   *  between the by-rule/by-column lenses in ApplyRulesTab. */
  onColumnClick: (columnName: string) => void;
}) {
  const { t } = useTranslation();
  const [filter, setFilter] = useState("");
  const parts = table.table_fqn.split(".");
  const [catalog, schema, tableName] = parts;

  const columnsQuery = useGetTableColumns(catalog ?? "", schema ?? "", tableName ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  const filteredColumns = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return columns;
    return columns.filter(
      (c) => c.name.toLowerCase().includes(q) || (c.comment ?? "").toLowerCase().includes(q),
    );
  }, [columns, filter]);

  return (
    <div className="space-y-8 pt-4">
      <section className="space-y-3">
        <h2 className="text-sm font-semibold">{t("monitoredTables.aboutSectionTitle")}</h2>
        <dl className="grid grid-cols-[160px_1fr] gap-x-4 gap-y-2 text-xs">
          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutCatalog")}</dt>
          <dd className="font-mono">{catalog ?? t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutSchema")}</dt>
          <dd className="font-mono">{schema ?? t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutTableName")}</dt>
          <dd className="font-mono">{tableName ?? t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutStatus")}</dt>
          <dd>
            <StatusBadge status={table.status} />
          </dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutSteward")}</dt>
          <dd>{table.steward || <span className="text-muted-foreground italic">{t("monitoredTables.aboutStewardNone")}</span>}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutLastProfiled")}</dt>
          <dd>{table.last_profiled_at ? formatDateShort(table.last_profiled_at) : t("monitoredTables.neverProfiled")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutCreatedBy")}</dt>
          <dd>{table.created_by || t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutCreatedAt")}</dt>
          <dd>{table.created_at ? formatDateShort(table.created_at) : t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutUpdatedBy")}</dt>
          <dd>{table.updated_by || t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutUpdatedAt")}</dt>
          <dd>{table.updated_at ? formatDateShort(table.updated_at) : t("monitoredTables.aboutUnknown")}</dd>
        </dl>
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold">
          {t("monitoredTables.aboutSchemaSectionTitle", { count: columns.length })}
        </h2>
        {columnsQuery.isError ? (
          <div className="flex flex-col items-center justify-center py-10 text-center border border-dashed rounded-lg">
            <AlertCircle className="h-8 w-8 text-destructive/30 mb-2" />
            <p className="text-sm font-medium text-muted-foreground">{t("monitoredTables.aboutLoadFailedTitle")}</p>
            <p className="text-xs text-muted-foreground/70 mt-1 max-w-sm">{t("monitoredTables.aboutLoadFailedHint")}</p>
          </div>
        ) : (
          <div className="rounded-md border overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/30">
                <tr>
                  <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2">
                    {t("monitoredTables.aboutColColumn")}
                  </th>
                  <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-24">
                    {t("monitoredTables.aboutColType")}
                  </th>
                  <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-24">
                    {t("monitoredTables.aboutColNullable")}
                  </th>
                  <th className="px-3 py-1.5">
                    <div className="flex items-center justify-between gap-3">
                      <span className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide">
                        {t("monitoredTables.aboutColDescription")}
                      </span>
                      <Input
                        placeholder={t("monitoredTables.aboutFilterColumnsPlaceholder")}
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                        className="max-w-xs h-7 text-xs font-normal normal-case tracking-normal"
                      />
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredColumns.map((c) => (
                  <tr key={c.name} className="border-t">
                    <td className="px-3 py-2 align-top">
                      <TooltipProvider delayDuration={300}>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button
                              type="button"
                              onClick={() => onColumnClick(c.name)}
                              className="font-mono text-xs text-left hover:underline hover:text-primary focus-visible:underline focus-visible:outline-none"
                            >
                              {c.name}
                            </button>
                          </TooltipTrigger>
                          <TooltipContent>{t("monitoredTables.aboutColumnJumpTooltip")}</TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    </td>
                    <td className="px-3 py-2 align-top">
                      <Badge variant="outline" className="text-[10px] font-mono">
                        {c.type_name}
                      </Badge>
                    </td>
                    <td className="px-3 py-2 align-top text-xs text-muted-foreground">
                      {c.nullable === false ? "—" : "✓"}
                    </td>
                    <td className="px-3 py-2 align-top">
                      {c.comment ? (
                        <span className="text-xs">{c.comment}</span>
                      ) : (
                        <span className="text-muted-foreground italic text-xs">{t("monitoredTables.aboutNoDescription")}</span>
                      )}
                    </td>
                  </tr>
                ))}
                {filteredColumns.length === 0 && (
                  <tr>
                    <td colSpan={4} className="text-center text-muted-foreground py-6 text-sm">
                      {t("monitoredTables.aboutNoMatchingColumns")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Profile tab
// ---------------------------------------------------------------------------

const LLM_PK_SUMMARY_KEY = "llm_primary_key_detection";

interface LlmPrimaryKeyInfo {
  detected_columns?: string[];
  confidence?: string;
}

function ProfileTab({ bindingId, tableFqn }: { bindingId: string; tableFqn: string }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const profileQuery = useGetMonitoredTableProfile(bindingId);
  const profile = profileQuery.data?.data;

  const parts = tableFqn.split(".");
  const [catalog, schema, tableName] = parts;
  const columnsQuery = useGetTableColumns(catalog ?? "", schema ?? "", tableName ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columnTypes = useMemo(() => {
    const map: Record<string, string> = {};
    for (const c of columnsQuery.data?.data ?? []) map[c.name] = c.type_name;
    return map;
  }, [columnsQuery.data]);

  const [runId, setRunId] = useState<string | null>(null);
  const [jobRunId, setJobRunId] = useState<number | null>(null);

  const submitMutation = useSubmitProfileRun();

  const fetchStatus = useCallback(async () => {
    if (!runId) throw new Error("No active profile run");
    const resp = await getProfileRunStatus(runId);
    return resp.data;
  }, [runId]);

  useJobPolling({
    fetchStatus,
    enabled: jobRunId !== null && runId !== null,
    interval: 3000,
    onComplete: async (status) => {
      if (status.result_state === "SUCCESS") {
        toast.success(t("monitoredTables.profileToastSucceeded"));
        await queryClient.invalidateQueries({ queryKey: getGetMonitoredTableProfileQueryKey(bindingId) });
      } else {
        toast.error(t("monitoredTables.profileToastFailed"));
      }
      setJobRunId(null);
      setRunId(null);
    },
    onError: () => {
      toast.error(t("monitoredTables.profileToastFailed"));
      setJobRunId(null);
      setRunId(null);
    },
  });

  const running = submitMutation.isPending || jobRunId !== null;

  const handleRunProfile = useCallback(() => {
    submitMutation.mutate(
      { data: { table_fqn: tableFqn } },
      {
        onSuccess: (resp) => {
          setRunId(resp.data.run_id);
          setJobRunId(resp.data.job_run_id);
          toast.success(t("monitoredTables.profileToastStarted"));
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.profileToastFailed")), { duration: 6000 });
        },
      },
    );
  }, [submitMutation, tableFqn, t]);

  const summary = (profile?.summary ?? {}) as Record<string, unknown>;
  const columnStats = useMemo(() => {
    const entries = Object.entries(summary).filter(
      (e): e is [string, Record<string, unknown>] =>
        e[0] !== LLM_PK_SUMMARY_KEY && typeof e[1] === "object" && e[1] !== null && !Array.isArray(e[1]),
    );
    return Object.fromEntries(entries);
  }, [summary]);

  if (profileQuery.isLoading) {
    return (
      <div className="space-y-2 pt-4">
        <Skeleton className="h-16 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  const hasProfile = !profileQuery.error && profile != null;
  const pkInfo = summary[LLM_PK_SUMMARY_KEY] as LlmPrimaryKeyInfo | undefined;
  const pkColumns = pkInfo?.detected_columns ?? [];
  const hasColumns = Object.keys(columnStats).length > 0;

  return (
    <div className="space-y-4 pt-4">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="text-xs text-muted-foreground space-y-1">
          <div>
            <span className="uppercase tracking-wide">{t("monitoredTables.profileHeaderLastProfile")}</span>{" "}
            <span className="font-mono">
              {profile?.profiled_at ? formatDateShort(profile.profiled_at) : "—"}
            </span>
          </div>
          <div>
            <span className="uppercase tracking-wide">{t("monitoredTables.profileHeaderRows")}</span>{" "}
            <span className="font-mono">
              {profile?.rows_profiled != null ? profile.rows_profiled.toLocaleString() : "—"}
            </span>
          </div>
          {pkColumns.length > 0 && (
            <div className="flex items-center gap-1.5 pt-0.5">
              <KeyRound className="h-3 w-3 text-emerald-500" />
              <Badge variant="secondary" className="font-mono text-[10px]">
                {t("monitoredTables.profilePrimaryKeyBadge", { columns: pkColumns.join(", ") })}
              </Badge>
              {pkInfo?.confidence && (
                <span className="text-[10px] text-muted-foreground">({pkInfo.confidence})</span>
              )}
            </div>
          )}
        </div>
        <Button variant="outline" size="sm" onClick={handleRunProfile} disabled={running}>
          <RefreshCw className={cn("h-3.5 w-3.5 mr-1.5", running && "animate-spin")} />
          {running ? t("monitoredTables.profileRunningButton") : t("monitoredTables.profileRefreshButton")}
        </Button>
      </div>

      {!hasProfile ? (
        <div className="flex flex-col items-center justify-center py-16 text-center border border-dashed rounded-lg">
          {running ? (
            <Loader2 className="h-10 w-10 text-muted-foreground/40 mb-3 animate-spin" />
          ) : (
            <BarChart3 className="h-10 w-10 text-muted-foreground/30 mb-3" />
          )}
          <p className="text-sm font-medium text-muted-foreground mb-1">
            {running ? t("monitoredTables.profileRunningButton") : t("monitoredTables.profileEmptyTitle")}
          </p>
          <p className="text-muted-foreground/70 text-xs mt-1 max-w-md mb-4">
            {t("monitoredTables.profileEmptyHint")}
          </p>
          {!running && (
            <Button size="sm" className="gap-2" onClick={handleRunProfile}>
              <RefreshCw className="h-3.5 w-3.5" />
              {t("monitoredTables.profileRunButton")}
            </Button>
          )}
        </div>
      ) : hasColumns ? (
        <ProfileColumnList columnStats={columnStats} columnTypes={columnTypes} rowCount={profile?.rows_profiled} />
      ) : (
        <div className="py-8 text-center text-xs text-muted-foreground border border-dashed rounded-md">
          {t("monitoredTables.profileNoColumnsMatch")}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Apply Rules tab
// ---------------------------------------------------------------------------

function ApplyRulesTab({
  bindingId,
  tableFqn,
  stagedRows,
  setStagedRows,
  canEdit,
  initialJumpColumn,
  onJumpColumnConsumed,
}: {
  bindingId: string;
  tableFqn: string;
  /** The tab's local staged editor state (P16-F) — the caller
   *  (`MonitoredTableDetailPage`) owns it so it survives switching away from
   *  this tab (Radix unmounts inactive `TabsContent`) and so the header's
   *  Save-as-draft/Publish buttons and the unsaved-changes nav guard can see
   *  it too. Every control in this tab mutates it via `setStagedRows` only —
   *  nothing here writes to the network. */
  stagedRows: AppliedRuleOut[];
  setStagedRows: (updater: (prev: AppliedRuleOut[]) => AppliedRuleOut[]) => void;
  canEdit: boolean;
  /** Column to land on when this tab is entered via the About-tab schema
   *  row's "deep link" (item 1 / P19-F chip-jump handoff, reused across
   *  tabs) — opens the by-column lens straight to that column's card. Radix
   *  unmounts inactive `TabsContent`, so this component remounts fresh on
   *  every tab entry and this only needs to seed the initial lens/open
   *  column, not react to later changes. */
  initialJumpColumn?: string | null;
  onJumpColumnConsumed?: () => void;
}) {
  const { t } = useTranslation();
  const [lens, setLens] = useState<"by-rule" | "by-column">(initialJumpColumn ? "by-column" : "by-rule");
  const [addOpen, setAddOpen] = useState(false);
  const [addColumnContext, setAddColumnContext] = useState<ColumnRef | null>(null);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [removeTarget, setRemoveTarget] = useState<AppliedRuleOut | null>(null);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<"all" | "needs-attention">("all");
  // Set by the by-column lens's "jump to rule" action, or right after
  // AddRulesDialog stages new rule(s), so the target card(s) auto-expand in
  // the by-rule lens instead of just scrolling into view.
  const [expandRuleIds, setExpandRuleIds] = useState<string[]>([]);

  const parts = tableFqn.split(".");
  const columnsQuery = useGetTableColumns(parts[0] ?? "", parts[1] ?? "", parts[2] ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];
  // Lifted from RulesByColumn (mirrors dqlake's AppliedRulesList) so the
  // column card can auto-expand right after a rule is added to it via the
  // "+ Add rule" CTA on a column card.
  const [openColumnName, setOpenColumnName] = useState<string | null>(initialJumpColumn ?? null);

  // Scroll the target column's card into view once, on entry via the About
  // tab's deep link, then tell the parent to clear the pending jump so
  // switching tabs away and back doesn't re-trigger it.
  useEffect(() => {
    if (!initialJumpColumn) return;
    const timeout = setTimeout(() => {
      document
        .getElementById(`column-card-${initialJumpColumn}`)
        ?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 50);
    onJumpColumnConsumed?.();
    return () => clearTimeout(timeout);
  }, [initialJumpColumn, onJumpColumnConsumed]);

  const aiAvailability = useAiAvailability();
  const { available: aiAvailable, reportUnavailable } = aiAvailability;

  // AI rule suggestions are PREFETCHED the moment this tab is entered (dqlake
  // behaviour). Radix unmounts inactive `TabsContent`, so this component only
  // mounts on tab entry — mount == enter. The result is cached in
  // `suggestState` so opening the Suggest dialog is instant instead of waiting
  // on the round-trip; a manual "Refresh" in the dialog re-runs it.
  const suggestMutation = useSuggestRulesForTable();
  const { mutate: suggestRules, isPending: suggestPending } = suggestMutation;
  const [suggestState, setSuggestState] = useState<SuggestRulesState | null>(null);
  const suggestPrefetchedRef = useRef(false);
  // Guards against a stale response/timeout clobbering a newer request's
  // result — bumped on every runSuggest() call (initial prefetch AND manual
  // Refresh); callbacks only apply if they're still the latest.
  const suggestRequestIdRef = useRef(0);
  const suggestTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearSuggestTimeout = useCallback(() => {
    if (suggestTimeoutRef.current !== null) {
      clearTimeout(suggestTimeoutRef.current);
      suggestTimeoutRef.current = null;
    }
  }, []);

  const runSuggest = useCallback(() => {
    const requestId = ++suggestRequestIdRef.current;
    setSuggestState(null);
    clearSuggestTimeout();
    // The backend call has no server-side deadline visible to the client,
    // and a blocked/hung/never-responding request would otherwise leave the
    // dialog spinning forever with Refresh disabled (bug: infinite "Finding
    // good matches..." spinner). Bound it client-side and fall back to the
    // same honest-reason error state a real 4xx/5xx would produce.
    suggestTimeoutRef.current = setTimeout(() => {
      if (suggestRequestIdRef.current !== requestId) return;
      setSuggestState({
        available: false,
        reason: t("monitoredTables.suggestRulesTimeoutReason"),
        suggestions: [],
      });
    }, SUGGEST_RULES_TIMEOUT_MS);

    suggestRules(
      { bindingId },
      {
        onSuccess: (resp) => {
          if (suggestRequestIdRef.current !== requestId) return;
          clearSuggestTimeout();
          setSuggestState({
            available: resp.data.available,
            reason: resp.data.reason,
            suggestions: resp.data.suggestions ?? [],
          });
        },
        onError: (err) => {
          if (suggestRequestIdRef.current !== requestId) return;
          clearSuggestTimeout();
          const reason = aiUnavailableReason(err);
          if (reason) reportUnavailable(reason);
          setSuggestState({
            available: false,
            reason: reason ?? t("monitoredTables.suggestRulesFetchFailed"),
            suggestions: [],
          });
        },
      },
    );
  }, [bindingId, suggestRules, reportUnavailable, clearSuggestTimeout, t]);

  useEffect(() => {
    if (suggestPrefetchedRef.current || !aiAvailable) return;
    suggestPrefetchedRef.current = true;
    runSuggest();
  }, [aiAvailable, runSuggest]);

  // Unmount safety — don't let a late timeout fire setState on a gone tab.
  useEffect(() => clearSuggestTimeout, [clearSuggestTimeout]);

  // `loading` never gets stuck true: the timeout above guarantees
  // `suggestState` transitions out of null within SUGGEST_RULES_TIMEOUT_MS
  // even if the request itself never settles, so Refresh re-enables.
  const suggestLoading = suggestPending && suggestState === null;

  // DQX materializes one staged ROW per mapping group — a rule applied to
  // several columns produces several rows sharing the same rule_id. Group
  // them into one card per rule_id (dqlake's "N mapping groups under one
  // rule" model) for the by-rule lens.
  const ruleGroups = useMemo(() => groupAppliedRulesByRuleId(stagedRows), [stagedRows]);
  const mergedRules = useMemo(() => ruleGroups.map(mergeRuleRowGroup), [ruleGroups]);

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);
  const severityValues = useMemo(
    () => orderSeverityValuesForDisplay(labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? []),
    [labelDefinitions],
  );

  const {
    data: registryData,
    isLoading: registryLoading,
    isError: registryError,
    refetch: refetchRegistry,
  } = useListRegistryRules({ status: "approved" });
  const publishedRules = useMemo(() => registryData?.data ?? [], [registryData]);
  const ruleById = useMemo(() => {
    const m = new Map<string, RegistryRuleOut>();
    for (const r of publishedRules) m.set(r.rule_id, r);
    return m;
  }, [publishedRules]);

  // Completeness status per rule group — drives the "needs attention"
  // filter and the by-rule/by-column incomplete-mapping indicators.
  const statuses = useMemo(
    () => mergedRules.map((rule) => computeStatus(rule, ruleById.get(rule.rule_id)?.definition.slots ?? [])),
    [mergedRules, ruleById],
  );
  const incompleteCount = statuses.filter((s) => s.kind === "incomplete").length;

  // Total checks: aggregate rules (0 slots) count as 1 check each;
  // column-mapped rules count as the number of non-empty mapping groups
  // across every row sharing that rule_id.
  const totalChecks = useMemo(() => {
    return mergedRules.reduce((sum, rule) => {
      const slots = ruleById.get(rule.rule_id)?.definition.slots ?? [];
      if (slots.length === 0) return sum + 1;
      const groups = rule.column_mapping ?? [];
      return sum + groups.filter((entry) => slots.some((s) => Boolean(entry[s.name]))).length;
    }, 0);
  }, [mergedRules, ruleById]);

  const visibleMergedRules = useMemo(() => {
    let filtered = mergedRules;
    if (filter === "needs-attention") {
      filtered = mergedRules.filter((_, i) => statuses[i].kind === "incomplete");
    }
    const q = search.trim().toLowerCase();
    if (q) {
      filtered = filtered.filter((rule) => {
        if ((rule.rule_name ?? "").toLowerCase().includes(q)) return true;
        if (rule.rule_id.toLowerCase().includes(q)) return true;
        for (const group of rule.column_mapping ?? []) {
          for (const col of Object.values(group)) {
            if (col && col.toLowerCase().includes(q)) return true;
          }
        }
        return false;
      });
    }
    return filtered;
  }, [mergedRules, statuses, filter, search]);

  const openAddDialog = (column?: ColumnRef) => {
    setAddColumnContext(column ?? null);
    setAddOpen(true);
  };

  // Every add path (AddRulesDialog, AiSuggestionDialog) hands up a batch of
  // brand-new locally-staged rows — append them and jump straight to their
  // mapping UI in the by-rule lens, mirroring the old "auto-expand after
  // apply" behaviour but with zero network round-trips.
  const stageNewRows = (rows: AppliedRuleOut[]) => {
    setStagedRows((prev) => [...prev, ...rows]);
    setFilter("all");
    setSearch("");
    setLens("by-rule");
    setExpandRuleIds(rows.map((r) => r.rule_id));
  };

  const confirmRemove = () => {
    if (!removeTarget) return;
    const ruleId = removeTarget.rule_id;
    setRemoveTarget(null);
    setStagedRows((prev) => prev.filter((r) => r.rule_id !== ruleId));
    toast.success(t("monitoredTables.toastRemoved"));
  };

  // Removes the mapping group at `groupIdx` (its owning staged row) from a
  // rule's combined mapping — each row owns exactly one mapping group by
  // convention (see `groupAppliedRulesByRuleId`), so this deletes
  // rowsForRule[groupIdx]. Local-only: no network call.
  const handleRemoveMappingGroup = (ruleId: string, groupIdx: number) => {
    setStagedRows((prev) => {
      const rowsForRule = prev.filter((r) => r.rule_id === ruleId);
      const target = rowsForRule[groupIdx];
      if (!target) return prev;
      return prev.filter((r) => r !== target);
    });
  };

  // Reassigns one slot's column within one mapping group (an editable
  // MappingChips chip) — local-only mutation of that group's owning row.
  const handleChangeMapping = (ruleId: string, groupIdx: number, slotName: string, colName: string) => {
    setStagedRows((prev) => {
      const rowsForRule = prev.filter((r) => r.rule_id === ruleId);
      const target = rowsForRule[groupIdx];
      if (!target) return prev;
      const nextGroup = { ...(target.column_mapping?.[0] ?? {}), [slotName]: colName };
      return prev.map((r) => (r === target ? { ...r, column_mapping: [nextGroup] } : r));
    });
  };

  // "+ Apply to another column" (or completing a freshly-staged rule's
  // still-empty first group): fills the sole empty-mapping row in place, or
  // appends a brand-new staged row (cloning the rule's display metadata and
  // current pin/severity so the "+ Apply to another column" flow doesn't
  // reset them) — mirrors the old apply_rule-per-group model, just local.
  const handleAddMapping = (ruleId: string, group: Record<string, string>) => {
    setStagedRows((prev) => {
      const rowsForRule = prev.filter((r) => r.rule_id === ruleId);
      const emptyRow =
        rowsForRule.length === 1 && (rowsForRule[0].column_mapping ?? []).length === 0 ? rowsForRule[0] : null;
      if (emptyRow) {
        return prev.map((r) => (r === emptyRow ? { ...r, column_mapping: [group] } : r));
      }
      const template = rowsForRule[0];
      if (!template) return prev;
      const newRow: AppliedRuleOut = {
        ...template,
        id: nextLocalRowId(),
        column_mapping: [group],
        mapping_hash: null,
        created_at: null,
      };
      return [...prev, newRow];
    });
  };

  const handlePinChange = (rule: AppliedRuleOut, value: string) => {
    const registryRule = ruleById.get(rule.rule_id);
    const pinned_version = value === "latest" ? null : registryRule?.version ?? rule.pinned_version ?? null;
    setStagedRows((prev) => prev.map((r) => (r.rule_id === rule.rule_id ? { ...r, pinned_version } : r)));
  };

  const handleSeverityChange = (rule: AppliedRuleOut, value: string) => {
    const severity_override = value === "none" ? null : value;
    setStagedRows((prev) => prev.map((r) => (r.rule_id === rule.rule_id ? { ...r, severity_override } : r)));
  };

  return (
    <div className="space-y-4 pt-4">
      <div className="flex items-center gap-3 flex-wrap">
        {stagedRows.length > 0 && (
          <div className="flex items-center gap-1">
            <span className="text-sm font-semibold">
              {t("monitoredTables.checksCount", { count: totalChecks })}{" "}
              {t("monitoredTables.viaRulesCount", { count: mergedRules.length })}
            </span>
            {/* Explicit "?" trigger (mirrors dqlake's ScoreBox help icon and
                the app's own HelpTooltip pattern) instead of relying on the
                text itself being hoverable — item 26. */}
            <HelpTooltip text={t("monitoredTables.checksViaRulesTooltip")} />
          </div>
        )}

        {incompleteCount > 0 && (
          <div className="flex gap-1">
            <button
              type="button"
              onClick={() => setFilter("all")}
              className={`h-8 px-3 rounded text-xs border transition-colors ${
                filter === "all"
                  ? "bg-primary text-primary-foreground border-primary"
                  : "border-input bg-background text-foreground hover:bg-accent"
              }`}
            >
              {t("monitoredTables.filterAll")}
            </button>
            <button
              type="button"
              onClick={() => setFilter("needs-attention")}
              className={`h-8 px-3 rounded text-xs border transition-colors ${
                filter === "needs-attention"
                  ? "bg-yellow-500/20 text-yellow-700 dark:text-yellow-300 border-yellow-500/50"
                  : "border-yellow-500/30 bg-background text-yellow-600 dark:text-yellow-400 hover:bg-yellow-500/10"
              }`}
            >
              {t("monitoredTables.filterNeedsAttention")}
            </button>
          </div>
        )}

        {stagedRows.length > 0 && (
          <div className="relative w-60">
            <Input
              placeholder={t("monitoredTables.searchRulesAndColumnsPlaceholder")}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-8 text-xs pr-7"
            />
            {search && (
              <button
                type="button"
                aria-label={t("monitoredTables.clearSearchLabel")}
                onClick={() => setSearch("")}
                className="absolute right-1 top-1/2 -translate-y-1/2 inline-flex items-center justify-center h-5 w-5 text-muted-foreground hover:text-foreground rounded"
              >
                <X className="h-3 w-3" />
              </button>
            )}
          </div>
        )}

        <div className="ml-auto flex items-center gap-2">
          {/* Sliding-pill segmented switch — same animated-indicator approach
              as PredicatePolaritySwitch (item 27): a `<span>` glides between
              the two equal-width slots instead of each button just snapping
              its own background on/off. */}
          <div className="relative inline-grid grid-cols-2 items-stretch rounded-md border bg-muted/30 p-0.5">
            <span
              aria-hidden
              className="absolute top-0.5 bottom-0.5 left-0.5 w-[calc(50%-2px)] rounded bg-primary transition-transform duration-200 ease-out"
              style={{ transform: `translateX(${lens === "by-column" ? "100%" : "0%"})` }}
            />
            {(
              [
                { key: "by-rule", label: t("monitoredTables.lensByRule") },
                { key: "by-column", label: t("monitoredTables.lensByColumn") },
              ] as const
            ).map((mode) => (
              <button
                key={mode.key}
                type="button"
                onClick={() => setLens(mode.key)}
                className={`relative z-10 px-3 py-1.5 rounded text-xs font-medium transition-colors duration-200 ease-out ${
                  lens === mode.key ? "text-primary-foreground" : "text-muted-foreground hover:text-foreground"
                }`}
              >
                {mode.label}
              </button>
            ))}
          </div>
          {canEdit && (
            <>
              {aiAvailability.available && (
                <Button
                  size="sm"
                  className={`gap-2 ${AI_BUTTON_BG}`}
                  onClick={() => {
                    // Usually already prefetched on tab entry — just open. Re-fire
                    // only when there's nothing usable yet (prefetch skipped or
                    // came back unavailable); an in-flight prefetch is left alone.
                    if (!suggestPending && (suggestState === null || !suggestState.available)) {
                      runSuggest();
                    }
                    setSuggestOpen(true);
                  }}
                >
                  <Sparkles className="h-3.5 w-3.5" />
                  {t("monitoredTables.suggestRulesButton")}
                </Button>
              )}
              <Button size="sm" className="gap-2" onClick={() => openAddDialog()}>
                <Plus className="h-3.5 w-3.5" />
                {t("monitoredTables.addRulesButton")}
              </Button>
            </>
          )}
        </div>
      </div>

      {stagedRows.length === 0 && lens === "by-rule" ? (
        <div className="flex flex-col items-center justify-center py-16 text-center border border-dashed rounded-lg">
          <Columns3 className="h-10 w-10 text-muted-foreground/30 mb-3" />
          <p className="text-sm text-muted-foreground mb-3 max-w-sm">
            {canEdit ? t("monitoredTables.emptyAppliedRulesCta") : t("monitoredTables.emptyAppliedRules")}
          </p>
          {canEdit && (
            <Button size="sm" className="gap-2" onClick={() => openAddDialog()}>
              <Plus className="h-3.5 w-3.5" />
              {t("monitoredTables.addRulesButton")}
            </Button>
          )}
        </div>
      ) : lens === "by-rule" ? (
        <div className="space-y-3">
          {visibleMergedRules.length === 0 ? (
            <div className="rounded-lg border border-dashed p-10 text-center text-sm text-muted-foreground">
              {search.trim()
                ? t("monitoredTables.noRulesMatchFilter")
                : filter === "needs-attention"
                  ? t("monitoredTables.noRulesNeedAttention")
                  : t("monitoredTables.emptyAppliedRules")}
            </div>
          ) : (
            visibleMergedRules.map((rule) => (
              <RuleConfigCard
                key={rule.rule_id}
                rule={rule}
                registryRule={ruleById.get(rule.rule_id)}
                labelDefinitions={labelDefinitions}
                severityValues={severityValues}
                canEdit={canEdit}
                busy={false}
                onPinChange={(v) => handlePinChange(rule, v)}
                onSeverityChange={(v) => handleSeverityChange(rule, v)}
                onRemove={() => setRemoveTarget(rule)}
                onRemoveMapping={(groupIdx) => handleRemoveMappingGroup(rule.rule_id, groupIdx)}
                onChangeMapping={(groupIdx, slotName, colName) =>
                  handleChangeMapping(rule.rule_id, groupIdx, slotName, colName)
                }
                onAddMapping={(group) => handleAddMapping(rule.rule_id, group)}
                columns={columns}
                forceOpen={expandRuleIds.includes(rule.rule_id)}
                onJumpToColumn={(colName) => {
                  setFilter("all");
                  setSearch("");
                  setLens("by-column");
                  // Expand the target column's card too — not just switch
                  // views (item 24: state handoff between the by-rule and
                  // by-column lenses).
                  setOpenColumnName(colName);
                  setTimeout(() => {
                    document.getElementById(`column-card-${colName}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
                  }, 50);
                }}
              />
            ))
          )}
        </div>
      ) : (
        <RulesByColumn
          appliedRules={stagedRows}
          tableFqn={tableFqn}
          canEdit={canEdit}
          search={search}
          openColumn={openColumnName}
          onOpenColumnChange={setOpenColumnName}
          onAddRule={(column) => openAddDialog(column)}
          onJumpToRule={(ruleId) => {
            setFilter("all");
            setSearch("");
            setLens("by-rule");
            setExpandRuleIds([ruleId]);
            setTimeout(() => {
              document.getElementById(`rule-card-${ruleId}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
            }, 50);
          }}
        />
      )}

      <AddRulesDialog
        open={addOpen}
        onOpenChange={(next) => {
          setAddOpen(next);
          if (!next) {
            // If the dialog was opened from a column card's "+ Add rule"
            // CTA, auto-expand that column card so it's ready to show the
            // newly-added rule if the user switches back to the by-column
            // lens — mirrors dqlake's RulesByColumn/AppliedRulesList
            // "auto-expand after add" behavior. Fires on any close (not
            // just a successful apply): re-expanding an already-open card
            // is harmless.
            if (addColumnContext) {
              setOpenColumnName(addColumnContext.name);
            }
            setAddColumnContext(null);
          }
        }}
        bindingId={bindingId}
        publishedRules={publishedRules}
        labelDefinitions={labelDefinitions}
        onAdd={stageNewRows}
        onApplied={() => {}}
        initialColumn={addColumnContext}
        rulesLoading={registryLoading}
        rulesError={registryError}
        onRetryRules={() => void refetchRegistry()}
      />

      <AiSuggestionDialog
        open={suggestOpen}
        onOpenChange={setSuggestOpen}
        bindingId={bindingId}
        labelDefinitions={labelDefinitions}
        state={suggestState}
        loading={suggestLoading}
        onRefresh={runSuggest}
        onAdd={stageNewRows}
        onApplied={() => {}}
      />

      <AlertDialog open={removeTarget !== null} onOpenChange={(open) => !open && setRemoveTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("monitoredTables.removeConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.removeConfirmDescription", {
                name: removeTarget?.rule_name || removeTarget?.rule_id || "",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className="bg-destructive text-white hover:bg-destructive/90" onClick={confirmRemove}>
              {t("monitoredTables.removeRuleButton")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

// ---------------------------------------------------------------------------
// View Data tab (P22-B) — first 500 rows via OBO + pragmatic ask-a-question
// ---------------------------------------------------------------------------

function DataGrid({ columns, rows, emptyLabel }: { columns: string[]; rows: Record<string, string | null>[]; emptyLabel: string }) {
  if (rows.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-center border border-dashed rounded-lg">
        <Database className="h-5 w-5 text-muted-foreground mb-2" />
        <p className="text-sm font-medium text-muted-foreground">{emptyLabel}</p>
      </div>
    );
  }
  return (
    <div className="border rounded-lg overflow-auto max-h-[520px]">
      <table className="w-full text-xs">
        <thead className="sticky top-0 bg-muted/80 backdrop-blur-sm">
          <tr className="border-b">
            <th className="text-left p-2 font-medium whitespace-nowrap tabular-nums">#</th>
            {columns.map((col) => (
              <th key={col} className="text-left p-2 font-medium whitespace-nowrap font-mono">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx} className="border-b last:border-b-0 hover:bg-muted/30">
              <td className="p-2 tabular-nums text-muted-foreground">{idx + 1}</td>
              {columns.map((col) => (
                <td
                  key={col}
                  className="p-2 max-w-[240px] truncate font-mono"
                  title={String(row[col] ?? "")}
                >
                  {String(row[col] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ViewDataTab({ tableFqn }: { tableFqn: string }) {
  const { t } = useTranslation();
  const previewMutation = usePreviewTableData();
  const queryMutation = useQueryTableData();
  const [preview, setPreview] = useState<TableDataOut | null>(null);
  const [answer, setAnswer] = useState<TableDataOut | null>(null);
  const [question, setQuestion] = useState("");
  const loadedRef = useRef(false);

  const loadPreview = useCallback(() => {
    previewMutation.mutate(
      { data: { table_fqn: tableFqn } },
      {
        onSuccess: (resp) => {
          setPreview(resp.data);
          setAnswer(null);
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.viewData.loadFailed")), { duration: 6000 }),
      },
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableFqn]);

  useEffect(() => {
    if (loadedRef.current) return;
    loadedRef.current = true;
    loadPreview();
  }, [loadPreview]);

  const runQuestion = (raw: string) => {
    const trimmed = raw.trim();
    if (!trimmed) return;
    setQuestion(trimmed);
    queryMutation.mutate(
      { data: { table_fqn: tableFqn, question: trimmed } },
      {
        onSuccess: (resp) => setAnswer(resp.data),
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.viewData.queryFailed")), { duration: 6000 }),
      },
    );
  };

  const aiAvailable = preview?.ai_available ?? false;
  const active = answer ?? preview;

  const suggestions = useMemo(() => {
    const cols = preview?.columns ?? [];
    const first = cols[0];
    const out = [t("monitoredTables.viewData.suggestCount")];
    if (first) {
      out.push(t("monitoredTables.viewData.suggestNulls", { column: first }));
      out.push(t("monitoredTables.viewData.suggestGroup", { column: first }));
    }
    return out;
  }, [preview, t]);

  if (previewMutation.isPending && !preview) {
    return <Skeleton className="h-64 w-full mt-4" />;
  }

  if (previewMutation.isError && !preview) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center border border-dashed rounded-lg mt-4">
        <AlertTriangle className="h-6 w-6 text-muted-foreground mb-3" />
        <p className="text-sm font-medium text-muted-foreground mb-3">{t("monitoredTables.viewData.loadFailed")}</p>
        <Button size="sm" variant="outline" onClick={loadPreview} className="gap-1.5">
          <RefreshCw className="h-3.5 w-3.5" />
          {t("common.retry")}
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-4 pt-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">
          {t("monitoredTables.viewData.description", { count: TableDataService_PREVIEW_LIMIT })}
        </p>
        <Button size="sm" variant="ghost" onClick={loadPreview} disabled={previewMutation.isPending} className="gap-1.5">
          <RefreshCw className={`h-3.5 w-3.5 ${previewMutation.isPending ? "animate-spin" : ""}`} />
          {t("common.refresh")}
        </Button>
      </div>

      {aiAvailable && (
        <div className="space-y-2 rounded-lg border bg-muted/20 p-3">
          <div className="flex items-center gap-2">
            <Input
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") runQuestion(question);
              }}
              placeholder={t("monitoredTables.viewData.askPlaceholder")}
              disabled={queryMutation.isPending}
              className="h-8 text-xs"
            />
            <Button
              size="sm"
              onClick={() => runQuestion(question)}
              disabled={queryMutation.isPending || !question.trim()}
              className="gap-1.5 shrink-0"
            >
              {queryMutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Send className="h-3.5 w-3.5" />}
              {t("monitoredTables.viewData.askButton")}
            </Button>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {suggestions.map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => runQuestion(s)}
                disabled={queryMutation.isPending}
                className="rounded-full border px-2.5 py-1 text-[11px] text-muted-foreground hover:bg-muted disabled:opacity-50"
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}

      {answer && (
        <div className="space-y-2">
          <div className="flex items-center justify-between gap-2">
            <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">
              {t("monitoredTables.viewData.generatedSqlLabel")}
            </p>
            <Button size="sm" variant="ghost" onClick={() => setAnswer(null)} className="gap-1.5 h-7 text-xs">
              <ArrowLeft className="h-3.5 w-3.5" />
              {t("monitoredTables.viewData.backToSample")}
            </Button>
          </div>
          {answer.generated_sql && (
            <pre className="rounded-md border bg-muted/40 p-3 text-[11px] font-mono overflow-x-auto whitespace-pre-wrap">
              {answer.generated_sql}
            </pre>
          )}
        </div>
      )}

      {active && (
        <DataGrid
          columns={active.columns}
          rows={active.rows}
          emptyLabel={t("monitoredTables.viewData.noRows")}
        />
      )}
    </div>
  );
}

// The preview row cap mirrors the backend TableDataService.PREVIEW_LIMIT.
const TableDataService_PREVIEW_LIMIT = 500;

// ---------------------------------------------------------------------------
// Results tab
// ---------------------------------------------------------------------------

function ResultsTab({ tableFqn, status }: { tableFqn: string; status: string }) {
  const { t } = useTranslation();

  // Submitting the table materializes its applied rules into dq_quality_rules
  // and moves them into the pending_approval queue; they only actually run
  // once an approver flips them to `approved`, exactly like a manually
  // authored rule. Nothing runs while the binding is still `draft` (nothing
  // has been materialized yet), so only fetch/count checks once it has left
  // draft, and point the steward at the existing Drafts & Review queue for
  // any still awaiting review.
  const materialized = status !== "draft";
  const rulesQuery = useGetRules(tableFqn, {
    query: { enabled: materialized, retry: false },
  });
  const checks = rulesQuery.data?.data ?? [];
  const pendingCount = checks.filter((c) => c.status === "draft" || c.status === "pending_approval").length;

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle className="text-sm flex items-center gap-2">
          <ClipboardList className="h-4 w-4" />
          {t("monitoredTables.resultsTitle")}
        </CardTitle>
        <CardDescription>{t("monitoredTables.resultsDescription")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {!materialized && (
          <p className="text-sm text-muted-foreground">{t("monitoredTables.notYetPublishedResultsHint")}</p>
        )}
        {pendingCount > 0 && (
          <div className="flex items-start gap-3 p-3 rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-700">
            <AlertTriangle className="h-4 w-4 text-amber-600 shrink-0 mt-0.5" />
            <div className="space-y-1.5">
              <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                {t("monitoredTables.resultsApprovalBannerTitle")}
              </p>
              <p className="text-sm text-amber-800/90 dark:text-amber-300/90">
                {t("monitoredTables.resultsApprovalBannerBody", { count: pendingCount })}
              </p>
              <Button asChild size="sm" variant="outline" className="gap-1.5 h-7 text-xs border-amber-400 text-amber-700 hover:bg-amber-100 dark:text-amber-300 dark:hover:bg-amber-900">
                <Link to="/rules/drafts">{t("monitoredTables.resultsApprovalBannerCta")}</Link>
              </Button>
            </div>
          </div>
        )}
        <p className="text-sm text-muted-foreground">
          {t("monitoredTables.resultsTableFqnHint", { table: tableFqn })}
        </p>
        <Button asChild variant="outline" size="sm" className="gap-2">
          <Link to="/runs-history">
            <ArrowLeft className="h-3.5 w-3.5 rotate-180" />
            {t("monitoredTables.viewRunsHistory")}
          </Link>
        </Button>
      </CardContent>
    </Card>
  );
}
