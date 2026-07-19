import { createFileRoute, Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { startTransition, Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { HelpTooltip } from "@/components/HelpTooltip";
import {
  AlertCircle,
  AlertTriangle,
  ArrowLeft,
  BarChart3,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Clock,
  Columns3,
  Database,
  ExternalLink,
  GitCompare,
  History,
  Info,
  KeyRound,
  LineChart,
  Loader2,
  MoreVertical,
  FileDown,
  Play,
  Plus,
  RefreshCw,
  RotateCcw,
  Save,
  Send,
  Sparkles,
  Trash2,
  Undo2,
  X,
  XCircle,
} from "lucide-react";
import {
  useGetMonitoredTableSuspense,
  useDeleteMonitoredTable,
  useGetMonitoredTableProfile,
  getGetMonitoredTableProfileQueryKey,
  useSubmitProfileRun,
  getProfileRunStatus,
  useListProfileRuns,
  useGetProfileRunResults,
  getListProfileRunsQueryKey,
  useSubmitMonitoredTable,
  useApproveMonitoredTable,
  useRejectMonitoredTable,
  useRevertMonitoredTable,
  useSaveAppliedRules,
  useListRegistryRules,
  useGetTableColumns,
  useGetTableTags,
  useListMonitoredTableVersions,
  useRunMonitoredTable,
  useSuggestRulesForTable,
  usePreviewTableData,
  useQueryTableData,
  useGetSampleQuestions,
  useGetDefaultPassThreshold,
  getListTagSuggestionsQueryOptions,
  type AppliedRuleOut,
  type ColumnOut,
  type MonitoredTableOut,
  type MonitoredTableVersionOut,
  type RegistryRuleOut,
  type SuggestedRuleMappingOut,
  type TableDataOut,
  type TagRuleSuggestionOut,
} from "@/lib/api";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Pagination } from "@/components/Pagination";
import { StatusBadge } from "@/components/RegistryRuleBadges";
import { PermissionsTab } from "@/components/permissions/PermissionsTab";
import { CommentThread } from "@/components/CommentThread";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { invalidateResultsAfterRuleApplicationChange } from "@/lib/results-invalidation";
import {
  exportMonitoredTable,
  downloadExportFile,
  useLabelDefinitions,
  useListPendingApplications,
  useWorkspaceHost,
} from "@/lib/api-custom";
import type { ExportFormat } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { useApprovalsMode } from "@/hooks/use-approvals-mode";
import { isRunStale, useRequireDraftRunBeforeSubmit } from "@/hooks/use-require-draft-run";
import { useMonitoredTableRunActivity } from "@/hooks/use-monitored-table-run-activity";
import { useJobPolling } from "@/hooks/use-job-polling";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import { usePassThresholdEnabled } from "@/hooks/use-pass-threshold-enabled";
import { formatDateShort } from "@/lib/format-utils";
import { cn } from "@/lib/utils";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_BUTTON_BG, AI_BANNER_BG, AI_BANNER_BORDER, AI_GRADIENT_URL } from "@/lib/ai-style";
import { AddRulesDialog } from "@/components/apply-rules/AddRulesDialog";
import { AiSuggestionDialog, type SuggestRulesState } from "@/components/apply-rules/AiSuggestionDialog";
import { suggestionKey } from "@/components/apply-rules/ai-suggestion-utils";
import { RuleConfigCard, computeStatus } from "@/components/apply-rules/RuleConfigCard";
import { RulesByColumn, type ColumnRef } from "@/components/apply-rules/RulesByColumn";
import {
  MonitoredTableDiffDialog,
  type MonitoredTableDiffTarget,
} from "@/components/drafts/ChangeDiffDialog";
import { slotTagsFromUserMetadata } from "@/lib/registry-rule-conversion";
import {
  RESERVED_SEVERITY_KEY,
  buildDesiredApplications,
  computeRunGating,
  desiredApplicationsKey,
  extractApiError,
  groupAppliedRulesByRuleId,
  mergeRuleRowGroup,
  nextLocalRowId,
  normalizeStagedRows,
} from "@/components/apply-rules/shared";
import { orderSeverityValuesForDisplay } from "@/components/RegistryRuleBadges";
import { ProfileColumnList } from "@/components/bindings/ProfileColumnList";
import { ProfileSuggestionsCard } from "@/components/bindings/ProfileSuggestionsCard";
import { BindingResultsTab } from "@/components/monitored-tables/BindingResultsTab";
import { MonitoredTableSchedulingTab } from "@/components/monitored-tables/MonitoredTableSchedulingTab";
import { MonitoredTableHistoryTab } from "@/components/monitored-tables/MonitoredTableHistoryTab";

// Schedule is its own tab again (P25 item 1 reverted P23 item 13's move into
// the header ⋮ menu), matching dqlake's binding detail tab strip. Schedule
// and History sit in a second, right-aligned TabsList — dqlake's
// `BindingTabsShell` RIGHT_TABS = [scheduling, history], same layout the
// table-spaces `ProductTabsShell` uses.
//
// P26 item 3 removed the View Data tab: its content (preview grid +
// ask-a-question) now lives inside the About tab, below the About/Schema
// card row. Old `?tab=view-data` deep links fall through the key check in
// `activeTab` and land on About — where the content now is — so no
// redirect is needed.
const DETAIL_TAB_KEYS = ["about", "permissions", "profile", "apply-rules", "results", "schedule", "history"] as const;
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
  const { willAutoApprove } = useApprovalsMode();
  const requireDraftRun = useRequireDraftRunBeforeSubmit();
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
      // B2-23: mark the tab-switch navigate as a transition so that if any
      // query elsewhere on the page re-suspends as a result of the URL change,
      // React keeps the CURRENT tab content on screen instead of surfacing the
      // page-level DetailSkeleton fallback and blanking the whole page.
      startTransition(() => {
        navigate({
          to: "/monitored-tables/$bindingId",
          params: { bindingId },
          search: (prev) => ({ ...prev, tab: next }),
        });
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

  // Binding-scoped run-in-progress signal (item 69). Tracks a run set
  // triggered from this page (persisted across reloads via sessionStorage),
  // polls it to completion, and refreshes this table's results/score once it
  // settles. Shared by the Run action (spinner/disable) and the Results-tab
  // in-progress banner — replaces the old in-component `trackedRunSetId` state,
  // which was lost on reload.
  const runActivity = useMonitoredTableRunActivity(bindingId, { tableFqn: table.table_fqn });

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

  const runRuleMutation = useRunMonitoredTable();
  const [runningRuleId, setRunningRuleId] = useState<string | null>(null);
  const handleRunRule = useCallback(
    (ruleId: string) => {
      const source = table.status === "approved" && !isDirty ? "approved" : "draft";
      setRunningRuleId(ruleId);
      runRuleMutation.mutate(
        { bindingId, data: { source, rule_ids: [ruleId] } },
        {
          onSuccess: (resp) => {
            runActivity.registerRun(resp.data.run_set_id);
            toast.success(t("monitoredTables.toastRunRuleStarted"), {
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
          onSettled: () => setRunningRuleId(null),
        },
      );
    },
    [bindingId, isDirty, navigate, runActivity, runRuleMutation, t, table.status],
  );
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
  const revertMutation = useRevertMonitoredTable();
  const deleteMutation = useDeleteMonitoredTable();
  const [rejectConfirmOpen, setRejectConfirmOpen] = useState(false);
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [exporting, setExporting] = useState(false);
  // View-changes diff dialog target — mirrors the overview row's GitCompare
  // action so the same review affordance is available on the detail banner.
  const [diffTarget, setDiffTarget] = useState<MonitoredTableDiffTarget | null>(null);

  // Export this table's checks as DQX or ODCS YAML from the ⋮ menu (moved off
  // a standalone header button). Mirrors ExportYamlMenu's fetch→download→toast.
  const handleExport = useCallback(
    async (format: ExportFormat) => {
      if (exporting) return;
      setExporting(true);
      try {
        const res = await exportMonitoredTable(bindingId, format);
        downloadExportFile(res.data);
        toast.success(t("exportYaml.success", { filename: res.data.filename }));
      } catch (err) {
        toast.error(extractApiError(err, t("exportYaml.failed")));
      } finally {
        setExporting(false);
      }
    },
    [exporting, bindingId, t],
  );
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
      // The persisted applied-rule set changed (apply/unapply/severity
      // override), which moves the dq-score / dq-results aggregates —
      // notably a rule score's `applied_to_count`, which gates the registry
      // rule's Results tab. Those queries never refetch on their own
      // (staleTime Infinity), so invalidate them here or the tab stays
      // stale-disabled until a full reload.
      invalidateResultsAfterRuleApplicationChange(queryClient);
      return true;
    } catch (err: unknown) {
      toast.error(extractApiError(err, t("monitoredTables.toastSaveFailed")), { duration: 6000 });
      return false;
    }
  }, [persistStagedRows, invalidateLifecycleQueries, queryClient, t]);

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
          // Invalidate HERE — as soon as the persist succeeds — not in the
          // shared success handler below: the applied-rule set has already
          // changed server-side at this point, so if the follow-on submit
          // fails the score/results caches must still refresh (staleTime
          // Infinity — see saveDraft for the full rationale). The !isDirty
          // branch persists nothing, so it needs no invalidation.
          invalidateResultsAfterRuleApplicationChange(queryClient);
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

  // Withdraw a pending submission back to draft — the author's counterpart to
  // submit (reject is the approver's decision). Leaves no rejected audit trail.
  const handleRevert = () => {
    revertMutation.mutateAsync({ bindingId }).then(
      () => {
        toast.success(t("monitoredTables.toastReverted"));
        invalidateLifecycleQueries();
      },
      (err: unknown) => {
        toast.error(extractApiError(err, t("monitoredTables.toastRevertFailed")), { duration: 6000 });
      },
    );
  };

  // Delete the binding, then leave for the list. `justSavedRef` bypasses the
  // unsaved-changes nav guard — staged edits on a just-deleted binding aren't
  // worth a "discard changes?" prompt.
  const handleDelete = () => {
    deleteMutation.mutateAsync({ bindingId }).then(
      () => {
        toast.success(t("monitoredTables.toastDeleted"));
        invalidateLifecycleQueries();
        // Deleting the binding unapplies every rule that was applied to it —
        // see saveDraft for why the score/results caches must be invalidated
        // when the applied-rule set changes.
        invalidateResultsAfterRuleApplicationChange(queryClient);
        justSavedRef.current = true;
        void navigate({ to: "/monitored-tables" });
      },
      (err: unknown) => {
        toast.error(extractApiError(err, t("monitoredTables.toastDeleteFailed")), { duration: 6000 });
      },
    );
  };

  const lifecycleBusy =
    saveMutation.isPending ||
    submitMutation.isPending ||
    approveMutation.isPending ||
    rejectMutation.isPending ||
    revertMutation.isPending ||
    deleteMutation.isPending;

  // Nothing to resubmit: Save-as-draft is already disabled (no staged
  // edits) and this version is already approved, so "Submit for review"
  // would just be a no-op re-approval request (item 11).
  const submitDisabledNoChanges = !isDirty && table.status === "approved";

  // Require-draft-run gate (issues B2-12 / B2-118): when the admin setting is
  // on, block submit until a draft run has been recorded for this table AND
  // that run is newer than the last edit. ``last_run_at`` (excludes preview /
  // in-flight runs) and ``updated_at`` (bumped on every applied-rules save) are
  // both cache-friendly signals already on the binding payload. A run older
  // than ``updated_at`` is stale — the rules changed since it ran. The backend
  // enforces the authoritative check and returns 409 either way.
  const staleDraftRun = isRunStale(table.last_run_at, table.updated_at);
  const needsDraftRun = requireDraftRun && (!table.last_run_at || staleDraftRun);
  const submitBlocked = submitDisabledNoChanges || needsDraftRun;

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
          </div>
          <div className="flex items-center gap-2">
            {perms.canCreateRules && (
              <>
                <Button
                  variant="outline"
                  onClick={handleSaveAsDraft}
                  disabled={!isDirty || lifecycleBusy}
                  className="gap-2"
                >
                  {saveMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                  {t("monitoredTables.saveAsDraftButton")}
                </Button>
                <TooltipProvider delayDuration={200}>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className={cn(submitBlocked && "cursor-not-allowed")}>
                        <Button
                          onClick={handleSubmit}
                          disabled={lifecycleBusy || submitBlocked}
                          className="gap-2"
                        >
                          {saveMutation.isPending || submitMutation.isPending ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <Send className="h-4 w-4" />
                          )}
                          {submitMutation.isPending
                            ? t("monitoredTables.submitting")
                            : willAutoApprove
                              ? t("monitoredTables.saveAndPublishButton")
                              : t("monitoredTables.submitButton")}
                        </Button>
                      </span>
                    </TooltipTrigger>
                    {(needsDraftRun || submitDisabledNoChanges) && (
                      <TooltipContent side="bottom">
                        {needsDraftRun
                          ? staleDraftRun
                            ? t("monitoredTables.submitDisabledStaleDraftRunHint")
                            : t("monitoredTables.submitDisabledNeedsDraftRunHint")
                          : t("monitoredTables.submitDisabledNoChangesHint")}
                      </TooltipContent>
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
                runInProgress={runActivity.hasActive}
                onSaveDraft={saveDraft}
                onRunStarted={runActivity.registerRun}
                {...computeRunGating(baseline.length, stagedRows.length)}
              />
            )}
            {/* ⋮ menu — Export (DQX / ODCS) + "View in Runs History" (all
                roles) + Delete (editors only). Export moved here off a
                standalone header button. */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-8 w-8 p-0"
                  aria-label={t("monitoredTables.actionsMenuLabel")}
                >
                  <MoreVertical className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem
                  onClick={() => void handleExport("dqx")}
                  disabled={exporting}
                  className="gap-2"
                >
                  <FileDown className="h-3.5 w-3.5" />
                  {t("exportYaml.dqxOption")}
                </DropdownMenuItem>
                <DropdownMenuItem
                  onClick={() => void handleExport("odcs")}
                  disabled={exporting}
                  className="gap-2"
                >
                  <FileDown className="h-3.5 w-3.5" />
                  {t("exportYaml.odcsOption")}
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  onClick={() =>
                    void navigate({ to: "/runs-history", search: { tableFqn: table.table_fqn } })
                  }
                  className="gap-2"
                >
                  <History className="h-3.5 w-3.5" />
                  {t("runsHistory.menuViewRuns")}
                </DropdownMenuItem>
                {perms.canCreateRules && (
                  <DropdownMenuItem
                    onClick={() => setDeleteConfirmOpen(true)}
                    variant="destructive"
                    className="gap-2"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                    {t("monitoredTables.actionDelete")}
                  </DropdownMenuItem>
                )}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>

        {/* Approve/Reject live INSIDE the pending-approval banner (not the
            header action row) so the review decision reads in the same place
            that explains why the table is waiting. Buttons are gated to
            approvers; everyone else sees the banner as before. */}
        {table.status === "pending_approval" && (
          <div className="flex items-start gap-3 p-3 rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-700">
            <Clock className="h-4 w-4 text-amber-600 shrink-0 mt-0.5" />
            <div className="flex flex-1 flex-wrap items-center gap-x-4 gap-y-2">
              <div className="flex-1 min-w-[16rem] space-y-1">
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
              <div className="flex items-center gap-2 ml-auto shrink-0">
                {/* View changes — read-only diff of the pending vs. last-approved
                    check set, matching the overview row's GitCompare action. */}
                <Button
                  variant="outline"
                  size="sm"
                  disabled={lifecycleBusy}
                  onClick={() =>
                    setDiffTarget({ bindingId, name: table.table_fqn, version: table.version ?? 0 })
                  }
                  className="gap-1.5 h-7 text-xs text-purple-700 border-purple-300 hover:bg-purple-50 dark:text-purple-300 dark:border-purple-700 dark:hover:bg-purple-950"
                >
                  <GitCompare className="h-3.5 w-3.5" />
                  {t("rulesDrafts.diff.viewChanges")}
                </Button>
                {/* Revert — the author withdraws their own pending submission
                    back to draft (authors-and-above; backend enforces owner). */}
                {perms.canCreateRules && (
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={lifecycleBusy}
                    onClick={handleRevert}
                    className="gap-1.5 h-7 text-xs text-amber-700 border-amber-400 hover:bg-amber-100 dark:text-amber-300 dark:hover:bg-amber-900"
                  >
                    {revertMutation.isPending ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Undo2 className="h-3.5 w-3.5" />
                    )}
                    {t("monitoredTables.revertAction")}
                  </Button>
                )}
                {perms.canApproveRules && (
                  <>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={lifecycleBusy}
                      onClick={handleApprove}
                      className="gap-1.5 h-7 text-xs text-emerald-700 border-emerald-400 hover:bg-emerald-50 dark:text-emerald-300 dark:hover:bg-emerald-950"
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
                      className="gap-1.5 h-7 text-xs text-red-700 border-red-400 hover:bg-red-50 dark:text-red-300 dark:hover:bg-red-950"
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
              </div>
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

        <MonitoredTableDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

        <Tabs value={activeTab} onValueChange={handleTabChange}>
          {/* Tab order (P23 item 14): About, Permissions first (View Data
              folded into About in P26 item 3),
              then the working group (Profile, Apply Rules), then Results.
              Schedule + History live in a second, RIGHT-aligned TabsList —
              the table-spaces `ProductTabsShell` / dqlake `BindingTabsShell`
              two-group layout (RIGHT_TABS = [scheduling, history]). */}
          <div className="w-full flex items-center justify-between">
            <TabsList>
              <TabsTrigger value="about" className="gap-1.5">
                <Info className="h-3.5 w-3.5" />
                {t("monitoredTables.tabAbout")}
              </TabsTrigger>
              <TabsTrigger value="permissions" className="gap-1.5">
                <KeyRound className="h-3.5 w-3.5" />
                {t("monitoredTables.tabPermissions")}
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
                <LineChart className="h-3.5 w-3.5" />
                {t("monitoredTables.tabResults")}
              </TabsTrigger>
            </TabsList>
            <TabsList>
              <TabsTrigger value="schedule" className="gap-1.5">
                <Clock className="h-3.5 w-3.5" />
                {t("monitoredTables.tabSchedule")}
              </TabsTrigger>
              <TabsTrigger value="history" className="gap-1.5">
                <History className="h-3.5 w-3.5" />
                {t("monitoredTables.tabHistory")}
              </TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="about">
            <AboutTab bindingId={bindingId} table={table} onColumnClick={handleColumnDeepLink} />
          </TabsContent>

          {/* pt-4 matches the other tabs' top spacing (About/Schedule own it internally). */}
          <TabsContent value="permissions" className="pt-4">
            <PermissionsTab
              objectType="monitored_table"
              objectId={bindingId}
              showSteward
              canEditSteward={false}
              steward={table.steward ?? ""}
            />
          </TabsContent>

          <TabsContent value="profile">
            <ProfileTab bindingId={bindingId} tableFqn={table.table_fqn} canApply={perms.canCreateRules} />
          </TabsContent>

          <TabsContent value="apply-rules">
            <ApplyRulesTab
              bindingId={bindingId}
              tableFqn={table.table_fqn}
              stagedRows={stagedRows}
              setStagedRows={setStagedRows}
              canEdit={perms.canCreateRules}
              canRunRules={perms.canRunRules}
              isDirty={isDirty}
              runInProgress={runActivity.hasActive || runRuleMutation.isPending}
              runningRuleId={runningRuleId}
              onRunRule={handleRunRule}
              initialJumpColumn={pendingColumnJump}
              onJumpColumnConsumed={() => setPendingColumnJump(null)}
            />
          </TabsContent>

          <TabsContent value="results">
            <BindingResultsTab
              bindingId={bindingId}
              tableName={tableName}
              tableFqn={table.table_fqn}
              neverApproved={(table.version ?? 0) === 0}
              runInProgress={runActivity.hasActive}
            />
          </TabsContent>

          <TabsContent value="schedule">
            <MonitoredTableSchedulingTab table={table} canEdit={perms.canCreateRules} />
          </TabsContent>

          <TabsContent value="history">
            <MonitoredTableHistoryTab bindingId={bindingId} />
          </TabsContent>
        </Tabs>
      </div>

      <AlertDialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("monitoredTables.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.deleteConfirmDescription", { table: table.table_fqn })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteMutation.isPending}>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              disabled={deleteMutation.isPending}
              onClick={(e) => {
                e.preventDefault();
                setDeleteConfirmOpen(false);
                handleDelete();
              }}
            >
              {t("monitoredTables.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

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
  runInProgress = false,
  onSaveDraft,
  onRunStarted,
  runNowHasRules,
  runDraftHasRules,
}: {
  bindingId: string;
  table: MonitoredTableOut;
  isDirty: boolean;
  /** True while a run triggered from this page is still executing (item 69) —
   *  shows a spinner and (item 58) disables every run affordance so a second
   *  run can't be launched on top of the in-flight one. */
  runInProgress?: boolean;
  onSaveDraft: () => Promise<boolean>;
  /** Reports the just-submitted run set's id so the page can track it to
   *  completion (in-progress banner + results refresh on settle). */
  onRunStarted?: (runSetId: string) => void;
  /** True when there is a persisted (approved) applied-rule set to run —
   *  i.e. the last-saved baseline is non-empty. "Run now" executes that
   *  server-side snapshot, so it must gate on the baseline, NOT on the
   *  volatile local edit buffer (item 31 fix): editing/removing staged rows
   *  without saving must never disable "Run now", and clearing the baseline
   *  via a saved removal must disable it even if unsaved staged rows exist. */
  runNowHasRules: boolean;
  /** True when the local staged-edit buffer has at least one applied rule.
   *  "Run draft" executes (a saved copy of) that buffer, so it gates on
   *  `stagedRows`, independently of `runNowHasRules`. */
  runDraftHasRules: boolean;
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
  const noRulesRunNow = !runNowHasRules;
  const noRulesRunDraft = !runDraftHasRules;
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
          onRunStarted?.(resp.data.run_set_id);
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

  // Any run mutation, the save-then-run leg, or an already-running run set all
  // block launching another (item 58).
  const busy = runMutation.isPending || runDraftBusy || runInProgress;
  // When there's a draft to run (draft status OR unsaved edits), Run draft is
  // the PRIMARY action and Run now (approved) is demoted into the menu; with no
  // draft, Run now (approved) is primary and Run draft is demoted (item 59).
  // Draft wins as primary whenever both a draft and an approved version exist.
  const draftIsPrimary = canRunDraft;

  // The split-button arrow only offers the demoted run action plus any past
  // approved versions; disable it when NONE of those is actionable (item 34),
  // not merely while a run is pending. The demoted action is Run now (approved)
  // when Run draft is primary, else Run draft.
  const demotedActionable = draftIsPrimary
    ? hasApproved && !noRulesRunNow
    : canRunDraft && !noRulesRunDraft;
  const menuHasActionable = versions.length > 0 || demotedActionable;

  const approvedDisabled = !hasApproved || noRulesRunNow || busy;
  // Even when enabled, the approved run shows a tooltip naming the version it
  // runs (item 52) — the button label itself is a plain "Run now".
  const approvedTooltip = runInProgress
    ? t("monitoredTables.runInProgressHint")
    : noRulesRunNow
      ? t("monitoredTables.runDisabledNoRulesHint")
      : !hasApproved
        ? t("monitoredTables.runNowDisabledHint")
        : t("monitoredTables.runNowTooltip", { version: table.version });
  const draftDisabled = !canRunDraft || noRulesRunDraft || busy;
  const draftTooltip = runInProgress
    ? t("monitoredTables.runInProgressHint")
    : noRulesRunDraft
      ? t("monitoredTables.runDisabledNoRulesHint")
      : !canRunDraft
        ? t("monitoredTables.runDraftDisabledHint")
        : null;

  const spinnerOrPlay = busy ? (
    <Loader2 className="h-4 w-4 animate-spin" />
  ) : (
    <Play className="h-4 w-4" />
  );

  const runNowPrimary = (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn(approvedDisabled && "cursor-not-allowed")}>
          <Button
            onClick={() => handleRun("approved")}
            disabled={approvedDisabled}
            className="gap-2 rounded-r-none"
          >
            {spinnerOrPlay}
            {t("monitoredTables.runNowButtonNoVersion")}
          </Button>
        </span>
      </TooltipTrigger>
      {approvedTooltip && <TooltipContent side="bottom">{approvedTooltip}</TooltipContent>}
    </Tooltip>
  );

  const runDraftPrimary = (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn(draftDisabled && "cursor-not-allowed")}>
          <Button
            onClick={() => void handleRunDraft()}
            disabled={draftDisabled}
            className="gap-2 rounded-r-none"
          >
            {spinnerOrPlay}
            {t("monitoredTables.runDraftAction")}
          </Button>
        </span>
      </TooltipTrigger>
      {draftTooltip && <TooltipContent side="bottom">{draftTooltip}</TooltipContent>}
    </Tooltip>
  );

  const runNowMenuItem = (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn(approvedDisabled && "cursor-not-allowed")}>
          <DropdownMenuItem
            disabled={approvedDisabled}
            onSelect={(e) => {
              e.preventDefault();
              handleRun("approved");
            }}
          >
            {t("monitoredTables.runNowApprovedOption")}
          </DropdownMenuItem>
        </span>
      </TooltipTrigger>
      {approvedTooltip && <TooltipContent side="left">{approvedTooltip}</TooltipContent>}
    </Tooltip>
  );

  const runDraftMenuItem = (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn(draftDisabled && "cursor-not-allowed")}>
          <DropdownMenuItem
            disabled={draftDisabled}
            onSelect={(e) => {
              e.preventDefault();
              void handleRunDraft();
            }}
          >
            {t("monitoredTables.runDraftAction")}
          </DropdownMenuItem>
        </span>
      </TooltipTrigger>
      {draftTooltip && <TooltipContent side="left">{draftTooltip}</TooltipContent>}
    </Tooltip>
  );

  return (
    <div className="inline-flex" role="group" aria-label={t("monitoredTables.runActionGroupAria")}>
      <TooltipProvider delayDuration={200}>
        {draftIsPrimary ? runDraftPrimary : runNowPrimary}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              className="rounded-l-none border-l border-primary-foreground/20 px-2"
              disabled={busy || !menuHasActionable}
              aria-label={t("monitoredTables.runMenuAria")}
            >
              <ChevronDown className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {/* The action NOT shown as primary is demoted here at the top. */}
            {draftIsPrimary ? runNowMenuItem : runDraftMenuItem}
            {versions.length > 0 && <DropdownMenuSeparator />}
            {versions.map((v: MonitoredTableVersionOut) => (
              <DropdownMenuItem
                key={v.version}
                disabled={busy}
                onSelect={() => handleRun("approved", v.version)}
              >
                {t("monitoredTables.runVersionOption", { version: v.version })}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </TooltipProvider>
    </div>
  );
}

// ---------------------------------------------------------------------------
// About tab
// ---------------------------------------------------------------------------

// Schema section page size — deliberately small so the schema section lands
// at roughly the same height as the About section beside it: About renders
// ten definition rows (text-xs, gap-y-2) plus the Unity Catalog link line,
// and the schema table adds a header row and a pagination footer around its
// compact (h-8) body rows. Six body rows lands the pagination footer level
// with About's "Open in Unity Catalog" link line.
const SCHEMA_PAGE_SIZE = 6;

/** Applied governed tags for one schema column, rendered on a single
 *  fixed-height line. Shows the first tag as a compact chip; if there are more,
 *  a trailing `+N` badge whose tooltip lists every applied tag. Never wraps —
 *  the row stays `h-8`. Empty → a muted "-" (matches the Description empty
 *  state). */
function ColumnTagsCell({ tags }: { tags: string[] }) {
  const { t } = useTranslation();
  if (tags.length === 0) {
    return <span className="text-muted-foreground text-xs">-</span>;
  }
  const [first, ...rest] = tags;
  return (
    <div className="flex items-center gap-1 overflow-hidden whitespace-nowrap">
      <Badge variant="secondary" className="text-[10px] font-mono max-w-full shrink" title={first}>
        <span className="truncate">{first}</span>
      </Badge>
      {rest.length > 0 && (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge variant="outline" className="text-[10px] shrink-0 cursor-default">
                +{rest.length}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>
              <span className="text-xs">{t("monitoredTables.aboutColTagsAllLabel", { tags: tags.join(", ") })}</span>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )}
    </div>
  );
}

function AboutTab({
  bindingId,
  table,
  onColumnClick,
}: {
  bindingId: string;
  table: MonitoredTableOut;
  /** Deep-links to the Apply Rules "by column" lens, expanded to that
   *  column (item 1) — reuses the jump+expand handoff already wired
   *  between the by-rule/by-column lenses in ApplyRulesTab. */
  onColumnClick: (columnName: string) => void;
}) {
  const { t } = useTranslation();
  const [filter, setFilter] = useState("");
  // 1-indexed schema page (P26 item 2) — the shared `Pagination` footer
  // convention. Reset to 1 whenever the filter changes so a narrowed
  // result set can't strand the view on a now-empty page.
  const [page, setPage] = useState(1);
  const parts = table.table_fqn.split(".");
  const [catalog, schema, tableName] = parts;

  const columnsQuery = useGetTableColumns(catalog ?? "", schema ?? "", tableName ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  // Applied governed tags per column (from `get_table_tags`, sourced from
  // information_schema.column_tags). Absent/failed → no tags shown.
  const tagsQuery = useGetTableTags(catalog ?? "", schema ?? "", tableName ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columnTags: Record<string, string[]> = tagsQuery.data?.data?.column_tags ?? {};

  const filteredColumns = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return columns;
    return columns.filter(
      (c) => c.name.toLowerCase().includes(q) || (c.comment ?? "").toLowerCase().includes(q),
    );
  }, [columns, filter]);

  const pagedColumns = useMemo(
    () => filteredColumns.slice((page - 1) * SCHEMA_PAGE_SIZE, page * SCHEMA_PAGE_SIZE),
    [filteredColumns, page],
  );

  // Layout (P26 item 3): About and Schema sit side-by-side as plain sections
  // (stacking on narrow widths) — same heading treatment as the View Data
  // section below — with the View Data content (preview grid +
  // ask-a-question, formerly its own tab) below the row.
  const workspaceHostQuery = useWorkspaceHost();
  const ucExploreUrl = useMemo(() => {
    const host = (workspaceHostQuery.data?.workspace_host ?? "").replace(/\/$/, "");
    if (!host || parts.length !== 3) return null;
    return `${host}/explore/data/${catalog}/${schema}/${tableName}`;
  }, [workspaceHostQuery.data?.workspace_host, parts.length, catalog, schema, tableName]);

  return (
    <div className="space-y-6 pt-4">
      {/* About (metadata) : Schema (columns table) — the schema table gets the
          larger share so its Column/Type/Tags/Description columns + filter box
          breathe; About's definition list is narrow by nature. */}
      <div className="grid gap-6 lg:grid-cols-[5fr_7fr] items-start">
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
          {/* Deep link into the Unity Catalog explorer — mirrors dqlake's
              BindingAboutTab (label, external-link icon, placement after the
              definition list). Hidden when the host is unknown (local dev). */}
          {ucExploreUrl && (
            <a
              href={ucExploreUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1"
            >
              {t("monitoredTables.aboutOpenInUnityCatalog")} <ExternalLink className="h-3 w-3" />
            </a>
          )}
        </section>

        <section className="space-y-3">
          <h2 className="text-sm font-semibold">
            {t("monitoredTables.aboutSchemaSectionTitle", { count: columns.length })}
          </h2>
          <div>
            {columnsQuery.isError ? (
              <div className="flex flex-col items-center justify-center py-10 text-center border border-dashed rounded-lg">
                <AlertCircle className="h-8 w-8 text-destructive/30 mb-2" />
                <p className="text-sm font-medium text-muted-foreground">{t("monitoredTables.aboutLoadFailedTitle")}</p>
                <p className="text-xs text-muted-foreground/70 mt-1 max-w-sm">{t("monitoredTables.aboutLoadFailedHint")}</p>
              </div>
            ) : (
              <>
                <div className="rounded-md border overflow-hidden">
                  {/* table-fixed + truncate keeps every row a single fixed-height
                      line — long names/types/comments ellipsize and stay
                      recoverable on hover (tooltip / title). */}
                  <table className="w-full text-sm table-fixed">
                    <thead className="bg-muted/30">
                      <tr>
                        <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-[26%]">
                          {t("monitoredTables.aboutColColumn")}
                        </th>
                        <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-20">
                          {t("monitoredTables.aboutColType")}
                        </th>
                        <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-[28%]">
                          {t("monitoredTables.aboutColTags")}
                        </th>
                        <th className="px-3 py-1.5">
                          <div className="flex items-center justify-between gap-3">
                            <span className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide">
                              {t("monitoredTables.aboutColDescription")}
                            </span>
                            <Input
                              placeholder={t("monitoredTables.aboutFilterColumnsPlaceholder")}
                              value={filter}
                              onChange={(e) => {
                                setFilter(e.target.value);
                                setPage(1);
                              }}
                              className="w-44 h-7 text-xs font-normal normal-case tracking-normal"
                            />
                          </div>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {pagedColumns.map((c) => (
                        <tr
                          key={c.name}
                          className="border-t h-8 cursor-pointer hover:bg-muted"
                          role="button"
                          tabIndex={0}
                          aria-label={t("monitoredTables.aboutColumnJumpTooltip")}
                          onClick={() => {
                            // A click that ends a text selection shouldn't also
                            // navigate — let the user copy a value in peace.
                            if (window.getSelection()?.toString()) return;
                            onColumnClick(c.name);
                          }}
                          onKeyDown={(e) => {
                            if (e.key === "Enter" || e.key === " ") {
                              e.preventDefault();
                              onColumnClick(c.name);
                            }
                          }}
                        >
                          <td className="px-3 py-1 overflow-hidden">
                            <span className="font-mono text-xs block max-w-full truncate">{c.name}</span>
                          </td>
                          <td className="px-3 py-1 overflow-hidden">
                            <Badge
                              variant="outline"
                              className="text-[10px] font-mono max-w-full"
                              title={c.type_name}
                            >
                              <span className="truncate">{c.type_name}</span>
                            </Badge>
                          </td>
                          <td className="px-3 py-1 overflow-hidden">
                            <ColumnTagsCell tags={columnTags[c.name] ?? []} />
                          </td>
                          <td className="px-3 py-1 overflow-hidden">
                            {c.comment ? (
                              <span className="text-xs block truncate" title={c.comment}>
                                {c.comment}
                              </span>
                            ) : (
                              <span className="text-muted-foreground text-xs">-</span>
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
                <Pagination
                  page={page}
                  totalItems={filteredColumns.length}
                  pageSize={SCHEMA_PAGE_SIZE}
                  onPageChange={setPage}
                  className="flex items-center justify-between pt-3"
                />
              </>
            )}
          </div>
        </section>
      </div>

      {/* View Data (P26 item 3) — relocated from its own tab into the About
          tab, below the About/Schema row where the schema section used to
          sit. Preview grid + ask-a-question, unchanged behaviour. */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold">{t("monitoredTables.viewData.sectionTitle")}</h2>
        <ViewDataTab tableFqn={table.table_fqn} />
      </section>

      {/* Table-level notes (steward context, ownership, general discussion)
          live here in About — moved off the header ⋮ menu. Run-specific
          discussion instead lives with the run on the Results tab. */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold">{t("monitoredTables.commentsSectionTitle")}</h2>
        <CommentThread entityType="monitored_table" entityId={bindingId} />
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

function ProfileTab({
  bindingId,
  tableFqn,
  canApply,
}: {
  bindingId: string;
  tableFqn: string;
  canApply: boolean;
}) {
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

  // Track recently completed run IDs to prevent re-attaching to a run that just
  // finished while the cache still shows it as RUNNING. This prevents duplicate
  // toasts and redundant polling until the list refetches.
  const completedRunIdsRef = useRef<Set<string>>(new Set());

  // P23 item 4: profile-run history is already persisted per-run in
  // dq_profiling_results (no table changes needed) — this exposes it and,
  // critically, lets the tab re-attach to a RUNNING run after the user
  // navigated away. Radix unmounts an inactive tab, so a run kicked off here
  // otherwise vanishes from local state on return; without re-attaching, the
  // in-flight run "disappears" (the exact reported bug).
  // Scoped server-side to this table (item 49) so we fetch only its own runs
  // instead of the full history. The client-side filter stays as a cheap
  // guard in case a shared cache entry ever carries other tables' rows.
  const runsQuery = useListProfileRuns({ table_fqn: tableFqn });
  const runsForTable = useMemo(
    () => (runsQuery.data?.data ?? []).filter((r) => r.source_table_fqn === tableFqn),
    [runsQuery.data, tableFqn],
  );
  const inProgressRun = useMemo(
    () => runsForTable.find((r) => r.status === "RUNNING") ?? null,
    [runsForTable],
  );
  // Successful past runs power the version switcher (dqlake-style). Every run's
  // full result set is already persisted per run_id (dq_profiling_results), so
  // switching just re-points the column list at getProfileRunResults(runId) —
  // no extra storage. Newest first (runsForTable is already created_at DESC).
  const successfulRuns = useMemo(
    () => runsForTable.filter((r) => r.status === "SUCCESS" && r.run_id),
    [runsForTable],
  );

  // null = show the latest profile (via getMonitoredTableProfile); a run_id =
  // that historical run's persisted results (via getProfileRunResults).
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const historyResultsQuery = useGetProfileRunResults(selectedRunId ?? "", {
    query: { enabled: selectedRunId !== null },
  });

  // A picked run that no longer exists (history refetched, run rolled off) or
  // clearing the table selection falls back to the latest profile.
  useEffect(() => {
    if (selectedRunId !== null && !successfulRuns.some((r) => r.run_id === selectedRunId)) {
      setSelectedRunId(null);
    }
  }, [selectedRunId, successfulRuns]);

  const submitMutation = useSubmitProfileRun();

  // Clear completed run IDs when table changes.
  useEffect(() => {
    completedRunIdsRef.current.clear();
  }, [tableFqn]);

  // Re-attach the poller to a server-side RUNNING run whenever we don't have a
  // local run in flight (fresh mount / tab re-entry). Skip re-attaching to any
  // run in the completed set to avoid duplicate toasts while the cache refetches.
  useEffect(() => {
    if (runId === null && inProgressRun?.run_id && !completedRunIdsRef.current.has(inProgressRun.run_id)) {
      setRunId(inProgressRun.run_id);
    }
  }, [inProgressRun, runId]);

  const fetchStatus = useCallback(async () => {
    if (!runId) throw new Error("No active profile run");
    const resp = await getProfileRunStatus(runId);
    return resp.data;
  }, [runId]);

  useJobPolling({
    fetchStatus,
    enabled: runId !== null,
    interval: 3000,
    onComplete: async (status) => {
      if (status.result_state === "SUCCESS") {
        toast.success(t("monitoredTables.profileToastSucceeded"));
        await queryClient.invalidateQueries({ queryKey: getGetMonitoredTableProfileQueryKey(bindingId) });
      } else {
        toast.error(t("monitoredTables.profileToastFailed"));
      }
      // Track the completed run ID to prevent re-attach while cache refetches.
      if (runId !== null) {
        completedRunIdsRef.current.add(runId);
      }
      setRunId(null);
      await queryClient.invalidateQueries({ queryKey: getListProfileRunsQueryKey() });
    },
    onError: () => {
      toast.error(t("monitoredTables.profileToastFailed"));
      // Track the failed run ID as well to prevent re-attach.
      if (runId !== null) {
        completedRunIdsRef.current.add(runId);
      }
      setRunId(null);
    },
  });

  const running = submitMutation.isPending || runId !== null;

  const handleRunProfile = useCallback(() => {
    submitMutation.mutate(
      { data: { table_fqn: tableFqn } },
      {
        onSuccess: (resp) => {
          setRunId(resp.data.run_id);
          toast.success(t("monitoredTables.profileToastStarted"));
          void queryClient.invalidateQueries({ queryKey: getListProfileRunsQueryKey() });
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.profileToastFailed")), { duration: 6000 });
        },
      },
    );
  }, [submitMutation, tableFqn, t, queryClient]);

  const summary = (profile?.summary ?? {}) as Record<string, unknown>;

  // The column list reflects the run picked in the version switcher: the latest
  // profile by default, or a selected historical run's persisted results. The
  // header stats stay pinned to the latest profile.
  const historyResult = historyResultsQuery.data?.data;
  const activeSummary =
    selectedRunId !== null ? ((historyResult?.summary ?? {}) as Record<string, unknown>) : summary;
  const activeRowCount = selectedRunId !== null ? historyResult?.rows_profiled : profile?.rows_profiled;

  const columnStats = useMemo(() => {
    const entries = Object.entries(activeSummary).filter(
      (e): e is [string, Record<string, unknown>] =>
        e[0] !== LLM_PK_SUMMARY_KEY && typeof e[1] === "object" && e[1] !== null && !Array.isArray(e[1]),
    );
    return Object.fromEntries(entries);
  }, [activeSummary]);

  // Staggered paint (items 49/96): the run/refresh header renders immediately;
  // only the column-list region waits on its own query. This stops the whole
  // tab from blocking behind one skeleton (slow-to-load) and lets the empty
  // state / Run CTA appear promptly when there's no profile yet.
  const initialLoading = profileQuery.isLoading;
  const historyLoading = selectedRunId !== null && historyResultsQuery.isLoading;
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

      {initialLoading ? (
        <Skeleton className="h-64 w-full" />
      ) : !hasProfile ? (
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
      ) : (
        <div className="space-y-3">
          {/* Profiler rule suggestions (B2-82) — dqlake-style placement: the
              profiler's generated checks live here on the Profile page (not in
              the AI Suggest-rules dialog). Only shown for the latest profile,
              not a historical run. */}
          {selectedRunId === null && <ProfileSuggestionsCard bindingId={bindingId} canApply={canApply} />}

          {/* Version switcher (items 50/31) — pick any past run; its full
              results are already persisted per run_id, so this just re-points
              the column list. Replaces the old log-style history list. */}
          {successfulRuns.length > 1 && (
            <div className="flex items-center justify-end gap-2">
              <span className="text-[11px] uppercase tracking-wide text-muted-foreground">
                {t("monitoredTables.profileVersionLabel")}
              </span>
              <Select
                value={selectedRunId ?? profile?.run_id ?? ""}
                onValueChange={(v) => setSelectedRunId(v === profile?.run_id ? null : v)}
              >
                <SelectTrigger className="h-8 w-[280px] text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {successfulRuns.map((r, i) => (
                    <SelectItem key={r.run_id} value={r.run_id ?? ""} className="text-xs">
                      {i === 0
                        ? t("monitoredTables.profileVersionOptionLatest", {
                            date: r.created_at ? formatDateShort(r.created_at) : "—",
                            count: r.rows_profiled ?? 0,
                          })
                        : t("monitoredTables.profileVersionOption", {
                            date: r.created_at ? formatDateShort(r.created_at) : "—",
                            count: r.rows_profiled ?? 0,
                          })}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {historyLoading ? (
            <Skeleton className="h-64 w-full" />
          ) : hasColumns ? (
            <ProfileColumnList columnStats={columnStats} columnTypes={columnTypes} rowCount={activeRowCount} />
          ) : (
            <div className="py-8 text-center text-xs text-muted-foreground border border-dashed rounded-md">
              {t("monitoredTables.profileNoColumnsMatch")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Apply Rules tab
// ---------------------------------------------------------------------------

// Client-side page size for the by-rule lens — rule config cards are tall
// (expandable), so a screenful is ~10 before the list runs off the bottom
// (item 51). The by-column lens paginates its own, denser rows internally.
const RULE_PAGE_SIZE = 10;

/** Read-only panel listing applications staged against this binding that are
 *  still waiting on their rule's approval (recorded by Bulk Contract Import
 *  when a rule lands `pending_approval`). These aren't `dq_applied_rules` rows
 *  yet — they auto-attach when the rule is approved — so surfacing them here
 *  stops the tab from looking empty and gives approvers a jump-off to the
 *  rule. Renders nothing when there are none (or on load/error). */
function PendingApplicationsPanel({ bindingId }: { bindingId: string }) {
  const { t } = useTranslation();
  const { data } = useListPendingApplications(bindingId);
  const pending = data?.data ?? [];
  if (pending.length === 0) return null;

  return (
    <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 space-y-2">
      <div className="flex items-center gap-1.5 text-xs font-medium text-amber-700 dark:text-amber-400">
        <Clock className="h-3.5 w-3.5" />
        {t("monitoredTables.pendingApplications.title", { count: pending.length })}
      </div>
      <p className="text-xs text-muted-foreground">
        {t("monitoredTables.pendingApplications.description")}
      </p>
      <ul className="space-y-1">
        {pending.map((p) => {
          const cols = [...new Set(p.column_mapping.flatMap((g) => Object.values(g)))];
          return (
            <li key={p.id} className="flex flex-wrap items-center gap-2 text-xs">
              <Link
                to="/registry-rules/$ruleId"
                params={{ ruleId: p.rule_id }}
                className="font-medium text-primary hover:underline"
              >
                {p.rule_name || p.rule_id}
              </Link>
              {p.rule_status && <StatusBadge status={p.rule_status} />}
              {cols.length > 0 && (
                <span className="font-mono text-muted-foreground">
                  {t("monitoredTables.pendingApplications.columns", { columns: cols.join(", ") })}
                </span>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function ApplyRulesTab({
  bindingId,
  tableFqn,
  stagedRows,
  setStagedRows,
  canEdit,
  canRunRules,
  isDirty,
  runInProgress,
  runningRuleId,
  onRunRule,
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
  canRunRules: boolean;
  isDirty: boolean;
  runInProgress: boolean;
  runningRuleId: string | null;
  onRunRule: (ruleId: string) => void;
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
  // 1-indexed page for the by-rule lens (item 51). Reset on search/filter
  // change below; jumped to the target rule's page when a card is
  // auto-expanded (after staging or a by-column "jump to rule").
  const [rulePage, setRulePage] = useState(1);

  const queryClient = useQueryClient();
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
  const { isPending: suggestPending } = suggestMutation;
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

    // Fire BOTH the AI suggester AND the tag-suggestions fetch and show their
    // UNION. The two are independent: a workspace with no AI configured still
    // gets tag suggestions, and an AI failure/timeout never discards tag
    // results. `allSettled` lets each settle on its own; the requestId guard
    // (and the timeout above) still ensures a stale response can't clobber a
    // newer one.
    void (async () => {
      const [aiResp, tagResp] = await Promise.allSettled([
        suggestMutation.mutateAsync({ bindingId }),
        queryClient.fetchQuery(getListTagSuggestionsQueryOptions(bindingId)),
      ]);
      if (suggestRequestIdRef.current !== requestId) return;
      clearSuggestTimeout();

      // Tag suggestions map field-by-field onto the AI suggestion shape (the
      // two generated types are structurally identical but nominally distinct).
      const tagSuggestions: SuggestedRuleMappingOut[] =
        tagResp.status === "fulfilled"
          ? (tagResp.value.data.suggestions ?? []).map((s: TagRuleSuggestionOut) => ({
              rule_id: s.rule_id,
              rule_name: s.rule_name,
              dimension: s.dimension,
              severity: s.severity,
              column_mapping: s.column_mapping,
              explanation: s.explanation,
            }))
          : [];

      let aiAvailableResult = false;
      let aiReason: string | undefined;
      let aiSuggestions: SuggestedRuleMappingOut[] = [];
      if (aiResp.status === "fulfilled") {
        aiAvailableResult = aiResp.value.data.available;
        aiReason = aiResp.value.data.reason;
        aiSuggestions = aiResp.value.data.suggestions ?? [];
      } else {
        const reason = aiUnavailableReason(aiResp.reason);
        if (reason) reportUnavailable(reason);
        aiReason = reason ?? t("monitoredTables.suggestRulesFetchFailed");
      }

      // Union, de-duped by (rule_id + sorted slot->column set). AI is listed
      // first so on an exact collision its explanation wins; every (rule,
      // column) the AI did NOT also suggest keeps its "Matched tag …" reason.
      const merged: SuggestedRuleMappingOut[] = [];
      const seen = new Set<string>();
      for (const s of [...aiSuggestions, ...tagSuggestions]) {
        const key = suggestionKey(s);
        if (seen.has(key)) continue;
        seen.add(key);
        merged.push(s);
      }

      // Available when EITHER source has something to offer. When tag
      // suggestions exist the dialog is available regardless of the AI, so the
      // AI reason is moot; with no tag suggestions we preserve the exact prior
      // reason plumbing (AI reason on both the available and unavailable path).
      const available = aiAvailableResult || tagSuggestions.length > 0;
      setSuggestState({
        available,
        reason: tagSuggestions.length > 0 ? undefined : aiReason,
        suggestions: merged,
      });
    })();
  }, [bindingId, suggestMutation, queryClient, reportUnavailable, clearSuggestTimeout, t]);

  // Mirrors dqlake's `canSuggest` gate on `AiSuggestionButton`: the table's
  // columns come from Unity Catalog (`columnsQuery`), and when that read
  // fails the suggest call cannot produce a real column mapping either —
  // firing it anyway just burns an AI-judge round-trip for a request that's
  // doomed. Skip the prefetch (and the button's on-demand retry) until the
  // column read succeeds; `columnsQuery` re-fires on its own if the table
  // becomes reachable later, so this re-evaluates automatically.
  const canSuggest = !columnsQuery.isError;
  useEffect(() => {
    if (suggestPrefetchedRef.current || !aiAvailable || !canSuggest) return;
    suggestPrefetchedRef.current = true;
    runSuggest();
  }, [aiAvailable, canSuggest, runSuggest]);

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

  // Rule ids already staged on this table — the AddRulesDialog picker renders
  // these checked + disabled so they can't be applied twice (B2-115).
  const appliedRuleIds = useMemo(() => new Set(stagedRows.map((r) => r.rule_id)), [stagedRows]);

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

  // Admin default threshold — used as the bottom of the precedence chain
  // (rule_override ?? registry_default ?? admin_default) for the ThresholdPill
  // placeholder. A non-admin user won't have write access to this setting but
  // can always read it; the fallback (70) matches the compiled-in default.
  const { data: defaultThresholdData } = useGetDefaultPassThreshold();
  const adminDefaultThreshold = defaultThresholdData?.data?.default_pass_threshold ?? 70;
  // Feature gate — when disabled by admin, hide all threshold UI (pills).
  const thresholdEnabled = usePassThresholdEnabled();

  // Applied governed tags per column — sourced from Unity Catalog
  // `information_schema.column_tags` via `get_table_tags`. Used to render
  // matched governed tag chips alongside column chips in both lenses.
  // Same call as the About tab; the query is deduplicated by React Query.
  const tableTagsQuery = useGetTableTags(parts[0] ?? "", parts[1] ?? "", parts[2] ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columnTags: Record<string, string[]> = tableTagsQuery.data?.data?.column_tags ?? {};

  // slot_tags per rule: rule_id → {slotName → [governedTag, ...]}. Built once
  // from `ruleById` so `useRulesByColumn` can compute matched tags per entry
  // without re-parsing `user_metadata` on every render.
  const ruleSlotTagsById = useMemo(() => {
    const m = new Map<string, Record<string, string[]>>();
    for (const [ruleId, rule] of ruleById) {
      const st = slotTagsFromUserMetadata(rule.user_metadata as Record<string, unknown> | undefined);
      if (Object.keys(st).length > 0) m.set(ruleId, st);
    }
    return m;
  }, [ruleById]);

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

  // Clamp the requested page against the live list length so removing rules
  // on the last page (or a filter narrowing the set) can't strand the view
  // on an empty page.
  const ruleTotalPages = Math.max(1, Math.ceil(visibleMergedRules.length / RULE_PAGE_SIZE));
  const safeRulePage = Math.min(rulePage, ruleTotalPages);
  const pagedMergedRules = useMemo(
    () => visibleMergedRules.slice((safeRulePage - 1) * RULE_PAGE_SIZE, safeRulePage * RULE_PAGE_SIZE),
    [visibleMergedRules, safeRulePage],
  );

  // Reset to the first page whenever the search or filter changes so a
  // narrowed result set starts from the top (item 51). Declared before the
  // auto-expand effect so that when both fire on the same commit (staging
  // resets search+filter AND sets expandRuleIds), the jump-to-page wins.
  useEffect(() => {
    setRulePage(1);
  }, [search, filter]);

  // Jump to the page holding the first auto-expanded card so a rule freshly
  // staged (appended to the end) or targeted by a by-column "jump to rule"
  // is actually on-screen, not hidden behind pagination.
  useEffect(() => {
    if (expandRuleIds.length === 0) return;
    const idx = visibleMergedRules.findIndex((r) => expandRuleIds.includes(r.rule_id));
    if (idx >= 0) setRulePage(Math.floor(idx / RULE_PAGE_SIZE) + 1);
  }, [expandRuleIds, visibleMergedRules]);

  const openAddDialog = (column?: ColumnRef) => {
    setAddColumnContext(column ?? null);
    setAddOpen(true);
  };

  // Every add path (AddRulesDialog, AiSuggestionDialog) hands up a batch of
  // brand-new locally-staged rows. When the add came from a column CTA
  // (addColumnContext is non-null), stay in the by-column lens and expand
  // the target column so the newly-mapped rule appears inline; otherwise
  // jump to the by-rule lens and auto-expand the new rule cards for mapping.
  const stageNewRows = (rows: AppliedRuleOut[]) => {
    setStagedRows((prev) => [...prev, ...rows]);
    setFilter("all");
    setSearch("");
    if (addColumnContext) {
      // Origin was a column CTA — stay in by-column and show the column.
      setLens("by-column");
      setOpenColumnName(addColumnContext.name);
    } else {
      // Origin was the header "+ Add rule" button or AI suggestion — go to
      // by-rule and auto-expand the newly-staged cards for mapping.
      setLens("by-rule");
      setExpandRuleIds(rows.map((r) => r.rule_id));
    }
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

  // `version` is `null` (follow latest) or a specific published version
  // number to pin to — `VersionPinDropdown` now offers the rule's FULL
  // version history (not just "latest" vs. "current version"), so this
  // stages whatever version the steward picked directly instead of
  // re-deriving it from the registry rule's live `version` (P24 fix).
  const handlePinChange = (rule: AppliedRuleOut, version: number | null) => {
    setStagedRows((prev) =>
      prev.map((r) => (r.rule_id === rule.rule_id ? { ...r, pinned_version: version } : r)),
    );
  };

  const handleSeverityChange = (rule: AppliedRuleOut, value: string) => {
    const severity_override = value === "none" ? null : value;
    setStagedRows((prev) => prev.map((r) => (r.rule_id === rule.rule_id ? { ...r, severity_override } : r)));
  };

  // Per-rule pass threshold — like severity/pin above, this is a property of
  // the rule application (shared across all of a rule_id's staged rows), so
  // update every row for that rule_id. Pure local staged mutation.
  const handlePassThresholdChange = (rule: AppliedRuleOut, value: number | null) => {
    setStagedRows((prev) => prev.map((r) => (r.rule_id === rule.rule_id ? { ...r, pass_threshold: value } : r)));
  };

  // Per-column threshold override — update every staged row for the given
  // rule_id, merging the new column value into each row's column_pass_thresholds
  // map. `value === null` deletes the key (revert to rule default); `value === 0`
  // is a real threshold and must be stored, not deleted. Uses === null strictly.
  const handleColumnThresholdChange = (ruleId: string, column: string, value: number | null) => {
    setStagedRows((prev) =>
      prev.map((r) => {
        if (r.rule_id !== ruleId) return r;
        const existing = r.column_pass_thresholds ?? {};
        if (value === null) {
          // Remove the column key — spread and delete immutably.
          const { [column]: _removed, ...rest } = existing;
          return { ...r, column_pass_thresholds: rest };
        }
        return { ...r, column_pass_thresholds: { ...existing, [column]: value } };
      }),
    );
  };

  return (
    <div className="space-y-4 pt-4">
      {/* Table unreachable via Unity Catalog — mirrors dqlake's AppliedRulesList
          banner: explain up front why the Suggest button's prefetch was
          skipped, rather than letting the steward discover it only after
          clicking. */}
      {columnsQuery.isError && (
        <div className="rounded border border-yellow-500/40 bg-yellow-500/5 px-3 py-2 text-xs text-yellow-700 dark:text-yellow-400">
          {t("monitoredTables.suggestRulesTableUnavailableReason")}
        </div>
      )}
      <PendingApplicationsPanel bindingId={bindingId} />
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
          {/* Divider between the always-visible lens toggle and the
              edit-only actions — mirrors dqlake's AppliedRulesList toolbar.
              Only shown alongside the Suggest button so it never dangles. */}
          {canEdit && aiAvailability.available && <div className="h-5 w-px bg-border mx-1" />}
          {canEdit && (
            <>
              {aiAvailability.available && (
                <Button
                  size="sm"
                  className={`gap-2 ${AI_BUTTON_BG}`}
                  onClick={() => {
                    // Table unreachable: the prefetch was skipped on purpose (see
                    // `canSuggest` above) and a network retry would only fail the
                    // same way, so open straight to the honest unavailable state
                    // instead of a dialog stuck on "Finding good matches…".
                    if (!canSuggest) {
                      setSuggestState({
                        available: false,
                        reason: t("monitoredTables.suggestRulesTableUnavailableReason"),
                        suggestions: [],
                      });
                      setSuggestOpen(true);
                      return;
                    }
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
              {/* Toolbar "Apply rules" button removed (item 36) — the wide
                  dashed CTA at the bottom of the by-rule list and the
                  per-column "+ Add rule" CTAs in the by-column lens now cover
                  adding rules, mirroring dqlake's AppliedRulesList. */}
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
            pagedMergedRules.map((rule) => (
              <RuleConfigCard
                key={rule.rule_id}
                rule={rule}
                registryRule={ruleById.get(rule.rule_id)}
                labelDefinitions={labelDefinitions}
                severityValues={severityValues}
                canEdit={canEdit}
                canRunRule={canRunRules}
                runRuleBusy={runInProgress || runningRuleId === rule.rule_id}
                runRuleDisabled={isDirty || runInProgress}
                onRunRule={() => onRunRule(rule.rule_id)}
                busy={false}
                onPinChange={(v) => handlePinChange(rule, v)}
                onSeverityChange={(v) => handleSeverityChange(rule, v)}
                onPassThresholdChange={(v) => handlePassThresholdChange(rule, v)}
                resolvedDefaultThreshold={rule.rule_pass_threshold ?? adminDefaultThreshold}
                thresholdEnabled={thresholdEnabled}
                onRemove={() => setRemoveTarget(rule)}
                onRemoveMapping={(groupIdx) => handleRemoveMappingGroup(rule.rule_id, groupIdx)}
                onChangeMapping={(groupIdx, slotName, colName) =>
                  handleChangeMapping(rule.rule_id, groupIdx, slotName, colName)
                }
                onAddMapping={(group) => handleAddMapping(rule.rule_id, group)}
                columns={columns}
                forceOpen={expandRuleIds.includes(rule.rule_id)}
                columnTags={Object.keys(columnTags).length > 0 ? columnTags : undefined}
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
          <Pagination
            page={safeRulePage}
            totalItems={visibleMergedRules.length}
            pageSize={RULE_PAGE_SIZE}
            onPageChange={setRulePage}
            className="flex items-center justify-between pt-1"
          />
          {/* Wide dashed "Apply rules" CTA — the primary add affordance in
              the by-rule lens now that the toolbar button is gone (items
              36/51), ported from dqlake's AppliedRulesList bottom CTA. */}
          {canEdit && (
            <button
              type="button"
              onClick={() => openAddDialog()}
              className="mt-2 w-full border border-dashed rounded-lg py-4 text-sm text-muted-foreground hover:text-foreground hover:border-foreground/40 transition-colors flex items-center justify-center gap-1.5"
            >
              <Plus className="h-3.5 w-3.5" />
              {t("monitoredTables.addRulesButton")}
            </button>
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
          columnTags={Object.keys(columnTags).length > 0 ? columnTags : undefined}
          ruleSlotTagsById={ruleSlotTagsById.size > 0 ? ruleSlotTagsById : undefined}
          adminDefault={adminDefaultThreshold}
          onColumnThresholdChange={handleColumnThresholdChange}
          thresholdEnabled={thresholdEnabled}
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
        appliedRuleIds={appliedRuleIds}
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
        appliedRules={stagedRows}
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
// View Data (P22-B) — first 500 rows via OBO + pragmatic ask-a-question.
// Hosted inside the About tab since P26 item 3.
// ---------------------------------------------------------------------------

// Client-side sort over the fetched preview/answer rows — tri-state
// asc/desc/none per the app's header-sort convention (RulesPicker's
// `handleHeaderClick`: click cycles unsorted -> asc -> desc -> unsorted,
// with ChevronUp/ChevronDown shown only on the active column).
function DataGrid({ columns, rows, emptyLabel }: { columns: string[]; rows: Record<string, string | null>[]; emptyLabel: string }) {
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc" | null>(null);

  const handleHeaderClick = (col: string) => {
    if (sortCol !== col) {
      setSortCol(col);
      setSortDir("asc");
      return;
    }
    if (sortDir === "asc") {
      setSortDir("desc");
      return;
    }
    setSortCol(null);
    setSortDir(null);
  };

  const sortedRows = useMemo(() => {
    if (!sortCol || !sortDir) return rows;
    const copy = [...rows];
    copy.sort((a, b) => {
      const av = a[sortCol] ?? "";
      const bv = b[sortCol] ?? "";
      // Numeric comparison when both values parse cleanly, so numeric
      // columns don't sort lexicographically ("10" before "2").
      const an = Number(av);
      const bn = Number(bv);
      let cmp: number;
      if (av !== "" && bv !== "" && !Number.isNaN(an) && !Number.isNaN(bn)) {
        cmp = an - bn;
      } else {
        cmp = av.localeCompare(bv);
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return copy;
  }, [rows, sortCol, sortDir]);

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
            {columns.map((col) => {
              const isSorted = sortCol === col && sortDir !== null;
              return (
                <th
                  key={col}
                  className="text-left p-2 font-medium whitespace-nowrap font-mono cursor-pointer select-none hover:text-foreground"
                  onClick={() => handleHeaderClick(col)}
                  aria-sort={isSorted ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
                >
                  <span className="inline-flex items-center gap-1">
                    {col}
                    {isSorted &&
                      (sortDir === "asc" ? (
                        <ChevronUp className="h-3 w-3" aria-hidden />
                      ) : (
                        <ChevronDown className="h-3 w-3" aria-hidden />
                      ))}
                  </span>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, idx) => (
            <tr key={idx} className="border-b last:border-b-0 hover:bg-muted/30">
              <td className="p-2 tabular-nums text-muted-foreground">{idx + 1}</td>
              {columns.map((col) => (
                <td
                  key={col}
                  className="p-2 max-w-[240px] truncate font-mono"
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

// Composed loading skeleton mirroring the real View Data layout (item 70):
// the ask bar (icon + input + button), a row of ~3 pill chips, and a table
// with a header row and eight body rows — far closer to the eventual content
// than the old flat 256px block.
function ViewDataSkeleton() {
  return (
    <div className="space-y-4">
      <div className="space-y-2 rounded-lg border px-3 py-3">
        <div className="flex items-center gap-2">
          <Skeleton className="h-3.5 w-3.5 shrink-0 rounded-full" />
          <Skeleton className="h-8 flex-1" />
          <Skeleton className="h-8 w-20 shrink-0" />
        </div>
        <div className="flex flex-wrap gap-1.5">
          {[0, 1, 2].map((i) => (
            <Skeleton key={i} className="h-6 w-44 rounded-full" />
          ))}
        </div>
      </div>
      <div className="overflow-hidden rounded-md border">
        <div className="border-b bg-muted/30 p-2">
          <Skeleton className="h-4 w-full" />
        </div>
        <div className="divide-y">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="p-2">
              <Skeleton className="h-4 w-full" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// Rendered inside the About tab since P26 item 3 (below the About/Schema
// card row) — no longer a Radix TabsContent of its own, so no pt-4 here;
// the hosting <section> owns the spacing.
function ViewDataTab({ tableFqn }: { tableFqn: string }) {
  const { t } = useTranslation();
  const previewMutation = usePreviewTableData();
  const queryMutation = useQueryTableData();
  const [preview, setPreview] = useState<TableDataOut | null>(null);
  const [answer, setAnswer] = useState<TableDataOut | null>(null);
  const [question, setQuestion] = useState("");
  // Generated-SQL disclosure — collapsed by default, and re-collapsed on
  // every new answer so each ask starts from the compact one-line header.
  const [sqlOpen, setSqlOpen] = useState(false);
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
        onSuccess: (resp) => {
          setAnswer(resp.data);
          setSqlOpen(false);
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.viewData.queryFailed")), { duration: 6000 }),
      },
    );
  };

  const aiAvailable = preview?.ai_available ?? false;
  const active = answer ?? preview;

  // Schema-aware AI sample questions (Databricks-style chips). Fetched once
  // per table (staleTime Infinity; the query key carries the FQN) and only
  // when the preview said AI is available. The backend returns an empty list
  // on any failure, and `retry: false` keeps errors quiet — either way the
  // static i18n prompts below take over, so the chips never break the panel.
  const sampleQuestionsQuery = useGetSampleQuestions(
    { table_fqn: tableFqn },
    { query: { enabled: aiAvailable, staleTime: Infinity, retry: false } },
  );
  const aiQuestions = sampleQuestionsQuery.data?.data.questions ?? [];
  const chipsLoading = aiAvailable && sampleQuestionsQuery.isLoading;

  const staticSuggestions = useMemo(() => {
    const cols = preview?.columns ?? [];
    const first = cols[0];
    const out = [t("monitoredTables.viewData.suggestCount")];
    if (first) {
      out.push(t("monitoredTables.viewData.suggestNulls", { column: first }));
      out.push(t("monitoredTables.viewData.suggestGroup", { column: first }));
    }
    return out;
  }, [preview, t]);

  const suggestions = aiQuestions.length > 0 ? aiQuestions : staticSuggestions;

  if (previewMutation.isPending && !preview) {
    return <ViewDataSkeleton />;
  }

  if (previewMutation.isError && !preview) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center border border-dashed rounded-lg">
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
    <div className="space-y-4">
      {aiAvailable && (
        // Purple AI gradient motif — matches the Build-with-AI banner
        // conventions (RegistryRuleFormDialog's `buildWithAiBanner`):
        // AI_BANNER_BG/BORDER + the cursor-following `ai-glow-mouse` glow.
        <div
          className={cn(
            "ai-glow-mouse space-y-2 rounded-lg px-3 py-3 shadow-sm",
            AI_BANNER_BG,
            AI_BANNER_BORDER,
          )}
          onMouseMove={(e) => {
            const r = e.currentTarget.getBoundingClientRect();
            e.currentTarget.style.setProperty("--ai-mx", `${e.clientX - r.left}px`);
            e.currentTarget.style.setProperty("--ai-my", `${e.clientY - r.top}px`);
          }}
        >
          <div className="flex items-center gap-2">
            <Sparkles className="h-3.5 w-3.5 shrink-0" stroke={AI_GRADIENT_URL} />
            <Input
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") runQuestion(question);
              }}
              placeholder={t("monitoredTables.viewData.askPlaceholder")}
              disabled={queryMutation.isPending}
              className="h-8 text-xs bg-background/60"
            />
            <Button
              size="sm"
              onClick={() => runQuestion(question)}
              disabled={queryMutation.isPending || !question.trim()}
              className={`gap-1.5 shrink-0 ${AI_BUTTON_BG}`}
            >
              {queryMutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Send className="h-3.5 w-3.5" />}
              {t("monitoredTables.viewData.askButton")}
            </Button>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {/* On hover the chips fill with the same purple AI gradient as
                the Ask button (AI_BUTTON_BG / AI_GRADIENT_FROM_VIA_TO) —
                hover: variants must be literal for Tailwind, so the
                violet→fuchsia→pink stops are repeated here (P25 item 2).

                Gradient clipping (P26 item 1): with `transition-colors`,
                Tailwind v4 also transitions the registered --tw-gradient-*
                stop properties, so the hover gradient repaints every frame
                mid-animation and bled past the pill's rounded corners as
                square edges. The Ask button never has this problem because
                it neither carries a border nor animates its gradient
                (hover:opacity-90 only) — mirror that: scope the transition
                to color/border-color so the gradient snaps on fully formed,
                paint it from the border box (bg-origin-border) so it fills
                flush under the hover-transparent 1px border exactly like a
                borderless gradient pill, and add overflow-hidden so the
                rounded-full radius always clips it. */}
            {chipsLoading
              ? // Subtle pill-shaped placeholders while the AI sample
                // questions load — same footprint as the rendered chips.
                [0, 1, 2].map((i) => <Skeleton key={i} className="h-6 w-44 rounded-full bg-background/60" />)
              : suggestions.map((s) => (
                  <button
                    key={s}
                    type="button"
                    onClick={() => runQuestion(s)}
                    disabled={queryMutation.isPending}
                    className="overflow-hidden rounded-full border bg-origin-border px-2.5 py-1 text-[11px] text-muted-foreground transition-[color,border-color] hover:bg-gradient-to-r hover:from-violet-500 hover:via-fuchsia-500 hover:to-pink-500 hover:text-white hover:border-transparent disabled:pointer-events-none disabled:opacity-50"
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
            {answer.generated_sql ? (
              // Compact one-line disclosure header — collapsed by default,
              // chevron rotates open exactly like AdvancedDisclosure's.
              <button
                type="button"
                onClick={() => setSqlOpen((v) => !v)}
                aria-expanded={sqlOpen}
                className="inline-flex items-center gap-1 text-[11px] font-medium text-muted-foreground uppercase tracking-wide hover:text-foreground transition-colors"
              >
                <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", sqlOpen && "rotate-90")} aria-hidden />
                {t("monitoredTables.viewData.generatedSqlLabel")}
              </button>
            ) : (
              <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">
                {t("monitoredTables.viewData.generatedSqlLabel")}
              </p>
            )}
            <Button size="sm" variant="ghost" onClick={() => setAnswer(null)} className="gap-1.5 h-7 text-xs">
              <ArrowLeft className="h-3.5 w-3.5" />
              {t("monitoredTables.viewData.backToSample")}
            </Button>
          </div>
          {answer.generated_sql && (
            // grid-rows transition trick — same collapse mechanism as
            // AdvancedDisclosure / RuleConfigCard, which animates both open
            // AND closed; opacity fades along with the height sweep.
            <div
              className={cn(
                "grid transition-[grid-template-rows,opacity] duration-200 ease-out",
                sqlOpen ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0",
              )}
            >
              <div className="min-h-0 overflow-hidden">
                <pre className="rounded-md border bg-muted/40 p-3 text-[11px] font-mono overflow-x-auto whitespace-pre-wrap">
                  {answer.generated_sql}
                </pre>
              </div>
            </div>
          )}
        </div>
      )}

      {active && (
        <div className="space-y-1.5">
          <DataGrid
            columns={active.columns}
            rows={active.rows}
            emptyLabel={t("monitoredTables.viewData.noRows")}
          />
          <p className="text-xs text-muted-foreground">
            {t("monitoredTables.viewData.description", { count: TableDataService_PREVIEW_LIMIT })}
          </p>
        </div>
      )}
    </div>
  );
}

// The preview row cap mirrors the backend TableDataService.PREVIEW_LIMIT.
const TableDataService_PREVIEW_LIMIT = 500;
