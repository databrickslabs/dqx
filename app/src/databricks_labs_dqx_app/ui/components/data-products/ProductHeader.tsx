/**
 * Ported from dqlake's `products/$productId.tsx`'s `ProductHeader` section.
 * Adapted: no `SyncStatusChip`/`sync_state` concept (DQX runs stay in-app,
 * there is no external Databricks-side reconcile step to poll); RBAC gates
 * edit actions on `canEdit` (RULE_AUTHOR+) and run actions on `canRun`
 * (RUNNER, orthogonal) rather than dqlake's single per-object `can_edit`.
 * A Table Space carries its own submit-for-review lifecycle (P21 item 30):
 * "Submit for review" replaces Publish, and approvers see Approve/Reject
 * inside the amber pending-approval banner below the header when it's
 * pending. Runs is dqlake-exact — it lives in the ⋮ menu
 * (item 29) alongside Run draft and Delete, not in the visible tab strip.
 */
import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  useDeleteDataProduct,
  useRunDataProduct,
  useApproveDataProduct,
  useRejectDataProduct,
  useRevertDataProduct,
  useApproveMonitoredTable,
  useRejectMonitoredTable,
  RunDataProductInSource,
  getGetDataProductQueryKey,
  getListDataProductsQueryKey,
  getGetMonitoredTableQueryKey,
  getListMonitoredTablesQueryKey,
  type DataProductOut,
  type DataProductMemberOut,
} from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";
import { useApprovalsMode } from "@/hooks/use-approvals-mode";
import { isRunStale, useRequireDraftRunBeforeSubmit } from "@/hooks/use-require-draft-run";
import { useProductRunSets } from "@/hooks/use-product-run-sets";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { CheckCircle2, Clock, Download, GitCompare, History, Loader2, MessageSquare, MoreVertical, Play, Save, Send, Trash2, Undo2, XCircle } from "lucide-react";
import { CommentsDialog } from "@/components/CommentThread";
import { TableSpaceDiffDialog, type TableSpaceDiffTarget } from "@/components/drafts/ChangeDiffDialog";
import { exportDataProduct } from "@/lib/api-custom";
import { ExportDialog } from "@/components/ExportDialog";
import { cn } from "@/lib/utils";
import type { EditProductState } from "@/components/data-products/useEditProductState";

/**
 * Products have no approver gate of their own (draft/publish only, by
 * design — spec §3.3): "approve/reject the product's pending changes"
 * really means approving/rejecting the pending MATERIALIZED checks on its
 * member monitored tables. This surfaces that as a single top-right
 * affordance — consistent with the Approve/Reject buttons now on the
 * Registry Rule and Monitored Table detail headers — instead of forcing the
 * approver to open each pending member individually.
 */
function ReviewPendingChangesButton({
  productId,
  members,
  editState,
}: {
  productId: string;
  members: DataProductMemberOut[];
  editState: EditProductState;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  const pendingMembers = useMemo(
    () => members.filter((m) => m.binding_status === "pending_approval"),
    [members],
  );

  const approveMutation = useApproveMonitoredTable();
  const rejectMutation = useRejectMonitoredTable();
  const [busyBindingId, setBusyBindingId] = useState<string | null>(null);
  const [rejectTarget, setRejectTarget] = useState<DataProductMemberOut | null>(null);
  // Controlled so the popover can stay open (showing an empty state) after
  // the last pending member resolves, instead of the trigger button — and
  // the popover along with it — vanishing out from under the user mid-review.
  const [popoverOpen, setPopoverOpen] = useState(false);

  const invalidateAfterAction = (bindingId: string) => {
    queryClient.invalidateQueries({ queryKey: getGetMonitoredTableQueryKey(bindingId) });
    queryClient.invalidateQueries({ queryKey: getListMonitoredTablesQueryKey() });
    queryClient.invalidateQueries({ queryKey: getGetDataProductQueryKey(productId) });
    queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
  };

  const handleApprove = (member: DataProductMemberOut) => {
    if (busyBindingId) return;
    // Captured before the mutation: only suppress the nav guard for the
    // approve-triggered refetch window when there were no unsaved edits to
    // begin with, so a genuine unsaved edit still warns (B2-66).
    const wasClean = !editState.isDirty;
    setBusyBindingId(member.binding_id);
    approveMutation.mutate(
      { bindingId: member.binding_id },
      {
        onSuccess: () => {
          if (wasClean) editState.markApprovedWhenClean();
          toast.success(t("dataProducts.reviewChangesToastApproved", { table: member.table_fqn }));
          invalidateAfterAction(member.binding_id);
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.toastApproveFailed")), {
            duration: 6000,
          });
        },
        onSettled: () => setBusyBindingId(null),
      },
    );
  };

  const handleConfirmReject = () => {
    const member = rejectTarget;
    if (!member) return;
    setRejectTarget(null);
    setBusyBindingId(member.binding_id);
    rejectMutation.mutate(
      { bindingId: member.binding_id },
      {
        onSuccess: () => {
          toast.success(t("dataProducts.reviewChangesToastRejected", { table: member.table_fqn }));
          invalidateAfterAction(member.binding_id);
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.toastRejectFailed")), {
            duration: 6000,
          });
        },
        onSettled: () => setBusyBindingId(null),
      },
    );
  };

  // Hide only when the popover is closed and there's nothing to review —
  // if it's open (even with zero pending members left, right after the
  // last one resolved) we keep the trigger mounted so the popover doesn't
  // get yanked out from under the user; see the empty state below.
  if (pendingMembers.length === 0 && !popoverOpen) return null;

  return (
    <>
      <Popover open={popoverOpen} onOpenChange={setPopoverOpen}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            size="sm"
            className="gap-2 text-amber-700 border-amber-400 hover:bg-amber-50 dark:text-amber-300 dark:hover:bg-amber-950"
          >
            <Clock className="h-4 w-4" />
            {pendingMembers.length > 0
              ? t("dataProducts.reviewChangesButton", { count: pendingMembers.length })
              : t("dataProducts.reviewChangesEmptyState")}
          </Button>
        </PopoverTrigger>
        <PopoverContent align="end" className="w-96">
          <div className="space-y-1 mb-2">
            <p className="text-sm font-medium">{t("dataProducts.reviewChangesTitle")}</p>
            <p className="text-xs text-muted-foreground">{t("dataProducts.reviewChangesDescription")}</p>
          </div>
          {pendingMembers.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4 text-center">
              {t("dataProducts.reviewChangesEmptyState")}
            </p>
          ) : (
          <div className="space-y-1 max-h-72 overflow-y-auto">
            {pendingMembers.map((member) => {
              const busy = busyBindingId === member.binding_id;
              return (
                <div
                  key={member.binding_id}
                  className="flex items-center justify-between gap-2 rounded-md border p-2"
                >
                  <Link
                    to="/monitored-tables/$bindingId"
                    params={{ bindingId: member.binding_id }}
                    search={{ tab: "results" }}
                    className="font-mono text-xs truncate hover:underline"
                    title={member.table_fqn}
                  >
                    {member.table_fqn}
                  </Link>
                  {busy ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground shrink-0" />
                  ) : (
                    <div className="flex items-center gap-1 shrink-0">
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-emerald-600"
                            aria-label={t("monitoredTables.approveAction")}
                            onClick={() => handleApprove(member)}
                          >
                            <CheckCircle2 className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("monitoredTables.approveAction")}</TooltipContent>
                      </Tooltip>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-destructive"
                            aria-label={t("monitoredTables.rejectAction")}
                            onClick={() => setRejectTarget(member)}
                          >
                            <XCircle className="h-3.5 w-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>{t("monitoredTables.rejectAction")}</TooltipContent>
                      </Tooltip>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          )}
        </PopoverContent>
      </Popover>

      <AlertDialog open={!!rejectTarget} onOpenChange={(open) => !open && setRejectTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("monitoredTables.rejectConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.rejectConfirmDescription", { table: rejectTarget?.table_fqn ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={handleConfirmReject}
            >
              {t("monitoredTables.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

/** `vN` when the space's current state matches its last-approved snapshot;
 *  "Modified since vN" when there are unpublished edits on top of an older
 *  approved snapshot (`display_status === "modified"`); nothing at v0
 *  (never approved). Mirrors the Monitored Table / Rules Registry detail
 *  headers' version badge composition (P24 item 1). */
function TableSpaceVersionBadge({ product }: { product: DataProductOut }) {
  const { t } = useTranslation();
  const version = product.version ?? 0;
  if (version <= 0) return null;
  if (product.display_status === "modified") {
    return (
      <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
        {t("dataProducts.modifiedSinceVersion", { version })}
      </Badge>
    );
  }
  return (
    <Badge variant="secondary" className="font-mono text-[10px]">
      {t("dataProducts.versionBadge", { version })}
    </Badge>
  );
}

interface Props {
  product: DataProductOut;
  canEdit: boolean;
  editState: EditProductState;
}

export function ProductHeader({ product, canEdit, editState }: Props) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const perms = usePermissions();
  const { willAutoApprove } = useApprovalsMode();
  const requireDraftRun = useRequireDraftRunBeforeSubmit();
  const canRun = perms.canRunRules;
  const canApprove = perms.canApproveRules;

  const runMut = useRunDataProduct({ mutation: { onError: () => {} } });
  const deleteMut = useDeleteDataProduct({ mutation: { onError: () => {} } });
  const approveMut = useApproveDataProduct({ mutation: { onError: () => {} } });
  const rejectMut = useRejectDataProduct({ mutation: { onError: () => {} } });
  const revertMut = useRevertDataProduct({ mutation: { onError: () => {} } });

  const [deleteOpen, setDeleteOpen] = useState(false);
  const [rejectOpen, setRejectOpen] = useState(false);
  const [commentsOpen, setCommentsOpen] = useState(false);
  const [diffTarget, setDiffTarget] = useState<TableSpaceDiffTarget | null>(null);
  const [busyRun, setBusyRun] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  // Bridges the gap between a successful submit and the next 4s poll
  // catching the new RUNNING run set, so the button doesn't flash back to
  // "Run now" for a moment after submission.
  const [justSubmitted, setJustSubmitted] = useState(false);
  // Shares its `listRunSets` query with `ProductRunsTab` (identical params)
  // so the two don't each poll the endpoint independently.
  const { hasActive } = useProductRunSets(product.product_id, { extraPoll: justSubmitted });
  useEffect(() => {
    if (hasActive) setJustSubmitted(false);
  }, [hasActive]);

  // Safety net: a submit can return 200 without ever producing an active run
  // set (e.g. the backend accepted the request but every member failed to
  // launch, so the run set was rolled back). Without this, `justSubmitted`
  // would keep the button on "Running…" forever. Clear it if no active run
  // set confirms within 30s so the button always returns to a terminal state.
  useEffect(() => {
    if (!justSubmitted) return;
    const id = window.setTimeout(() => setJustSubmitted(false), 30_000);
    return () => window.clearTimeout(id);
  }, [justSubmitted]);

  const runPending = busyRun || hasActive || justSubmitted;

  // Nothing to submit: no staged edits AND already approved — mirrors the
  // monitored-table "Submit for review" disabled-no-changes guard so the
  // button doesn't imply there's something to re-submit when there isn't.
  const submitDisabledNoChanges = !editState.isDirty && product.status === "approved";
  // Require-draft-run gate (issues B2-12 / B2-118): block submit until a draft
  // run has been recorded for this space AND that run is newer than the last
  // edit. ``last_run_at`` (excludes preview / in-flight runs, derived from the
  // members' runs) and ``updated_at`` (bumped on every membership / config
  // edit, which flips the space back to draft) are both on the product payload.
  // A run older than ``updated_at`` is stale — the space changed since it ran.
  // The backend enforces the authoritative check (409) regardless.
  const staleDraftRun = isRunStale(product.last_run_at, product.updated_at);
  const needsDraftRun = requireDraftRun && (!product.last_run_at || staleDraftRun);
  const isPending = product.status === "pending_approval";
  const lifecycleBusy = approveMut.isPending || rejectMut.isPending || revertMut.isPending || editState.submitPending;

  const runnableCount = product.runnable_count ?? 0;

  const invalidateLifecycle = () => {
    queryClient.invalidateQueries({ queryKey: getGetDataProductQueryKey(product.product_id) });
    queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
  };

  const handleApprove = async () => {
    // See ReviewPendingChangesButton.handleApprove: only bypass the guard for
    // the post-approve refetch window when nothing was unsaved (B2-66).
    const wasClean = !editState.isDirty;
    try {
      await approveMut.mutateAsync({ productId: product.product_id });
      if (wasClean) editState.markApprovedWhenClean();
      toast.success(t("dataProducts.toastApproved"));
      invalidateLifecycle();
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastApproveFailed")), { duration: 6000 });
    }
  };

  const handleReject = async () => {
    try {
      await rejectMut.mutateAsync({ productId: product.product_id });
      toast.success(t("dataProducts.toastRejected"));
      invalidateLifecycle();
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastRejectFailed")), { duration: 6000 });
    }
  };

  // Withdraw a pending submission back to draft — the author's counterpart to
  // submit (reject is the approver's decision). Leaves no rejected audit trail.
  const handleRevert = async () => {
    try {
      await revertMut.mutateAsync({ productId: product.product_id });
      toast.success(t("dataProducts.toastReverted"));
      invalidateLifecycle();
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastRevertFailed")), { duration: 6000 });
    }
  };

  // Deep-link into the global Runs History, pre-filtered to this table
  // space's member tables (Stream I #17). The in-page Runs tab is reachable
  // from the tab bar, so the ⋮ menu only offers this global-history link
  // (matching the Monitored Table detail menu — avoids a duplicate "Runs").
  const goToRunsHistory = () =>
    void navigate({ to: "/runs-history", search: { productId: product.product_id } });

  const handleRun = async (source: (typeof RunDataProductInSource)[keyof typeof RunDataProductInSource]) => {
    setBusyRun(true);
    try {
      const resp = await runMut.mutateAsync({ productId: product.product_id, data: { source } });
      // The run endpoint returns 200 even when EVERY member failed to launch
      // (their failures collected into `skipped`, `submitted` left empty, and
      // the empty run set rolled back). That is not a started run, so treat an
      // empty `submitted` as a failure: surface it and do NOT enter the
      // "Running…" bridge state, which would otherwise stick forever since no
      // active run set will ever confirm it.
      if ((resp.data.submitted?.length ?? 0) === 0) {
        toast.error(t("dataProducts.toastRunNoneStarted"), { duration: 6000 });
        return;
      }
      setJustSubmitted(true);
      toast.success(t("dataProducts.toastRunStarted"));
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastRunFailed")), { duration: 6000 });
    } finally {
      setBusyRun(false);
    }
  };

  // Run draft is available only when there is a draft to run: either the
  // space is a draft, or there are pending edits Save-as-draft would persist
  // (item 15). It also needs at least one member. When there are pending
  // edits, they are SAVED first (so the draft run reflects them) and a save
  // failure is surfaced without running.
  const canRunDraft = (product.status === "draft" || editState.isDirty) && editState.members.length > 0;
  // When there's a draft to run, Run draft is the PRIMARY button and Run now
  // (approved) is demoted into the ⋮ menu; otherwise Run now is primary and
  // Run draft is demoted. Draft wins as primary when both exist (item 59).
  const draftIsPrimary = canRunDraft;

  const handleRunDraft = async () => {
    // Spans the whole save-then-run sequence, not just the run mutation, so a
    // fast double-click can't fire a second save while the first is still in
    // flight (the save leg predates `handleRun`'s own `busyRun` toggle).
    setBusyRun(true);
    try {
      if (editState.isDirty) {
        const ok = await editState.handleSaveDraft();
        if (!ok) return;
      }
      await handleRun(RunDataProductInSource.draft);
    } finally {
      setBusyRun(false);
    }
  };

  const handleDelete = async () => {
    try {
      await deleteMut.mutateAsync({ productId: product.product_id });
      toast.success(t("dataProducts.toastDeleted"));
      void navigate({ to: "/table-spaces" });
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastDeleteFailed")), { duration: 6000 });
    }
  };

  return (
    <div className="space-y-4 border-b pb-4 max-w-5xl">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <h1 className="text-xl font-semibold">{product.name}</h1>
            <TableSpaceVersionBadge product={product} />
            {isPending && (
              <Badge variant="outline" className="border-amber-500 text-amber-600">
                {t("dataProducts.statusPendingApproval")}
              </Badge>
            )}
            {product.status === "rejected" && (
              <Badge variant="outline" className="border-red-500 text-red-600">
                {t("dataProducts.statusRejected")}
              </Badge>
            )}
          </div>
        </div>
        <div className="flex gap-2 items-center">
          {canApprove && (
            <ReviewPendingChangesButton
              productId={product.product_id}
              members={editState.members}
              editState={editState}
            />
          )}

          {canEdit && (
            <Button
              onClick={() => void editState.handleSaveDraft()}
              disabled={!editState.canSave || editState.savePending}
              variant="outline"
              size="sm"
              className="gap-2"
            >
              {editState.savePending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              {t("dataProducts.saveAsDraftButton")}
            </Button>
          )}

          {canEdit && (
            <Button
              onClick={() => void editState.handleSubmit()}
              disabled={editState.submitPending || needsDraftRun || (submitDisabledNoChanges && !editState.canSave)}
              size="sm"
              className="gap-2"
              title={
                needsDraftRun
                  ? staleDraftRun
                    ? t("dataProducts.submitDisabledStaleDraftRunHint")
                    : t("dataProducts.submitDisabledNeedsDraftRunHint")
                  : submitDisabledNoChanges
                    ? t("dataProducts.submitDisabledNoChangesHint")
                    : undefined
              }
            >
              {editState.submitPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              {willAutoApprove
                ? t("dataProducts.saveAndPublishButton")
                : t("dataProducts.submitForReviewButton")}
            </Button>
          )}

          {canRun &&
            (draftIsPrimary ? (
              <Button
                onClick={() => void handleRunDraft()}
                disabled={runPending}
                size="sm"
                className="gap-2"
                title={hasActive ? t("dataProducts.runInProgressHint") : undefined}
              >
                {runPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {runPending ? t("dataProducts.runningLabel") : t("dataProducts.runDraftAction")}
              </Button>
            ) : (
              <Button
                onClick={() => void handleRun(RunDataProductInSource.approved)}
                disabled={runPending || runnableCount === 0}
                size="sm"
                className="gap-2"
                title={
                  runnableCount === 0
                    ? t("dataProducts.runNowDisabledHint")
                    : hasActive
                      ? t("dataProducts.runInProgressHint")
                      : undefined
                }
              >
                {runPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {runPending ? t("dataProducts.runningLabel") : t("dataProducts.runNowButton")}
              </Button>
            ))}

          {/* ⋮ menu — Export (DQX / ODCS), Runs (dqlake-exact, item 29),
              Run draft, Delete. Export moved here off a standalone button. */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button size="sm" variant="ghost" aria-label={t("dataProducts.actionsMenuLabel")}>
                <MoreVertical className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem
                onSelect={(e) => {
                  e.preventDefault();
                  setExportOpen(true);
                }}
                className="gap-2"
              >
                <Download className="h-3.5 w-3.5" />
                {t("exportYaml.button")}…
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              {/* The run action NOT shown as the primary button is demoted here
                  at the top of the ⋮ menu (item 59). Both save-then-run (draft)
                  and its disabled/tooltip conventions are preserved. */}
              {canRun &&
                (draftIsPrimary ? (
                  <TooltipProvider delayDuration={200}>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className={cn(runnableCount === 0 && "cursor-not-allowed")}>
                          <DropdownMenuItem
                            onSelect={(e) => {
                              e.preventDefault();
                              void handleRun(RunDataProductInSource.approved);
                            }}
                            disabled={runPending || runnableCount === 0}
                            className="gap-2"
                          >
                            <Play className="h-3.5 w-3.5" />
                            {t("dataProducts.runNowApprovedOption")}
                          </DropdownMenuItem>
                        </span>
                      </TooltipTrigger>
                      {runnableCount === 0 && (
                        <TooltipContent side="left">{t("dataProducts.runNowDisabledHint")}</TooltipContent>
                      )}
                    </Tooltip>
                  </TooltipProvider>
                ) : (
                  <TooltipProvider delayDuration={200}>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className={cn(!canRunDraft && "cursor-not-allowed")}>
                          <DropdownMenuItem
                            onSelect={(e) => {
                              e.preventDefault();
                              void handleRunDraft();
                            }}
                            disabled={runPending || !canRunDraft}
                            className="gap-2"
                          >
                            <Play className="h-3.5 w-3.5" />
                            {t("dataProducts.runDraftAction")}
                          </DropdownMenuItem>
                        </span>
                      </TooltipTrigger>
                      {!canRunDraft && (
                        <TooltipContent side="left">{t("dataProducts.runDraftDisabledHint")}</TooltipContent>
                      )}
                    </Tooltip>
                  </TooltipProvider>
                ))}
              <DropdownMenuItem onSelect={goToRunsHistory} className="gap-2">
                <History className="h-3.5 w-3.5" />
                {t("runsHistory.menuViewRuns")}
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={() => setCommentsOpen(true)} className="gap-2">
                <MessageSquare className="h-3.5 w-3.5" />
                {t("dataProducts.actionComments")}
              </DropdownMenuItem>
              {canEdit && <DropdownMenuSeparator />}
              {canEdit && (
                <DropdownMenuItem onSelect={() => setDeleteOpen(true)} variant="destructive" className="gap-2">
                  <Trash2 className="h-3.5 w-3.5" />
                  {t("dataProducts.deleteAction")}
                </DropdownMenuItem>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
          <ExportDialog
            open={exportOpen}
            onOpenChange={setExportOpen}
            fetchDqx={() => exportDataProduct(product.product_id, "dqx")}
            fetchOdcs={() => exportDataProduct(product.product_id, "odcs")}
          />
        </div>
      </div>

      {/* Product-level Approve/Reject — the space's OWN review lifecycle
          (P21 item 30). The buttons live INSIDE the pending-approval banner
          (not the header action row) so the review decision reads in the same
          place that explains why the space is waiting. Distinct from the
          member-table ReviewPendingChangesButton above (P19-I). Buttons are
          gated to approvers; everyone else sees the banner text only. */}
      {isPending && (
        <div className="flex items-start gap-3 p-3 rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-700">
          <Clock className="h-4 w-4 text-amber-600 shrink-0 mt-0.5" />
          <div className="flex flex-1 flex-wrap items-center gap-x-4 gap-y-2">
            <div className="flex-1 min-w-[16rem] space-y-1">
              <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                {t("dataProducts.pendingBannerTitle")}
              </p>
              <p className="text-sm text-amber-800/90 dark:text-amber-300/90">
                {t("dataProducts.pendingBannerBody")}
              </p>
              {!canApprove && (
                <p className="text-xs text-amber-700/80 dark:text-amber-300/70 italic">
                  {t("dataProducts.awaitingApproval")}
                </p>
              )}
            </div>
            <div className="flex items-center gap-2 ml-auto shrink-0">
              {/* View changes — read-only diff of the space's proposed member
                  set, matching the overview row's GitCompare action. */}
              <Button
                variant="outline"
                size="sm"
                disabled={lifecycleBusy}
                onClick={() => setDiffTarget({ productId: product.product_id, name: product.name })}
                className="gap-1.5 h-7 text-xs text-purple-700 border-purple-300 hover:bg-purple-50 dark:text-purple-300 dark:border-purple-700 dark:hover:bg-purple-950"
              >
                <GitCompare className="h-3.5 w-3.5" />
                {t("rulesDrafts.diff.viewChanges")}
              </Button>
              {/* Revert — the author withdraws their own pending submission back
                  to draft (authors-and-above; backend enforces the transition). */}
              {canEdit && (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={lifecycleBusy}
                  onClick={() => void handleRevert()}
                  className="gap-1.5 h-7 text-xs text-amber-700 border-amber-400 hover:bg-amber-100 dark:text-amber-300 dark:hover:bg-amber-900"
                >
                  {revertMut.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Undo2 className="h-3.5 w-3.5" />}
                  {t("dataProducts.revertAction")}
                </Button>
              )}
              {canApprove && (
                <>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={lifecycleBusy}
                    onClick={() => void handleApprove()}
                    className="gap-1.5 h-7 text-xs text-emerald-700 border-emerald-400 hover:bg-emerald-50 dark:text-emerald-300 dark:hover:bg-emerald-950"
                  >
                    {approveMut.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <CheckCircle2 className="h-3.5 w-3.5" />}
                    {t("dataProducts.approveAction")}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={lifecycleBusy}
                    onClick={() => setRejectOpen(true)}
                    className="gap-1.5 h-7 text-xs text-red-700 border-red-400 hover:bg-red-50 dark:text-red-300 dark:hover:bg-red-950"
                  >
                    {rejectMut.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <XCircle className="h-3.5 w-3.5" />}
                    {t("dataProducts.rejectAction")}
                  </Button>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      <AlertDialog open={rejectOpen} onOpenChange={setRejectOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.rejectConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("dataProducts.rejectConfirmDescription", { name: product.name })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={() => {
                setRejectOpen(false);
                void handleReject();
              }}
            >
              {t("dataProducts.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.deleteConfirmDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteMut.isPending}>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className={cn("bg-destructive text-white hover:bg-destructive/90")}
              disabled={deleteMut.isPending}
              onClick={(e) => {
                e.preventDefault();
                setDeleteOpen(false);
                void handleDelete();
              }}
            >
              {t("dataProducts.deleteAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <TableSpaceDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

      <CommentsDialog
        entityType="data_product"
        entityId={product.product_id}
        open={commentsOpen}
        onOpenChange={setCommentsOpen}
      />
    </div>
  );
}
