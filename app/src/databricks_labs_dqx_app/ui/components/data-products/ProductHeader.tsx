/**
 * Ported from dqlake's `products/$productId.tsx`'s `ProductHeader` section.
 * Adapted: no `SyncStatusChip`/`sync_state` concept (DQX runs stay in-app,
 * there is no external Databricks-side reconcile step to poll); RBAC gates
 * edit actions on `canEdit` (RULE_AUTHOR+) and run actions on `canRun`
 * (RUNNER, orthogonal) rather than dqlake's single per-object `can_edit`.
 * A Table Space carries its own submit-for-review lifecycle (P21 item 30):
 * "Submit for review" replaces Publish, and approvers see Approve/Reject
 * top-right when it's pending. Runs is dqlake-exact — it lives in the ⋮ menu
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
import { CheckCircle2, Clock, History, Loader2, MoreVertical, Play, Save, Send, Trash2, XCircle } from "lucide-react";
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
}: {
  productId: string;
  members: DataProductMemberOut[];
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
    setBusyBindingId(member.binding_id);
    approveMutation.mutate(
      { bindingId: member.binding_id },
      {
        onSuccess: () => {
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
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0 text-emerald-600"
                        title={t("monitoredTables.approveAction")}
                        onClick={() => handleApprove(member)}
                      >
                        <CheckCircle2 className="h-3.5 w-3.5" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0 text-destructive"
                        title={t("monitoredTables.rejectAction")}
                        onClick={() => setRejectTarget(member)}
                      >
                        <XCircle className="h-3.5 w-3.5" />
                      </Button>
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
  const canRun = perms.canRunRules;
  const canApprove = perms.canApproveRules;

  const runMut = useRunDataProduct({ mutation: { onError: () => {} } });
  const deleteMut = useDeleteDataProduct({ mutation: { onError: () => {} } });
  const approveMut = useApproveDataProduct({ mutation: { onError: () => {} } });
  const rejectMut = useRejectDataProduct({ mutation: { onError: () => {} } });

  const [deleteOpen, setDeleteOpen] = useState(false);
  const [rejectOpen, setRejectOpen] = useState(false);
  const [busyRun, setBusyRun] = useState(false);
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
  const isPending = product.status === "pending_approval";
  const lifecycleBusy = approveMut.isPending || rejectMut.isPending || editState.submitPending;

  const runnableCount = product.runnable_count ?? 0;

  const invalidateLifecycle = () => {
    queryClient.invalidateQueries({ queryKey: getGetDataProductQueryKey(product.product_id) });
    queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
  };

  const handleApprove = async () => {
    try {
      await approveMut.mutateAsync({ productId: product.product_id });
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

  const goToRuns = () =>
    void navigate({
      to: "/table-spaces/$productId",
      params: { productId: product.product_id },
      search: (prev) => ({ ...prev, tab: "runs" }),
    });

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

  const handleRunDraft = async () => {
    if (editState.isDirty) {
      const ok = await editState.handleSaveDraft();
      if (!ok) return;
    }
    await handleRun(RunDataProductInSource.draft);
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
    <div className="flex items-start justify-between gap-4 border-b pb-4 max-w-5xl">
      <div className="space-y-1">
        <div className="flex items-center gap-2">
          <h1 className="text-xl font-semibold">{product.name}</h1>
          {product.status === "approved" && (
            <Badge>{t("dataProducts.versionBadge", { version: product.version })}</Badge>
          )}
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
        {/* Product-level Approve/Reject — the space's OWN review lifecycle
            (P21 item 30), gated to approvers when it's pending. Distinct from
            the member-table ReviewPendingChangesButton below (P19-I). */}
        {isPending && canApprove && (
          <>
            <Button
              variant="outline"
              size="sm"
              disabled={lifecycleBusy}
              onClick={() => void handleApprove()}
              className="gap-1.5 text-emerald-600 border-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950"
            >
              {approveMut.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <CheckCircle2 className="h-3.5 w-3.5" />}
              {t("dataProducts.approveAction")}
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={lifecycleBusy}
              onClick={() => setRejectOpen(true)}
              className="gap-1.5 text-red-600 border-red-400 hover:bg-red-50 dark:hover:bg-red-950"
            >
              {rejectMut.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <XCircle className="h-3.5 w-3.5" />}
              {t("dataProducts.rejectAction")}
            </Button>
          </>
        )}

        {canApprove && (
          <ReviewPendingChangesButton productId={product.product_id} members={editState.members} />
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
            disabled={editState.submitPending || (submitDisabledNoChanges && !editState.canSave)}
            size="sm"
            className="gap-2"
            title={submitDisabledNoChanges ? t("dataProducts.submitDisabledNoChangesHint") : undefined}
          >
            {editState.submitPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            {t("dataProducts.submitForReviewButton")}
          </Button>
        )}

        {canRun && (
          <Button
            onClick={() => void handleRun(RunDataProductInSource.approved)}
            disabled={runPending || runnableCount === 0}
            size="sm"
            className="gap-2"
            title={runnableCount === 0 ? t("dataProducts.runNowDisabledHint") : undefined}
          >
            {runPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            {runPending ? t("dataProducts.runningLabel") : t("dataProducts.runNowButton")}
          </Button>
        )}

        {/* ⋮ menu — Runs (dqlake-exact, item 29), Run draft, Delete. */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button size="sm" variant="ghost" aria-label={t("dataProducts.actionsMenuLabel")}>
              <MoreVertical className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {/* Run draft at the TOP (item 15): only when the space is a draft
                or has pending edits; those edits are saved first. Disabled +
                tooltip otherwise, matching the app's convention. */}
            {canRun && (
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
            )}
            <DropdownMenuItem onSelect={goToRuns} className="gap-2">
              <History className="h-3.5 w-3.5" />
              {t("dataProducts.tabRuns")}
            </DropdownMenuItem>
            {canEdit && <DropdownMenuSeparator />}
            {canEdit && (
              <DropdownMenuItem
                onSelect={() => setDeleteOpen(true)}
                className={cn("gap-2 text-destructive focus:text-destructive")}
              >
                <Trash2 className="h-3.5 w-3.5" />
                {t("dataProducts.deleteAction")}
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

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
    </div>
  );
}
