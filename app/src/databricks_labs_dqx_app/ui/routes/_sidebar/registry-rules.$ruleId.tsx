import { createFileRoute, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { Suspense, useCallback, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
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
import {
  AlertCircle,
  Braces,
  CheckCircle2,
  Download,
  Loader2,
  MessageSquare,
  MoreVertical,
  RotateCcw,
  Table2,
  Trash2,
  Undo2,
  XCircle,
} from "lucide-react";
import { CommentsDialog } from "@/components/CommentThread";
import {
  useGetRegistryRuleSuspense,
  getGetRegistryRuleQueryKey,
  useDeleteRegistryRule,
  useApproveRegistryRule,
  useRejectRegistryRule,
  useRevokeRegistryRule,
} from "@/lib/api";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import type { User as UserType } from "@/lib/api";
import { useLabelDefinitions, exportRegistryRule } from "@/lib/api-custom";
import { ExportDialog } from "@/components/ExportDialog";
import { usePermissions } from "@/hooks/use-permissions";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import {
  RegistryRuleFormDialog,
  type PageTab,
} from "@/components/RegistryRuleFormDialog";
import { ApplyRuleModal } from "@/components/registry-rules/ApplyRuleModal";
import { RegistryRuleJsonDialog } from "@/components/registry-rules/RegistryRuleJsonDialog";
import { StatusBadge, ModifiedBadge, RuleVersionBadge, getTag, RESERVED_NAME_KEY } from "@/components/RegistryRuleBadges";
import { invalidateAfterRegistryRuleApprovalChange } from "@/lib/registry-rule-invalidation";
import { cn } from "@/lib/utils";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

export const Route = createFileRoute("/_sidebar/registry-rules/$ruleId")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={RegistryRuleDetailError}>
          <Suspense fallback={<RegistryRuleDetailSkeleton />}>
            <RegistryRuleDetailPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function RegistryRuleDetailError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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

function RegistryRuleDetailSkeleton() {
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

function RegistryRuleDetailPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { ruleId } = useParams({ from: "/_sidebar/registry-rules/$ruleId" });
  const { tab } = useSearch({ from: "/_sidebar/registry-rules/$ruleId" });

  const { data } = useGetRegistryRuleSuspense(ruleId);
  const rule = data.data.rule;
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);

  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [rejectConfirmOpen, setRejectConfirmOpen] = useState(false);
  const [applyModalOpen, setApplyModalOpen] = useState(false);
  const [commentsOpen, setCommentsOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  // Read-only view / "save as new draft" clone dialog (rule isn't editable
  // in place — see `RegistryRuleJsonDialog`).
  const [jsonDialogOpen, setJsonDialogOpen] = useState(false);
  // "As JSON" edit-in-place dialog — applies straight into the form below
  // rather than persisting (P24-C item 11). Only meaningful when `canEdit`;
  // drives `RegistryRuleFormDialog`'s controlled `jsonDialogOpen` prop.
  const [formJsonDialogOpen, setFormJsonDialogOpen] = useState(false);
  const [isDirty, setIsDirty] = useState(false);
  // Set right before a successful save navigates us away, so the guard
  // doesn't fire a spurious "unsaved changes" prompt on our own redirect.
  const justSavedRef = useRef(false);

  const invalidateDetail = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getGetRegistryRuleQueryKey(ruleId) }),
    [queryClient, ruleId],
  );

  // Approving/rejecting can re-materialize monitored tables the rule is
  // applied to server-side, so this invalidates the registry rules list
  // (which also feeds the Drafts & Review queue), the monitored-tables
  // list, and every monitored-table detail/versions query — see helper
  // doc for why we can't target specific binding IDs here.
  const invalidateAfterLifecycleChange = useCallback(
    () => invalidateAfterRegistryRuleApprovalChange(queryClient),
    [queryClient],
  );

  const backToList = useCallback(
    () => navigate({ to: "/registry-rules" }),
    [navigate],
  );

  const { blocker } = useUnsavedGuard({ hasUnsavedChanges: isDirty, bypassRef: justSavedRef });

  const deleteMutation = useDeleteRegistryRule();
  const handleConfirmDelete = useCallback(() => {
    setDeleteConfirmOpen(false);
    deleteMutation.mutate(
      { ruleId },
      {
        onSuccess: () => {
          toast.success(t("rulesRegistry.toastDeleted"));
          backToList();
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("rulesRegistry.toastDeleteFailed")), { duration: 6000 });
        },
      },
    );
  }, [deleteMutation, ruleId, t, backToList]);

  const approveMutation = useApproveRegistryRule();
  const rejectMutation = useRejectRegistryRule();
  const revokeMutation = useRevokeRegistryRule();
  const lifecycleBusy = approveMutation.isPending || rejectMutation.isPending || revokeMutation.isPending;

  const handleApprove = useCallback(() => {
    approveMutation.mutate(
      { ruleId },
      {
        onSuccess: () => {
          toast.success(t("rulesRegistry.toastApproved"));
          invalidateDetail();
          invalidateAfterLifecycleChange();
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("rulesRegistry.toastApproveFailed")), { duration: 6000 });
        },
      },
    );
  }, [approveMutation, ruleId, t, invalidateDetail, invalidateAfterLifecycleChange]);

  const handleConfirmReject = useCallback(() => {
    setRejectConfirmOpen(false);
    rejectMutation.mutate(
      { ruleId },
      {
        onSuccess: () => {
          toast.success(t("rulesRegistry.toastRejected"));
          invalidateDetail();
          invalidateAfterLifecycleChange();
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("rulesRegistry.toastRejectFailed")), { duration: 6000 });
        },
      },
    );
  }, [rejectMutation, ruleId, t, invalidateDetail, invalidateAfterLifecycleChange]);

  const canRevokeSubmission =
    rule.status === "pending_approval" &&
    (perms.canApproveRules ||
      (perms.canCreateRules &&
        currentUserEmail &&
        (rule.updated_by ?? rule.created_by ?? "").toLowerCase() === currentUserEmail.toLowerCase()));

  const handleRevoke = useCallback(() => {
    revokeMutation.mutate(
      { ruleId },
      {
        onSuccess: () => {
          toast.success(t("rulesRegistry.toastRevoked"));
          invalidateDetail();
          invalidateAfterLifecycleChange();
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("rulesRegistry.toastRevokeFailed")), { duration: 6000 });
        },
      },
    );
  }, [revokeMutation, ruleId, t, invalidateDetail, invalidateAfterLifecycleChange]);

  const handleActiveTabChange = useCallback(
    (nextTab: PageTab) => {
      navigate({
        to: "/registry-rules/$ruleId",
        params: { ruleId },
        search: (prev) => ({ ...prev, tab: nextTab }),
      });
    },
    [navigate, ruleId],
  );

  // Draft AND approved rules are editable in place (RegistryService's
  // EDITABLE_STATUSES). Editing an approved rule is the edit-in-place
  // REVISION path: the live definition changes but the frozen vN
  // dq_rule_versions snapshot keeps serving everywhere until the revision is
  // re-submitted and re-approved as vN+1 — the rule reads "Modified since vN"
  // meanwhile. pending_approval / rejected / deprecated are not editable; for
  // those (and any rule) the "View / edit JSON" action can still clone the
  // rule into a fresh draft to author independently, without touching the
  // original (the standalone "Duplicate" menu item was removed — P24-C item
  // 12 — the clone endpoint stays, reached only via that JSON flow now).
  const canEdit = (rule.status === "draft" || rule.status === "approved") && perms.canCreateRules;
  const canDuplicate = perms.canCreateRules;
  const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;

  // The backend deleteRegistryRule route allows admin/approver/author
  // (create_rules). This menu deliberately restricts delete further to
  // approver/admin — deleting from the detail page (any status, including
  // published rules) is a heavier action than the list page's draft-only
  // delete, which stays scoped to canCreateRules.
  const canDelete = perms.canApproveRules;
  // Surfaces the same Approve/Reject actions the Drafts & Review queue
  // offers, right in the detail header, so an approver doesn't have to
  // leave the rule's page to act on it.
  const canApproveReject = rule.status === "pending_approval" && perms.canApproveRules;
  // Apply requires a published rule — the backend rejects a non-approved
  // rule with 409 (RuleNotPublishedError) — plus create-rule permission.
  const canApply = perms.canCreateRules && rule.status === "approved";
  // "View / edit JSON" is always offered (read-only when the viewer can't
  // edit), so the actions menu is always shown once any menu item applies.
  const showActionsMenu = true;

  // When editable in place, "View / edit JSON" opens the apply-to-form
  // dialog hosted inside `RegistryRuleFormDialog` (P24-C item 11) — the user
  // still saves via the normal Save/Submit buttons below. Otherwise it opens
  // the read-only/"save as new draft" dialog.
  const handleOpenJsonDialog = useCallback(() => {
    if (canEdit) {
      setFormJsonDialogOpen(true);
    } else {
      setJsonDialogOpen(true);
    }
  }, [canEdit]);

  // Approve/Reject + the "…" actions menu. Passed DOWN into
  // RegistryRuleFormDialog's page-variant header so they render inline,
  // immediately after the Save/Submit buttons, in ONE top-right action row —
  // matching the MT/TS headers where the buttons and the ⋮ menu sit together
  // (B2-7). Save/Submit stay owned by the form (their enabled/disabled state
  // is deep in its edit state); these route-owned controls just join them.
  const headerActions = (
    <>
      {canApproveReject && (
        <>
          <Button
            variant="outline"
            size="sm"
            className="gap-2 h-8 text-emerald-600 border-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950"
            onClick={handleApprove}
            disabled={lifecycleBusy}
          >
            {approveMutation.isPending ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <CheckCircle2 className="h-3.5 w-3.5" />
            )}
            {t("rulesRegistry.actionApprove")}
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-2 h-8 text-red-600 border-red-400 hover:bg-red-50 dark:hover:bg-red-950"
            onClick={() => setRejectConfirmOpen(true)}
            disabled={lifecycleBusy}
          >
            {rejectMutation.isPending ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <XCircle className="h-3.5 w-3.5" />
            )}
            {t("rulesRegistry.actionReject")}
          </Button>
        </>
      )}
      {canRevokeSubmission && !canApproveReject && (
        <Button
          variant="outline"
          size="sm"
          className="gap-2 h-8 text-amber-600 border-amber-400 hover:bg-amber-50 dark:hover:bg-amber-950"
          onClick={handleRevoke}
          disabled={lifecycleBusy}
        >
          {revokeMutation.isPending ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Undo2 className="h-3.5 w-3.5" />
          )}
          {t("rulesRegistry.actionRevoke")}
        </Button>
      )}
      {showActionsMenu && (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0"
              aria-label={t("rulesRegistry.actionsMenuLabel")}
            >
              <MoreVertical className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {canApply && (
              <DropdownMenuItem onClick={() => setApplyModalOpen(true)} className="gap-2">
                <Table2 className="h-3.5 w-3.5" />
                {t("rulesRegistry.actionApplyToTables")}
              </DropdownMenuItem>
            )}
            <DropdownMenuItem onClick={() => setExportOpen(true)} className="gap-2">
              <Download className="h-3.5 w-3.5" />
              {t("exportYaml.button")}…
            </DropdownMenuItem>
            <DropdownMenuItem onClick={handleOpenJsonDialog} className="gap-2">
              <Braces className="h-3.5 w-3.5" />
              {t("rulesRegistry.actionViewJson")}
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => setCommentsOpen(true)} className="gap-2">
              <MessageSquare className="h-3.5 w-3.5" />
              {t("rulesRegistry.actionComments")}
            </DropdownMenuItem>
            {canDelete && (
              <>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  onClick={() => setDeleteConfirmOpen(true)}
                  variant="destructive"
                  className="gap-2"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                  {t("rulesRegistry.actionDelete")}
                </DropdownMenuItem>
              </>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      )}
      <ExportDialog
        open={exportOpen}
        onOpenChange={setExportOpen}
        fetchDqx={() => exportRegistryRule(ruleId)}
      />
    </>
  );

  // Name + status + version title line — matches the Monitored Table /
  // Data Product detail headers (item 21). "Modified since vN" takes
  // priority over the plain vN badge, same precedence as the MT header's
  // VersionBadge. Passed to the form as `headerTitle` so it renders on the
  // LEFT of the same row as the Save/Submit + Approve/Reject + ⋮ actions
  // (B2-78) — the title and actions share one line, tabs sit directly below,
  // with no dropped action row or extra vertical gap.
  const headerTitle = (
    <div className="flex flex-wrap items-center gap-2 min-w-0">
      <h1 className="text-2xl font-semibold tracking-tight leading-none truncate">{name}</h1>
      <StatusBadge status={rule.status} />
      {rule.display_status === "modified" ? (
        <ModifiedBadge version={rule.version} />
      ) : (
        <RuleVersionBadge version={rule.version} />
      )}
    </div>
  );

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]} page={name} />

        <RegistryRuleFormDialog
          variant="page"
          open
          onOpenChange={(next) => {
            if (!next) backToList();
          }}
          editingRule={canEdit ? rule : null}
          viewingRule={canEdit ? null : rule}
          labelDefinitions={labelDefinitions}
          onSaved={() => {
            justSavedRef.current = true;
            invalidateDetail();
          }}
          activeTab={tab as PageTab | undefined}
          onActiveTabChange={handleActiveTabChange}
          onDirtyChange={setIsDirty}
          jsonDialogOpen={formJsonDialogOpen}
          onJsonDialogOpenChange={setFormJsonDialogOpen}
          headerActions={headerActions}
          headerTitle={headerTitle}
        />
      </div>

      {canApply && (
        <ApplyRuleModal
          open={applyModalOpen}
          onOpenChange={setApplyModalOpen}
          rule={rule}
          onApplied={invalidateDetail}
        />
      )}

      <CommentsDialog
        entityType="rule"
        entityId={rule.rule_id}
        open={commentsOpen}
        onOpenChange={setCommentsOpen}
      />

      <RegistryRuleJsonDialog
        open={jsonDialogOpen}
        onOpenChange={setJsonDialogOpen}
        rule={rule}
        editable={!canEdit && canDuplicate}
        onSaved={(newRuleId) => {
          justSavedRef.current = true;
          if (newRuleId && newRuleId !== rule.rule_id) {
            navigate({
              to: "/registry-rules/$ruleId",
              params: { ruleId: newRuleId },
              search: { tab: "about" },
            });
          } else {
            invalidateDetail();
          }
        }}
      />

      <AlertDialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesRegistry.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesRegistry.deleteConfirmDescription", { name })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className={cn("bg-destructive text-white hover:bg-destructive/90")}
              onClick={handleConfirmDelete}
            >
              {t("rulesRegistry.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={rejectConfirmOpen} onOpenChange={setRejectConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesRegistry.rejectConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesRegistry.rejectConfirmDescription", { name })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className={cn("bg-destructive text-white hover:bg-destructive/90")}
              onClick={handleConfirmReject}
            >
              {t("rulesRegistry.actionReject")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("common.unsavedChanges")}</AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.unsavedChangesDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>{t("common.stayOnPage")}</AlertDialogCancel>
            <AlertDialogAction
              className={cn("bg-destructive text-white hover:bg-destructive/90")}
              onClick={() => blocker.proceed?.()}
            >
              {t("rulesRegistry.discardAndLeave")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
