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
import { AlertCircle, Braces, MoreVertical, Pencil, RotateCcw, Table2, Trash2 } from "lucide-react";
import {
  useGetRegistryRuleSuspense,
  getGetRegistryRuleQueryKey,
  useDeleteRegistryRule,
  useCreateRegistryRule,
  type CreateRegistryRuleIn,
} from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import {
  RegistryRuleFormDialog,
  type PageTab,
} from "@/components/RegistryRuleFormDialog";
import { ApplyRuleModal } from "@/components/registry-rules/ApplyRuleModal";
import { RegistryRuleJsonDialog } from "@/components/registry-rules/RegistryRuleJsonDialog";
import { StatusBadge, ModeBadge, AuthorKindBadge, getTag, RESERVED_NAME_KEY } from "@/components/RegistryRuleBadges";
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
        <ErrorBoundary onReset={reset} fallbackRender={RegistryRuleDetailError}>
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

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);

  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [applyModalOpen, setApplyModalOpen] = useState(false);
  const [jsonDialogOpen, setJsonDialogOpen] = useState(false);
  const [isDirty, setIsDirty] = useState(false);
  // Set right before a successful save navigates us away, so the guard
  // doesn't fire a spurious "unsaved changes" prompt on our own redirect.
  const justSavedRef = useRef(false);

  const invalidateDetail = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getGetRegistryRuleQueryKey(ruleId) }),
    [queryClient, ruleId],
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

  // Only drafts are editable in place — the backend's update_draft rejects
  // any other status (pending approval, approved, rejected, deprecated)
  // because an approved rule's published snapshot must stay immutable (see
  // RegistryService.approve's frozen dq_rule_versions row). For every other
  // status we instead offer "Edit as new draft" below, which clones the
  // rule into a fresh draft the user can freely edit and submit — mirroring
  // dqlake's "editing a published rule creates a new version" behavior
  // without touching the frozen original.
  const canEdit = rule.status === "draft" && perms.canCreateRules;
  const canEditAsNewDraft = rule.status !== "draft" && perms.canCreateRules;
  const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;

  // The backend deleteRegistryRule route allows admin/approver/author
  // (create_rules). This menu deliberately restricts delete further to
  // approver/admin — deleting from the detail page (any status, including
  // published rules) is a heavier action than the list page's draft-only
  // delete, which stays scoped to canCreateRules.
  const canDelete = perms.canApproveRules;
  // Apply requires a published rule — the backend rejects a non-approved
  // rule with 409 (RuleNotPublishedError) — plus create-rule permission.
  const canApply = perms.canCreateRules && rule.status === "approved";
  // "View / edit JSON" is always offered (read-only when the viewer can't
  // edit), so the actions menu is always shown once any menu item applies.
  const showActionsMenu = true;

  const createMutation = useCreateRegistryRule();
  const handleEditAsNewDraft = useCallback(() => {
    const payload: CreateRegistryRuleIn = {
      mode: rule.mode,
      definition: rule.definition,
      polarity: rule.polarity ?? null,
      user_metadata: rule.user_metadata ?? {},
      steward: rule.steward ?? null,
      author_kind: rule.author_kind ?? "human",
    };
    createMutation.mutate(
      { data: payload },
      {
        onSuccess: (resp) => {
          toast.success(t("rulesRegistry.toastDraftCopyCreated"));
          navigate({
            to: "/registry-rules/$ruleId",
            params: { ruleId: resp.data.rule.rule_id },
            search: { tab: "about" },
          });
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("rulesRegistry.saveFailed")), { duration: 6000 });
        },
      },
    );
  }, [createMutation, rule, navigate, t]);

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]} page={name} />

        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight truncate">{name}</h1>
          <StatusBadge status={rule.status} />
          <ModeBadge mode={rule.mode} />
          <AuthorKindBadge authorKind={rule.author_kind ?? undefined} />
          {canEditAsNewDraft && (
            <Button
              variant="outline"
              size="sm"
              className={cn("gap-2 h-8", !showActionsMenu && "ml-auto")}
              onClick={handleEditAsNewDraft}
              disabled={createMutation.isPending}
              title={t("rulesRegistry.editAsNewDraftTooltip")}
            >
              <Pencil className="h-3.5 w-3.5" />
              {t("rulesRegistry.actionEditAsNewDraft")}
            </Button>
          )}
          {showActionsMenu && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className={cn("h-8 w-8 p-0", !canEditAsNewDraft && "ml-auto")}
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
                <DropdownMenuItem onClick={() => setJsonDialogOpen(true)} className="gap-2">
                  <Braces className="h-3.5 w-3.5" />
                  {t("rulesRegistry.actionViewJson")}
                </DropdownMenuItem>
                {canDelete && (
                  <DropdownMenuItem
                    onClick={() => setDeleteConfirmOpen(true)}
                    className={cn("gap-2 text-destructive focus:text-destructive")}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                    {t("rulesRegistry.actionDelete")}
                  </DropdownMenuItem>
                )}
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>

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

      <RegistryRuleJsonDialog
        open={jsonDialogOpen}
        onOpenChange={setJsonDialogOpen}
        rule={rule}
        canEdit={canEdit}
        canEditAsNewDraft={canEditAsNewDraft}
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
