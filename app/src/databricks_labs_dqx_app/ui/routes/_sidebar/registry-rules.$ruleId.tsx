import { createFileRoute, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { Suspense, useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertCircle, RotateCcw } from "lucide-react";
import {
  useGetRegistryRuleSuspense,
  getGetRegistryRuleQueryKey,
} from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import {
  RegistryRuleFormDialog,
  type PageTab,
} from "@/components/RegistryRuleFormDialog";
import { StatusBadge, ModeBadge, AuthorKindBadge, getTag, RESERVED_NAME_KEY } from "@/components/RegistryRuleBadges";

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

  const invalidateDetail = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getGetRegistryRuleQueryKey(ruleId) }),
    [queryClient, ruleId],
  );

  const backToList = useCallback(
    () => navigate({ to: "/registry-rules" }),
    [navigate],
  );

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

  // Only drafts are editable in place — every other status (pending
  // approval, approved, rejected, deprecated) is opened read-only, matching
  // the old modal's edit-vs-view split.
  const canEdit = rule.status === "draft" && perms.canCreateRules;
  const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]} page={name} />

        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight truncate">{name}</h1>
          <StatusBadge status={rule.status} />
          <ModeBadge mode={rule.mode} />
          <AuthorKindBadge authorKind={rule.author_kind ?? undefined} />
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
          onSaved={invalidateDetail}
          activeTab={tab as PageTab | undefined}
          onActiveTabChange={handleActiveTabChange}
        />
      </div>
    </FadeIn>
  );
}
