import { createFileRoute, useNavigate, useSearch } from "@tanstack/react-router";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { getListRegistryRulesQueryKey } from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import {
  RegistryRuleFormDialog,
  type PageTab,
} from "@/components/RegistryRuleFormDialog";

export const Route = createFileRoute("/_sidebar/registry-rules/new")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  component: RegistryRuleCreatePage,
});

function RegistryRuleCreatePage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { tab } = useSearch({ from: "/_sidebar/registry-rules/new" });

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);

  const backToList = useCallback(() => navigate({ to: "/registry-rules" }), [navigate]);

  const handleActiveTabChange = useCallback(
    (nextTab: PageTab) => {
      navigate({
        to: "/registry-rules/new",
        search: (prev) => ({ ...prev, tab: nextTab }),
      });
    },
    [navigate],
  );

  const handleSaved = useCallback(
    (ruleId?: string) => {
      queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() });
      if (ruleId) {
        navigate({ to: "/registry-rules/$ruleId", params: { ruleId } });
      } else {
        backToList();
      }
    },
    [queryClient, navigate, backToList],
  );

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]} page={t("rulesRegistry.newRule")} />

        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight truncate">{t("rulesRegistry.createTitle")}</h1>
        </div>

        <RegistryRuleFormDialog
          variant="page"
          open
          onOpenChange={(next) => {
            if (!next) backToList();
          }}
          editingRule={null}
          viewingRule={null}
          labelDefinitions={labelDefinitions}
          onSaved={handleSaved}
          activeTab={tab as PageTab | undefined}
          onActiveTabChange={handleActiveTabChange}
        />
      </div>
    </FadeIn>
  );
}
