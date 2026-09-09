import { createFileRoute, useNavigate, Navigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import {
  ImportRulesWorkspace,
  coerceImportTab,
  type ImportTab,
} from "@/components/registry-rules/ImportRulesWorkspace";

const IMPORT_ROUTE = "/registry-rules/import";

interface ImportSearchParams {
  from?: string;
  tab?: ImportTab;
}

export const Route = createFileRoute("/_sidebar/registry-rules/import")({
  component: RegistryRulesImportPage,
  validateSearch: (search: Record<string, unknown>): ImportSearchParams => ({
    from: typeof search.from === "string" ? search.from : undefined,
    tab: coerceImportTab(search.tab),
  }),
});

function RegistryRulesImportPage() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/registry-rules" replace />;
  return <RegistryRulesImportPageInner />;
}

function RegistryRulesImportPageInner() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { tab: tabParam } = Route.useSearch();
  const tab: ImportTab = tabParam ?? "yaml";

  const onDone = () => navigate({ to: "/registry-rules" });

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]}
          page={t("rulesImport.breadcrumb")}
        />
        <ImportRulesWorkspace
          tab={tab}
          onTabChange={(next) =>
            navigate({
              to: IMPORT_ROUTE,
              search: (prev) => ({ ...prev, tab: next }),
              replace: true,
            })
          }
          onDone={onDone}
        />
      </div>
    </FadeIn>
  );
}
