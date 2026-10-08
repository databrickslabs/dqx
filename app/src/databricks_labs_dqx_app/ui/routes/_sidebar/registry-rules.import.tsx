import { createFileRoute, useNavigate, Navigate, redirect } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { ImportRulesWorkspace } from "@/components/registry-rules/ImportRulesWorkspace";

interface ImportSearchParams {
  from?: string;
}

export const Route = createFileRoute("/_sidebar/registry-rules/import")({
  component: RegistryRulesImportPage,
  // ``?tab=contract`` / ``?tab=tables`` were the ODCS imports, which now live under Tables.
  beforeLoad: ({ location }) => {
    const { tab } = location.search as Record<string, unknown>;
    if (tab === "contract" || tab === "tables") {
      throw redirect({ to: "/monitored-tables/import", replace: true });
    }
  },
  validateSearch: (search: Record<string, unknown>): ImportSearchParams => ({
    from: typeof search.from === "string" ? search.from : undefined,
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
  const onDone = () => navigate({ to: "/registry-rules" });

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]}
          page={t("rulesImport.breadcrumb")}
        />
        <ImportRulesWorkspace onDone={onDone} />
      </div>
    </FadeIn>
  );
}
