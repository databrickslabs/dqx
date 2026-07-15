import { createFileRoute, Navigate, useNavigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { BulkContractImportWorkspace } from "@/components/registry-rules/BulkContractImportWorkspace";

export const Route = createFileRoute("/_sidebar/registry-rules/bulk-import")({
  component: RegistryRulesBulkImportPage,
});

function RegistryRulesBulkImportPage() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/registry-rules" replace />;
  return <RegistryRulesBulkImportPageInner />;
}

function RegistryRulesBulkImportPageInner() {
  const { t } = useTranslation();
  const navigate = useNavigate();

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[
            { label: t("rulesRegistry.title"), to: "/registry-rules" },
            { label: t("rulesImport.breadcrumb"), to: "/registry-rules/import" },
          ]}
          page={t("rulesBulkImport.breadcrumb")}
        />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">{t("rulesBulkImport.title")}</h1>
          <p className="text-muted-foreground">{t("rulesBulkImport.subtitle")}</p>
        </div>
        <BulkContractImportWorkspace onDone={() => navigate({ to: "/registry-rules" })} />
      </div>
    </FadeIn>
  );
}
