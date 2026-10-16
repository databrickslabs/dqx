import { createFileRoute, Navigate, useNavigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { ExternalLink } from "lucide-react";
import { BulkContractImportWorkspace } from "@/components/registry-rules/BulkContractImportWorkspace";
import { FadeIn } from "@/components/anim/FadeIn";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { usePermissions } from "@/hooks/use-permissions";
import { IMPORT_EXAMPLES, ImportExampleLinks } from "@/components/imports/ImportExampleLinks";

const CONTRACT_DOCS_URL =
  "https://databrickslabs.github.io/dqx/docs/guide/data_contract_quality_rules_generation/";

export const Route = createFileRoute("/_sidebar/monitored-tables/import")({
  component: MonitoredTablesImportPage,
});

function MonitoredTablesImportPage() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/monitored-tables" replace />;
  return <MonitoredTablesImportPageInner />;
}

function MonitoredTablesImportPageInner() {
  const { t } = useTranslation();
  const navigate = useNavigate();

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[{ label: t("monitoredTables.title"), to: "/monitored-tables" }]}
          page={t("rulesImport.sectionTables")}
        />
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("rulesImport.sectionTables")}</h1>
            <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{t("rulesImport.scopeNoteTables")}</p>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <a
              href={CONTRACT_DOCS_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex h-8 items-center gap-1.5 rounded-md px-2 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            >
              {t("rulesFromContract.viewDocs")}
              <ExternalLink className="h-3 w-3" />
            </a>
            <ImportExampleLinks examplePath={IMPORT_EXAMPLES.dataContract} />
          </div>
        </div>
        <BulkContractImportWorkspace onDone={() => navigate({ to: "/monitored-tables" })} />
      </div>
    </FadeIn>
  );
}
