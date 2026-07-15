import { createFileRoute, useNavigate, Navigate, Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { Layers, ChevronRight } from "lucide-react";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Card, CardContent } from "@/components/ui/card";
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

        <Link to="/registry-rules/bulk-import" className="block">
          <Card className="transition-colors hover:bg-muted/40">
            <CardContent className="flex items-center gap-3 p-4">
              <div className="rounded-md bg-primary/10 p-2 text-primary">
                <Layers className="h-5 w-5" />
              </div>
              <div className="flex-1">
                <div className="text-sm font-medium">
                  {t("rulesBulkImport.entryCard.title")}
                </div>
                <div className="text-xs text-muted-foreground">
                  {t("rulesBulkImport.entryCard.description")}
                </div>
              </div>
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
            </CardContent>
          </Card>
        </Link>
      </div>
    </FadeIn>
  );
}
