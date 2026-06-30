import { createFileRoute, Link, Navigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Sparkles,
  Database,
  BarChart3,
  Upload,
  ArrowRight,
} from "lucide-react";

export const Route = createFileRoute("/_sidebar/rules/create")({
  component: CreateRulesLanding,
});

function CreateRulesLanding() {
  const { t } = useTranslation();
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;

  // ``/rules/import`` is now a tabbed landing that hosts both DQX YAML
  // imports and ODCS data-contract generation — the standalone "From
  // data contract" tile was removed to keep this grid focused on
  // distinct *kinds* of rule sources rather than file formats.
  //
  // The standalone "Validate table schema" tile was also removed: schema
  // validation and other reference-table checks (foreign_key, …) are
  // authored and edited inside the single-table editor, so users have one
  // place to build and maintain per-table rules.
  const OPTIONS = [
    {
      to: "/rules/single-table",
      icon: Sparkles,
      title: t("rulesCreate.singleTableTitle"),
      description: t("rulesCreate.singleTableDescription"),
    },
    {
      to: "/rules/create-sql",
      icon: Database,
      title: t("rulesCreate.crossTableTitle"),
      description: t("rulesCreate.crossTableDescription"),
    },
    {
      to: "/profiler",
      icon: BarChart3,
      title: t("rulesCreate.profileTitle"),
      description: t("rulesCreate.profileDescription"),
    },
    {
      to: "/rules/import",
      icon: Upload,
      title: t("rulesCreate.importTitle"),
      description: t("rulesCreate.importDescription"),
    },
  ] as const;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[]} page={t("rulesCreate.breadcrumb")} />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">{t("rulesCreate.title")}</h1>
          <p className="text-muted-foreground">
            {t("rulesCreate.subtitle")}
          </p>
        </div>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        {OPTIONS.map(({ to, icon: Icon, title, description }) => (
          <Link key={to} to={to} search={{ from: "create" }} className="block group">
            <Card className="h-full transition-colors hover:border-primary/50 hover:bg-muted/30">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Icon className="h-4 w-4 text-primary" />
                  {title}
                  <ArrowRight className="h-3.5 w-3.5 ml-auto opacity-0 -translate-x-1 transition-all group-hover:opacity-60 group-hover:translate-x-0" />
                </CardTitle>
              </CardHeader>
              <CardContent>
                <CardDescription>{description}</CardDescription>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
