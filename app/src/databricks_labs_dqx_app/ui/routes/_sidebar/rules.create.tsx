import { createFileRoute, Link, Navigate } from "@tanstack/react-router";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Sparkles, Database, BarChart3, Upload, ArrowRight } from "lucide-react";

export const Route = createFileRoute("/_sidebar/rules/create")({
  component: CreateRulesLanding,
});

const OPTIONS = [
  {
    to: "/rules/single-table",
    icon: Sparkles,
    title: "Single table rules",
    description: "Define column-level checks for individual tables, with optional AI assistance.",
  },
  {
    to: "/rules/create-sql",
    icon: Database,
    title: "Cross-table rules",
    description: "Write SQL queries that validate data across multiple tables or run dataset-level aggregation checks.",
  },
  {
    to: "/profiler",
    icon: BarChart3,
    title: "Profile & generate",
    description: "Profile your data to auto-generate quality rules based on column statistics.",
  },
  {
    to: "/rules/import",
    icon: Upload,
    title: "Import rules",
    description: "Import rules from a YAML/JSON file or an existing Delta table.",
  },
] as const;

function CreateRulesLanding() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[]} page="Create Rules" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Create Rules</h1>
          <p className="text-muted-foreground">
            Choose how you want to create data quality rules.
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
