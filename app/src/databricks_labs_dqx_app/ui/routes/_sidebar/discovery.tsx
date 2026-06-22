import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { useGetTableColumns, useGetRules, ColumnOut } from "@/lib/api";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  AlertCircle,
  Database,
  Table2,
  Columns3,
  Search,
  BookCheck,
  Plus,
  Pencil,
  FileEdit,
  Clock,
  CheckCircle2,
  XCircle,
} from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";

export const Route = createFileRoute("/_sidebar/discovery")({
  component: DiscoveryPage,
});

function DiscoveryPage() {
  const { t } = useTranslation();
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");

  const handleTableSelected = (fqn: string) => {
    const parts = fqn.split(".");
    if (parts.length === 3) {
      setCatalog(parts[0]);
      setSchema(parts[1]);
      setTable(parts[2]);
    }
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb page={t("discovery.breadcrumb")} />
        <h1 className="text-2xl font-bold tracking-tight">
          <ShinyText text={t("discovery.title")} speed={6} className="font-bold" />
        </h1>
        <p className="text-muted-foreground">
          {t("discovery.subtitle")}
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            {t("discovery.browseCatalog")}
          </CardTitle>
          <CardDescription>
            {t("discovery.browseCatalogDescription")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <CatalogBrowser onChange={handleTableSelected} />
        </CardContent>
      </Card>

      {catalog && schema && table && (
        <FadeIn duration={0.3}>
          <div className="space-y-6">
            <ColumnsTable catalog={catalog} schema={schema} table={table} />
            <RulesPanel tableFqn={`${catalog}.${schema}.${table}`} />
          </div>
        </FadeIn>
      )}

      {!table && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
            <Database className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-medium text-muted-foreground">
            {t("discovery.selectTableTitle")}
          </h3>
          <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
            {t("discovery.selectTableHint")}
          </p>
        </div>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "draft":
      return (
        <Badge variant="secondary" className="gap-1">
          <FileEdit className="h-3 w-3" />
          {t("discovery.draft")}
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3" />
          {t("discovery.pending")}
        </Badge>
      );
    case "approved":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          {t("discovery.approved")}
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          {t("discovery.rejected")}
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status}</Badge>;
  }
}

function RulesPanel({ tableFqn }: { tableFqn: string }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { data: rulesResp, isLoading, error } = useGetRules(tableFqn);
  const entry = rulesResp?.data?.[0];
  const hasRules = !!entry && !error;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <BookCheck className="h-5 w-5" />
          {t("discovery.qualityRules")}
        </CardTitle>
        <CardDescription>
          {isLoading
            ? t("discovery.checkingRules")
            : error
              ? t("discovery.failedLoadRules")
              : hasRules
                ? t("discovery.rulesDefined", { count: entry.checks.length })
                : t("discovery.noRulesYet")}
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading && (
          <Skeleton className="h-12 w-full" />
        )}

        {!isLoading && error && (
          <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4 shrink-0" />
            <span>{t("discovery.couldNotLoadRules", { error: (error as Error).message })}</span>
          </div>
        )}

        {hasRules && (
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 text-sm">
              <span className="tabular-nums font-medium">v{entry.version}</span>
              <span className="text-muted-foreground">·</span>
              <StatusBadge status={entry.status} />
              <span className="text-muted-foreground">·</span>
              <span className="tabular-nums">{t("discovery.rulesCount", { count: entry.checks.length })}</span>
              {entry.updated_at && (
                <>
                  <span className="text-muted-foreground">·</span>
                  <span className="text-muted-foreground text-xs">
                    {t("discovery.updated", { date: new Date(entry.updated_at).toLocaleDateString() })}
                  </span>
                </>
              )}
            </div>
            <Button
              size="sm"
              className="gap-2"
              onClick={() =>
                navigate({
                  to: "/rules/single-table",
                  search: { table: tableFqn },
                })
              }
            >
              <Pencil className="h-3.5 w-3.5" />
              {t("discovery.editRules")}
            </Button>
          </div>
        )}

        {!isLoading && !hasRules && (
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              {t("discovery.getStartedAi")}
            </p>
            <Button
              size="sm"
              className="gap-2"
              onClick={() =>
                navigate({
                  to: "/rules/single-table",
                  search: { table: tableFqn },
                })
              }
            >
              <Plus className="h-3.5 w-3.5" />
              {t("discovery.createRules")}
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function ColumnsTable({
  catalog,
  schema,
  table,
}: {
  catalog: string;
  schema: string;
  table: string;
}) {
  const { t } = useTranslation();
  const { data: columnsResp, isLoading, error } = useGetTableColumns(
    catalog,
    schema,
    table,
  );
  const columns: ColumnOut[] = columnsResp?.data ?? [];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Table2 className="h-5 w-5" />
          <code className="text-base font-mono">
            {catalog}.{schema}.{table}
          </code>
        </CardTitle>
        <CardDescription className="flex items-center gap-2">
          <Columns3 className="h-4 w-4" />
          {isLoading
            ? t("discovery.loadingColumns")
            : t("discovery.columnsCount", { count: columns.length })}
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading && (
          <div className="space-y-2">
            {[1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        )}

        {error && (
          <p className="text-destructive text-sm">
            {t("discovery.failedLoadColumns", { error: (error as Error).message })}
          </p>
        )}

        {!isLoading && !error && columns.length > 0 && (
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-3 font-medium">{t("discovery.columnHash")}</th>
                  <th className="text-left p-3 font-medium">{t("discovery.columnName")}</th>
                  <th className="text-left p-3 font-medium">{t("discovery.columnType")}</th>
                  <th className="text-left p-3 font-medium">{t("discovery.columnNullable")}</th>
                  <th className="text-left p-3 font-medium">{t("discovery.columnComment")}</th>
                </tr>
              </thead>
              <tbody>
                {columns.map((col) => (
                  <tr
                    key={col.name}
                    className="border-b last:border-b-0 hover:bg-muted/30 transition-colors"
                  >
                    <td className="p-3 text-muted-foreground tabular-nums">
                      {col.position}
                    </td>
                    <td className="p-3 font-mono font-medium">{col.name}</td>
                    <td className="p-3">
                      <Badge variant="secondary" className="font-mono text-xs">
                        {col.type_name}
                      </Badge>
                    </td>
                    <td className="p-3">
                      <Badge
                        variant={col.nullable ? "outline" : "default"}
                        className="text-xs"
                      >
                        {col.nullable ? t("common.yes") : t("common.no")}
                      </Badge>
                    </td>
                    <td className="p-3 text-muted-foreground max-w-xs truncate">
                      {col.comment || "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {!isLoading && !error && columns.length === 0 && (
          <p className="text-muted-foreground text-sm text-center py-4">
            {t("discovery.noColumns")}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
