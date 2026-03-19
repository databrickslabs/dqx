import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { useGetTableColumns, ColumnOut } from "@/lib/api";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Database, Table2, Columns3, Search } from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";

export const Route = createFileRoute("/_sidebar/discovery")({
  component: DiscoveryPage,
});

function DiscoveryPage() {
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
        <PageBreadcrumb page="Discovery" />
        <h1 className="text-2xl font-bold tracking-tight">
          <ShinyText text="Discovery" speed={6} className="font-bold" />
        </h1>
        <p className="text-muted-foreground">
          Browse Unity Catalog assets to find tables for your data quality
          checks.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            Browse Catalog
          </CardTitle>
          <CardDescription>
            Select a catalog, schema, and table to view its columns.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <CatalogBrowser onChange={handleTableSelected} />
        </CardContent>
      </Card>

      {catalog && schema && table && (
        <FadeIn duration={0.3}>
          <ColumnsTable catalog={catalog} schema={schema} table={table} />
        </FadeIn>
      )}

      {!table && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
            <Database className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-medium text-muted-foreground">
            Select a Table
          </h3>
          <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
            Choose a catalog, schema, and table above to view its column
            definitions.
          </p>
        </div>
      )}
    </div>
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
            ? "Loading columns..."
            : `${columns.length} column${columns.length !== 1 ? "s" : ""}`}
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
            Failed to load columns: {(error as Error).message}
          </p>
        )}

        {!isLoading && !error && columns.length > 0 && (
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-3 font-medium">#</th>
                  <th className="text-left p-3 font-medium">Name</th>
                  <th className="text-left p-3 font-medium">Type</th>
                  <th className="text-left p-3 font-medium">Nullable</th>
                  <th className="text-left p-3 font-medium">Comment</th>
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
                        {col.nullable ? "yes" : "no"}
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
            No columns found for this table.
          </p>
        )}
      </CardContent>
    </Card>
  );
}
