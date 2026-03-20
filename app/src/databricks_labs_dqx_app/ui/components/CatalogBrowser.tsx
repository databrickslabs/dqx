import { useState, useEffect } from "react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Loader2 } from "lucide-react";
import {
  CatalogOut,
  SchemaOut,
  TableOut,
  useListCatalogs,
  useListSchemas,
  useListTables,
} from "@/lib/api";

interface CatalogBrowserProps {
  value?: string;
  onChange: (fqn: string) => void;
  disabled?: boolean;
}

export function CatalogBrowser({
  value,
  onChange,
  disabled,
}: CatalogBrowserProps) {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");

  // Parse initial value (catalog.schema.table)
  useEffect(() => {
    if (value) {
      const parts = value.split(".");
      if (parts.length === 3) {
        setCatalog(parts[0]);
        setSchema(parts[1]);
        setTable(parts[2]);
      }
    }
  }, [value]);

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs();
  const { data: schemasResp, isLoading: schemasLoading } = useListSchemas(
    catalog,
    { query: { enabled: !!catalog } },
  );
  const { data: tablesResp, isLoading: tablesLoading } = useListTables(
    catalog,
    schema,
    { query: { enabled: !!catalog && !!schema } },
  );

  const catalogs: CatalogOut[] = catalogsResp?.data ?? [];
  const schemas: SchemaOut[] = schemasResp?.data ?? [];
  const tables: TableOut[] = tablesResp?.data ?? [];

  // Auto-select when only one option is available
  useEffect(() => {
    if (!schemasLoading && schemas.length === 1 && !schema) {
      handleSchemaChange(schemas[0].name);
    }
  }, [schemas, schemasLoading]);

  useEffect(() => {
    if (!tablesLoading && tables.length === 1 && !table) {
      handleTableChange(tables[0].name);
    }
  }, [tables, tablesLoading]);

  const handleCatalogChange = (val: string) => {
    setCatalog(val);
    setSchema("");
    setTable("");
  };

  const handleSchemaChange = (val: string) => {
    setSchema(val);
    setTable("");
  };

  const handleTableChange = (val: string) => {
    setTable(val);
    if (catalog && schema && val) {
      onChange(`${catalog}.${schema}.${val}`);
    }
  };

  return (
    <div className="grid grid-cols-3 gap-3">
      <div className="grid gap-1.5">
        <Label className="text-xs text-muted-foreground">Catalog</Label>
        <Select
          value={catalog}
          onValueChange={handleCatalogChange}
          disabled={disabled || catalogsLoading}
        >
          <SelectTrigger>
            {catalogsLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <SelectValue placeholder="Select catalog" />
            )}
          </SelectTrigger>
          <SelectContent>
            {catalogs.map((c) => (
              <SelectItem key={c.name} value={c.name}>
                {c.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="grid gap-1.5">
        <Label className="text-xs text-muted-foreground">Schema</Label>
        <Select
          value={schema}
          onValueChange={handleSchemaChange}
          disabled={disabled || !catalog || schemasLoading}
        >
          <SelectTrigger>
            {schemasLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <SelectValue placeholder="Select schema" />
            )}
          </SelectTrigger>
          <SelectContent>
            {schemas.map((s) => (
              <SelectItem key={s.name} value={s.name}>
                {s.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="grid gap-1.5">
        <Label className="text-xs text-muted-foreground">Table</Label>
        <Select
          value={table}
          onValueChange={handleTableChange}
          disabled={disabled || !schema || tablesLoading}
        >
          <SelectTrigger>
            {tablesLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <SelectValue placeholder="Select table" />
            )}
          </SelectTrigger>
          <SelectContent>
            {tables.map((t) => (
              <SelectItem key={t.name} value={t.name}>
                {t.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  );
}
