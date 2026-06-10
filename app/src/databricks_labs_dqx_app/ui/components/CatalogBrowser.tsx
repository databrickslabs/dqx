import { useState, useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Loader2, CheckSquare, Square } from "lucide-react";
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
  multiSelect?: boolean;
  selectedTables?: string[];
  onMultiChange?: (tables: string[]) => void;
  onAllTablesLoaded?: (fqns: string[]) => void;
  disabledTables?: string[];
}

export function CatalogBrowser({
  value,
  onChange,
  disabled,
  multiSelect = false,
  selectedTables = [],
  onMultiChange,
  onAllTablesLoaded,
  disabledTables,
}: CatalogBrowserProps) {
  const { t } = useTranslation();
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");

  // Parse initial value (catalog.schema.table)
  useEffect(() => {
    if (value && !multiSelect) {
      const parts = value.split(".");
      if (parts.length === 3) {
        setCatalog(parts[0]);
        setSchema(parts[1]);
        setTable(parts[2]);
      }
    }
  }, [value, multiSelect]);

  // In multi-select mode, try to infer catalog/schema from selected tables
  useEffect(() => {
    if (multiSelect && selectedTables.length > 0 && !catalog) {
      const parts = selectedTables[0].split(".");
      if (parts.length === 3) {
        setCatalog(parts[0]);
        setSchema(parts[1]);
      }
    }
  }, [multiSelect, selectedTables, catalog]);

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

  const prevFqnsRef = useRef<string>("");
  useEffect(() => {
    if (!onAllTablesLoaded) return;
    let fqns: string[] = [];
    if (catalog && schema && !tablesLoading && tables.length > 0) {
      fqns = tables.map((t) => `${catalog}.${schema}.${t.name}`);
    }
    const key = fqns.join(",");
    if (key !== prevFqnsRef.current) {
      prevFqnsRef.current = key;
      onAllTablesLoaded(fqns);
    }
  }, [catalog, schema, tables, tablesLoading, onAllTablesLoaded]);

  // Auto-select when only one option is available (single-select mode only)
  useEffect(() => {
    if (!multiSelect && !schemasLoading && schemas.length === 1 && !schema) {
      handleSchemaChange(schemas[0].name);
    }
  }, [schemas, schemasLoading, multiSelect]);

  useEffect(() => {
    if (!multiSelect && !tablesLoading && tables.length === 1 && !table) {
      handleTableChange(tables[0].name);
    }
  }, [tables, tablesLoading, multiSelect]);

  const handleCatalogChange = (val: string) => {
    setCatalog(val);
    setSchema("");
    setTable("");
    if (!multiSelect && onMultiChange) {
      onMultiChange([]);
    }
  };

  const handleSchemaChange = (val: string) => {
    setSchema(val);
    setTable("");
    if (!multiSelect && onMultiChange) {
      onMultiChange([]);
    }
  };

  const handleTableChange = (val: string) => {
    setTable(val);
    if (catalog && schema && val) {
      onChange(`${catalog}.${schema}.${val}`);
    }
  };

  const toggleTable = (tableName: string) => {
    if (!onMultiChange) return;
    const fqn = `${catalog}.${schema}.${tableName}`;
    if (selectedTables.includes(fqn)) {
      onMultiChange(selectedTables.filter((t) => t !== fqn));
    } else {
      onMultiChange([...selectedTables, fqn]);
    }
  };

  const selectAllTables = () => {
    if (!onMultiChange) return;
    const disabledSet = new Set(disabledTables ?? []);
    const allFqns = tables
      .map((t) => `${catalog}.${schema}.${t.name}`)
      .filter((fqn) => !disabledSet.has(fqn));
    const merged = [...new Set([...selectedTables, ...allFqns])];
    onMultiChange(merged);
  };

  const clearAllTables = () => {
    if (!onMultiChange) return;
    const prefix = `${catalog}.${schema}.`;
    onMultiChange(selectedTables.filter((t) => !t.startsWith(prefix)));
  };

  const currentSchemaSelectedCount = selectedTables.filter(
    (t) => t.startsWith(`${catalog}.${schema}.`)
  ).length;

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-3">
        <div className="grid gap-1.5">
          <Label className="text-xs text-muted-foreground">{t("catalogBrowser.catalog")}</Label>
          <Select
            value={catalog}
            onValueChange={handleCatalogChange}
            disabled={disabled || catalogsLoading}
          >
            <SelectTrigger>
              {catalogsLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <SelectValue placeholder={t("catalogBrowser.selectCatalog")} />
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
          <Label className="text-xs text-muted-foreground">{t("catalogBrowser.schema")}</Label>
          <Select
            value={schema}
            onValueChange={handleSchemaChange}
            disabled={disabled || !catalog || schemasLoading}
          >
            <SelectTrigger>
              {schemasLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <SelectValue placeholder={t("catalogBrowser.selectSchema")} />
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

        {!multiSelect && (
          <div className="grid gap-1.5">
            <Label className="text-xs text-muted-foreground">{t("catalogBrowser.table")}</Label>
            <Select
              value={table}
              onValueChange={handleTableChange}
              disabled={disabled || !schema || tablesLoading}
            >
              <SelectTrigger>
                {tablesLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <SelectValue placeholder={t("catalogBrowser.selectTable")} />
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
        )}

        {multiSelect && (
          <div className="grid gap-1.5">
            <Label className="text-xs text-muted-foreground">
              {t("catalogBrowser.tables")} {currentSchemaSelectedCount > 0 && (selectedTables.length > currentSchemaSelectedCount
                ? t("catalogBrowser.tablesInSchemaWithTotal", { schema: currentSchemaSelectedCount, total: selectedTables.length })
                : t("catalogBrowser.tablesInSchema", { schema: currentSchemaSelectedCount }))}
            </Label>
            <div className="h-9 flex items-center text-sm text-muted-foreground">
              {!schema ? t("catalogBrowser.selectSchemaFirst") : tablesLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                t("catalogBrowser.tablesAvailable", { count: tables.length })
              )}
            </div>
          </div>
        )}
      </div>

      {multiSelect && schema && !tablesLoading && tables.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-xs gap-1"
              onClick={selectAllTables}
              disabled={disabled || currentSchemaSelectedCount === tables.length}
            >
              <CheckSquare className="h-3 w-3" />
              {t("catalogBrowser.selectAll")}
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-xs gap-1"
              onClick={clearAllTables}
              disabled={disabled || currentSchemaSelectedCount === 0}
            >
              <Square className="h-3 w-3" />
              {t("catalogBrowser.clear")}
            </Button>
          </div>
          <div className="border rounded-md p-2 max-h-48 overflow-y-auto">
            <div className="grid grid-cols-2 md:grid-cols-3 gap-1">
              {tables.map((tbl) => {
                const fqn = `${catalog}.${schema}.${tbl.name}`;
                const isSelected = selectedTables.includes(fqn);
                const isDisabledByFilter = disabledTables?.includes(fqn);
                const isItemDisabled = disabled || isDisabledByFilter;
                return (
                  <label
                    key={tbl.name}
                    title={isDisabledByFilter ? t("catalogBrowser.missingColumns") : undefined}
                    className={`flex items-center gap-2 px-2 py-1.5 rounded text-sm transition-colors ${
                      isItemDisabled
                        ? "opacity-40 cursor-not-allowed"
                        : isSelected
                          ? "bg-primary/10 text-primary cursor-pointer"
                          : "hover:bg-muted cursor-pointer"
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => !isItemDisabled && toggleTable(tbl.name)}
                      disabled={isItemDisabled}
                      className="h-4 w-4 rounded border-gray-300"
                    />
                    <span className="truncate font-mono text-xs">{tbl.name}</span>
                  </label>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
