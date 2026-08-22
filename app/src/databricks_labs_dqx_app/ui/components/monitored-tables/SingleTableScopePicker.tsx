import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useListCatalogs, useListSchemas, useListTables } from "@/lib/api";
import { MultiSelectPopover } from "@/components/monitored-tables/MultiSelectPopover";

/**
 * Single-select cascading catalog → schema → table picker that matches the
 * new-monitored-table look (MultiSelectPopover in `single` mode), as opposed
 * to CatalogBrowser's three plain `<Select>` elements.
 *
 * Props:
 *  - `value`    — fully-qualified table name ("catalog.schema.table"), or "" for
 *                 unset.
 *  - `onChange` — called with the new fully-qualified name whenever a table is
 *                 picked; called with "" when the catalog or schema is changed
 *                 (clearing the downstream selection).
 *  - `disabled` — disables all three pickers.
 */
export function SingleTableScopePicker({
  value,
  onChange,
  disabled,
}: {
  value: string;
  onChange: (fqn: string) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();

  // Derive initial catalog/schema/table from the controlled `value` prop.
  const [catalog, setCatalog] = useState<string>(() => {
    const parts = value.split(".");
    return parts.length === 3 && parts[0] ? parts[0] : "";
  });
  const [schema, setSchema] = useState<string>(() => {
    const parts = value.split(".");
    return parts.length === 3 && parts[1] ? parts[1] : "";
  });
  const [table, setTable] = useState<string>(() => {
    const parts = value.split(".");
    return parts.length === 3 && parts[2] ? parts[2] : "";
  });

  // Keep internal state in sync when the controlled `value` changes from
  // outside (e.g. the parent resets to "").
  useEffect(() => {
    if (!value) {
      setCatalog("");
      setSchema("");
      setTable("");
      return;
    }
    const parts = value.split(".");
    if (parts.length === 3) {
      setCatalog(parts[0]);
      setSchema(parts[1]);
      setTable(parts[2]);
    }
  }, [value]);

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs();
  const { data: schemasResp, isLoading: schemasLoading } = useListSchemas(catalog, {
    query: { enabled: !!catalog },
  });
  const { data: tablesResp, isLoading: tablesLoading } = useListTables(catalog, schema, {
    query: { enabled: !!catalog && !!schema },
  });

  const catalogOptions = (catalogsResp?.data ?? []).map((c) => ({ value: c.name, label: c.name }));
  const schemaOptions = (schemasResp?.data ?? []).map((s) => ({ value: s.name, label: s.name }));
  const tableOptions = (tablesResp?.data ?? []).map((t) => ({ value: t.name, label: t.name }));

  const handleCatalogChange = useCallback(
    (values: string[]) => {
      const next = values[0] ?? "";
      setCatalog(next);
      setSchema("");
      setTable("");
      onChange("");
    },
    [onChange],
  );

  const handleSchemaChange = useCallback(
    (values: string[]) => {
      const next = values[0] ?? "";
      setSchema(next);
      setTable("");
      onChange("");
    },
    [onChange],
  );

  const handleTableChange = useCallback(
    (values: string[]) => {
      const next = values[0] ?? "";
      setTable(next);
      if (catalog && schema && next) {
        onChange(`${catalog}.${schema}.${next}`);
      } else {
        onChange("");
      }
    },
    [catalog, schema, onChange],
  );

  return (
    <div className="grid grid-cols-3 gap-2">
      <MultiSelectPopover
        label={t("monitoredTables.wizard.catalogsLabel")}
        placeholder={t("monitoredTables.wizard.catalogsPlaceholder")}
        searchPlaceholder={t("monitoredTables.wizard.searchCatalogs")}
        options={catalogOptions}
        selected={catalog ? [catalog] : []}
        onChange={handleCatalogChange}
        isLoading={catalogsLoading}
        disabled={disabled}
        emptyText={t("monitoredTables.wizard.emptyCatalogs")}
        single
      />
      <MultiSelectPopover
        label={t("monitoredTables.wizard.schemasLabel")}
        placeholder={t("monitoredTables.wizard.schemasPlaceholder", { count: catalog ? 1 : 0 })}
        searchPlaceholder={t("monitoredTables.wizard.searchSchemas")}
        options={schemaOptions}
        selected={schema ? [schema] : []}
        onChange={handleSchemaChange}
        isLoading={schemasLoading}
        disabled={disabled || !catalog}
        disabledHint={t("monitoredTables.wizard.selectCatalogFirst")}
        emptyText={t("monitoredTables.wizard.emptySchemas")}
        single
      />
      <MultiSelectPopover
        label={t("monitoredTables.wizard.tablesLabel")}
        placeholder={t("monitoredTables.wizard.tablesPlaceholder", { count: schema ? 1 : 0 })}
        searchPlaceholder={t("monitoredTables.wizard.searchTables")}
        options={tableOptions}
        selected={table ? [table] : []}
        onChange={handleTableChange}
        isLoading={tablesLoading}
        disabled={disabled || !schema}
        disabledHint={t("monitoredTables.wizard.selectSchemaFirst")}
        emptyText={t("monitoredTables.wizard.emptyTables")}
        single
      />
    </div>
  );
}
