import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { Loader2, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useListImplementedRules, type ImplementedRuleOut, type ListImplementedRulesParams } from "@/lib/api";

/** Distinct mapped columns of one rule application, or "" for a table-level rule. */
export function mappedRuleColumns(mapping: Array<Record<string, string>> | undefined): string {
  const columns = (mapping ?? []).flatMap((group) => Object.values(group));
  return Array.from(new Set(columns)).join(", ");
}

/** Orders rule applications for display: by table FQN (a rule's tables) or by rule name (a table's rules). */
export function sortImplementedRules(rows: ImplementedRuleOut[], by: "table" | "rule"): ImplementedRuleOut[] {
  const keyOf = (row: ImplementedRuleOut) => (by === "table" ? row.table_fqn : (row.rule_name ?? row.rule_id));
  return [...rows].sort((a, b) => keyOf(a).localeCompare(keyOf(b)));
}

/** Concrete rule applications, listed under a table (showTable=false) or a rule (showTable=true). */
export function RuleAssignmentsList({
  rows,
  showTable,
}: {
  rows: ImplementedRuleOut[];
  showTable: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div className="max-w-5xl overflow-x-auto rounded-md border bg-background">
      <table className="w-full text-xs">
        <thead className="bg-muted/40 text-left text-muted-foreground">
          <tr>
            <th className="px-3 py-2 font-medium">
              {showTable ? t("monitoredTables.implementedRules.colTable") : t("monitoredTables.implementedRules.colRule")}
            </th>
            <th className="px-3 py-2 font-medium">{t("monitoredTables.implementedRules.colColumns")}</th>
            <th className="px-3 py-2 font-medium">{t("monitoredTables.implementedRules.colThreshold")}</th>
            <th className="px-3 py-2 font-medium">{t("monitoredTables.implementedRules.colRowFilter")}</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {rows.map((row) => (
            <tr key={row.id ?? `${row.binding_id}-${row.rule_id}`} className="hover:bg-muted/20">
              <td className="px-3 py-2 font-medium">
                {showTable ? (
                  <Link
                    to="/monitored-tables/$bindingId"
                    params={{ bindingId: row.binding_id }}
                    className="text-primary hover:underline"
                  >
                    {row.table_fqn}
                  </Link>
                ) : (
                  <Link
                    to="/registry-rules/$ruleId"
                    params={{ ruleId: row.rule_id }}
                    className="text-primary hover:underline"
                  >
                    {row.rule_name ?? row.rule_id}
                  </Link>
                )}
              </td>
              <td className="px-3 py-2">
                {mappedRuleColumns(row.column_mapping) || t("monitoredTables.implementedRules.wholeTable")}
              </td>
              <td className="px-3 py-2">
                {row.pass_threshold == null
                  ? t("monitoredTables.implementedRules.ruleDefault")
                  : `${row.pass_threshold}%`}
              </td>
              <td className="max-w-72 truncate px-3 py-2 font-mono" title={row.row_filter ?? ""}>
                {row.row_filter || (
                  <span className="font-sans text-muted-foreground">{t("monitoredTables.implementedRules.allRows")}</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Fetches one scope of rule applications on mount (i.e. only once its row is expanded) and lists them. */
function ImplementedRulesPanel({
  params,
  showTable,
  emptyText,
}: {
  params: ListImplementedRulesParams;
  showTable: boolean;
  emptyText: string;
}) {
  const { t } = useTranslation();
  const { data, isPending, isError, refetch } = useListImplementedRules(params);
  const rows = useMemo(
    () => sortImplementedRules(data?.data ?? [], showTable ? "table" : "rule"),
    [data, showTable],
  );

  if (isPending) {
    return (
      <div className="flex items-center gap-2 py-4 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        {t("common.loading")}
      </div>
    );
  }
  if (isError) {
    return (
      <div className="flex items-center gap-3 py-4 text-xs text-muted-foreground">
        {t("common.loadFailed")}
        <Button variant="outline" size="sm" className="h-7 gap-1.5 text-xs" onClick={() => void refetch()}>
          <RotateCcw className="h-3 w-3" />
          {t("common.retry")}
        </Button>
      </div>
    );
  }
  if (rows.length === 0) return <p className="py-4 text-xs text-muted-foreground">{emptyText}</p>;
  return <RuleAssignmentsList rows={rows} showTable={showTable} />;
}

/** A monitored table's applied rules — the Tables overview's "All rules" row expansion. */
export function TableAppliedRulesPanel({ bindingId }: { bindingId: string }) {
  const { t } = useTranslation();
  return (
    <ImplementedRulesPanel
      params={{ binding_id: bindingId }}
      showTable={false}
      emptyText={t("monitoredTables.implementedRules.noneForTable")}
    />
  );
}

/** The monitored tables a registry rule is applied to — the Rules overview's "Applied tables" row expansion. */
export function RuleAppliedTablesPanel({ ruleId }: { ruleId: string }) {
  const { t } = useTranslation();
  return (
    <ImplementedRulesPanel
      params={{ rule_id: ruleId }}
      showTable
      emptyText={t("monitoredTables.implementedRules.noneForRule")}
    />
  );
}
