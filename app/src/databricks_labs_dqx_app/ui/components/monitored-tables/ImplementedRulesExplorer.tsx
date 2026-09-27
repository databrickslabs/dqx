import { Fragment, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { ChevronRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ImplementedRuleOut } from "@/lib/api";
import { cn } from "@/lib/utils";

/** Distinct mapped columns of one rule application, or "" for a table-level rule. */
export function mappedRuleColumns(mapping: Array<Record<string, string>> | undefined): string {
  const columns = (mapping ?? []).flatMap((group) => Object.values(group));
  return Array.from(new Set(columns)).join(", ");
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

export interface RuleGroup {
  ruleId: string;
  name: string;
  dimension?: string | null;
  severity?: string | null;
  source?: string | null;
  rows: ImplementedRuleOut[];
}

/** Groups rule applications by registry rule, filtered by a free-text query over rule, table and columns. */
export function groupImplementedRules(rows: ImplementedRuleOut[], query: string): RuleGroup[] {
  const needle = query.trim().toLocaleLowerCase();
  const grouped = new Map<string, RuleGroup>();
  for (const row of rows) {
    const searchable = [row.rule_name, row.rule_id, row.table_fqn, mappedRuleColumns(row.column_mapping)].some(
      (value) => value?.toLocaleLowerCase().includes(needle),
    );
    if (needle && !searchable) continue;
    const group = grouped.get(row.rule_id) ?? {
      ruleId: row.rule_id,
      name: row.rule_name ?? row.rule_id,
      dimension: row.rule_dimension,
      severity: row.rule_severity,
      source: row.rule_source,
      rows: [],
    };
    group.rows.push(row);
    grouped.set(row.rule_id, group);
  }
  return [...grouped.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/** The Tables page's "Group by: Rule" view — one expandable row per applied registry rule. */
export function RulesGroupedTable({ rows, query }: { rows: ImplementedRuleOut[]; query: string }) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const groups = useMemo(() => groupImplementedRules(rows, query), [rows, query]);

  const toggle = (ruleId: string) => {
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(ruleId)) next.delete(ruleId);
      else next.add(ruleId);
      return next;
    });
  };

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50 hover:bg-muted/50">
            <TableHead className="w-10" />
            <TableHead className="text-xs font-medium">{t("monitoredTables.implementedRules.colRule")}</TableHead>
            <TableHead className="text-xs font-medium">{t("monitoredTables.implementedRules.colTables")}</TableHead>
            <TableHead className="text-xs font-medium">{t("monitoredTables.colDimension")}</TableHead>
            <TableHead className="text-xs font-medium">{t("monitoredTables.colSeverity")}</TableHead>
            <TableHead className="text-xs font-medium">{t("monitoredTables.implementedRules.colSource")}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {groups.map((group) => {
            const isOpen = expanded.has(group.ruleId);
            return (
              <Fragment key={group.ruleId}>
                <TableRow className="cursor-pointer" onClick={() => toggle(group.ruleId)}>
                  <TableCell className="w-10 p-2">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7"
                      aria-label={t(
                        isOpen ? "monitoredTables.implementedRules.collapse" : "monitoredTables.implementedRules.expand",
                        { name: group.name },
                      )}
                      aria-expanded={isOpen}
                    >
                      <ChevronRight className={cn("h-4 w-4 transition-transform", isOpen && "rotate-90")} />
                    </Button>
                  </TableCell>
                  <TableCell className="p-2 text-sm font-medium">{group.name}</TableCell>
                  <TableCell className="p-2 tabular-nums">{group.rows.length}</TableCell>
                  <TableCell className="p-2">{group.dimension || "—"}</TableCell>
                  <TableCell className="p-2">{group.severity || "—"}</TableCell>
                  <TableCell className="p-2">
                    {group.source ? <Badge variant="outline">{group.source}</Badge> : "—"}
                  </TableCell>
                </TableRow>
                {isOpen && (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={6} className="bg-muted/15 px-10 py-3">
                      <RuleAssignmentsList rows={group.rows} showTable />
                    </TableCell>
                  </TableRow>
                )}
              </Fragment>
            );
          })}
          {groups.length === 0 && (
            <TableRow>
              <TableCell colSpan={6} className="py-12 text-center text-sm text-muted-foreground">
                {query.trim()
                  ? t("monitoredTables.implementedRules.emptySearch")
                  : t("monitoredTables.implementedRules.empty")}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  );
}
