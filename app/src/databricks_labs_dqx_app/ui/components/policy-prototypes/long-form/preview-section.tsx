// THROWAWAY prototype — dry-run preview panel for the policy form (fed by mockPreview).
import { Fragment, useDeferredValue, useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { mockPreview, ruleById, type Policy } from "./model";

function StatTile({ value, one, many }: { value: number; one: string; many: string }) {
  return (
    <div className="rounded-lg border bg-card px-3 py-2.5">
      <div className="text-xl font-semibold tabular-nums">{value}</div>
      <div className="text-xs text-muted-foreground">{value === 1 ? one : many}</div>
    </div>
  );
}


function Mapping({ mapping }: { mapping: Record<string, string[]> }) {
  const entries = Object.entries(mapping);
  if (entries.length === 0) return <span className="text-muted-foreground">Whole table</span>;
  return (
    <div className="space-y-0.5 font-mono text-xs">
      {entries.map(([slot, cols]) => (
        <div key={slot}>
          <span className="text-muted-foreground">{`{{${slot}}}`}</span> → {cols.join(", ")}
        </div>
      ))}
    </div>
  );
}

export function PreviewSection({ policies }: { policies: Policy[] }) {
  const [ruleId, setRuleId] = useState(policies[0].rule_id);
  const current = policies.find((p) => p.rule_id === ruleId) ?? policies[0];
  const deferred = useDeferredValue(current);
  const preview = useMemo(() => mockPreview(deferred), [deferred]);
  const [showTables, setShowTables] = useState(false);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const toggle = (fqn: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(fqn)) next.delete(fqn);
      else next.add(fqn);
      return next;
    });

  // Only what WILL be attached: confident ("auto") attachments, on tables that have at least one.
  const withMatches = preview.tables
    .map((t) => ({ ...t, attachments: t.attachments.filter((a) => a.status === "auto") }))
    .filter((t) => t.attachments.length > 0);
  const active = withMatches.filter((t) => !t.skipped_reason);
  const listed = active;
  const columns = active.reduce(
    (n, t) =>
      n + new Set(t.attachments.filter((a) => a.status === "auto").flatMap((a) => Object.values(a.mapping).flat())).size,
    0,
  );
  const toRegister = current.register_tables ? active.filter((t) => !t.monitored).length : 0;
  const tableLevel = (ruleById(current.rule_id)?.slots.length ?? 0) === 0;

  return (
    <div className="space-y-4">
      {policies.length > 1 && (
        <div role="tablist" aria-label="Preview for rule" className="flex flex-wrap gap-1.5">
          {policies.map((p) => {
            const on = p.rule_id === current.rule_id;
            return (
              <button
                key={p.rule_id}
                type="button"
                role="tab"
                aria-selected={on}
                onClick={() => setRuleId(p.rule_id)}
                className={cn(
                  "rounded-full border px-2.5 py-1 text-xs font-medium transition-colors",
                  on
                    ? "border-primary bg-primary/10 text-foreground"
                    : "text-muted-foreground hover:border-foreground/30 hover:text-foreground",
                )}
              >
                {ruleById(p.rule_id)?.name ?? p.rule_id}
              </button>
            );
          })}
        </div>
      )}
      <div className="space-y-2">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          <StatTile value={active.length} one="table matched" many="tables matched" />
          {!tableLevel && <StatTile value={columns} one="column matched" many="columns matched" />}
          {current.register_tables && (
            <StatTile value={toRegister} one="new table to monitor" many="new tables to monitor" />
          )}
        </div>
        {preview.truncated && (
          <p className="text-xs text-amber-700 dark:text-amber-300">
            Preview stopped at 5,000 candidate tables. Narrow the scope for a complete count.
          </p>
        )}
      </div>

      <Button
        type="button"
        variant="outline"
        size="sm"
        className="gap-1.5 text-xs"
        onClick={() => setShowTables((v) => !v)}
        aria-expanded={showTables}
      >
        {showTables ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
        {showTables
          ? "Hide matching tables"
          : `Show ${listed.length} matching ${listed.length === 1 ? "table" : "tables"}`}
      </Button>

      {showTables && (
        <div className="overflow-hidden rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8 px-2" />
                <TableHead className="px-2">Table</TableHead>
                <TableHead className="px-2">State</TableHead>
                <TableHead className="px-2 text-right">Columns</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {listed.map((t) => {
                const open = expanded.has(t.table_fqn);
                return (
                  <Fragment key={t.table_fqn}>
                    <TableRow className="cursor-pointer" onClick={() => toggle(t.table_fqn)} aria-expanded={open}>
                      <TableCell className="px-2">
                        {open ? (
                          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                        ) : (
                          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                        )}
                      </TableCell>
                      <TableCell className="px-2 font-mono text-xs">{t.table_fqn}</TableCell>
                      <TableCell className="px-2 text-xs">
                        {t.monitored ? (
                          <span className="text-muted-foreground">Monitored</span>
                        ) : (
                          <span className="text-muted-foreground">Will be added (draft, unscheduled)</span>
                        )}
                      </TableCell>
                      <TableCell className="px-2 text-right text-xs tabular-nums">{new Set(t.attachments.flatMap((x) => Object.values(x.mapping).flat())).size || "—"}
                      </TableCell>
                    </TableRow>
                    {open && (
                      <TableRow className="hover:bg-transparent">
                        <TableCell />
                        <TableCell colSpan={3} className="px-2 pb-3">
                          <div className="overflow-hidden rounded-md border bg-muted/20">
                            <table className="w-full text-xs">
                              <thead className="text-left text-muted-foreground">
                                <tr className="border-b">
                                  <th className="px-3 py-1.5 font-medium">Column mapping</th>
                                  <th className="px-3 py-1.5 font-medium">Matched on</th>
                                </tr>
                              </thead>
                              <tbody>
                                {t.attachments.map((a, i) => (
                                  <tr key={i} className="border-b last:border-0 align-top">
                                    <td className="px-3 py-1.5">
                                      <Mapping mapping={a.mapping} />
                                    </td>
                                    <td className="px-3 py-1.5 text-muted-foreground">{a.reason}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </TableCell>
                      </TableRow>
                    )}
                  </Fragment>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
