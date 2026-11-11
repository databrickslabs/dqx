// THROWAWAY prototype — dry-run preview panel for the policy form (fed by mockPreview).
import { Fragment, useDeferredValue, useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { mockPreview, ruleById, type Policy, type PreviewAttachment } from "./model";

function AttachmentStatus({ a, skipped }: { a: PreviewAttachment; skipped: boolean }) {
  const label = skipped
    ? "Not attached · skipped"
    : a.status === "partial"
      ? "Not attached · incomplete match"
      : a.status === "ambiguous"
        ? "Not attached — several possible columns; shown as a suggestion on the table"
        : "Will attach";
  const tone = skipped
    ? "bg-muted text-muted-foreground ring-border"
    : a.status === "auto"
      ? "bg-emerald-500/10 text-emerald-700 ring-emerald-500/30 dark:text-emerald-300"
      : a.status === "ambiguous"
        ? "bg-amber-500/10 text-amber-700 ring-amber-500/30 dark:text-amber-300"
        : "bg-muted text-muted-foreground ring-border";
  return <span className={cn("inline-flex rounded-md px-2 py-0.5 text-[10px] font-medium ring-1", tone)}>{label}</span>;
}

function plural(n: number, one: string, many: string): string {
  return `${n} ${n === 1 ? one : many}`;
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

  const active = preview.tables.filter((t) => !t.skipped_reason);
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
      <div className="space-y-1 rounded-lg border bg-card px-4 py-3">
        <p className="text-sm">
          <span className="font-semibold tabular-nums">
            {plural(active.length, "table", "tables")}
            {!tableLevel && ` · ${plural(columns, "column", "columns")}`}
          </span>{" "}
          would get this rule
          {toRegister > 0 && (
            <>
              , <span className="font-semibold tabular-nums">{toRegister}</span> not yet monitored would be added
            </>
          )}
          .
        </p>
        {preview.skipped > 0 && (
          <p className="text-xs text-muted-foreground">
            {preview.skipped === 1
              ? "1 matching table is skipped because you can't apply rules to it."
              : `${preview.skipped} matching tables are skipped because you can't apply rules to them.`}
          </p>
        )}
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
        {showTables ? "Hide matching tables" : `Show ${preview.tables.length} matching tables`}
      </Button>

      {showTables && (
        <div className="overflow-hidden rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8 px-2" />
                <TableHead className="px-2">Table</TableHead>
                <TableHead className="px-2">State</TableHead>
                <TableHead className="px-2 text-right">Attachments</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {preview.tables.map((t) => {
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
                        {t.skipped_reason ? (
                          <span className="text-muted-foreground">Skipped · you can't apply rules here</span>
                        ) : t.monitored ? (
                          <span className="text-muted-foreground">Monitored</span>
                        ) : (
                          <span className="text-muted-foreground">Will be added (draft, unscheduled)</span>
                        )}
                      </TableCell>
                      <TableCell className="px-2 text-right text-xs tabular-nums">{t.skipped_reason ? 0 : t.attachments.filter((a) => a.status === "auto").length}
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
                                  <th className="px-3 py-1.5 font-medium">Result</th>
                                  <th className="px-3 py-1.5 font-medium">Reason</th>
                                </tr>
                              </thead>
                              <tbody>
                                {t.attachments.map((a, i) => (
                                  <tr key={i} className="border-b last:border-0 align-top">
                                    <td className="px-3 py-1.5">
                                      <Mapping mapping={a.mapping} />
                                    </td>
                                    <td className="px-3 py-1.5">
                                      <AttachmentStatus a={a} skipped={!!t.skipped_reason} />
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
