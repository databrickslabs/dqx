// THROWAWAY prototype — dry-run preview panel for the policy form (fed by mockPreview).
import { Fragment, useDeferredValue, useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { mockPreview, type Policy, type PreviewAttachment } from "./model";

function Stat({ label, value, tone }: { label: string; value: number; tone?: "warn" | "muted" }) {
  return (
    <div className="rounded-lg border bg-card px-3 py-2.5">
      <div
        className={cn(
          "text-xl font-semibold tabular-nums",
          tone === "warn" && value > 0 && "text-amber-600 dark:text-amber-400",
          tone === "muted" && "text-muted-foreground",
        )}
      >
        {value}
      </div>
      <div className="text-xs text-muted-foreground">{label}</div>
    </div>
  );
}

function AttachmentStatus({ a, action }: { a: PreviewAttachment; action: Policy["action"] }) {
  const label =
    a.status === "partial"
      ? "Partial · not attached"
      : a.status === "ambiguous" || action === "suggest"
        ? "Will be suggested"
        : "Will attach";
  const tone =
    a.status === "partial"
      ? "bg-muted text-muted-foreground ring-border"
      : a.status === "ambiguous" || action === "suggest"
        ? "bg-amber-500/10 text-amber-700 ring-amber-500/30 dark:text-amber-300"
        : "bg-emerald-500/10 text-emerald-700 ring-emerald-500/30 dark:text-emerald-300";
  return (
    <span className={cn("inline-flex rounded-full px-2 py-0.5 text-[10px] font-medium whitespace-nowrap ring-1", tone)}>
      {label}
    </span>
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

export function PreviewSection({ draft }: { draft: Policy }) {
  const deferred = useDeferredValue(draft);
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

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
        <Stat label="Monitored tables matched" value={preview.matched_monitored} />
        <Stat label={draft.action === "suggest" ? "Suggestions" : "Attachments"} value={preview.attachments} />
        <Stat label="Tables to register" value={draft.register_tables ? preview.to_register : 0} tone="muted" />
        <Stat label="Skipped · no APPLY or MANAGE" value={preview.skipped} tone="warn" />
        <Stat label="Ambiguous · suggested" value={preview.ambiguous} tone="warn" />
      </div>
      {preview.truncated && (
        <p className="text-xs text-amber-700 dark:text-amber-300">
          Preview stopped at 5,000 candidate tables. Narrow the scope for a complete count.
        </p>
      )}

      <Button
        type="button"
        variant="outline"
        size="sm"
        className="gap-1.5 text-xs"
        onClick={() => setShowTables((v) => !v)}
        aria-expanded={showTables}
      >
        {showTables ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
        {showTables ? "Hide matched tables" : `Show ${preview.tables.length} matched tables`}
      </Button>

      {showTables && (
        <div className="overflow-hidden rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8 px-2" />
                <TableHead className="px-2">Table</TableHead>
                <TableHead className="px-2">State</TableHead>
                <TableHead className="px-2 text-right">Rules</TableHead>
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
                          <span className="text-amber-700 dark:text-amber-300">Skipped · {t.skipped_reason}</span>
                        ) : t.monitored ? (
                          <span className="text-muted-foreground">Monitored</span>
                        ) : (
                          <span className="text-muted-foreground">Will be registered (draft, unscheduled)</span>
                        )}
                      </TableCell>
                      <TableCell className="px-2 text-right text-xs tabular-nums">{t.attachments.length}</TableCell>
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
                                      <AttachmentStatus a={a} action={draft.action} />
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
