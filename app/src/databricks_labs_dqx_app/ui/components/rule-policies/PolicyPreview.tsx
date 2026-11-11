// Dry-run preview for the policy form: what saving would attach, for the calling user.
// Only what WILL attach is shown — the backend already drops partial/ambiguous matches
// and tables the caller can't apply rules to.
import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, ChevronRight, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { usePreviewRulePolicy, type RulePolicyPreviewOut, type RulePolicyPreviewTableOut } from "@/lib/api";
import { cn } from "@/lib/utils";
import { distinctColumnCount, reasonLabel, toPreviewPayload, type PolicyDraft } from "./policy-form-state";

const PREVIEW_DEBOUNCE_MS = 400;
/** Stand-in name so an unnamed draft can still be previewed (name is never persisted here). */
const PREVIEW_NAME = "preview";

export interface PreviewTarget {
  ruleId: string;
  ruleName: string;
  hasSlots: boolean;
  draft: PolicyDraft;
}

function StatTile({ value, label }: { value: number; label: string }) {
  return (
    <div className="rounded-lg border bg-card px-3 py-2.5">
      <div className="text-xl font-semibold tabular-nums">{value}</div>
      <div className="text-xs text-muted-foreground">{label}</div>
    </div>
  );
}

function Mapping({ mapping }: { mapping: Record<string, string> }) {
  const { t } = useTranslation();
  const entries = Object.entries(mapping);
  if (entries.length === 0) return <span className="text-muted-foreground">{t("rulePolicies.preview.wholeTable")}</span>;
  return (
    <div className="space-y-0.5 font-mono text-xs">
      {entries.map(([slot, cols]) => (
        <div key={slot}>
          <span className="text-muted-foreground">{`{{${slot}}}`}</span> → {cols.split(",").join(", ")}
        </div>
      ))}
    </div>
  );
}

function MatchingTables({ tables }: { tables: RulePolicyPreviewTableOut[] }) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const toggle = (fqn: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(fqn)) next.delete(fqn);
      else next.add(fqn);
      return next;
    });
  return (
    <div className="overflow-hidden rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-8 px-2" />
            <TableHead className="px-2">{t("rulePolicies.preview.colTable")}</TableHead>
            <TableHead className="px-2">{t("rulePolicies.preview.colState")}</TableHead>
            <TableHead className="px-2 text-right">{t("rulePolicies.preview.colColumns")}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tables.map((tbl) => {
            const open = expanded.has(tbl.table_fqn);
            const columns = distinctColumnCount(tbl);
            return (
              <Fragment key={tbl.table_fqn}>
                <TableRow
                  className="cursor-pointer"
                  tabIndex={0}
                  onClick={() => toggle(tbl.table_fqn)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      toggle(tbl.table_fqn);
                    }
                  }}
                  aria-expanded={open}
                >
                  <TableCell className="px-2">
                    {open ? (
                      <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                    ) : (
                      <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                    )}
                  </TableCell>
                  <TableCell className="px-2 font-mono text-xs">{tbl.table_fqn}</TableCell>
                  <TableCell className="px-2 text-xs text-muted-foreground">
                    {tbl.monitored ? t("rulePolicies.preview.monitored") : t("rulePolicies.preview.willBeAdded")}
                  </TableCell>
                  <TableCell className="px-2 text-right text-xs tabular-nums">{columns || "—"}</TableCell>
                </TableRow>
                {open && (
                  <TableRow className="hover:bg-transparent">
                    <TableCell />
                    <TableCell colSpan={3} className="px-2 pb-3">
                      <div className="overflow-hidden rounded-md border bg-muted/20">
                        <table className="w-full text-xs">
                          <thead className="text-left text-muted-foreground">
                            <tr className="border-b">
                              <th className="px-3 py-1.5 font-medium">{t("rulePolicies.preview.columnMapping")}</th>
                              <th className="px-3 py-1.5 font-medium">{t("rulePolicies.preview.matchedOn")}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {tbl.mappings.map((m, i) => (
                              <tr key={i} className="border-b align-top last:border-0">
                                <td className="px-3 py-1.5">
                                  <Mapping mapping={m.column_mapping} />
                                </td>
                                <td className="px-3 py-1.5 text-muted-foreground">{reasonLabel(m.reason, t)}</td>
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
  );
}

/**
 * Debounced (400 ms) dry run of the selected rule's draft. Rules in *blockedRuleIds* (field
 * errors the backend would reject) skip the request.
 */
export function PolicyPreview({
  targets,
  blockedRuleIds,
}: {
  targets: PreviewTarget[];
  blockedRuleIds: Set<string>;
}) {
  const { t } = useTranslation();
  const [selectedRuleId, setSelectedRuleId] = useState(targets[0]?.ruleId ?? "");
  const current = targets.find((x) => x.ruleId === selectedRuleId) ?? targets[0];
  const [showTables, setShowTables] = useState(false);
  const [result, setResult] = useState<RulePolicyPreviewOut | null>(null);
  const [failed, setFailed] = useState(false);
  const [pending, setPending] = useState(false);
  const { mutateAsync } = usePreviewRulePolicy();
  const blocked = current ? blockedRuleIds.has(current.ruleId) : true;
  const seq = useRef(0);

  const payloadJson = useMemo(() => {
    if (!current) return "";
    const payload = toPreviewPayload(current.draft);
    return JSON.stringify({ ...payload, policy: { ...payload.policy, name: payload.policy.name || PREVIEW_NAME } });
  }, [current]);

  useEffect(() => {
    if (!payloadJson || blocked) return;
    const id = ++seq.current;
    setPending(true);
    const timer = setTimeout(() => {
      mutateAsync({ data: JSON.parse(payloadJson) })
        .then((res) => {
          if (id !== seq.current) return;
          setResult(res.data);
          setFailed(false);
        })
        .catch(() => {
          if (id !== seq.current) return;
          setFailed(true);
        })
        .finally(() => {
          if (id === seq.current) setPending(false);
        });
    }, PREVIEW_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [payloadJson, blocked, mutateAsync]);

  if (!current) return null;

  const tables = result?.tables ?? [];
  const unreadable = result?.unreadable_catalogs?.length ?? 0;

  return (
    <div className="space-y-4">
      {targets.length > 1 && (
        <div role="tablist" aria-label={t("rulePolicies.preview.forRule")} className="flex flex-wrap gap-1.5">
          {targets.map((target) => {
            const on = target.ruleId === current.ruleId;
            return (
              <button
                key={target.ruleId}
                type="button"
                role="tab"
                aria-selected={on}
                onClick={() => setSelectedRuleId(target.ruleId)}
                className={cn(
                  "rounded-full border px-2.5 py-1 text-xs font-medium transition-colors",
                  on
                    ? "border-primary bg-primary/10 text-foreground"
                    : "text-muted-foreground hover:border-foreground/30 hover:text-foreground",
                )}
              >
                {target.ruleName}
              </button>
            );
          })}
        </div>
      )}

      {blocked ? (
        <p className="text-xs text-muted-foreground">{t("rulePolicies.preview.incomplete")}</p>
      ) : failed && !result ? (
        <p className="text-xs text-destructive">{t("rulePolicies.preview.failed")}</p>
      ) : !result ? (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
          {t("common.loading")}
        </div>
      ) : (
        <div className={cn("space-y-4 transition-opacity", pending && "opacity-60")} aria-busy={pending}>
          <div className="space-y-2">
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
              <StatTile
                value={result.tables_matched}
                label={t("rulePolicies.preview.tablesMatched", { count: result.tables_matched })}
              />
              {current.hasSlots && (
                <StatTile
                  value={result.columns_matched}
                  label={t("rulePolicies.preview.columnsMatched", { count: result.columns_matched })}
                />
              )}
              {current.draft.registerTables && (
                <StatTile
                  value={result.new_tables}
                  label={t("rulePolicies.preview.newTables", { count: result.new_tables })}
                />
              )}
            </div>
            {failed && <p className="text-xs text-destructive">{t("rulePolicies.preview.failed")}</p>}
            {unreadable > 0 && (
              <p className="text-xs text-muted-foreground">
                {t("rulePolicies.preview.unreadableCatalogs", { count: unreadable })}
              </p>
            )}
            {result.truncated && <p className="text-xs text-muted-foreground">{t("rulePolicies.preview.truncated")}</p>}
          </div>

          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            onClick={() => setShowTables((v) => !v)}
            aria-expanded={showTables}
            disabled={tables.length === 0}
          >
            {showTables ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            {showTables
              ? t("rulePolicies.preview.hideTables")
              : t("rulePolicies.preview.showTables", { count: tables.length })}
          </Button>

          {showTables && tables.length > 0 && <MatchingTables tables={tables} />}
        </div>
      )}
    </div>
  );
}
