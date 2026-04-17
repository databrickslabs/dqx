import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState, Suspense, useMemo } from "react";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  ShieldCheck,
  Plus,
  ChevronDown,
  ChevronRight,
  Database,
  Trash2,
} from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { toast } from "sonner";
import {
  useListRules,
  type RuleCatalogEntryOut,
} from "@/lib/api";
import { deleteRuleById } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { parseFqn, formatUser } from "@/lib/format-utils";

const SQL_CHECK_PREFIX = "__sql_check__/";

export const Route = createFileRoute("/_sidebar/rules/active")({
  component: () => (
    <Suspense fallback={<ActiveRulesSkeleton />}>
      <ActiveRulesPage />
    </Suspense>
  ),
});

function ActiveRulesSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-4 w-64" />
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

type ViewMode = "by-table" | "by-rule" | "sql-checks";

function ActiveRulesPage() {
  const navigate = useNavigate();
  const [viewMode, setViewMode] = useState<ViewMode>("by-table");
  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const { canCreateRules, canApproveRules } = usePermissions();
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);

  const { data: rulesResp, isLoading, error, refetch } = useListRules({ status: "approved" });
  const allRules: RuleCatalogEntryOut[] = Array.isArray(rulesResp?.data) ? rulesResp.data : [];

  const { catalogs, schemasByCatalog, sources } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    const sourceSet = new Set<string>();
    for (const rule of allRules) {
      const { catalog, schema } = parseFqn(rule.table_fqn);
      if (catalog) {
        catalogSet.add(catalog);
        if (!schemaMap.has(catalog)) schemaMap.set(catalog, new Set());
        if (schema) schemaMap.get(catalog)!.add(schema);
      }
      if (rule.source) sourceSet.add(rule.source);
    }
    return {
      catalogs: Array.from(catalogSet).sort(),
      schemasByCatalog: Object.fromEntries(
        Array.from(schemaMap.entries()).map(([cat, schemas]) => [cat, Array.from(schemas).sort()]),
      ),
      sources: Array.from(sourceSet).sort(),
    };
  }, [allRules]);

  const filteredRules = useMemo(() => {
    return allRules.filter((rule) => {
      const { catalog, schema } = parseFqn(rule.table_fqn);
      if (catalogFilter !== "all" && catalog !== catalogFilter) return false;
      if (schemaFilter !== "all" && schema !== schemaFilter) return false;
      if (sourceFilter !== "all" && (rule.source ?? "ui") !== sourceFilter) return false;
      return true;
    });
  }, [allRules, catalogFilter, schemaFilter, sourceFilter]);

  const availableSchemas = catalogFilter !== "all" ? schemasByCatalog[catalogFilter] || [] : [];

  const handleCatalogChange = (value: string) => {
    setCatalogFilter(value);
    setSchemaFilter("all");
  };

  const toggleTable = (fqn: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev);
      next.has(fqn) ? next.delete(fqn) : next.add(fqn);
      return next;
    });
  };

  const allChecks = useMemo(() => {
    return filteredRules.map((rule) => {
      const check = rule.checks[0] ?? {};
      return { ...check, _tableFqn: rule.table_fqn, _displayName: rule.display_name || rule.table_fqn, _version: rule.version, _ruleId: rule.rule_id };
    });
  }, [filteredRules]);

  const groupedByTable = useMemo(() => {
    const map = new Map<string, RuleCatalogEntryOut[]>();
    for (const rule of filteredRules) {
      const key = rule.table_fqn;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(rule);
    }
    return Array.from(map.entries()).map(([fqn, rules]) => ({ fqn, displayName: rules[0].display_name || fqn, rules }));
  }, [filteredRules]);

  const totalCheckCount = allRules.length;

  const handleDelete = async (rule: RuleCatalogEntryOut) => {
    if (pendingDelete) return;
    const label = rule.display_name || rule.table_fqn;
    if (!confirm(`Delete this rule for ${label}? This cannot be undone.`)) return;
    const ruleId = rule.rule_id;
    if (!ruleId) return;
    setPendingDelete(ruleId);
    try {
      await deleteRuleById(ruleId);
      toast.success("Rule deleted");
      refetch();
    } catch {
      toast.error("Failed to delete rule");
    } finally {
      setPendingDelete(null);
    }
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[]} page="Active Rules" />
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Active rules</h1>
            <p className="text-muted-foreground">
              Approved rules currently enforced on your tables.
            </p>
          </div>
          {canCreateRules && (
            <Button onClick={() => navigate({ to: "/rules/create" })} className="gap-2">
              <Plus className="h-4 w-4" />
              Create rules
            </Button>
          )}
        </div>
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-col gap-4">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <ShieldCheck className="h-5 w-5" />
                  Approved rule sets
                </CardTitle>
                <CardDescription>
                  {isLoading
                    ? "Loading..."
                    : `${filteredRules.length} table${filteredRules.length !== 1 ? "s" : ""} · ${totalCheckCount} total checks`}
                </CardDescription>
              </div>
            </div>

            <div className="flex items-center gap-2 flex-wrap">
              <Select value={catalogFilter} onValueChange={handleCatalogChange}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder="All Catalogs" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Catalogs</SelectItem>
                  {catalogs.map((cat) => (
                    <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={schemaFilter} onValueChange={setSchemaFilter} disabled={catalogFilter === "all"}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder="All Schemas" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Schemas</SelectItem>
                  {availableSchemas.map((sch) => (
                    <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={sourceFilter} onValueChange={setSourceFilter}>
                <SelectTrigger className="w-[140px]">
                  <SelectValue placeholder="All Sources" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Sources</SelectItem>
                  {sources.map((src) => (
                    <SelectItem key={src} value={src}>
                      {src === "imported" ? "Imported" : src === "ai" ? "AI" : "UI"}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              {(catalogFilter !== "all" || schemaFilter !== "all" || sourceFilter !== "all") && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-9 text-xs"
                  onClick={() => { setCatalogFilter("all"); setSchemaFilter("all"); setSourceFilter("all"); }}
                >
                  Clear filters
                </Button>
              )}

              <div className="ml-auto flex items-center gap-1 border rounded-md p-0.5">
                {(
                  [
                    { key: "by-table", label: "By table" },
                    { key: "by-rule", label: "By rule" },
                    { key: "sql-checks", label: "SQL checks" },
                  ] as const
                ).map((mode) => (
                  <button
                    key={mode.key}
                    type="button"
                    onClick={() => setViewMode(mode.key)}
                    className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                      viewMode === mode.key
                        ? "bg-primary text-primary-foreground"
                        : "text-muted-foreground hover:bg-muted"
                    }`}
                  >
                    {mode.label}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </CardHeader>

        <CardContent>
          {isLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} className="h-14 w-full" />)}
            </div>
          )}

          {error && (
            <p className="text-destructive text-sm">Failed to load rules: {(error as Error).message}</p>
          )}

          {!isLoading && !error && filteredRules.length > 0 && (
            <FadeIn duration={0.3}>
              {viewMode === "by-table" && (
                <ByTableView
                  groups={groupedByTable}
                  expandedTables={expandedTables}
                  onToggle={toggleTable}
                  onNavigate={(fqn) =>
                    fqn.startsWith(SQL_CHECK_PREFIX)
                      ? navigate({ to: "/rules/create-sql", search: { edit: fqn } })
                      : navigate({ to: "/rules/generate", search: { table: fqn } })
                  }
                  canDelete={canApproveRules}
                  onDelete={handleDelete}
                  pendingDelete={pendingDelete}
                />
              )}
              {viewMode === "by-rule" && (
                <ByRuleView checks={allChecks} />
              )}
              {viewMode === "sql-checks" && (
                <SqlChecksView checks={allChecks} />
              )}
            </FadeIn>
          )}

          {!isLoading && !error && filteredRules.length === 0 && (
            <EmptyState canCreate={canCreateRules} onNavigate={() => navigate({ to: "/rules/create" })} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

// ── By table view: collapsible per-table groups ─────────────────────────────

interface TableGroup {
  fqn: string;
  displayName: string;
  rules: RuleCatalogEntryOut[];
}

interface ByTableViewProps {
  groups: TableGroup[];
  expandedTables: Set<string>;
  onToggle: (fqn: string) => void;
  onNavigate: (fqn: string) => void;
  canDelete: boolean;
  onDelete: (rule: RuleCatalogEntryOut) => void;
  pendingDelete: string | null;
}

function ByTableView({ groups, expandedTables, onToggle, onNavigate, canDelete, onDelete, pendingDelete }: ByTableViewProps) {
  return (
    <div className="space-y-2">
      {groups.map(({ fqn, displayName, rules }) => {
        const isOpen = expandedTables.has(fqn);
        return (
          <div key={fqn} className="border rounded-lg overflow-hidden">
            <button
              type="button"
              className="w-full flex items-center gap-3 p-3 text-sm hover:bg-muted/30 transition-colors text-left"
              onClick={() => onToggle(fqn)}
            >
              {isOpen ? (
                <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
              ) : (
                <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />
              )}
              <code className="font-mono text-xs font-medium flex-1">{displayName}</code>
              <span className="text-xs text-muted-foreground">
                {rules.length} rule{rules.length !== 1 ? "s" : ""}
              </span>
            </button>
            {isOpen && (
              <div className="border-t">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-muted/30">
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">Function</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">Column(s)</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">Criticality</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">Source</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">Created by</th>
                      {canDelete && (
                        <th className="text-right p-2 px-4 font-medium text-xs text-muted-foreground" />
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {rules.map((rule) => {
                      const check = (rule.checks[0] ?? {}) as Record<string, unknown>;
                      const checkObj = (check.check as Record<string, unknown>) ?? {};
                      const args = (checkObj.arguments as Record<string, unknown>) ?? {};
                      const fn = String(checkObj.function ?? "—");
                      const col = String(args.column ?? (Array.isArray(args.columns) ? args.columns.join(", ") : args.columns) ?? "—");
                      const criticality = String(check.criticality ?? "warn");
                      return (
                        <tr key={rule.rule_id ?? rule.table_fqn} className="border-t border-border/50 hover:bg-muted/20">
                          <td className="p-2 px-4 font-mono text-xs">{fn}</td>
                          <td className="p-2 px-4 text-xs text-muted-foreground">{col}</td>
                          <td className="p-2 px-4">
                            <Badge
                              variant="outline"
                              className={`text-[10px] ${criticality === "error" ? "border-red-500 text-red-600" : "border-amber-500 text-amber-600"}`}
                            >
                              {criticality}
                            </Badge>
                          </td>
                          <td className="p-2 px-4">
                            <Badge
                              variant="outline"
                              className={`text-[10px] py-0 px-1.5 ${
                                rule.source === "imported"
                                  ? "border-blue-500 text-blue-600"
                                  : rule.source === "ai"
                                    ? "border-purple-500 text-purple-600"
                                    : "border-emerald-500 text-emerald-600"
                              }`}
                            >
                              {rule.source === "imported" ? "Imported" : rule.source === "ai" ? "AI" : "UI"}
                            </Badge>
                          </td>
                          <td className="p-2 px-4 text-xs text-muted-foreground">{formatUser(rule.created_by)}</td>
                          {canDelete && (
                            <td className="p-2 px-4 text-right">
                              <Button
                                variant="ghost"
                                size="sm"
                                disabled={pendingDelete === rule.rule_id}
                                onClick={() => onDelete(rule)}
                                className="h-6 text-xs text-destructive"
                              >
                                <Trash2 className="h-3 w-3" />
                              </Button>
                            </td>
                          )}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                <div className="border-t p-2 px-4 flex justify-end gap-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={() => onNavigate(fqn)}
                  >
                    View / edit rules
                  </Button>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── By rule view: flat list of all checks across tables ─────────────────────

interface CheckWithMeta {
  _tableFqn: string;
  _displayName: string;
  _version: number;
  _ruleId?: string | null;
  [key: string]: unknown;
}

function ByRuleView({ checks }: { checks: CheckWithMeta[] }) {
  const nonSqlChecks = checks.filter((c) => {
    const checkObj = (c.check as Record<string, unknown>) ?? {};
    return String(checkObj.function ?? "") !== "sql_query";
  });

  if (nonSqlChecks.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground text-sm">
        No column / dataset rules found. Switch to "SQL checks" to view query-based rules.
      </div>
    );
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b bg-muted/50">
            <th className="text-left p-3 font-medium">Name</th>
            <th className="text-left p-3 font-medium">Function</th>
            <th className="text-left p-3 font-medium">Column(s)</th>
            <th className="text-left p-3 font-medium">Table</th>
            <th className="text-left p-3 font-medium">Criticality</th>
            <th className="text-right p-3 font-medium">Weight</th>
          </tr>
        </thead>
        <tbody>
          {nonSqlChecks.map((check, idx) => {
            const checkObj = (check.check as Record<string, unknown>) ?? {};
            const args = (checkObj.arguments as Record<string, unknown>) ?? {};
            const fn = String(checkObj.function ?? "—");
            const col = String(args.column ?? checkObj.for_each_column ?? "—");
            const criticality = String(check.criticality ?? "warn");
            const weight = check.weight != null ? Number(check.weight) : null;
            const name = check.name ? String(check.name) : null;
            return (
              <tr key={idx} className="border-b last:border-b-0 hover:bg-muted/30">
                <td className="p-3 text-xs text-muted-foreground">
                  {name ?? <span className="italic text-muted-foreground/60">—</span>}
                </td>
                <td className="p-3 font-mono text-xs">{fn}</td>
                <td className="p-3 text-xs text-muted-foreground">{col}</td>
                <td className="p-3 font-mono text-xs">{check._displayName}</td>
                <td className="p-3">
                  <Badge
                    variant="outline"
                    className={`text-[10px] ${criticality === "error" ? "border-red-500 text-red-600" : "border-amber-500 text-amber-600"}`}
                  >
                    {criticality}
                  </Badge>
                </td>
                <td className="p-3 text-right tabular-nums text-xs">
                  {weight != null ? weight : "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── SQL checks view ─────────────────────────────────────────────────────────

function SqlChecksView({ checks }: { checks: CheckWithMeta[] }) {
  const sqlChecks = checks.filter((c) => {
    const checkObj = (c.check as Record<string, unknown>) ?? {};
    return String(checkObj.function ?? "") === "sql_query";
  });

  if (sqlChecks.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        <Database className="h-10 w-10 mx-auto mb-3 opacity-40" />
        <p className="text-sm font-medium">No SQL checks</p>
        <p className="text-xs mt-1">SQL query-based dataset checks will appear here once created and approved.</p>
      </div>
    );
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b bg-muted/50">
            <th className="text-left p-3 font-medium">Name</th>
            <th className="text-left p-3 font-medium">Mode</th>
            <th className="text-left p-3 font-medium">Table</th>
            <th className="text-left p-3 font-medium">Criticality</th>
          </tr>
        </thead>
        <tbody>
          {sqlChecks.map((check, idx) => {
            const checkObj = (check.check as Record<string, unknown>) ?? {};
            const args = (checkObj.arguments as Record<string, unknown>) ?? {};
            const name = String(check.name ?? args.name ?? "sql_query");
            const mergeColumns = args.merge_columns as string[] | undefined;
            const mode = mergeColumns && mergeColumns.length > 0 ? "Row-level" : "Dataset-level";
            const criticality = String(check.criticality ?? "warn");
            return (
              <tr key={idx} className="border-b last:border-b-0 hover:bg-muted/30">
                <td className="p-3 font-mono text-xs">{name}</td>
                <td className="p-3">
                  <Badge variant="secondary" className="text-[10px]">{mode}</Badge>
                </td>
                <td className="p-3 font-mono text-xs">{check._displayName}</td>
                <td className="p-3">
                  <Badge
                    variant="outline"
                    className={`text-[10px] ${criticality === "error" ? "border-red-500 text-red-600" : "border-amber-500 text-amber-600"}`}
                  >
                    {criticality}
                  </Badge>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── Empty state ─────────────────────────────────────────────────────────────

function EmptyState({ canCreate, onNavigate }: { canCreate: boolean; onNavigate: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
        <ShieldCheck className="h-8 w-8 text-muted-foreground" />
      </div>
      <h3 className="text-lg font-medium text-muted-foreground">No active rules</h3>
      <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
        {canCreate
          ? "Create and approve rules to see them here. Active rules are enforced during quality runs."
          : "No rules have been approved yet. Check Drafts & review for pending rule sets."}
      </p>
      {canCreate && (
        <Button onClick={onNavigate} className="mt-4 gap-2">
          <Plus className="h-4 w-4" />
          Create rules
        </Button>
      )}
    </div>
  );
}
