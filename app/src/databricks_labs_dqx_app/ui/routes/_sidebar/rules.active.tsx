import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState, Suspense, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
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
  Download,
  Upload,
} from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { toast } from "sonner";
import yaml from "js-yaml";
import {
  useListRules,
  type RuleCatalogEntryOut,
} from "@/lib/api";
import { deleteRuleById, pushRulesToTable, type PushToTableIn } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { parseFqn, formatUser, getUserMetadata, labelToken } from "@/lib/format-utils";
import { LabelFilter, LabelsBadges, labelsMatchFilter } from "@/components/Labels";
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogCancel,
  AlertDialogAction,
} from "@/components/ui/alert-dialog";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const SQL_CHECK_PREFIX = "__sql_check__/";
const CROSS_TABLE_CATALOG = "Cross-table rules";

function isCrossTableRule(rule: { table_fqn: string; checks: unknown[] }): boolean {
  if (rule.table_fqn.startsWith(SQL_CHECK_PREFIX)) return true;
  const check = rule.checks[0] as Record<string, unknown> | undefined;
  return (check?.check as Record<string, unknown>)?.function === "sql_query";
}

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
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [viewMode, setViewMode] = useState<ViewMode>("by-table");
  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const { canCreateRules, canApproveRules, canExportRules } = usePermissions();
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [labelFilter, setLabelFilter] = useState<Set<string>>(new Set());

  // Push to table dialog state
  const [pushTarget, setPushTarget] = useState<{ fqn: string; rules: RuleCatalogEntryOut[] } | null>(null);
  const [pushTableFqn, setPushTableFqn] = useState("");
  const [pushRunConfig, setPushRunConfig] = useState("");
  const [pushMode, setPushMode] = useState<"append" | "overwrite">("append");
  const [isPushing, setIsPushing] = useState(false);

  const openPushDialog = (fqn: string, rules: RuleCatalogEntryOut[]) => {
    setPushTarget({ fqn, rules });
    setPushTableFqn("");
    setPushRunConfig(fqn);
    setPushMode("append");
  };

  const handlePushToTable = async () => {
    if (!pushTarget || !pushTableFqn.trim()) return;
    const allChecks = pushTarget.rules.flatMap((r) => r.checks as Array<Record<string, unknown>>);
    const body: PushToTableIn = {
      checks: allChecks,
      target_table: pushTableFqn.trim(),
      run_config_name: pushRunConfig.trim() || pushTarget.fqn,
      mode: pushMode,
    };
    setIsPushing(true);
    try {
      const resp = await pushRulesToTable(body);
      toast.success(t("rulesActive.pushSuccess", { count: resp.data.pushed_count, table: pushTableFqn.trim() }));
      setPushTarget(null);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? t("rulesActive.pushFailed");
      toast.error(msg);
    } finally {
      setIsPushing(false);
    }
  };

  const { data: rulesResp, isLoading, error, refetch } = useListRules({ status: "approved" });
  const allRules: RuleCatalogEntryOut[] = Array.isArray(rulesResp?.data) ? rulesResp.data : [];

  // Collect every distinct ``key=value`` pair seen on an approved rule's
  // checks. Used to populate the LabelFilter dropdown.
  const availableLabels = useMemo(() => {
    const seen = new Set<string>();
    const out: { key: string; value: string }[] = [];
    for (const rule of allRules) {
      for (const check of rule.checks) {
        const md = getUserMetadata(check as Record<string, unknown>);
        for (const [key, value] of Object.entries(md)) {
          const tok = labelToken(key, value);
          if (!seen.has(tok)) {
            seen.add(tok);
            out.push({ key, value });
          }
        }
      }
    }
    return out;
  }, [allRules]);

  const { catalogs, schemasByCatalog, sources } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    const sourceSet = new Set<string>();
    for (const rule of allRules) {
      if (isCrossTableRule(rule)) {
        catalogSet.add(CROSS_TABLE_CATALOG);
      } else {
        const { catalog, schema } = parseFqn(rule.table_fqn);
        if (catalog) {
          catalogSet.add(catalog);
          if (!schemaMap.has(catalog)) schemaMap.set(catalog, new Set());
          if (schema) schemaMap.get(catalog)!.add(schema);
        }
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
      const isSqlCheck = isCrossTableRule(rule);
      if (catalogFilter !== "all") {
        if (catalogFilter === CROSS_TABLE_CATALOG) {
          if (!isSqlCheck) return false;
        } else {
          if (isSqlCheck) return false;
          const { catalog } = parseFqn(rule.table_fqn);
          if (catalog !== catalogFilter) return false;
        }
      }
      if (schemaFilter !== "all" && !isSqlCheck) {
        const { schema } = parseFqn(rule.table_fqn);
        if (schema !== schemaFilter) return false;
      }
      if (sourceFilter !== "all" && (rule.source ?? "ui") !== sourceFilter) return false;
      // Match the label filter against the user_metadata of *any* check on
      // the rule entry. ``labelFilter`` is a Set of ``key=value`` tokens; an
      // empty set means "no label filter applied".
      if (labelFilter.size > 0) {
        const matched = rule.checks.some((c) =>
          labelsMatchFilter(getUserMetadata(c as Record<string, unknown>), labelFilter),
        );
        if (!matched) return false;
      }
      return true;
    });
  }, [allRules, catalogFilter, schemaFilter, sourceFilter, labelFilter]);

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

  const [deleteTarget, setDeleteTarget] = useState<RuleCatalogEntryOut | null>(null);

  const requestDelete = (rule: RuleCatalogEntryOut) => {
    if (pendingDelete) return;
    setDeleteTarget(rule);
  };

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    const ruleId = deleteTarget.rule_id;
    setDeleteTarget(null);
    if (!ruleId) return;
    setPendingDelete(ruleId);
    try {
      await deleteRuleById(ruleId);
      toast.success(t("rulesActive.ruleDeleted"));
      refetch();
    } catch {
      toast.error(t("rulesActive.failedDelete"));
    } finally {
      setPendingDelete(null);
    }
  };

  const exportRulesAsYaml = (rules: RuleCatalogEntryOut[], filename: string) => {
    // Project each saved check into a clean, deterministic shape so the
    // exported YAML is stable, easy to diff, and *always* surfaces
    // user_metadata (labels, including weight) when present. Building the
    // dict explicitly also guards against any internal/UI-only fields
    // (``_tableFqn``, etc.) leaking through, and against js-yaml dropping
    // properties from objects with non-enumerable / inherited keys.
    const projectCheck = (raw: Record<string, unknown>): Record<string, unknown> => {
      const out: Record<string, unknown> = {};
      if (typeof raw.name === "string" && raw.name) out.name = raw.name;
      if (typeof raw.criticality === "string") out.criticality = raw.criticality;
      // Fold any legacy top-level numeric ``weight`` into user_metadata so
      // older rows still export with their weight visible as a label.
      const labels = getUserMetadata(raw);
      if (typeof raw.weight === "number" && !("weight" in labels)) {
        labels.weight = String(raw.weight);
      }
      const checkObj = (raw.check as Record<string, unknown>) ?? null;
      if (checkObj && typeof checkObj === "object") {
        out.check = {
          function: String(checkObj.function ?? ""),
          arguments: (checkObj.arguments as Record<string, unknown>) ?? {},
        };
      }
      if (Object.keys(labels).length > 0) {
        // Plain object literal so js-yaml dumps it as a nested mapping.
        out.user_metadata = { ...labels };
      }
      return out;
    };

    const grouped = new Map<string, Record<string, unknown>[]>();
    for (const rule of rules) {
      const fqn = rule.table_fqn;
      if (!grouped.has(fqn)) grouped.set(fqn, []);
      for (const c of rule.checks) {
        grouped.get(fqn)!.push(projectCheck(c as Record<string, unknown>));
      }
    }

    const sections: string[] = [];
    for (const [fqn, checks] of grouped) {
      sections.push(
        `# ${fqn}\n${yaml.dump(checks, { lineWidth: -1, sortKeys: false, noRefs: true })}`,
      );
    }

    const content = sections.join("\n");
    const blob = new Blob([content], { type: "text/yaml;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    toast.success(t("rulesActive.exportedRulesCount", { count: rules.length }));
  };

  const handleExportAll = () => {
    if (filteredRules.length === 0) return;
    exportRulesAsYaml(filteredRules, "dqx-rules.yml");
  };

  const handleExportTable = (fqn: string, rules: RuleCatalogEntryOut[]) => {
    const safeName = fqn.replace(/[^a-zA-Z0-9._-]/g, "_");
    exportRulesAsYaml(rules, `${safeName}-rules.yml`);
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[]} page={t("rulesActive.breadcrumb")} />
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">{t("rulesActive.title")}</h1>
            <p className="text-muted-foreground">
              {t("rulesActive.subtitle")}
            </p>
          </div>
          <div className="flex items-center gap-2">
            {canExportRules && filteredRules.length > 0 && (
              <Button variant="outline" onClick={handleExportAll} className="gap-2">
                <Download className="h-4 w-4" />
                {t("rulesActive.exportYaml")}
              </Button>
            )}
            {canCreateRules && (
              <Button onClick={() => navigate({ to: "/rules/create" })} className="gap-2">
                <Plus className="h-4 w-4" />
                {t("rulesActive.createRules")}
              </Button>
            )}
          </div>
        </div>
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-col gap-4">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <ShieldCheck className="h-5 w-5" />
                  {t("rulesActive.approvedRuleSets")}
                </CardTitle>
                <CardDescription>
                  {isLoading
                    ? t("common.loading")
                    : t("rulesActive.tablesAndChecks", {
                        tables: t("rulesActive.tablesCount", { count: filteredRules.length }),
                        checks: totalCheckCount,
                      })}
                </CardDescription>
              </div>
            </div>

            <div className="flex items-center gap-2 flex-wrap">
              <Select value={catalogFilter} onValueChange={handleCatalogChange}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder={t("common.selectCatalog")} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t("common.selectCatalog")}</SelectItem>
                  {catalogs.map((cat) => (
                    <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={schemaFilter} onValueChange={setSchemaFilter} disabled={catalogFilter === "all"}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder={t("common.selectSchema")} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t("common.selectSchema")}</SelectItem>
                  {availableSchemas.map((sch) => (
                    <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={sourceFilter} onValueChange={setSourceFilter}>
                <SelectTrigger className="w-[140px]">
                  <SelectValue placeholder={t("rulesActive.allSources")} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t("rulesActive.allSources")}</SelectItem>
                  {sources.map((src) => (
                    <SelectItem key={src} value={src}>
                      {src === "imported" ? t("rulesActive.imported") : src === "ai" ? t("rulesActive.ai") : t("rulesActive.ui")}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <LabelFilter
                available={availableLabels}
                selected={labelFilter}
                onChange={setLabelFilter}
              />

              {(catalogFilter !== "all" || schemaFilter !== "all" || sourceFilter !== "all" || labelFilter.size > 0) && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-9 text-xs"
                  onClick={() => {
                    setCatalogFilter("all");
                    setSchemaFilter("all");
                    setSourceFilter("all");
                    setLabelFilter(new Set());
                  }}
                >
                  {t("common.clearFilters")}
                </Button>
              )}

              <div className="ml-auto flex items-center gap-1 border rounded-md p-0.5">
                {(
                  [
                    { key: "by-table", label: t("rulesActive.byTable") },
                    { key: "by-rule", label: t("rulesActive.byRule") },
                    { key: "sql-checks", label: t("rulesActive.crossTableTab") },
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
            <p className="text-destructive text-sm">{t("rulesActive.failedLoadRules", { error: (error as Error).message })}</p>
          )}

          {!isLoading && !error && filteredRules.length > 0 && (
            <FadeIn duration={0.3}>
              {viewMode === "by-table" && (
                <ByTableView
                  groups={groupedByTable}
                  expandedTables={expandedTables}
                  onToggle={toggleTable}
                  onNavigate={(fqn) =>
                    groupedByTable.find((g) => g.fqn === fqn)?.rules.some(isCrossTableRule)
                      ? navigate({ to: "/rules/create-sql", search: { edit: fqn, from: "active" } })
                      : navigate({ to: "/rules/single-table", search: { table: fqn, from: "active" } })
                  }
                  canDelete={canApproveRules}
                  onDelete={requestDelete}
                  pendingDelete={pendingDelete}
                  canExport={canExportRules}
                  onExport={handleExportTable}
                  canPush={canExportRules}
                  onPush={openPushDialog}
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

      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesActive.deleteTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesActive.deleteConfirm")}
              <span className="font-medium text-foreground">
                {deleteTarget?.display_name || deleteTarget?.table_fqn}
              </span>
              {t("rulesActive.deleteCannotUndo")}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("common.delete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <Dialog open={!!pushTarget} onOpenChange={(open) => !open && setPushTarget(null)}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Upload className="h-4 w-4" />
              {t("rulesActive.pushDialogTitle")}
            </DialogTitle>
            <DialogDescription>
              {t("rulesActive.pushDialogDesc", { table: pushTarget?.fqn ?? "" })}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label htmlFor="push-target-table">{t("rulesActive.pushTargetTableLabel")}</Label>
              <Input
                id="push-target-table"
                placeholder="catalog.schema.checks_table"
                value={pushTableFqn}
                onChange={(e) => setPushTableFqn(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">{t("rulesActive.pushTargetTableHint")}</p>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="push-run-config">{t("rulesActive.pushRunConfigLabel")}</Label>
              <Input
                id="push-run-config"
                placeholder="my_run_config"
                value={pushRunConfig}
                onChange={(e) => setPushRunConfig(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">{t("rulesActive.pushRunConfigHint")}</p>
            </div>
            <div className="space-y-1.5">
              <Label>{t("rulesActive.pushModeLabel")}</Label>
              <div className="flex gap-3">
                {(["append", "overwrite"] as const).map((m) => (
                  <button
                    key={m}
                    type="button"
                    onClick={() => setPushMode(m)}
                    className={`flex-1 rounded-md border px-3 py-2 text-sm transition-colors ${
                      pushMode === m
                        ? "border-primary bg-primary/10 text-primary font-medium"
                        : "border-border text-muted-foreground hover:bg-muted"
                    }`}
                  >
                    {t(`rulesActive.pushMode_${m}`)}
                  </button>
                ))}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPushTarget(null)} disabled={isPushing}>
              {t("common.cancel")}
            </Button>
            <Button
              onClick={handlePushToTable}
              disabled={isPushing || !pushTableFqn.trim()}
              className="gap-2"
            >
              <Upload className="h-4 w-4" />
              {isPushing ? t("rulesActive.pushing") : t("rulesActive.pushConfirm")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
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
  canExport: boolean;
  onExport: (fqn: string, rules: RuleCatalogEntryOut[]) => void;
  canPush: boolean;
  onPush: (fqn: string, rules: RuleCatalogEntryOut[]) => void;
}

function ByTableView({ groups, expandedTables, onToggle, onNavigate, canDelete, onDelete, pendingDelete, canExport, onExport, canPush, onPush }: ByTableViewProps) {
  const { t } = useTranslation();
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
                {t("rulesActive.rulesCount", { count: rules.length })}
              </span>
            </button>
            {isOpen && (
              <div className="border-t">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-muted/30">
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">{t("rulesActive.headerFunction")}</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">{t("rulesActive.headerColumns")}</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">{t("rulesActive.headerCriticality")}</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">{t("rulesActive.headerLabels")}</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">{t("rulesActive.headerSource")}</th>
                      <th className="text-left p-2 px-4 font-medium text-xs text-muted-foreground">{t("rulesActive.headerCreatedBy")}</th>
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
                      const labels = getUserMetadata(check);
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
                            {Object.keys(labels).length === 0 ? (
                              <span className="text-[10px] italic text-muted-foreground/60">—</span>
                            ) : (
                              <LabelsBadges labels={labels} max={3} size="sm" />
                            )}
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
                              {rule.source === "imported" ? t("rulesActive.imported") : rule.source === "ai" ? t("rulesActive.ai") : t("rulesActive.ui")}
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
                  {canExport && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs gap-1"
                      onClick={() => onExport(fqn, rules)}
                    >
                      <Download className="h-3 w-3" />
                      {t("rulesActive.exportYaml")}
                    </Button>
                  )}
                  {canPush && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs gap-1"
                      onClick={() => onPush(fqn, rules)}
                    >
                      <Upload className="h-3 w-3" />
                      {t("rulesActive.pushToTable")}
                    </Button>
                  )}
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={() => onNavigate(fqn)}
                  >
                    {t("rulesActive.viewEditRules")}
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

// ── By rule view: checks grouped by function name ───────────────────────────

interface CheckWithMeta {
  _tableFqn: string;
  _displayName: string;
  _version: number;
  _ruleId?: string | null;
  [key: string]: unknown;
}

interface ParsedCheck {
  fn: string;
  col: string;
  criticality: string;
  name: string | null;
  tableFqn: string;
  displayName: string;
  labels: Record<string, string>;
}

function ByRuleView({ checks }: { checks: CheckWithMeta[] }) {
  const { t } = useTranslation();
  const [expandedFns, setExpandedFns] = useState<Set<string>>(new Set());

  const grouped = useMemo(() => {
    const parsed: ParsedCheck[] = checks
      .map((check) => {
        const checkObj = (check.check as Record<string, unknown>) ?? {};
        const fn = String(checkObj.function ?? "");
        if (fn === "sql_query" || !fn) return null;
        const args = (checkObj.arguments as Record<string, unknown>) ?? {};
        const col = String(args.column ?? checkObj.for_each_column ?? "—");
        return {
          fn,
          col,
          criticality: String(check.criticality ?? "warn"),
          name: check.name ? String(check.name) : null,
          tableFqn: check._tableFqn,
          displayName: check._displayName,
          labels: getUserMetadata(check as Record<string, unknown>),
        };
      })
      .filter((c): c is ParsedCheck => c !== null);

    const groups = new Map<string, ParsedCheck[]>();
    for (const c of parsed) {
      const list = groups.get(c.fn) ?? [];
      list.push(c);
      groups.set(c.fn, list);
    }
    return [...groups.entries()].sort((a, b) => b[1].length - a[1].length);
  }, [checks]);

  const toggleFn = (fn: string) =>
    setExpandedFns((prev) => {
      const next = new Set(prev);
      if (next.has(fn)) next.delete(fn); else next.add(fn);
      return next;
    });

  if (grouped.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground text-sm">
        {t("rulesActive.noColumnRules")}
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {grouped.map(([fn, items]) => {
        const isOpen = expandedFns.has(fn);
        const tables = new Set(items.map((i) => i.displayName));
        const errorCount = items.filter((i) => i.criticality === "error").length;
        const warnCount = items.filter((i) => i.criticality === "warn").length;
        return (
          <div key={fn} className="border rounded-lg overflow-hidden">
            <button
              type="button"
              className="w-full flex items-center gap-3 p-3 text-left hover:bg-muted/30 transition-colors"
              onClick={() => toggleFn(fn)}
            >
              <ChevronRight className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${isOpen ? "rotate-90" : ""}`} />
              <span className="font-mono text-sm font-medium">{fn}</span>
              <span className="text-xs text-muted-foreground ml-auto flex items-center gap-3">
                {errorCount > 0 && (
                  <Badge variant="outline" className="text-[10px] border-red-500 text-red-600">
                    {t("rulesActive.errorsCount", { count: errorCount })}
                  </Badge>
                )}
                {warnCount > 0 && (
                  <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
                    {t("rulesActive.warnsCount", { count: warnCount })}
                  </Badge>
                )}
                <span>{t("rulesActive.checksAcrossTables", {
                  checks: t("rulesActive.checksCount", { count: items.length }),
                  tables: t("rulesActive.tablesCount", { count: tables.size }),
                })}</span>
              </span>
            </button>
            {isOpen && (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-t bg-muted/50">
                    <th className="text-left p-3 font-medium text-xs">{t("rulesActive.headerTable")}</th>
                    <th className="text-left p-3 font-medium text-xs">{t("rulesActive.headerColumns")}</th>
                    <th className="text-left p-3 font-medium text-xs">{t("rulesActive.headerCriticality")}</th>
                    <th className="text-left p-3 font-medium text-xs">{t("rulesActive.headerName")}</th>
                    <th className="text-left p-3 font-medium text-xs">{t("rulesActive.headerLabels")}</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((item, idx) => (
                    <tr key={idx} className="border-t last:border-b-0 hover:bg-muted/20">
                      <td className="p-3 font-mono text-xs">{item.displayName}</td>
                      <td className="p-3 text-xs text-muted-foreground">{item.col}</td>
                      <td className="p-3">
                        <Badge
                          variant="outline"
                          className={`text-[10px] ${item.criticality === "error" ? "border-red-500 text-red-600" : "border-amber-500 text-amber-600"}`}
                        >
                          {item.criticality}
                        </Badge>
                      </td>
                      <td className="p-3 text-xs text-muted-foreground">
                        {item.name ?? <span className="italic text-muted-foreground/60">—</span>}
                      </td>
                      <td className="p-3">
                        {Object.keys(item.labels).length === 0 ? (
                          <span className="text-[10px] italic text-muted-foreground/60">—</span>
                        ) : (
                          <LabelsBadges labels={item.labels} max={3} size="sm" />
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── SQL checks view ─────────────────────────────────────────────────────────

function SqlChecksView({ checks }: { checks: CheckWithMeta[] }) {
  const { t } = useTranslation();
  const sqlChecks = checks.filter((c) => {
    const checkObj = (c.check as Record<string, unknown>) ?? {};
    return String(checkObj.function ?? "") === "sql_query";
  });

  if (sqlChecks.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        <Database className="h-10 w-10 mx-auto mb-3 opacity-40" />
        <p className="text-sm font-medium">{t("rulesActive.noCrossTable")}</p>
        <p className="text-xs mt-1">{t("rulesActive.noCrossTableHint")}</p>
      </div>
    );
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b bg-muted/50">
            <th className="text-left p-3 font-medium">{t("rulesActive.headerName")}</th>
            <th className="text-left p-3 font-medium">{t("rulesActive.headerMode")}</th>
            <th className="text-left p-3 font-medium">{t("rulesActive.headerTable")}</th>
            <th className="text-left p-3 font-medium">{t("rulesActive.headerCriticality")}</th>
            <th className="text-left p-3 font-medium">{t("rulesActive.headerLabels")}</th>
          </tr>
        </thead>
        <tbody>
          {sqlChecks.map((check, idx) => {
            const checkObj = (check.check as Record<string, unknown>) ?? {};
            const args = (checkObj.arguments as Record<string, unknown>) ?? {};
            const name = String(check.name ?? args.name ?? "sql_query");
            const mergeColumns = args.merge_columns as string[] | undefined;
            const mode = mergeColumns && mergeColumns.length > 0 ? t("rulesActive.rowLevel") : t("rulesActive.datasetLevel");
            const criticality = String(check.criticality ?? "warn");
            const labels = getUserMetadata(check as Record<string, unknown>);
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
                <td className="p-3">
                  {Object.keys(labels).length === 0 ? (
                    <span className="text-[10px] italic text-muted-foreground/60">—</span>
                  ) : (
                    <LabelsBadges labels={labels} max={3} size="sm" />
                  )}
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
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
        <ShieldCheck className="h-8 w-8 text-muted-foreground" />
      </div>
      <h3 className="text-lg font-medium text-muted-foreground">{t("rulesActive.noActiveRules")}</h3>
      <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
        {canCreate
          ? t("rulesActive.noActiveRulesAuthor")
          : t("rulesActive.noActiveRulesViewer")}
      </p>
      {canCreate && (
        <Button onClick={onNavigate} className="mt-4 gap-2">
          <Plus className="h-4 w-4" />
          {t("rulesActive.createRules")}
        </Button>
      )}
    </div>
  );
}
