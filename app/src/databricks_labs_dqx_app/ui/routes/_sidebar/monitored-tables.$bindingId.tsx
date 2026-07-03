import { createFileRoute, Link, useParams } from "@tanstack/react-router";
import { Suspense, useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Input } from "@/components/ui/input";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  AlertCircle,
  ArrowLeft,
  BarChart3,
  ClipboardList,
  Columns3,
  Info,
  Loader2,
  Plus,
  RotateCcw,
  ShieldCheck,
  Sparkles,
  UploadCloud,
  X,
} from "lucide-react";
import {
  useGetMonitoredTableSuspense,
  getGetMonitoredTableQueryKey,
  useGetMonitoredTableProfile,
  usePublishMonitoredTable,
  useRemoveAppliedRule,
  useSetAppliedRulePin,
  useSetAppliedRuleSeverityOverride,
  useListRegistryRules,
  useGetTableColumns,
  type AppliedRuleOut,
  type ColumnOut,
  type MonitoredTableOut,
  type RegistryRuleOut,
} from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { formatDateShort } from "@/lib/format-utils";
import { ApprovalStepsBanner } from "@/components/ApprovalStepsBanner";
import { useAiAvailability } from "@/hooks/use-ai-availability";
import { AI_BUTTON_BG } from "@/lib/ai-style";
import { AddRulesDialog } from "@/components/apply-rules/AddRulesDialog";
import { AiSuggestionDialog } from "@/components/apply-rules/AiSuggestionDialog";
import { RuleConfigCard, computeStatus } from "@/components/apply-rules/RuleConfigCard";
import { RulesByColumn, type ColumnRef } from "@/components/apply-rules/RulesByColumn";
import { RESERVED_SEVERITY_KEY, extractApiError } from "@/components/apply-rules/shared";
import { orderSeverityValuesForDisplay } from "@/components/RegistryRuleBadges";

export const Route = createFileRoute("/_sidebar/monitored-tables/$bindingId")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} fallbackRender={DetailError}>
          <Suspense fallback={<DetailSkeleton />}>
            <MonitoredTableDetailPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function DetailError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("common.loadFailed")}</p>
      <p className="text-muted-foreground/70 text-xs mb-3">{t("common.retryHint")}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

function DetailSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-64" />
      </div>
      <Skeleton className="h-96 w-full" />
    </div>
  );
}

function MonitoredTableDetailPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const queryClient = useQueryClient();
  const { bindingId } = useParams({ from: "/_sidebar/monitored-tables/$bindingId" });

  const { data } = useGetMonitoredTableSuspense(bindingId);
  const detail = data.data;
  const table = detail.table;
  const appliedRules = useMemo(() => detail.applied_rules ?? [], [detail.applied_rules]);

  const [dirty, setDirty] = useState(false);

  const invalidateDetail = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getGetMonitoredTableQueryKey(bindingId) }),
    [queryClient, bindingId],
  );

  const onMutated = useCallback(() => {
    invalidateDetail();
    if (table.status === "published") setDirty(true);
  }, [invalidateDetail, table.status]);

  const publishMutation = usePublishMonitoredTable();
  const handlePublish = () => {
    publishMutation.mutate(
      { bindingId },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastPublished"));
          setDirty(false);
          invalidateDetail();
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.toastPublishFailed")), { duration: 6000 });
        },
      },
    );
  };

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb
          items={[{ label: t("monitoredTables.title"), to: "/monitored-tables" }]}
          page={table.table_fqn}
        />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <h1 className="text-2xl font-semibold tracking-tight font-mono">{table.table_fqn}</h1>
              {table.status === "published" ? (
                <Badge variant="outline" className="gap-1 border-emerald-500 text-emerald-600">
                  <ShieldCheck className="h-3 w-3" />
                  {t("monitoredTables.statusBadgePublished")}
                </Badge>
              ) : (
                <Badge variant="secondary">{t("monitoredTables.statusBadgeDraft")}</Badge>
              )}
            </div>
            {table.steward && (
              <p className="text-sm text-muted-foreground mt-1">{t("monitoredTables.colSteward")}: {table.steward}</p>
            )}
          </div>
          {perms.canCreateRules && (
            <Button onClick={handlePublish} disabled={publishMutation.isPending} className="gap-2">
              {publishMutation.isPending ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <UploadCloud className="h-4 w-4" />
              )}
              {publishMutation.isPending ? t("monitoredTables.publishing") : t("monitoredTables.publishButton")}
            </Button>
          )}
        </div>

        {table.status !== "published" && (
          <Card className="border-amber-500/50 bg-amber-500/5">
            <CardContent className="py-3 text-sm text-amber-700 dark:text-amber-400">
              {t("monitoredTables.neverPublishedDescription")}
            </CardContent>
          </Card>
        )}

        {table.status === "published" && dirty && (
          <Card className="border-amber-500/50 bg-amber-500/5">
            <CardHeader className="py-3 pb-0">
              <CardTitle className="text-sm text-amber-700 dark:text-amber-400">
                {t("monitoredTables.unpublishedChangesTitle")}
              </CardTitle>
            </CardHeader>
            <CardContent className="py-3 text-sm text-amber-700 dark:text-amber-400">
              {t("monitoredTables.unpublishedChangesDescription")}
            </CardContent>
          </Card>
        )}

        <Tabs defaultValue="apply-rules">
          <TabsList>
            <TabsTrigger value="about" className="gap-1.5">
              <Info className="h-3.5 w-3.5" />
              {t("monitoredTables.tabAbout")}
            </TabsTrigger>
            <TabsTrigger value="profile" className="gap-1.5">
              <BarChart3 className="h-3.5 w-3.5" />
              {t("monitoredTables.tabProfile")}
            </TabsTrigger>
            <TabsTrigger value="apply-rules" className="gap-1.5">
              <Columns3 className="h-3.5 w-3.5" />
              {t("monitoredTables.tabApplyRules")}
            </TabsTrigger>
            <TabsTrigger value="results" className="gap-1.5">
              <ClipboardList className="h-3.5 w-3.5" />
              {t("monitoredTables.tabResults")}
            </TabsTrigger>
          </TabsList>

          <TabsContent value="about">
            <AboutTab table={table} />
          </TabsContent>

          <TabsContent value="profile">
            <ProfileTab bindingId={bindingId} />
          </TabsContent>

          <TabsContent value="apply-rules">
            <ApplyRulesTab
              bindingId={bindingId}
              tableFqn={table.table_fqn}
              appliedRules={appliedRules}
              canEdit={perms.canCreateRules}
              onMutated={onMutated}
            />
          </TabsContent>

          <TabsContent value="results">
            <ResultsTab tableFqn={table.table_fqn} published={table.status === "published"} />
          </TabsContent>
        </Tabs>
      </div>
    </FadeIn>
  );
}

// ---------------------------------------------------------------------------
// About tab
// ---------------------------------------------------------------------------

function AboutTab({ table }: { table: MonitoredTableOut }) {
  const { t } = useTranslation();
  const [filter, setFilter] = useState("");
  const parts = table.table_fqn.split(".");
  const [catalog, schema, tableName] = parts;

  const columnsQuery = useGetTableColumns(catalog ?? "", schema ?? "", tableName ?? "", {
    query: { enabled: parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  const filteredColumns = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return columns;
    return columns.filter(
      (c) => c.name.toLowerCase().includes(q) || (c.comment ?? "").toLowerCase().includes(q),
    );
  }, [columns, filter]);

  return (
    <div className="space-y-8 pt-4">
      <section className="space-y-3">
        <h2 className="text-sm font-semibold">{t("monitoredTables.aboutSectionTitle")}</h2>
        <dl className="grid grid-cols-[160px_1fr] gap-x-4 gap-y-2 text-xs">
          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutCatalog")}</dt>
          <dd className="font-mono">{catalog ?? t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutSchema")}</dt>
          <dd className="font-mono">{schema ?? t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutTableName")}</dt>
          <dd className="font-mono">{tableName ?? t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutStatus")}</dt>
          <dd>
            {table.status === "published" ? (
              <Badge variant="outline" className="gap-1 border-emerald-500 text-emerald-600">
                <ShieldCheck className="h-2.5 w-2.5" />
                {t("monitoredTables.statusPublished")}
              </Badge>
            ) : (
              <Badge variant="secondary">{t("monitoredTables.statusDraft")}</Badge>
            )}
          </dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutSteward")}</dt>
          <dd>{table.steward || <span className="text-muted-foreground italic">{t("monitoredTables.aboutStewardNone")}</span>}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutLastProfiled")}</dt>
          <dd>{table.last_profiled_at ? formatDateShort(table.last_profiled_at) : t("monitoredTables.neverProfiled")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutCreatedBy")}</dt>
          <dd>{table.created_by || t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutCreatedAt")}</dt>
          <dd>{table.created_at ? formatDateShort(table.created_at) : t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutUpdatedBy")}</dt>
          <dd>{table.updated_by || t("monitoredTables.aboutUnknown")}</dd>

          <dt className="text-muted-foreground uppercase tracking-wide">{t("monitoredTables.aboutUpdatedAt")}</dt>
          <dd>{table.updated_at ? formatDateShort(table.updated_at) : t("monitoredTables.aboutUnknown")}</dd>
        </dl>
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold">
          {t("monitoredTables.aboutSchemaSectionTitle", { count: columns.length })}
        </h2>
        {columnsQuery.isError ? (
          <div className="flex flex-col items-center justify-center py-10 text-center border border-dashed rounded-lg">
            <AlertCircle className="h-8 w-8 text-destructive/30 mb-2" />
            <p className="text-sm font-medium text-muted-foreground">{t("monitoredTables.aboutLoadFailedTitle")}</p>
            <p className="text-xs text-muted-foreground/70 mt-1 max-w-sm">{t("monitoredTables.aboutLoadFailedHint")}</p>
          </div>
        ) : (
          <div className="rounded-md border overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/30">
                <tr>
                  <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2">
                    {t("monitoredTables.aboutColColumn")}
                  </th>
                  <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-24">
                    {t("monitoredTables.aboutColType")}
                  </th>
                  <th className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide px-3 py-2 w-24">
                    {t("monitoredTables.aboutColNullable")}
                  </th>
                  <th className="px-3 py-1.5">
                    <div className="flex items-center justify-between gap-3">
                      <span className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide">
                        {t("monitoredTables.aboutColDescription")}
                      </span>
                      <Input
                        placeholder={t("monitoredTables.aboutFilterColumnsPlaceholder")}
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                        className="max-w-xs h-7 text-xs font-normal normal-case tracking-normal"
                      />
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredColumns.map((c) => (
                  <tr key={c.name} className="border-t">
                    <td className="px-3 py-2 align-top">
                      <span className="font-mono text-xs">{c.name}</span>
                    </td>
                    <td className="px-3 py-2 align-top">
                      <Badge variant="outline" className="text-[10px] font-mono">
                        {c.type_name}
                      </Badge>
                    </td>
                    <td className="px-3 py-2 align-top text-xs text-muted-foreground">
                      {c.nullable === false ? "—" : "✓"}
                    </td>
                    <td className="px-3 py-2 align-top">
                      {c.comment ? (
                        <span className="text-xs">{c.comment}</span>
                      ) : (
                        <span className="text-muted-foreground italic text-xs">{t("monitoredTables.aboutNoDescription")}</span>
                      )}
                    </td>
                  </tr>
                ))}
                {filteredColumns.length === 0 && (
                  <tr>
                    <td colSpan={4} className="text-center text-muted-foreground py-6 text-sm">
                      {t("monitoredTables.aboutNoMatchingColumns")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Profile tab
// ---------------------------------------------------------------------------

function ProfileTab({ bindingId }: { bindingId: string }) {
  const { t } = useTranslation();
  const { data, isLoading, error } = useGetMonitoredTableProfile(bindingId);
  const profile = data?.data;

  if (isLoading) {
    return (
      <div className="space-y-2 pt-4">
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <BarChart3 className="h-10 w-10 text-muted-foreground/30 mb-3" />
        <p className="text-sm font-medium text-muted-foreground">{t("monitoredTables.noProfileTitle")}</p>
        <p className="text-muted-foreground/70 text-xs mt-1 max-w-md">{t("monitoredTables.noProfileHint")}</p>
      </div>
    );
  }

  if (!profile) return null;

  const summary = (profile.summary ?? {}) as Record<string, unknown>;
  const rows = Object.entries(summary).filter(
    (e): e is [string, Record<string, unknown>] =>
      typeof e[1] === "object" && e[1] !== null && !Array.isArray(e[1]),
  );

  return (
    <div className="space-y-4 pt-4">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Card>
          <CardContent className="py-3">
            <p className="text-xs text-muted-foreground">{t("monitoredTables.rowsProfiled")}</p>
            <p className="text-lg font-semibold tabular-nums">{profile.rows_profiled ?? "—"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="py-3">
            <p className="text-xs text-muted-foreground">{t("monitoredTables.columnsProfiled")}</p>
            <p className="text-lg font-semibold tabular-nums">{profile.columns_profiled ?? "—"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="py-3">
            <p className="text-xs text-muted-foreground">{t("monitoredTables.durationSeconds")}</p>
            <p className="text-lg font-semibold tabular-nums">
              {profile.duration_seconds != null
                ? t("monitoredTables.durationSecondsValue", { seconds: profile.duration_seconds })
                : "—"}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="py-3">
            <p className="text-xs text-muted-foreground">{t("monitoredTables.profiledAt", { date: "" }).replace(/\s*$/, "")}</p>
            <p className="text-sm font-medium">
              {profile.profiled_at ? formatDateShort(profile.profiled_at) : "—"}
            </p>
          </CardContent>
        </Card>
      </div>

      {rows.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">{t("monitoredTables.columnStatsTitle", { count: rows.length })}</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/30">
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colColumn")}</th>
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colNullPct")}</th>
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colCount")}</th>
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colMin")}</th>
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colMax")}</th>
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colMean")}</th>
                    <th className="text-left p-2 font-medium">{t("monitoredTables.colStddev")}</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(([column, stats]) => {
                    const count = Number(stats.count ?? 0);
                    const countNull = Number(stats.count_null ?? 0);
                    const nullPct = count > 0 ? ((countNull / count) * 100).toFixed(1) : "—";
                    return (
                      <tr key={column} className="border-b last:border-b-0">
                        <td className="p-2 font-mono font-medium">{column}</td>
                        <td className="p-2 tabular-nums">{nullPct === "—" ? nullPct : `${nullPct}%`}</td>
                        <td className="p-2 tabular-nums">{String(stats.count ?? "—")}</td>
                        <td className="p-2">{String(stats.min ?? "—")}</td>
                        <td className="p-2">{String(stats.max ?? "—")}</td>
                        <td className="p-2 tabular-nums">{String(stats.mean ?? "—")}</td>
                        <td className="p-2 tabular-nums">{String(stats.stddev ?? "—")}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Apply Rules tab
// ---------------------------------------------------------------------------

function ApplyRulesTab({
  bindingId,
  tableFqn,
  appliedRules,
  canEdit,
  onMutated,
}: {
  bindingId: string;
  tableFqn: string;
  appliedRules: AppliedRuleOut[];
  canEdit: boolean;
  onMutated: () => void;
}) {
  const { t } = useTranslation();
  const [lens, setLens] = useState<"by-rule" | "by-column">("by-rule");
  const [addOpen, setAddOpen] = useState(false);
  const [addColumnContext, setAddColumnContext] = useState<ColumnRef | null>(null);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [removeTarget, setRemoveTarget] = useState<AppliedRuleOut | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<"all" | "needs-attention">("all");
  const aiAvailability = useAiAvailability();

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);
  const severityValues = useMemo(
    () => orderSeverityValuesForDisplay(labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? []),
    [labelDefinitions],
  );

  const { data: registryData } = useListRegistryRules({ status: "approved" });
  const publishedRules = useMemo(() => registryData?.data ?? [], [registryData]);
  const ruleById = useMemo(() => {
    const m = new Map<string, RegistryRuleOut>();
    for (const r of publishedRules) m.set(r.rule_id, r);
    return m;
  }, [publishedRules]);

  // Completeness status per applied rule — drives the "needs attention"
  // filter and the by-rule/by-column incomplete-mapping indicators.
  const statuses = useMemo(
    () => appliedRules.map((rule) => computeStatus(rule, ruleById.get(rule.rule_id)?.definition.slots ?? [])),
    [appliedRules, ruleById],
  );
  const incompleteCount = statuses.filter((s) => s.kind === "incomplete").length;

  // Total checks: aggregate rules (0 slots) count as 1 check each;
  // column-mapped rules count as the number of non-empty mapping groups.
  const totalChecks = useMemo(() => {
    return appliedRules.reduce((sum, rule) => {
      const slots = ruleById.get(rule.rule_id)?.definition.slots ?? [];
      if (slots.length === 0) return sum + 1;
      const groups = rule.column_mapping ?? [];
      return sum + groups.filter((entry) => slots.some((s) => Boolean(entry[s.name]))).length;
    }, 0);
  }, [appliedRules, ruleById]);

  const visibleAppliedRules = useMemo(() => {
    let filtered = appliedRules;
    if (filter === "needs-attention") {
      filtered = appliedRules.filter((_, i) => statuses[i].kind === "incomplete");
    }
    const q = search.trim().toLowerCase();
    if (q) {
      filtered = filtered.filter((rule) => {
        if ((rule.rule_name ?? "").toLowerCase().includes(q)) return true;
        if (rule.rule_id.toLowerCase().includes(q)) return true;
        for (const group of rule.column_mapping ?? []) {
          for (const col of Object.values(group)) {
            if (col && col.toLowerCase().includes(q)) return true;
          }
        }
        return false;
      });
    }
    return filtered;
  }, [appliedRules, statuses, filter, search]);

  const removeMutation = useRemoveAppliedRule();
  const pinMutation = useSetAppliedRulePin();
  const overrideMutation = useSetAppliedRuleSeverityOverride();

  const openAddDialog = (column?: ColumnRef) => {
    setAddColumnContext(column ?? null);
    setAddOpen(true);
  };

  const confirmRemove = () => {
    if (!removeTarget?.id) return;
    const appliedRuleId = removeTarget.id;
    setRemoveTarget(null);
    setPendingId(appliedRuleId);
    removeMutation.mutate(
      { bindingId, appliedRuleId },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastRemoved"));
          onMutated();
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.toastRemoveFailed")), { duration: 6000 }),
        onSettled: () => setPendingId(null),
      },
    );
  };

  const handlePinChange = (rule: AppliedRuleOut, value: string) => {
    if (!rule.id) return;
    const appliedRuleId = rule.id;
    const registryRule = ruleById.get(rule.rule_id);
    const pinned_version = value === "latest" ? null : registryRule?.version ?? rule.pinned_version ?? null;
    setPendingId(appliedRuleId);
    pinMutation.mutate(
      { bindingId, appliedRuleId, data: { pinned_version } },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastPinSet"));
          onMutated();
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.toastPinFailed")), { duration: 6000 }),
        onSettled: () => setPendingId(null),
      },
    );
  };

  const handleSeverityChange = (rule: AppliedRuleOut, value: string) => {
    if (!rule.id) return;
    const appliedRuleId = rule.id;
    const severity = value === "none" ? null : value;
    setPendingId(appliedRuleId);
    overrideMutation.mutate(
      { bindingId, appliedRuleId, data: { severity } },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastOverrideSet"));
          onMutated();
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.toastOverrideFailed")), { duration: 6000 }),
        onSettled: () => setPendingId(null),
      },
    );
  };

  return (
    <div className="space-y-4 pt-4">
      <ApprovalStepsBanner />

      <div className="flex items-center gap-3 flex-wrap">
        {appliedRules.length > 0 && (
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-sm font-semibold cursor-help">
                  {t("monitoredTables.checksCount", { count: totalChecks })}{" "}
                  {t("monitoredTables.viaRulesCount", { count: appliedRules.length })}
                </span>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="max-w-xs text-center">
                {t("monitoredTables.checksViaRulesTooltip")}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )}

        {incompleteCount > 0 && (
          <div className="flex gap-1">
            <button
              type="button"
              onClick={() => setFilter("all")}
              className={`h-8 px-3 rounded text-xs border transition-colors ${
                filter === "all"
                  ? "bg-primary text-primary-foreground border-primary"
                  : "border-input bg-background text-foreground hover:bg-accent"
              }`}
            >
              {t("monitoredTables.filterAll")}
            </button>
            <button
              type="button"
              onClick={() => setFilter("needs-attention")}
              className={`h-8 px-3 rounded text-xs border transition-colors ${
                filter === "needs-attention"
                  ? "bg-yellow-500/20 text-yellow-700 dark:text-yellow-300 border-yellow-500/50"
                  : "border-yellow-500/30 bg-background text-yellow-600 dark:text-yellow-400 hover:bg-yellow-500/10"
              }`}
            >
              {t("monitoredTables.filterNeedsAttention")}
            </button>
          </div>
        )}

        {appliedRules.length > 0 && (
          <div className="relative w-60">
            <Input
              placeholder={t("monitoredTables.searchRulesAndColumnsPlaceholder")}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-8 text-xs pr-7"
            />
            {search && (
              <button
                type="button"
                aria-label={t("monitoredTables.clearSearchLabel")}
                onClick={() => setSearch("")}
                className="absolute right-1 top-1/2 -translate-y-1/2 inline-flex items-center justify-center h-5 w-5 text-muted-foreground hover:text-foreground rounded"
              >
                <X className="h-3 w-3" />
              </button>
            )}
          </div>
        )}

        <div className="ml-auto flex items-center gap-2">
          <div className="flex items-center gap-1 border rounded-md p-0.5">
            {(
              [
                { key: "by-rule", label: t("monitoredTables.lensByRule") },
                { key: "by-column", label: t("monitoredTables.lensByColumn") },
              ] as const
            ).map((mode) => (
              <button
                key={mode.key}
                type="button"
                onClick={() => setLens(mode.key)}
                className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                  lens === mode.key ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted"
                }`}
              >
                {mode.label}
              </button>
            ))}
          </div>
          {canEdit && (
            <>
              {aiAvailability.available && (
                <Button size="sm" className={`gap-2 ${AI_BUTTON_BG}`} onClick={() => setSuggestOpen(true)}>
                  <Sparkles className="h-3.5 w-3.5" />
                  {t("monitoredTables.suggestRulesButton")}
                </Button>
              )}
              <Button size="sm" className="gap-2" onClick={() => openAddDialog()}>
                <Plus className="h-3.5 w-3.5" />
                {t("monitoredTables.addRulesButton")}
              </Button>
            </>
          )}
        </div>
      </div>

      {appliedRules.length === 0 && lens === "by-rule" ? (
        <div className="flex flex-col items-center justify-center py-16 text-center border border-dashed rounded-lg">
          <Columns3 className="h-10 w-10 text-muted-foreground/30 mb-3" />
          <p className="text-sm text-muted-foreground mb-3 max-w-sm">
            {canEdit ? t("monitoredTables.emptyAppliedRulesCta") : t("monitoredTables.emptyAppliedRules")}
          </p>
          {canEdit && (
            <Button size="sm" className="gap-2" onClick={() => openAddDialog()}>
              <Plus className="h-3.5 w-3.5" />
              {t("monitoredTables.addRulesButton")}
            </Button>
          )}
        </div>
      ) : lens === "by-rule" ? (
        <div className="space-y-3">
          {visibleAppliedRules.length === 0 ? (
            <div className="rounded-lg border border-dashed p-10 text-center text-sm text-muted-foreground">
              {search.trim()
                ? t("monitoredTables.noRulesMatchFilter")
                : filter === "needs-attention"
                  ? t("monitoredTables.noRulesNeedAttention")
                  : t("monitoredTables.emptyAppliedRules")}
            </div>
          ) : (
            visibleAppliedRules.map((rule) => (
              <RuleConfigCard
                key={rule.id ?? rule.rule_id}
                rule={rule}
                registryRule={ruleById.get(rule.rule_id)}
                labelDefinitions={labelDefinitions}
                severityValues={severityValues}
                canEdit={canEdit}
                busy={pendingId === rule.id}
                onPinChange={(v) => handlePinChange(rule, v)}
                onSeverityChange={(v) => handleSeverityChange(rule, v)}
                onRemove={() => setRemoveTarget(rule)}
                onJumpToColumn={(colName) => {
                  setFilter("all");
                  setSearch("");
                  setLens("by-column");
                  setTimeout(() => {
                    document.getElementById(`column-card-${colName}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
                  }, 50);
                }}
              />
            ))
          )}
        </div>
      ) : (
        <RulesByColumn
          appliedRules={appliedRules}
          tableFqn={tableFqn}
          canEdit={canEdit}
          search={search}
          onAddRule={(column) => openAddDialog(column)}
          onJumpToRule={(ruleId) => {
            setFilter("all");
            setSearch("");
            setLens("by-rule");
            setTimeout(() => {
              document.getElementById(`rule-card-${ruleId}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
            }, 50);
          }}
        />
      )}

      <AddRulesDialog
        open={addOpen}
        onOpenChange={(next) => {
          setAddOpen(next);
          if (!next) setAddColumnContext(null);
        }}
        bindingId={bindingId}
        tableFqn={tableFqn}
        publishedRules={publishedRules}
        labelDefinitions={labelDefinitions}
        onApplied={onMutated}
        initialColumn={addColumnContext}
      />

      <AiSuggestionDialog
        open={suggestOpen}
        onOpenChange={setSuggestOpen}
        bindingId={bindingId}
        labelDefinitions={labelDefinitions}
        onApplied={onMutated}
        onAiUnavailable={aiAvailability.reportUnavailable}
      />

      <AlertDialog open={removeTarget !== null} onOpenChange={(open) => !open && setRemoveTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("monitoredTables.removeConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("monitoredTables.removeConfirmDescription", {
                name: removeTarget?.rule_name || removeTarget?.rule_id || "",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className="bg-destructive text-white hover:bg-destructive/90" onClick={confirmRemove}>
              {t("monitoredTables.removeRuleButton")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Results tab
// ---------------------------------------------------------------------------

function ResultsTab({ tableFqn, published }: { tableFqn: string; published: boolean }) {
  const { t } = useTranslation();
  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle className="text-sm flex items-center gap-2">
          <ClipboardList className="h-4 w-4" />
          {t("monitoredTables.resultsTitle")}
        </CardTitle>
        <CardDescription>{t("monitoredTables.resultsDescription")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {!published && (
          <p className="text-sm text-muted-foreground">{t("monitoredTables.notYetPublishedResultsHint")}</p>
        )}
        <p className="text-sm text-muted-foreground">
          {t("monitoredTables.resultsTableFqnHint", { table: tableFqn })}
        </p>
        <Button asChild variant="outline" size="sm" className="gap-2">
          <Link to="/runs-history">
            <ArrowLeft className="h-3.5 w-3.5 rotate-180" />
            {t("monitoredTables.viewRunsHistory")}
          </Link>
        </Button>
      </CardContent>
    </Card>
  );
}
