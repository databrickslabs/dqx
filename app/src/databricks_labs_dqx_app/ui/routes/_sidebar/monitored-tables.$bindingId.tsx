import { createFileRoute, Link, useParams } from "@tanstack/react-router";
import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Checkbox } from "@/components/ui/checkbox";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  AlertCircle,
  ArrowLeft,
  BarChart3,
  ClipboardList,
  Columns3,
  Loader2,
  Plus,
  RotateCcw,
  Search,
  ShieldCheck,
  Sparkles,
  Trash2,
  UploadCloud,
} from "lucide-react";
import {
  useGetMonitoredTableSuspense,
  getGetMonitoredTableQueryKey,
  useGetMonitoredTableProfile,
  usePublishMonitoredTable,
  useApplyRuleToTable,
  useRemoveAppliedRule,
  useSetAppliedRulePin,
  useSetAppliedRuleSeverityOverride,
  useListRegistryRules,
  getListRegistryRulesQueryKey,
  useGetTableColumns,
  useSuggestRulesForTable,
  type AppliedRuleOut,
  type RegistryRuleOut,
  type RuleSlot,
  type ColumnOut,
  type SuggestedRuleMappingOut,
} from "@/lib/api";
import { useLabelDefinitions, type LabelDefinition } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { formatDateShort } from "@/lib/format-utils";
import { RegistryRuleFormDialog } from "@/components/RegistryRuleFormDialog";
import { ApprovalStepsBanner } from "@/components/ApprovalStepsBanner";
import { HelpTooltip } from "@/components/HelpTooltip";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";

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

const RESERVED_NAME_KEY = "name";
const RESERVED_DIMENSION_KEY = "dimension";
const RESERVED_SEVERITY_KEY = "severity";

function getTag(rule: RegistryRuleOut, key: string): string {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const v = md[key];
  return typeof v === "string" ? v : "";
}

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

function colorFor(defs: LabelDefinition[], key: string, value: string): string | undefined {
  const def = defs.find((d) => d.key === key);
  return def?.value_colors?.[value] ?? undefined;
}

function TagBadge({ label, color }: { label: string; color?: string }) {
  if (!label) return null;
  return (
    <Badge variant="outline" className="gap-1 text-[10px] font-normal">
      {color && (
        <span className="inline-block h-1.5 w-1.5 rounded-full" style={{ backgroundColor: color }} aria-hidden />
      )}
      {label}
    </Badge>
  );
}

function familyForType(typeName: string): "numeric" | "text" | "temporal" | "boolean" | "any" {
  const t = typeName.toUpperCase();
  if (/^(INT|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|LONG|SHORT|BYTE)/.test(t)) return "numeric";
  if (/^(STRING|VARCHAR|CHAR)/.test(t)) return "text";
  if (/^(DATE|TIMESTAMP)/.test(t)) return "temporal";
  if (t === "BOOLEAN") return "boolean";
  return "any";
}

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
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [removeTarget, setRemoveTarget] = useState<AppliedRuleOut | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const aiAvailability = useAiAvailability();

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);
  const severityValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? [],
    [labelDefinitions],
  );

  const { data: registryData } = useListRegistryRules({ status: "approved" });
  const publishedRules = useMemo(() => registryData?.data ?? [], [registryData]);
  const ruleById = useMemo(() => {
    const m = new Map<string, RegistryRuleOut>();
    for (const r of publishedRules) m.set(r.rule_id, r);
    return m;
  }, [publishedRules]);

  const removeMutation = useRemoveAppliedRule();
  const pinMutation = useSetAppliedRulePin();
  const overrideMutation = useSetAppliedRuleSeverityOverride();

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

  const byColumn = useMemo(() => {
    const map = new Map<string, { ruleName: string; slot: string }[]>();
    for (const rule of appliedRules) {
      const name = rule.rule_name || rule.rule_id;
      for (const group of rule.column_mapping ?? []) {
        for (const [slot, column] of Object.entries(group)) {
          const list = map.get(column) ?? [];
          list.push({ ruleName: name, slot });
          map.set(column, list);
        }
      }
    }
    return map;
  }, [appliedRules]);

  return (
    <div className="space-y-4 pt-4">
      <ApprovalStepsBanner />

      <div className="flex items-center justify-between gap-3 flex-wrap">
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
          <div className="flex items-center gap-2">
            {aiAvailability.available && (
              <Button size="sm" variant="outline" className="gap-2" onClick={() => setSuggestOpen(true)}>
                <Sparkles className="h-3.5 w-3.5" />
                {t("monitoredTables.suggestRulesButton")}
              </Button>
            )}
            <Button size="sm" className="gap-2" onClick={() => setAddOpen(true)}>
              <Plus className="h-3.5 w-3.5" />
              {t("monitoredTables.addRulesButton")}
            </Button>
          </div>
        )}
      </div>

      {appliedRules.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <Columns3 className="h-10 w-10 text-muted-foreground/30 mb-3" />
          <p className="text-sm text-muted-foreground">
            {canEdit ? t("monitoredTables.emptyAppliedRulesCta") : t("monitoredTables.emptyAppliedRules")}
          </p>
        </div>
      ) : lens === "by-rule" ? (
        <div className="space-y-3">
          {appliedRules.map((rule) => {
            const dimension = rule.rule_dimension || "";
            const severity = rule.rule_severity || "";
            const registryRule = ruleById.get(rule.rule_id);
            const busy = pendingId === rule.id;
            return (
              <Card key={rule.id ?? rule.rule_id}>
                <CardContent className="py-3 space-y-2">
                  <div className="flex items-start justify-between gap-3 flex-wrap">
                    <div className="min-w-0">
                      <p className="font-medium text-sm truncate">{rule.rule_name || rule.rule_id}</p>
                      <div className="flex flex-wrap gap-1 mt-1">
                        <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
                        <TagBadge label={severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />
                      </div>
                      <div className="mt-2 space-y-1">
                        {(rule.column_mapping ?? []).map((group, i) => (
                          <div key={i} className="flex flex-wrap gap-1">
                            {Object.entries(group).map(([slot, column]) => (
                              <Badge key={slot} variant="secondary" className="text-[10px] font-mono">
                                {slot} → {column}
                              </Badge>
                            ))}
                          </div>
                        ))}
                      </div>
                    </div>
                    {canEdit && (
                      <div className="flex items-center gap-2 shrink-0">
                        {busy ? (
                          <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                        ) : (
                          <>
                            <Select
                              value={rule.pinned_version == null ? "latest" : "pinned"}
                              onValueChange={(v) => handlePinChange(rule, v)}
                            >
                              <SelectTrigger className="h-7 w-36 text-xs">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="latest" className="text-xs">{t("monitoredTables.pinFollowLatest")}</SelectItem>
                                <SelectItem value="pinned" className="text-xs">
                                  {t("monitoredTables.pinVersion", {
                                    version: rule.pinned_version ?? registryRule?.version ?? 1,
                                  })}
                                </SelectItem>
                              </SelectContent>
                            </Select>
                            <Select
                              value={rule.severity_override ?? "none"}
                              onValueChange={(v) => handleSeverityChange(rule, v)}
                            >
                              <SelectTrigger className="h-7 w-32 text-xs">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="none" className="text-xs">{t("monitoredTables.severityOverrideNone")}</SelectItem>
                                {severityValues.map((v) => (
                                  <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-destructive"
                              title={t("monitoredTables.removeRuleButton")}
                              onClick={() => setRemoveTarget(rule)}
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </Button>
                          </>
                        )}
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      ) : (
        <div className="space-y-2">
          {Array.from(byColumn.entries()).map(([column, uses]) => (
            <Card key={column}>
              <CardContent className="py-3">
                <p className="font-mono font-medium text-sm">{column}</p>
                <div className="flex flex-wrap gap-1 mt-1">
                  {uses.map((u, i) => (
                    <Badge key={i} variant="outline" className="text-[10px]">
                      {u.ruleName} ({u.slot})
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      <AddRuleDialog
        open={addOpen}
        onOpenChange={setAddOpen}
        bindingId={bindingId}
        tableFqn={tableFqn}
        publishedRules={publishedRules}
        labelDefinitions={labelDefinitions}
        onApplied={onMutated}
      />

      <SuggestRulesDialog
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
// Add Rule dialog (select published rule -> map slots to columns)
// ---------------------------------------------------------------------------

function AddRuleDialog({
  open,
  onOpenChange,
  bindingId,
  tableFqn,
  publishedRules,
  labelDefinitions,
  onApplied,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  tableFqn: string;
  publishedRules: RegistryRuleOut[];
  labelDefinitions: LabelDefinition[];
  onApplied: () => void;
}) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const [selectedRule, setSelectedRule] = useState<RegistryRuleOut | null>(null);
  const [mapping, setMapping] = useState<Record<string, string | string[]>>({});
  const [createOpen, setCreateOpen] = useState(false);
  const queryClient = useQueryClient();

  const parts = tableFqn.split(".");
  const columnsQuery = useGetTableColumns(parts[0] ?? "", parts[1] ?? "", parts[2] ?? "", {
    query: { enabled: open && parts.length === 3 },
  });
  const columns: ColumnOut[] = columnsQuery.data?.data ?? [];

  const filteredRules = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return publishedRules;
    return publishedRules.filter((r) => {
      const name = getTag(r, RESERVED_NAME_KEY).toLowerCase();
      return name.includes(q) || r.rule_id.toLowerCase().includes(q);
    });
  }, [publishedRules, search]);

  const applyMutation = useApplyRuleToTable();

  const reset = () => {
    setSelectedRule(null);
    setMapping({});
    setSearch("");
  };

  const handleClose = (next: boolean) => {
    if (!next) reset();
    onOpenChange(next);
  };

  const slots: RuleSlot[] = selectedRule?.definition.slots ?? [];
  const mappingComplete = slots.every((slot) => {
    const v = mapping[slot.name];
    if (slot.cardinality === "many") return Array.isArray(v) && v.length > 0;
    return typeof v === "string" && v.length > 0;
  });

  const handleApply = () => {
    if (!selectedRule || !mappingComplete) return;
    const group: Record<string, string> = {};
    for (const slot of slots) {
      const v = mapping[slot.name];
      group[slot.name] = Array.isArray(v) ? v.join(",") : (v as string);
    }
    applyMutation.mutate(
      { bindingId, data: { rule_id: selectedRule.rule_id, column_mapping: [group] } },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.toastApplied"));
          onApplied();
          handleClose(false);
        },
        onError: (err) => toast.error(extractApiError(err, t("monitoredTables.toastApplyFailed")), { duration: 6000 }),
      },
    );
  };

  const openCreateRule = () => {
    onOpenChange(false);
    setCreateOpen(true);
  };

  const handleCreateSaved = () => {
    queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() });
  };

  return (
    <>
      <Dialog open={open} onOpenChange={handleClose}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>{t("monitoredTables.addRuleDialogTitle")}</DialogTitle>
            <DialogDescription>{t("monitoredTables.addRuleDialogDescription")}</DialogDescription>
          </DialogHeader>

          {!selectedRule ? (
            <div className="space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {t("monitoredTables.stepSelectRule")}
              </p>
              <div className="relative">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder={t("monitoredTables.searchRulesPlaceholder")}
                  className="pl-7 h-8 text-xs"
                />
              </div>
              <div className="max-h-72 overflow-y-auto border rounded-md divide-y">
                {filteredRules.length === 0 ? (
                  <p className="text-sm text-muted-foreground text-center py-6">
                    {t("monitoredTables.noPublishedRules")}
                  </p>
                ) : (
                  filteredRules.map((rule) => {
                    const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;
                    const dimension = getTag(rule, RESERVED_DIMENSION_KEY);
                    const severity = getTag(rule, RESERVED_SEVERITY_KEY);
                    return (
                      <button
                        key={rule.rule_id}
                        type="button"
                        onClick={() => setSelectedRule(rule)}
                        className="w-full text-left p-3 hover:bg-muted/40 transition-colors"
                      >
                        <p className="text-sm font-medium">{name}</p>
                        <div className="flex flex-wrap gap-1 mt-1">
                          <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
                          <TagBadge label={severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />
                        </div>
                      </button>
                    );
                  })
                )}
              </div>
              <Button variant="outline" size="sm" className="gap-2 w-full" onClick={openCreateRule}>
                {t("monitoredTables.createNewRuleButton")}
              </Button>
              <p className="text-xs text-muted-foreground">{t("monitoredTables.createNewRuleHint")}</p>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="flex items-center gap-1.5">
                <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {t("monitoredTables.stepMapColumns")}
                </p>
                <HelpTooltip text={t("monitoredTables.mapColumnsTooltip")} />
              </div>
              <p className="text-sm font-medium">{getTag(selectedRule, RESERVED_NAME_KEY) || selectedRule.rule_id}</p>
              <div className="space-y-3">
                {slots.map((slot) => {
                  const familyMatches = columns.filter(
                    (c) => slot.family === "any" || familyForType(c.type_name) === slot.family,
                  );
                  if (slot.cardinality === "many") {
                    const selected = (mapping[slot.name] as string[] | undefined) ?? [];
                    return (
                      <div key={slot.name} className="space-y-1.5">
                        <p className="text-sm font-medium">
                          {t("monitoredTables.slotColumnsLabel", { slot: slot.name })}
                        </p>
                        {familyMatches.length === 0 ? (
                          <p className="text-xs text-muted-foreground">{t("monitoredTables.noMatchingColumns")}</p>
                        ) : (
                          <div className="grid grid-cols-2 gap-1.5 max-h-40 overflow-y-auto border rounded-md p-2">
                            {familyMatches.map((col) => {
                              const checked = selected.includes(col.name);
                              return (
                                <label key={col.name} className="flex items-center gap-1.5 text-xs">
                                  <Checkbox
                                    checked={checked}
                                    onCheckedChange={(v) => {
                                      const next = v
                                        ? [...selected, col.name]
                                        : selected.filter((c) => c !== col.name);
                                      setMapping((m) => ({ ...m, [slot.name]: next }));
                                    }}
                                  />
                                  <span className="font-mono truncate">{col.name}</span>
                                </label>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    );
                  }
                  return (
                    <div key={slot.name} className="space-y-1.5">
                      <p className="text-sm font-medium">
                        {t("monitoredTables.slotColumnLabel", { slot: slot.name })}
                      </p>
                      <Select
                        value={(mapping[slot.name] as string | undefined) ?? ""}
                        onValueChange={(v) => setMapping((m) => ({ ...m, [slot.name]: v }))}
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder={t("monitoredTables.selectColumnPlaceholder")} />
                        </SelectTrigger>
                        <SelectContent>
                          {familyMatches.length === 0 ? (
                            <div className="p-2 text-xs text-muted-foreground">
                              {t("monitoredTables.noMatchingColumns")}
                            </div>
                          ) : (
                            familyMatches.map((col) => (
                              <SelectItem key={col.name} value={col.name} className="text-xs font-mono">
                                {col.name}
                              </SelectItem>
                            ))
                          )}
                        </SelectContent>
                      </Select>
                    </div>
                  );
                })}
              </div>
              {!mappingComplete && (
                <p className="text-xs text-amber-600">{t("monitoredTables.mappingIncomplete")}</p>
              )}
            </div>
          )}

          <DialogFooter>
            {selectedRule && (
              <Button variant="outline" onClick={() => setSelectedRule(null)}>
                {t("monitoredTables.backButton")}
              </Button>
            )}
            <Button variant="outline" onClick={() => handleClose(false)}>
              {t("common.cancel")}
            </Button>
            {selectedRule && (
              <Button onClick={handleApply} disabled={!mappingComplete || applyMutation.isPending} className="gap-2">
                {applyMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                {applyMutation.isPending ? t("monitoredTables.applying") : t("monitoredTables.applyButton")}
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <RegistryRuleFormDialog
        open={createOpen}
        onOpenChange={(next) => {
          setCreateOpen(next);
          if (!next) onOpenChange(true);
        }}
        editingRule={null}
        viewingRule={null}
        labelDefinitions={labelDefinitions}
        onSaved={handleCreateSaved}
      />
    </>
  );
}

// ---------------------------------------------------------------------------
// Suggest Rules dialog (Phase 4D — AI mapping suggester)
// ---------------------------------------------------------------------------

interface SuggestRulesState {
  available: boolean;
  reason?: string;
  suggestions: SuggestedRuleMappingOut[];
}

function SuggestRulesDialog({
  open,
  onOpenChange,
  bindingId,
  labelDefinitions,
  onApplied,
  onAiUnavailable,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  labelDefinitions: LabelDefinition[];
  onApplied: () => void;
  onAiUnavailable: (reason: string) => void;
}) {
  const { t } = useTranslation();
  const suggestMutation = useSuggestRulesForTable();
  const applyMutation = useApplyRuleToTable();
  const [state, setState] = useState<SuggestRulesState | null>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [applying, setApplying] = useState(false);
  const fetchedForRef = useRef<string | null>(null);

  useEffect(() => {
    if (!open) {
      setState(null);
      setSelected(new Set());
      fetchedForRef.current = null;
      return;
    }
    if (fetchedForRef.current === bindingId) return;
    fetchedForRef.current = bindingId;
    suggestMutation.mutate(
      { bindingId },
      {
        onSuccess: (resp) => {
          const suggestions = resp.data.suggestions ?? [];
          setState({ available: resp.data.available, reason: resp.data.reason, suggestions });
          setSelected(new Set(suggestions.map((_, i) => i)));
        },
        onError: (err) => {
          const reason = aiUnavailableReason(err);
          if (reason) onAiUnavailable(reason);
          toast.error(t("monitoredTables.suggestRulesFetchFailed"));
          setState({ available: false, reason: reason ?? undefined, suggestions: [] });
        },
      },
    );
  }, [open, bindingId, suggestMutation, onAiUnavailable, t]);

  const toggle = (idx: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  };

  const handleAdd = async () => {
    if (!state) return;
    const chosen = state.suggestions.filter((_, i) => selected.has(i));
    if (chosen.length === 0) {
      toast.error(t("monitoredTables.suggestRulesNoneSelected"));
      return;
    }
    setApplying(true);
    let failures = 0;
    for (const suggestion of chosen) {
      try {
        await applyMutation.mutateAsync({
          bindingId,
          data: { rule_id: suggestion.rule_id, column_mapping: [suggestion.column_mapping] },
        });
      } catch {
        failures += 1;
      }
    }
    setApplying(false);
    const addedCount = chosen.length - failures;
    if (addedCount > 0) {
      toast.success(t("monitoredTables.suggestRulesAddedToast", { count: addedCount }));
      onApplied();
    }
    if (failures > 0) {
      toast.error(t("monitoredTables.suggestRulesAddFailed"));
    }
    if (failures === 0) {
      onOpenChange(false);
    }
  };

  const loading = suggestMutation.isPending && state === null;

  return (
    <Dialog open={open} onOpenChange={(next) => !applying && onOpenChange(next)}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-primary" />
            {t("monitoredTables.suggestRulesDialogTitle")}
          </DialogTitle>
          <DialogDescription>{t("monitoredTables.suggestRulesDialogDescription")}</DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <p className="text-sm text-muted-foreground">{t("monitoredTables.suggestRulesLoading")}</p>
          </div>
        ) : state && !state.available ? (
          <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
            <Sparkles className="h-8 w-8 text-muted-foreground/30" />
            <p className="text-sm font-medium text-muted-foreground">
              {t("monitoredTables.suggestRulesUnavailableTitle")}
            </p>
            {state.reason && <p className="text-xs text-muted-foreground/70 max-w-sm">{state.reason}</p>}
          </div>
        ) : state && state.suggestions.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
            <Sparkles className="h-8 w-8 text-muted-foreground/30" />
            <p className="text-sm font-medium text-muted-foreground">
              {t("monitoredTables.suggestRulesEmptyTitle")}
            </p>
            <p className="text-xs text-muted-foreground/70 max-w-sm">
              {t("monitoredTables.suggestRulesEmptyDescription")}
            </p>
          </div>
        ) : state ? (
          <div className="max-h-96 overflow-y-auto space-y-2">
            {state.suggestions.map((s, idx) => {
              const checked = selected.has(idx);
              return (
                <label
                  key={`${s.rule_id}-${idx}`}
                  className={`flex items-start gap-3 rounded-md border p-3 cursor-pointer transition-colors ${
                    checked ? "border-primary/40 bg-primary/5" : "hover:bg-muted/40"
                  }`}
                >
                  <Checkbox checked={checked} onCheckedChange={() => toggle(idx)} className="mt-0.5" />
                  <div className="min-w-0 flex-1 space-y-1.5">
                    <div className="flex items-center gap-2 flex-wrap">
                      <p className="text-sm font-medium">{s.rule_name || s.rule_id}</p>
                      {s.dimension && (
                        <TagBadge label={s.dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, s.dimension)} />
                      )}
                      {s.severity && (
                        <TagBadge label={s.severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, s.severity)} />
                      )}
                    </div>
                    {s.explanation && (
                      <p className="text-xs text-muted-foreground">{s.explanation}</p>
                    )}
                    <div className="flex flex-wrap gap-1">
                      {Object.entries(s.column_mapping ?? {}).map(([slot, column]) => (
                        <Badge key={slot} variant="secondary" className="text-[10px] font-mono">
                          {slot} → {column}
                        </Badge>
                      ))}
                    </div>
                  </div>
                </label>
              );
            })}
          </div>
        ) : null}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={applying}>
            {t("common.cancel")}
          </Button>
          {state && state.available && state.suggestions.length > 0 && (
            <Button onClick={handleAdd} disabled={applying || selected.size === 0} className="gap-2">
              {applying && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("monitoredTables.suggestRulesAddButton", { count: selected.size })}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
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
