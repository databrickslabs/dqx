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
import { Skeleton } from "@/components/ui/skeleton";
import { Checkbox } from "@/components/ui/checkbox";
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
  ShieldCheck,
  Sparkles,
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
  useSuggestRulesForTable,
  type AppliedRuleOut,
  type RegistryRuleOut,
  type SuggestedRuleMappingOut,
} from "@/lib/api";
import { useLabelDefinitions, type LabelDefinition } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { formatDateShort } from "@/lib/format-utils";
import { ApprovalStepsBanner } from "@/components/ApprovalStepsBanner";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_BANNER_BORDER, AI_BUTTON_BG, AI_ICON_COLOR, AI_TEXT_GRADIENT } from "@/lib/ai-style";
import { AddRulesDialog } from "@/components/apply-rules/AddRulesDialog";
import { MappingChips } from "@/components/apply-rules/MappingChips";
import { RuleConfigCard } from "@/components/apply-rules/RuleConfigCard";
import { RulesByColumn } from "@/components/apply-rules/RulesByColumn";
import {
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  TagBadge,
  colorFor,
  extractApiError,
} from "@/components/apply-rules/shared";

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
              <Button size="sm" className={`gap-2 ${AI_BUTTON_BG}`} onClick={() => setSuggestOpen(true)}>
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
        <div className="flex flex-col items-center justify-center py-16 text-center border border-dashed rounded-lg">
          <Columns3 className="h-10 w-10 text-muted-foreground/30 mb-3" />
          <p className="text-sm text-muted-foreground mb-3 max-w-sm">
            {canEdit ? t("monitoredTables.emptyAppliedRulesCta") : t("monitoredTables.emptyAppliedRules")}
          </p>
          {canEdit && (
            <Button size="sm" className="gap-2" onClick={() => setAddOpen(true)}>
              <Plus className="h-3.5 w-3.5" />
              {t("monitoredTables.addRulesButton")}
            </Button>
          )}
        </div>
      ) : lens === "by-rule" ? (
        <div className="space-y-3">
          {appliedRules.map((rule) => (
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
            />
          ))}
        </div>
      ) : (
        <RulesByColumn
          appliedRules={appliedRules}
          canEdit={canEdit}
          onAddRule={() => setAddOpen(true)}
        />
      )}

      <AddRulesDialog
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
            <Sparkles className={`h-4 w-4 ${AI_ICON_COLOR}`} />
            <span className={AI_TEXT_GRADIENT}>{t("monitoredTables.suggestRulesDialogTitle")}</span>
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
                    checked ? AI_BANNER_BORDER : "hover:bg-muted/40"
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
                    <MappingChips columnMapping={s.column_mapping ? [s.column_mapping] : []} />
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
            <Button onClick={handleAdd} disabled={applying || selected.size === 0} className={`gap-2 ${AI_BUTTON_BG}`}>
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
