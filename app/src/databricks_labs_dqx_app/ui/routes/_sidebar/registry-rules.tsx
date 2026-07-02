import { createFileRoute } from "@tanstack/react-router";
import { useCallback, useMemo, useState, Suspense } from "react";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  AlertCircle,
  Plus,
  RotateCcw,
  Search,
  FileEdit,
  Clock,
  ShieldCheck,
  XCircle,
  Archive,
  ArchiveRestore,
  SendHorizonal,
  CheckCircle2,
  Trash2,
  Pencil,
  Eye,
  Loader2,
  Lock,
} from "lucide-react";
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
  useListRegistryRules,
  getListRegistryRulesQueryKey,
  useSubmitRegistryRule,
  useApproveRegistryRule,
  useRejectRegistryRule,
  useDeprecateRegistryRule,
  useUndeprecateRegistryRule,
  useDeleteRegistryRule,
  type RegistryRuleOut,
} from "@/lib/api";
import { useLabelDefinitions, type LabelDefinition } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { formatDateShort } from "@/lib/format-utils";
import { cn } from "@/lib/utils";
import { RegistryRuleFormDialog } from "@/components/RegistryRuleFormDialog";
import { ApprovalStepsBanner } from "@/components/ApprovalStepsBanner";

const RESERVED_NAME_KEY = "name";
const RESERVED_DESCRIPTION_KEY = "description";
const RESERVED_DIMENSION_KEY = "dimension";
const RESERVED_SEVERITY_KEY = "severity";

export const Route = createFileRoute("/_sidebar/registry-rules")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} fallbackRender={RegistryRulesError}>
          <Suspense fallback={<RegistryRulesSkeleton />}>
            <RegistryRulesPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function RegistryRulesError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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

function RegistryRulesSkeleton() {
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

function getTag(rule: RegistryRuleOut, key: string): string {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const v = md[key];
  return typeof v === "string" ? v : "";
}

function freeTags(rule: RegistryRuleOut): Record<string, string> {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(md)) {
    if (k === RESERVED_NAME_KEY || k === RESERVED_DESCRIPTION_KEY || k === RESERVED_DIMENSION_KEY || k === RESERVED_SEVERITY_KEY) continue;
    if (typeof v === "string") out[k] = v;
  }
  return out;
}

function colorFor(defs: LabelDefinition[], key: string, value: string): string | undefined {
  const def = defs.find((d) => d.key === key);
  return def?.value_colors?.[value] ?? undefined;
}

function ColorDot({ color }: { color?: string }) {
  if (!color) return null;
  return (
    <span
      className="inline-block h-1.5 w-1.5 rounded-full"
      style={{ backgroundColor: color }}
      aria-hidden
    />
  );
}

function TagBadge({ label, color }: { label: string; color?: string }) {
  if (!label) return <span className="text-muted-foreground/50">—</span>;
  return (
    <Badge variant="outline" className="gap-1 text-[10px] font-normal">
      <ColorDot color={color} />
      {label}
    </Badge>
  );
}

function StatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "draft":
      return (
        <Badge variant="secondary" className="gap-1 text-[10px]">
          <FileEdit className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusDraft")}
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-amber-500 text-amber-600">
          <Clock className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusPendingApproval")}
        </Badge>
      );
    case "approved":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-emerald-500 text-emerald-600">
          <ShieldCheck className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusApproved")}
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-red-500 text-red-600">
          <XCircle className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusRejected")}
        </Badge>
      );
    case "deprecated":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-muted-foreground text-muted-foreground">
          <Archive className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusDeprecated")}
        </Badge>
      );
    default:
      return <Badge variant="secondary" className="text-[10px]">{status}</Badge>;
  }
}

function ModeBadge({ mode }: { mode: string }) {
  const { t } = useTranslation();
  const label =
    mode === "dqx_native"
      ? t("rulesRegistry.modeDqxNative")
      : mode === "lowcode"
        ? t("rulesRegistry.modeLowcode")
        : t("rulesRegistry.modeSql");
  return (
    <Badge variant="secondary" className="text-[10px] font-mono">
      {label}
    </Badge>
  );
}

const ALL = "all";

function RegistryRulesPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const queryClient = useQueryClient();

  const [statusFilter, setStatusFilter] = useState<string>(ALL);
  const [dimensionFilter, setDimensionFilter] = useState<string>(ALL);
  const [severityFilter, setSeverityFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState("");
  const [tagFilter, setTagFilter] = useState("");
  const [nameSearch, setNameSearch] = useState("");

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);
  const dimensionValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_DIMENSION_KEY)?.values ?? [],
    [labelDefinitions],
  );
  const severityValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? [],
    [labelDefinitions],
  );

  const queryParams = useMemo(
    () => ({
      status: statusFilter === ALL ? undefined : statusFilter,
      dimension: dimensionFilter === ALL ? undefined : dimensionFilter,
      severity: severityFilter === ALL ? undefined : severityFilter,
      steward: stewardFilter.trim() || undefined,
      tag: tagFilter.trim() || undefined,
    }),
    [statusFilter, dimensionFilter, severityFilter, stewardFilter, tagFilter],
  );

  const { data } = useListRegistryRules(queryParams);
  const allRules = useMemo(() => data?.data ?? [], [data]);

  const rules = useMemo(() => {
    const q = nameSearch.trim().toLowerCase();
    if (!q) return allRules;
    return allRules.filter((r) => {
      const name = getTag(r, RESERVED_NAME_KEY).toLowerCase();
      return name.includes(q) || r.rule_id.toLowerCase().includes(q);
    });
  }, [allRules, nameSearch]);

  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() }),
    [queryClient],
  );

  const [formOpen, setFormOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<RegistryRuleOut | null>(null);
  const [viewingRule, setViewingRule] = useState<RegistryRuleOut | null>(null);

  const openCreate = () => {
    setEditingRule(null);
    setViewingRule(null);
    setFormOpen(true);
  };
  const openEdit = (rule: RegistryRuleOut) => {
    setEditingRule(rule);
    setViewingRule(null);
    setFormOpen(true);
  };
  const openView = (rule: RegistryRuleOut) => {
    setEditingRule(null);
    setViewingRule(rule);
    setFormOpen(true);
  };

  const [pendingRuleId, setPendingRuleId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<RegistryRuleOut | null>(null);

  const submitMutation = useSubmitRegistryRule();
  const approveMutation = useApproveRegistryRule();
  const rejectMutation = useRejectRegistryRule();
  const deprecateMutation = useDeprecateRegistryRule();
  const undeprecateMutation = useUndeprecateRegistryRule();
  const deleteMutation = useDeleteRegistryRule();

  const runAction = useCallback(
    (
      ruleId: string,
      mutate: () => Promise<unknown>,
      successMsg: string,
      errorMsg: string,
    ) => {
      if (pendingRuleId) return;
      setPendingRuleId(ruleId);
      mutate()
        .then(() => {
          toast.success(successMsg);
          invalidate();
        })
        .catch((err: unknown) => {
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          toast.error(detail || errorMsg, { duration: 6000 });
        })
        .finally(() => setPendingRuleId(null));
    },
    [pendingRuleId, invalidate],
  );

  const handleSubmit = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => submitMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastSubmitted"),
      t("rulesRegistry.toastSubmitFailed"),
    );
  const handleApprove = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => approveMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastApproved"),
      t("rulesRegistry.toastApproveFailed"),
    );
  const handleReject = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => rejectMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastRejected"),
      t("rulesRegistry.toastRejectFailed"),
    );
  const handleDeprecate = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => deprecateMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastDeprecated"),
      t("rulesRegistry.toastDeprecateFailed"),
    );
  const handleUndeprecate = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => undeprecateMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastUndeprecated"),
      t("rulesRegistry.toastUndeprecateFailed"),
    );
  const confirmDelete = () => {
    if (!deleteTarget) return;
    const rule = deleteTarget;
    setDeleteTarget(null);
    runAction(
      rule.rule_id,
      () => deleteMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastDeleted"),
      t("rulesRegistry.toastDeleteFailed"),
    );
  };

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("rulesRegistry.title")} />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("rulesRegistry.title")}</h1>
            <p className="text-sm text-muted-foreground mt-1">{t("rulesRegistry.description")}</p>
          </div>
          {perms.canCreateRules && (
            <Button onClick={openCreate} className="gap-2">
              <Plus className="h-4 w-4" />
              {t("rulesRegistry.newRule")}
            </Button>
          )}
        </div>

        <ApprovalStepsBanner />

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">{t("rulesRegistry.filtersTitle")}</CardTitle>
            <CardDescription>{t("rulesRegistry.filtersDescription")}</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            <div className="relative w-52">
              <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                placeholder={t("rulesRegistry.searchPlaceholder")}
                value={nameSearch}
                onChange={(e) => setNameSearch(e.target.value)}
                className="h-8 text-xs pl-7"
              />
            </div>
            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="h-8 w-40 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allStatuses")}</SelectItem>
                <SelectItem value="draft" className="text-xs">{t("rulesRegistry.statusDraft")}</SelectItem>
                <SelectItem value="pending_approval" className="text-xs">{t("rulesRegistry.statusPendingApproval")}</SelectItem>
                <SelectItem value="approved" className="text-xs">{t("rulesRegistry.statusApproved")}</SelectItem>
                <SelectItem value="rejected" className="text-xs">{t("rulesRegistry.statusRejected")}</SelectItem>
                <SelectItem value="deprecated" className="text-xs">{t("rulesRegistry.statusDeprecated")}</SelectItem>
              </SelectContent>
            </Select>
            <Select value={dimensionFilter} onValueChange={setDimensionFilter}>
              <SelectTrigger className="h-8 w-40 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allDimensions")}</SelectItem>
                {dimensionValues.map((v) => (
                  <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={severityFilter} onValueChange={setSeverityFilter}>
              <SelectTrigger className="h-8 w-40 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allSeverities")}</SelectItem>
                {severityValues.map((v) => (
                  <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Input
              placeholder={t("rulesRegistry.stewardPlaceholder")}
              value={stewardFilter}
              onChange={(e) => setStewardFilter(e.target.value)}
              className="h-8 w-40 text-xs"
            />
            <Input
              placeholder={t("rulesRegistry.tagPlaceholder")}
              value={tagFilter}
              onChange={(e) => setTagFilter(e.target.value)}
              className="h-8 w-36 text-xs"
            />
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-0">
            {rules.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-center">
                <ShieldCheck className="h-10 w-10 text-muted-foreground/30 mb-3" />
                <p className="text-sm text-muted-foreground">{t("rulesRegistry.emptyState")}</p>
              </div>
            ) : (
              <div className="divide-y">
                <div className="hidden md:grid grid-cols-[1fr_auto_auto_auto_auto_auto_auto_auto_auto] gap-3 px-4 py-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground bg-muted/30">
                  <span>{t("rulesRegistry.colName")}</span>
                  <span>{t("rulesRegistry.colDimension")}</span>
                  <span>{t("rulesRegistry.colSeverity")}</span>
                  <span>{t("rulesRegistry.colStatus")}</span>
                  <span>{t("rulesRegistry.colVersion")}</span>
                  <span>{t("rulesRegistry.colMode")}</span>
                  <span>{t("rulesRegistry.colSteward")}</span>
                  <span>{t("rulesRegistry.colUpdated")}</span>
                  <span className="text-right">{t("rulesRegistry.colActions")}</span>
                </div>
                {rules.map((rule) => {
                  const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;
                  const dimension = getTag(rule, RESERVED_DIMENSION_KEY);
                  const severity = getTag(rule, RESERVED_SEVERITY_KEY);
                  const busy = pendingRuleId === rule.rule_id;
                  return (
                    <div
                      key={rule.rule_id}
                      className="grid grid-cols-1 md:grid-cols-[1fr_auto_auto_auto_auto_auto_auto_auto_auto] gap-2 md:gap-3 items-center px-4 py-3 hover:bg-muted/20 cursor-pointer transition-colors"
                      onClick={() => openView(rule)}
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-1.5 font-medium text-sm truncate">
                          {rule.is_builtin && (
                            <span title={t("rulesRegistry.builtinTooltip")}>
                              <Lock className="h-3 w-3 text-muted-foreground shrink-0" />
                            </span>
                          )}
                          <span className="truncate">{name}</span>
                        </div>
                        {Object.keys(freeTags(rule)).length > 0 && (
                          <div className="flex flex-wrap gap-1 mt-1">
                            {Object.entries(freeTags(rule)).slice(0, 3).map(([k, v]) => (
                              <Badge key={k} variant="outline" className="text-[9px] font-normal px-1 py-0">
                                {k}={v}
                              </Badge>
                            ))}
                          </div>
                        )}
                      </div>
                      <div><TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} /></div>
                      <div><TagBadge label={severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity)} /></div>
                      <div><StatusBadge status={rule.status} /></div>
                      <div className="text-xs text-muted-foreground font-mono">v{rule.version}</div>
                      <div><ModeBadge mode={rule.mode} /></div>
                      <div className="text-xs text-muted-foreground truncate max-w-[10rem]">{rule.steward || "—"}</div>
                      <div className="text-xs text-muted-foreground">{formatDateShort(rule.updated_at)}</div>
                      <div
                        className="flex items-center justify-end gap-1"
                        onClick={(e) => e.stopPropagation()}
                      >
                        {busy ? (
                          <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                        ) : (
                          <>
                            {rule.status === "draft" && perms.canCreateRules && (
                              <>
                                <Button variant="ghost" size="sm" className="h-7 w-7 p-0" title={t("rulesRegistry.actionEdit")} onClick={() => openEdit(rule)}>
                                  <Pencil className="h-3.5 w-3.5" />
                                </Button>
                                <Button variant="ghost" size="sm" className="h-7 w-7 p-0 text-blue-600" title={t("rulesRegistry.actionSubmit")} onClick={() => handleSubmit(rule)}>
                                  <SendHorizonal className="h-3.5 w-3.5" />
                                </Button>
                                <Button variant="ghost" size="sm" className="h-7 w-7 p-0 text-destructive" title={t("rulesRegistry.actionDelete")} onClick={() => setDeleteTarget(rule)}>
                                  <Trash2 className="h-3.5 w-3.5" />
                                </Button>
                              </>
                            )}
                            {rule.status === "pending_approval" && perms.canApproveRules && (
                              <>
                                <Button variant="ghost" size="sm" className="h-7 w-7 p-0 text-emerald-600" title={t("rulesRegistry.actionApprove")} onClick={() => handleApprove(rule)}>
                                  <CheckCircle2 className="h-3.5 w-3.5" />
                                </Button>
                                <Button variant="ghost" size="sm" className="h-7 w-7 p-0 text-destructive" title={t("rulesRegistry.actionReject")} onClick={() => handleReject(rule)}>
                                  <XCircle className="h-3.5 w-3.5" />
                                </Button>
                              </>
                            )}
                            {rule.status === "approved" && perms.canApproveRules && (
                              <Button variant="ghost" size="sm" className="h-7 w-7 p-0" title={t("rulesRegistry.actionDeprecate")} onClick={() => handleDeprecate(rule)}>
                                <Archive className="h-3.5 w-3.5" />
                              </Button>
                            )}
                            {rule.status === "deprecated" && perms.canApproveRules && (
                              <Button variant="ghost" size="sm" className="h-7 w-7 p-0" title={t("rulesRegistry.actionUndeprecate")} onClick={() => handleUndeprecate(rule)}>
                                <ArchiveRestore className="h-3.5 w-3.5" />
                              </Button>
                            )}
                            {rule.status === "rejected" && perms.canCreateRules && (
                              <Button variant="ghost" size="sm" className="h-7 w-7 p-0 text-destructive" title={t("rulesRegistry.actionDelete")} onClick={() => setDeleteTarget(rule)}>
                                <Trash2 className="h-3.5 w-3.5" />
                              </Button>
                            )}
                            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" title={t("rulesRegistry.actionView")} onClick={() => openView(rule)}>
                              <Eye className="h-3.5 w-3.5" />
                            </Button>
                          </>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <RegistryRuleFormDialog
        open={formOpen}
        onOpenChange={setFormOpen}
        editingRule={editingRule}
        viewingRule={viewingRule}
        labelDefinitions={labelDefinitions}
        onSaved={invalidate}
      />

      <AlertDialog open={deleteTarget !== null} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesRegistry.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesRegistry.deleteConfirmDescription", { name: deleteTarget ? getTag(deleteTarget, RESERVED_NAME_KEY) || deleteTarget.rule_id : "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className={cn("bg-destructive text-white hover:bg-destructive/90")} onClick={confirmDelete}>
              {t("rulesRegistry.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
