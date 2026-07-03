import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo, useState, Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Card, CardContent } from "@/components/ui/card";
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
  ShieldCheck,
  XCircle,
  Archive,
  ArchiveRestore,
  SendHorizonal,
  CheckCircle2,
  Trash2,
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
import { useLabelDefinitions } from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { formatDateShort } from "@/lib/format-utils";
import { cn } from "@/lib/utils";
import { RegistryRuleFormDialog } from "@/components/RegistryRuleFormDialog";
import { ApprovalStepsBanner } from "@/components/ApprovalStepsBanner";
import { Pagination } from "@/components/Pagination";
import {
  RESERVED_NAME_KEY,
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  getTag,
  freeTags,
  colorFor,
  TagBadge,
  StatusBadge,
  ModeBadge,
  AuthorKindBadge,
} from "@/components/RegistryRuleBadges";

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

const ALL = "all";
const PAGE_SIZE = 25;
const FILTER_CLASS = "h-8 w-36 text-xs";

function RegistryRulesPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [statusFilter, setStatusFilter] = useState<string>(ALL);
  const [dimensionFilter, setDimensionFilter] = useState<string>(ALL);
  const [severityFilter, setSeverityFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [tagFilter, setTagFilter] = useState("");
  const [nameSearch, setNameSearch] = useState("");
  const [page, setPage] = useState(1);

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

  // Steward isn't a label — it's a plain rule field — so the steward facet
  // is derived from the server-filtered result set below, and filtered
  // client-side alongside the free-text name search (same as the old
  // dqx Active Rules list, which filtered client-side over one fetched set).
  const serverParams = useMemo(
    () => ({
      status: statusFilter === ALL ? undefined : statusFilter,
      dimension: dimensionFilter === ALL ? undefined : dimensionFilter,
      severity: severityFilter === ALL ? undefined : severityFilter,
      tag: tagFilter.trim() || undefined,
    }),
    [statusFilter, dimensionFilter, severityFilter, tagFilter],
  );

  const { data } = useListRegistryRules(serverParams);
  const serverFilteredRules = useMemo(() => data?.data ?? [], [data]);

  const stewardValues = useMemo(() => {
    const set = new Set<string>();
    for (const r of serverFilteredRules) {
      if (r.steward) set.add(r.steward);
    }
    return Array.from(set).sort();
  }, [serverFilteredRules]);

  const rules = useMemo(() => {
    const q = nameSearch.trim().toLowerCase();
    return serverFilteredRules.filter((r) => {
      if (stewardFilter !== ALL && (r.steward ?? "") !== stewardFilter) return false;
      if (!q) return true;
      const name = getTag(r, RESERVED_NAME_KEY).toLowerCase();
      return name.includes(q) || r.rule_id.toLowerCase().includes(q);
    });
  }, [serverFilteredRules, stewardFilter, nameSearch]);

  const pagedRules = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return rules.slice(start, start + PAGE_SIZE);
  }, [rules, page]);

  const hasActiveFilters =
    statusFilter !== ALL ||
    dimensionFilter !== ALL ||
    severityFilter !== ALL ||
    stewardFilter !== ALL ||
    tagFilter.trim() !== "" ||
    nameSearch.trim() !== "";

  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() }),
    [queryClient],
  );

  const applyFilter = useCallback(
    (setter: (v: string) => void) => (v: string) => {
      setter(v);
      setPage(1);
    },
    [],
  );

  const [createOpen, setCreateOpen] = useState(false);

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

  const openRule = (rule: RegistryRuleOut) => {
    navigate({ to: "/registry-rules/$ruleId", params: { ruleId: rule.rule_id } });
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
            <Button onClick={() => setCreateOpen(true)} className="gap-2">
              <Plus className="h-4 w-4" />
              {t("rulesRegistry.newRule")}
            </Button>
          )}
        </div>

        <ApprovalStepsBanner />

        <div className="flex flex-wrap items-center gap-2">
          <div className="relative w-36">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
            <Input
              placeholder={t("rulesRegistry.searchPlaceholder")}
              value={nameSearch}
              onChange={(e) => applyFilter(setNameSearch)(e.target.value)}
              className={cn(FILTER_CLASS, "pl-7")}
            />
          </div>
          <Select value={statusFilter} onValueChange={applyFilter(setStatusFilter)}>
            <SelectTrigger className={FILTER_CLASS}>
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
          <Select value={dimensionFilter} onValueChange={applyFilter(setDimensionFilter)}>
            <SelectTrigger className={FILTER_CLASS}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allDimensions")}</SelectItem>
              {dimensionValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={severityFilter} onValueChange={applyFilter(setSeverityFilter)}>
            <SelectTrigger className={FILTER_CLASS}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allSeverities")}</SelectItem>
              {severityValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={stewardFilter} onValueChange={applyFilter(setStewardFilter)}>
            <SelectTrigger className={FILTER_CLASS}>
              <SelectValue placeholder={t("rulesRegistry.stewardPlaceholder")} />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allStewards")}</SelectItem>
              {stewardValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            placeholder={t("rulesRegistry.tagPlaceholder")}
            value={tagFilter}
            onChange={(e) => applyFilter(setTagFilter)(e.target.value)}
            className={FILTER_CLASS}
          />
          {hasActiveFilters && (
            <Button
              variant="ghost"
              size="sm"
              className="h-8 text-xs"
              onClick={() => {
                setStatusFilter(ALL);
                setDimensionFilter(ALL);
                setSeverityFilter(ALL);
                setStewardFilter(ALL);
                setTagFilter("");
                setNameSearch("");
                setPage(1);
              }}
            >
              {t("common.clearFilters")}
            </Button>
          )}
        </div>

        <Card>
          <CardContent className="p-0">
            {rules.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-center">
                <ShieldCheck className="h-10 w-10 text-muted-foreground/30 mb-3" />
                <p className="text-sm text-muted-foreground">
                  {hasActiveFilters
                    ? t("rulesRegistry.emptyState")
                    : perms.canCreateRules
                      ? t("rulesRegistry.emptyStateNoRulesCta")
                      : t("rulesRegistry.emptyStateNoRules")}
                </p>
              </div>
            ) : (
              <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-muted/50">
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colName")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colAuthor")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colDimension")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colSeverity")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colStatus")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colVersion")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colMode")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colSteward")}</th>
                        <th className="text-left p-3 font-medium text-xs">{t("rulesRegistry.colUpdated")}</th>
                        <th className="text-right p-3 font-medium text-xs">{t("rulesRegistry.colActions")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pagedRules.map((rule) => {
                        const name = getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;
                        const dimension = getTag(rule, RESERVED_DIMENSION_KEY);
                        const severity = getTag(rule, RESERVED_SEVERITY_KEY);
                        const busy = pendingRuleId === rule.rule_id;
                        const tags = freeTags(rule);
                        return (
                          <tr
                            key={rule.rule_id}
                            className="border-b last:border-b-0 hover:bg-muted/20 cursor-pointer transition-colors"
                            onClick={() => openRule(rule)}
                          >
                            <td className="p-3 min-w-[10rem] max-w-[16rem]">
                              <div className="flex items-center gap-1.5 font-medium text-sm truncate">
                                {rule.is_builtin && (
                                  <span title={t("rulesRegistry.builtinTooltip")}>
                                    <Lock className="h-3 w-3 text-muted-foreground shrink-0" />
                                  </span>
                                )}
                                <span className="truncate">{name}</span>
                              </div>
                              {Object.keys(tags).length > 0 && (
                                <div className="flex flex-wrap gap-1 mt-1">
                                  {Object.entries(tags).slice(0, 3).map(([k, v]) => (
                                    <Badge key={k} variant="outline" className="text-[9px] font-normal px-1 py-0">
                                      {k}={v}
                                    </Badge>
                                  ))}
                                </div>
                              )}
                            </td>
                            <td className="p-3">
                              <AuthorKindBadge authorKind={rule.author_kind ?? undefined} />
                            </td>
                            <td className="p-3">
                              <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
                            </td>
                            <td className="p-3">
                              <TagBadge label={severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />
                            </td>
                            <td className="p-3"><StatusBadge status={rule.status} /></td>
                            <td className="p-3 text-xs text-muted-foreground font-mono">v{rule.version}</td>
                            <td className="p-3"><ModeBadge mode={rule.mode} /></td>
                            <td className="p-3 text-xs text-muted-foreground truncate max-w-[10rem]">{rule.steward || "—"}</td>
                            <td className="p-3 text-xs text-muted-foreground">{formatDateShort(rule.updated_at)}</td>
                            <td className="p-3 text-right" onClick={(e) => e.stopPropagation()}>
                              {busy ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground inline-block" />
                              ) : (
                                <div className="flex items-center justify-end gap-1">
                                  {rule.status === "draft" && perms.canCreateRules && (
                                    <>
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
                                </div>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                <Pagination page={page} totalItems={rules.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <RegistryRuleFormDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        editingRule={null}
        viewingRule={null}
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
