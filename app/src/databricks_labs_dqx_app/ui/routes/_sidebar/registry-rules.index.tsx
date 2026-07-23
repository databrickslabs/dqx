import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo, useState, Suspense, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  AlertCircle,
  Library,
  Plus,
  Upload,
  RotateCcw,
  Search,
  XCircle,
  Archive,
  ArchiveRestore,
  SendHorizonal,
  CheckCircle2,
  Trash2,
  Loader2,
  Undo2,
  GitCompare,
  FileDown,
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
  useRevokeRegistryRule,
  type RegistryRuleOut,
} from "@/lib/api";
import {
  RegistryRuleDiffDialog,
  type RegistryDiffTarget,
} from "@/components/drafts/ChangeDiffDialog";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import type { User as UserType } from "@/lib/api";
import { useLabelDefinitions, exportRegistryRules } from "@/lib/api-custom";
import { ExportDialog } from "@/components/ExportDialog";
import { LabelFilter, labelsMatchFilter, type LabelSelection } from "@/components/Labels";
import { labelToken } from "@/lib/format-utils";
import { usePermissions } from "@/hooks/use-permissions";
import { invalidateAfterRegistryRuleApprovalChange } from "@/lib/registry-rule-invalidation";
import { cn } from "@/lib/utils";
import { Pagination } from "@/components/Pagination";
import { FILTER_TRIGGER_CLASS } from "@/components/data-table/filter-bar";
import { BulkActionBar } from "@/components/data-table/BulkActionBar";
import { SearchableSelect } from "@/components/data-table/SearchableSelect";
import {
  RulesTable,
  getRulesTableSortValue,
  getRulesTableSortConfig,
  type RulesTableSortKey,
  type RulesTableSelection,
} from "@/components/rules/RulesTable";
import { compareSortValues } from "@/components/data-table/sort";
import {
  RESERVED_NAME_KEY,
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  orderSeverityValuesForDisplay,
  getTag,
  freeTags,
  colorFor,
  ColorDot,
  type LabelColorDefinition,
} from "@/components/RegistryRuleBadges";

export const Route = createFileRoute("/_sidebar/registry-rules/")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={RegistryRulesError}>
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
const FILTER_CLASS = FILTER_TRIGGER_CLASS;

function RegistryRulesPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  const [dimensionFilter, setDimensionFilter] = useState<string>(ALL);
  const [severityFilter, setSeverityFilter] = useState<string>(ALL);
  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [labelFilter, setLabelFilter] = useState<LabelSelection>(new Map());
  const [nameSearch, setNameSearch] = useState("");
  const [page, setPage] = useState(1);

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);
  const dimensionValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_DIMENSION_KEY)?.values ?? [],
    [labelDefinitions],
  );
  const severityValues = useMemo(
    () => orderSeverityValuesForDisplay(labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? []),
    [labelDefinitions],
  );

  // Only dimension/severity are pushed to the server. Steward, free-text tags
  // (via the key-first LabelFilter) and the name search are all derived from
  // and filtered over the server-filtered result set client-side — the same
  // one-fetch-then-filter approach the old dqx Active Rules list used. Moving
  // tag filtering client-side (it was previously a single free-text `tag`
  // server param matching key presence) is what lets the LabelFilter offer
  // key + value multi-select without a new backend query shape.
  const serverParams = useMemo(
    () => ({
      dimension: dimensionFilter === ALL ? undefined : dimensionFilter,
      severity: severityFilter === ALL ? undefined : severityFilter,
    }),
    [dimensionFilter, severityFilter],
  );

  // Non-suspense on purpose: a failed or still-loading list fetch resolves to
  // `data === undefined` rather than suspending/throwing — which historically
  // rendered as a misleading empty "No rules yet" table (P19 bug #3: with a
  // cold cache the empty state showed for the entire fetch, reading as "my
  // rules are gone"). `isPending` (true only while there is no cached data)
  // renders a skeleton and `isError` renders the retry UI instead.
  const { data, isPending, isError, refetch } = useListRegistryRules(serverParams);
  const serverFilteredRules = useMemo(() => data?.data ?? [], [data]);

  const stewardValues = useMemo(() => {
    const set = new Set<string>();
    for (const r of serverFilteredRules) {
      if (r.steward) set.add(r.steward);
    }
    return Array.from(set).sort();
  }, [serverFilteredRules]);

  // Distinct free-text (non-reserved) tags across the server-filtered set,
  // feeding the key-first LabelFilter. Reserved keys (dimension/severity) are
  // excluded — they have their own dedicated Select facets above.
  const availableLabels = useMemo(() => {
    const seen = new Set<string>();
    const out: { key: string; value: string }[] = [];
    for (const r of serverFilteredRules) {
      for (const [key, value] of Object.entries(freeTags(r))) {
        const tok = labelToken(key, value);
        if (!seen.has(tok)) {
          seen.add(tok);
          out.push({ key, value });
        }
      }
    }
    return out;
  }, [serverFilteredRules]);

  const rules = useMemo(() => {
    const q = nameSearch.trim().toLowerCase();
    return serverFilteredRules.filter((r) => {
      if (stewardFilter !== ALL && (r.steward ?? "") !== stewardFilter) return false;
      if (!labelsMatchFilter(freeTags(r), labelFilter)) return false;
      if (!q) return true;
      const name = getTag(r, RESERVED_NAME_KEY).toLowerCase();
      return name.includes(q) || r.rule_id.toLowerCase().includes(q);
    });
  }, [serverFilteredRules, stewardFilter, labelFilter, nameSearch]);

  const [sortKey, setSortKey] = useState<RulesTableSortKey | null>("modified");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const handleHeaderClick = useCallback(
    (key: RulesTableSortKey) => {
      // First click uses the column's steward-first default direction (B2-92);
      // repeat clicks toggle to the opposite direction, then clear.
      const { dir } = getRulesTableSortConfig(key);
      if (sortKey !== key) {
        setSortKey(key);
        setSortDir(dir);
        return;
      }
      if (sortDir === dir) {
        setSortDir(dir === "asc" ? "desc" : "asc");
        return;
      }
      setSortKey(null);
    },
    [sortKey, sortDir],
  );

  const sortedRules = useMemo(() => {
    if (!sortKey) return rules;
    const { nullsFirst } = getRulesTableSortConfig(sortKey);
    const copy = [...rules];
    copy.sort((a, b) =>
      compareSortValues(
        getRulesTableSortValue(sortKey, a),
        getRulesTableSortValue(sortKey, b),
        sortDir,
        nullsFirst,
      ),
    );
    return copy;
  }, [rules, sortKey, sortDir]);

  const pagedRules = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sortedRules.slice(start, start + PAGE_SIZE);
  }, [sortedRules, page]);

  const hasActiveFilters =
    dimensionFilter !== ALL ||
    severityFilter !== ALL ||
    stewardFilter !== ALL ||
    labelFilter.size > 0 ||
    nameSearch.trim() !== "";

  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() }),
    [queryClient],
  );

  // Approve/reject can re-materialize monitored tables the rule is applied
  // to server-side, so those two actions invalidate the wider
  // monitored-tables surface in addition to the registry rules list — see
  // helper doc for why we can't target specific binding IDs here.
  const invalidateAfterApprovalChange = useCallback(
    () => invalidateAfterRegistryRuleApprovalChange(queryClient),
    [queryClient],
  );

  const applyFilter = useCallback(
    (setter: (v: string) => void) => (v: string) => {
      setter(v);
      setPage(1);
    },
    [],
  );

  const [pendingRuleId, setPendingRuleId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<RegistryRuleOut | null>(null);
  const [diffTarget, setDiffTarget] = useState<RegistryDiffTarget | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkBusy, setBulkBusy] = useState(false);
  const [bulkApproveOpen, setBulkApproveOpen] = useState(false);
  const [bulkRejectOpen, setBulkRejectOpen] = useState(false);
  const [bulkDeprecateOpen, setBulkDeprecateOpen] = useState(false);
  const [bulkRevokeOpen, setBulkRevokeOpen] = useState(false);
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);

  const isRuleAuthor = useCallback(
    (rule: RegistryRuleOut) => {
      if (!currentUserEmail) return false;
      const author = (rule.updated_by ?? rule.created_by ?? "").toLowerCase();
      return author === currentUserEmail.toLowerCase();
    },
    [currentUserEmail],
  );

  const canRevokeRule = useCallback(
    (rule: RegistryRuleOut) =>
      rule.status === "pending_approval" && (perms.canApproveRules || (perms.canCreateRules && isRuleAuthor(rule))),
    [perms.canApproveRules, perms.canCreateRules, isRuleAuthor],
  );

  // Terminal-state rules (deprecated / rejected) are cleanup candidates: an
  // author/approver can select them to export or delete. Mirrors the per-row
  // delete gate (rejected → canCreateRules; deprecated → canApproveRules).
  const canDeleteRule = useCallback(
    (rule: RegistryRuleOut) =>
      (rule.status === "rejected" && perms.canCreateRules) ||
      (rule.status === "deprecated" && perms.canApproveRules) ||
      (rule.status === "draft" && perms.canCreateRules),
    [perms.canCreateRules, perms.canApproveRules],
  );

  const selectableRuleIds = useMemo(() => {
    const ids = new Set<string>();
    for (const r of sortedRules) {
      if (r.is_builtin) continue;
      if (
        (r.status === "pending_approval" && perms.canApproveRules) ||
        (r.status === "approved" && perms.canApproveRules) ||
        canRevokeRule(r) ||
        canDeleteRule(r)
      ) {
        ids.add(r.rule_id);
      }
    }
    return ids;
  }, [sortedRules, perms.canApproveRules, canRevokeRule, canDeleteRule]);

  const selectedRules = useMemo(
    () => sortedRules.filter((r) => selectedIds.has(r.rule_id)),
    [sortedRules, selectedIds],
  );

  const toggleSelect = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const toggleSelectAll = useCallback(() => {
    setSelectedIds((prev) => {
      if (prev.size === selectableRuleIds.size) return new Set();
      return new Set(selectableRuleIds);
    });
  }, [selectableRuleIds]);

  const tableSelection = useMemo<RulesTableSelection>(
    () => ({
      selectedIds,
      selectableRuleIds,
      onToggle: toggleSelect,
      onToggleAll: toggleSelectAll,
    }),
    [selectedIds, selectableRuleIds, toggleSelect, toggleSelectAll],
  );

  const submitMutation = useSubmitRegistryRule();
  const approveMutation = useApproveRegistryRule();
  const rejectMutation = useRejectRegistryRule();
  const deprecateMutation = useDeprecateRegistryRule();
  const undeprecateMutation = useUndeprecateRegistryRule();
  const deleteMutation = useDeleteRegistryRule();
  const revokeMutation = useRevokeRegistryRule();

  const bulkAction = useCallback(
    async (
      rulesToAct: RegistryRuleOut[],
      mutate: (ruleId: string) => Promise<unknown>,
      successMsg: string,
      onSuccessInvalidate: () => void = invalidate,
    ) => {
      if (bulkBusy || rulesToAct.length === 0) return;
      setBulkBusy(true);
      let ok = 0;
      let fail = 0;
      let lastDetail = "";
      for (const rule of rulesToAct) {
        try {
          await mutate(rule.rule_id);
          ok++;
        } catch (err: unknown) {
          fail++;
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          if (detail) lastDetail = detail;
        }
      }
      setBulkBusy(false);
      setSelectedIds(new Set());
      onSuccessInvalidate();
      if (fail === 0) {
        toast.success(t("rulesRegistry.bulkSucceeded", { count: ok, msg: successMsg }));
      } else {
        toast.warning(
          t("rulesRegistry.bulkPartial", {
            ok,
            fail,
            reason: lastDetail ? ` — ${lastDetail}` : "",
          }),
        );
      }
    },
    [bulkBusy, invalidate, t],
  );

  const confirmBulkApprove = () => {
    setBulkApproveOpen(false);
    const eligible = selectedRules.filter((r) => r.status === "pending_approval");
    bulkAction(
      eligible,
      (ruleId) => approveMutation.mutateAsync({ ruleId }),
      t("rulesRegistry.bulkApproved"),
      invalidateAfterApprovalChange,
    );
  };

  const confirmBulkReject = () => {
    setBulkRejectOpen(false);
    const eligible = selectedRules.filter((r) => r.status === "pending_approval");
    bulkAction(
      eligible,
      (ruleId) => rejectMutation.mutateAsync({ ruleId }),
      t("rulesRegistry.bulkRejected"),
      invalidateAfterApprovalChange,
    );
  };

  const confirmBulkDeprecate = () => {
    setBulkDeprecateOpen(false);
    const eligible = selectedRules.filter((r) => r.status === "approved");
    bulkAction(
      eligible,
      (ruleId) => deprecateMutation.mutateAsync({ ruleId }),
      t("rulesRegistry.bulkDeprecated"),
    );
  };

  const confirmBulkRevoke = () => {
    setBulkRevokeOpen(false);
    const eligible = selectedRules.filter((r) => canRevokeRule(r));
    bulkAction(
      eligible,
      (ruleId) => revokeMutation.mutateAsync({ ruleId }),
      t("rulesRegistry.bulkRevoked"),
    );
  };

  const confirmBulkDelete = () => {
    setBulkDeleteOpen(false);
    const eligible = selectedRules.filter((r) => canDeleteRule(r));
    bulkAction(
      eligible,
      (ruleId) => deleteMutation.mutateAsync({ ruleId }),
      t("rulesRegistry.bulkDeleted"),
    );
  };

  const runAction = useCallback(
    (
      ruleId: string,
      mutate: () => Promise<unknown>,
      successMsg: string,
      errorMsg: string,
      onSuccessInvalidate: () => void = invalidate,
    ) => {
      if (pendingRuleId) return;
      setPendingRuleId(ruleId);
      mutate()
        .then(() => {
          toast.success(successMsg);
          onSuccessInvalidate();
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
      invalidateAfterApprovalChange,
    );
  const handleReject = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => rejectMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastRejected"),
      t("rulesRegistry.toastRejectFailed"),
      invalidateAfterApprovalChange,
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
  const handleRevoke = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => revokeMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesRegistry.toastRevoked"),
      t("rulesRegistry.toastRevokeFailed"),
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

  const renderActionsCell = useCallback(
    (rule: RegistryRuleOut): ReactNode => {
      const busy = pendingRuleId === rule.rule_id;
      if (busy) {
        return <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground inline-block" />;
      }
      return (
        <div className="flex items-center justify-end gap-1">
          {rule.status === "draft" && perms.canCreateRules && (
            <>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-blue-600"
                    aria-label={t("rulesRegistry.actionSubmit")}
                    onClick={() => handleSubmit(rule)}
                  >
                    <SendHorizonal className="h-3.5 w-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>{t("rulesRegistry.actionSubmit")}</TooltipContent>
              </Tooltip>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-destructive"
                    aria-label={t("rulesRegistry.actionDelete")}
                    onClick={() => setDeleteTarget(rule)}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>{t("rulesRegistry.actionDelete")}</TooltipContent>
              </Tooltip>
            </>
          )}
          {rule.status === "pending_approval" && perms.canApproveRules && (
            <>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-emerald-600"
                    aria-label={t("rulesRegistry.actionApprove")}
                    onClick={() => handleApprove(rule)}
                  >
                    <CheckCircle2 className="h-3.5 w-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>{t("rulesRegistry.actionApprove")}</TooltipContent>
              </Tooltip>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-destructive"
                    aria-label={t("rulesRegistry.actionReject")}
                    onClick={() => handleReject(rule)}
                  >
                    <XCircle className="h-3.5 w-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>{t("rulesRegistry.actionReject")}</TooltipContent>
              </Tooltip>
            </>
          )}
          {canRevokeRule(rule) && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0 text-amber-600"
                  aria-label={t("rulesRegistry.actionRevoke")}
                  onClick={() => handleRevoke(rule)}
                >
                  <Undo2 className="h-3.5 w-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t("rulesRegistry.actionRevoke")}</TooltipContent>
            </Tooltip>
          )}
          {rule.status === "approved" && perms.canApproveRules && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0"
                  aria-label={t("rulesRegistry.actionDeprecate")}
                  onClick={() => handleDeprecate(rule)}
                >
                  <Archive className="h-3.5 w-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t("rulesRegistry.actionDeprecate")}</TooltipContent>
            </Tooltip>
          )}
          {rule.status === "deprecated" && perms.canApproveRules && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0"
                  aria-label={t("rulesRegistry.actionUndeprecate")}
                  onClick={() => handleUndeprecate(rule)}
                >
                  <ArchiveRestore className="h-3.5 w-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t("rulesRegistry.actionUndeprecate")}</TooltipContent>
            </Tooltip>
          )}
          {(rule.display_status === "modified" || rule.status === "pending_approval") && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0 text-purple-600 dark:text-purple-400"
                  aria-label={t("rulesDrafts.diff.viewChanges")}
                  onClick={() =>
                    setDiffTarget({
                      ruleId: rule.rule_id,
                      name: getTag(rule, RESERVED_NAME_KEY) || rule.rule_id,
                      version: rule.version,
                      proposed: rule,
                    })
                  }
                >
                  <GitCompare className="h-3.5 w-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t("rulesDrafts.diff.viewChanges")}</TooltipContent>
            </Tooltip>
          )}
          {rule.status === "rejected" && perms.canCreateRules && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0 text-destructive"
                  aria-label={t("rulesRegistry.actionDelete")}
                  onClick={() => setDeleteTarget(rule)}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t("rulesRegistry.actionDelete")}</TooltipContent>
            </Tooltip>
          )}
        </div>
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [pendingRuleId, perms, canRevokeRule],
  );

  // Canonical action order across all three overviews (bug-bash-v4 item 14):
  // Run (where applicable) → Approve → Reject → [Deprecate → Revoke:
  // rules-only] → Export → Delete → Clear. Rules has no Run.
  const bulkToolbar = (
    <BulkActionBar
      count={selectedIds.size}
      label={t("rulesRegistry.selectedCount", { count: selectedIds.size })}
      busy={bulkBusy}
      onClear={() => setSelectedIds(new Set())}
      clearLabel={t("rulesRegistry.clearSelection")}
    >
      {perms.canApproveRules && selectedRules.some((r) => r.status === "pending_approval") && (
        <Button
          size="sm"
          variant="outline"
          className="gap-1 h-7 text-xs text-emerald-600"
          onClick={() => setBulkApproveOpen(true)}
        >
          <CheckCircle2 className="h-3 w-3" />
          {t("rulesRegistry.bulkApprove")}
        </Button>
      )}
      {perms.canApproveRules && selectedRules.some((r) => r.status === "pending_approval") && (
        <Button
          size="sm"
          variant="outline"
          className="gap-1 h-7 text-xs text-destructive"
          onClick={() => setBulkRejectOpen(true)}
        >
          <XCircle className="h-3 w-3" />
          {t("rulesRegistry.bulkReject")}
        </Button>
      )}
      {perms.canApproveRules && selectedRules.some((r) => r.status === "approved") && (
        <Button
          size="sm"
          variant="outline"
          className="gap-1 h-7 text-xs"
          onClick={() => setBulkDeprecateOpen(true)}
        >
          <Archive className="h-3 w-3" />
          {t("rulesRegistry.bulkDeprecate")}
        </Button>
      )}
      {selectedRules.some((r) => canRevokeRule(r)) && (
        <Button
          size="sm"
          variant="outline"
          className="gap-1 h-7 text-xs text-amber-600"
          onClick={() => setBulkRevokeOpen(true)}
        >
          <Undo2 className="h-3 w-3" />
          {t("rulesRegistry.bulkRevoke")}
        </Button>
      )}
      {/* Export moved off the always-on header into this selection action
          bar — it exports exactly the ticked rows (rule_id[] filter). */}
      <Button
        size="sm"
        variant="outline"
        className="gap-1 h-7 text-xs"
        onClick={() => setExportOpen(true)}
      >
        <FileDown className="h-3 w-3" />
        {t("exportYaml.button")}
      </Button>
      {selectedRules.some((r) => canDeleteRule(r)) && (
        <Button
          size="sm"
          variant="outline"
          className="gap-1 h-7 text-xs text-destructive"
          onClick={() => setBulkDeleteOpen(true)}
        >
          <Trash2 className="h-3 w-3" />
          {t("rulesRegistry.bulkDelete")}
        </Button>
      )}
    </BulkActionBar>
  );

  const filterControls = (
    <>
      <div className="relative w-56">
        <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
        <Input
          placeholder={t("rulesRegistry.searchPlaceholder")}
          value={nameSearch}
          onChange={(e) => applyFilter(setNameSearch)(e.target.value)}
          className="h-8 text-xs pl-7"
        />
      </div>
      <Select value={dimensionFilter} onValueChange={applyFilter(setDimensionFilter)}>
        <SelectTrigger className={FILTER_CLASS}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL} className="text-xs">{t("rulesRegistry.allDimensions")}</SelectItem>
          {dimensionValues.map((v) => (
            <SelectItem key={v} value={v} className="text-xs">
              <span className="flex items-center gap-1.5">
                <ColorDot color={colorFor(labelDefinitions as LabelColorDefinition[], RESERVED_DIMENSION_KEY, v)} />
                {v}
              </span>
            </SelectItem>
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
            <SelectItem key={v} value={v} className="text-xs">
              <span className="flex items-center gap-1.5">
                <ColorDot color={colorFor(labelDefinitions as LabelColorDefinition[], RESERVED_SEVERITY_KEY, v)} />
                {v}
              </span>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <SearchableSelect
        value={stewardFilter}
        onChange={applyFilter(setStewardFilter)}
        options={stewardValues.map((v) => ({ value: v, label: v }))}
        allValue={ALL}
        allLabel={t("rulesRegistry.allOwners")}
        searchPlaceholder={t("common.search")}
        emptyText={t("common.noMatches")}
        ariaLabel={t("rulesRegistry.stewardPlaceholder")}
      />
      <LabelFilter
        available={availableLabels}
        selected={labelFilter}
        onChange={(next) => {
          setLabelFilter(next);
          setPage(1);
        }}
        className={FILTER_CLASS}
      />
      {hasActiveFilters && (
        <Button
          variant="ghost"
          size="sm"
          className="h-8 text-xs"
          onClick={() => {
            setDimensionFilter(ALL);
            setSeverityFilter(ALL);
            setStewardFilter(ALL);
            setLabelFilter(new Map());
            setNameSearch("");
            setPage(1);
          }}
        >
          {t("common.clearFilters")}
        </Button>
      )}
    </>
  );

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("rulesRegistry.title")} />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("rulesRegistry.title")}</h1>
            <p className="text-sm text-muted-foreground mt-1">{t("rulesRegistry.description")}</p>
          </div>
          <div className="flex items-center gap-2">
            {/* Export lives in the selection action bar (exports the ticked
                rows), not this always-on header. */}
            {perms.canCreateRules && (
              <>
                <Button
                  variant="outline"
                  onClick={() => navigate({ to: "/registry-rules/import" })}
                  className="gap-2"
                >
                  <Upload className="h-4 w-4" />
                  {t("rulesRegistry.importRules")}
                </Button>
                <Button onClick={() => navigate({ to: "/registry-rules/new" })} className="gap-2">
                  <Plus className="h-4 w-4" />
                  {t("rulesRegistry.newRule")}
                </Button>
              </>
            )}
          </div>
        </div>

        {isError ? (
          <RegistryRulesError resetErrorBoundary={() => void refetch()} />
        ) : isPending ? (
          <Skeleton className="h-64 w-full" />
        ) : (
          <>
            {/* ExportDialog lives outside BulkActionBar so it stays mounted even
                when selection drops to 0 (BulkActionBar returns null when count≤0,
                which would abruptly unmount a dialog opened while deselecting). */}
            <ExportDialog
              open={exportOpen}
              onOpenChange={setExportOpen}
              fetchDqx={() => exportRegistryRules({ rule_id: [...selectedIds] })}
            />
            <div className="relative">
              {bulkToolbar}
              <RulesTable
              rows={pagedRules}
              labelDefinitions={labelDefinitions}
              sortKey={sortKey}
              sortDir={sortDir}
              onHeaderClick={handleHeaderClick}
              onRowClick={openRule}
              renderActions={renderActionsCell}
              toolbarExtra={filterControls}
              selection={tableSelection}
              emptyMessage={
                <div className="flex flex-col items-center justify-center text-center">
                  {/* h-12 w-12: an integer 2× of lucide's 24px grid renders
                      crisp 4px strokes; 40px (h-10) is a fractional 5/3 scale
                      whose 3.33px strokes anti-alias soft (P23 item 18). */}
                  <Library className="h-12 w-12 text-muted-foreground/30 mb-3" />
                  <p className="text-sm text-muted-foreground">
                    {hasActiveFilters
                      ? t("rulesRegistry.emptyState")
                      : perms.canCreateRules
                        ? t("rulesRegistry.emptyStateNoRulesCta")
                        : t("rulesRegistry.emptyStateNoRules")}
                  </p>
                </div>
              }
              />
            </div>
            <Pagination page={page} totalItems={rules.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
          </>
        )}
      </div>

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

      <AlertDialog open={bulkApproveOpen} onOpenChange={setBulkApproveOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("rulesRegistry.bulkApproveTitle", {
                count: selectedRules.filter((r) => r.status === "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.bulkApproveBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkApprove}>{t("rulesRegistry.actionApprove")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkRejectOpen} onOpenChange={setBulkRejectOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("rulesRegistry.bulkRejectTitle", {
                count: selectedRules.filter((r) => r.status === "pending_approval").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.bulkRejectBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmBulkReject}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesRegistry.actionReject")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkDeprecateOpen} onOpenChange={setBulkDeprecateOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("rulesRegistry.bulkDeprecateTitle", {
                count: selectedRules.filter((r) => r.status === "approved").length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.bulkDeprecateBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkDeprecate}>{t("rulesRegistry.actionDeprecate")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkRevokeOpen} onOpenChange={setBulkRevokeOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("rulesRegistry.bulkRevokeTitle", {
                count: selectedRules.filter((r) => canRevokeRule(r)).length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.bulkRevokeBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkRevoke}>{t("rulesRegistry.actionRevoke")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkDeleteOpen} onOpenChange={setBulkDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t("rulesRegistry.bulkDeleteTitle", {
                count: selectedRules.filter((r) => canDeleteRule(r)).length,
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.bulkDeleteBody")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmBulkDelete}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesRegistry.actionDelete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <RegistryRuleDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />
    </FadeIn>
  );
}
