import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { parseFqn, formatDateShort as formatDate, getUserMetadata, labelToken } from "@/lib/format-utils";
import { LabelFilter, LabelsBadges, labelsMatchFilter } from "@/components/Labels";
import { useTranslation } from "react-i18next";

const SQL_CHECK_PREFIX = "__sql_check__/";
const CROSS_TABLE_CATALOG = "Cross-table rules";

function isCrossTableRule(rule: { table_fqn: string; checks: unknown[] }): boolean {
  if (rule.table_fqn.startsWith(SQL_CHECK_PREFIX)) return true;
  const check = rule.checks[0] as Record<string, unknown> | undefined;
  return (check?.check as Record<string, unknown>)?.function === "sql_query";
}
import React, { useState, Suspense, useMemo, useCallback, useSyncExternalStore } from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useQueryClient } from "@tanstack/react-query";
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
  AlertTriangle,
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronDown,
  ChevronUp,
  ClipboardCheck,
  ExternalLink,
  Plus,
  Trash2,
  SendHorizonal,
  CheckCircle2,
  XCircle,
  Clock,
  FileEdit,
  User,
  Undo2,
  Loader2,
} from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { FadeIn } from "@/components/anim/FadeIn";
import { toast } from "sonner";
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
  useListRules,
  getListRulesQueryKey,
  type RuleCatalogEntryOut,
  type User as UserType,
} from "@/lib/api";
import {
  submitRuleForApproval,
  approveRule,
  rejectRule,
  revokeRule,
  deleteRuleById,
  backfillRuleIds,
} from "@/lib/api-custom";
import { usePermissions } from "@/hooks/use-permissions";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";

function describeCheck(rule: RuleCatalogEntryOut): string {
  const check = rule.checks[0] as Record<string, unknown> | undefined;
  if (!check) return "—";
  const checkObj = (check.check as Record<string, unknown>) ?? check;
  const args = (checkObj.arguments as Record<string, unknown>) ?? {};
  const fn = String(checkObj.function ?? "");
  const col = String(args.column ?? (Array.isArray(args.columns) ? (args.columns as string[]).join(", ") : args.columns) ?? "");
  if (fn === "sql_query") return "SQL check";
  return col ? `${fn}(${col})` : fn || "—";
}


export const Route = createFileRoute("/_sidebar/rules/drafts")({
  component: () => (
    <Suspense fallback={<DraftsSkeleton />}>
      <DraftsPage />
    </Suspense>
  ),
});

const _pendingSet = new Set<string>();
let _pendingVersion = 0;
const _listeners = new Set<() => void>();
function _getSnapshot() { return _pendingVersion; }
function _subscribe(cb: () => void) { _listeners.add(cb); return () => _listeners.delete(cb); }
function _markBusy(fqn: string) { _pendingSet.add(fqn); _pendingVersion++; _listeners.forEach((l) => l()); }
function _clearBusy(fqn: string) { _pendingSet.delete(fqn); _pendingVersion++; _listeners.forEach((l) => l()); }
function usePendingActions() {
  useSyncExternalStore(_subscribe, _getSnapshot);
  return _pendingSet;
}

function DraftsSkeleton() {
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

function useStatusOptions() {
  const { t } = useTranslation();
  return [
    { value: "all", label: t("rulesDrafts.allStatuses") },
    { value: "draft", label: t("rulesDrafts.statusDraft") },
    { value: "pending_approval", label: t("rulesDrafts.statusPendingApproval") },
    { value: "rejected", label: t("rulesDrafts.statusRejected") },
  ];
}

function StatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "draft":
      return (
        <Badge variant="secondary" className="gap-1">
          <FileEdit className="h-3 w-3" />
          {t("discovery.draft")}
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3" />
          {t("discovery.pending")}
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          {t("discovery.rejected")}
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status}</Badge>;
  }
}

function getCheckDetails(rule: RuleCatalogEntryOut): {
  fn: string;
  args: Record<string, unknown>;
  criticality: string;
  /** Labels stored in the check's ``user_metadata`` (includes ``weight``). */
  labels: Record<string, string>;
} {
  const check = rule.checks?.[0] as Record<string, unknown> | undefined;
  if (!check) return { fn: "—", args: {}, criticality: "—", labels: {} };
  const checkObj = (check.check as Record<string, unknown>) ?? check;
  // Surface labels from user_metadata; fold any legacy top-level numeric
  // weight into the labels map so existing drafts render consistently.
  const labels = getUserMetadata(check);
  if (typeof check.weight === "number" && !("weight" in labels)) {
    labels.weight = String(check.weight);
  }
  return {
    fn: String(checkObj.function ?? "—"),
    args: (checkObj.arguments as Record<string, unknown>) ?? {},
    criticality: String(check.criticality ?? checkObj.criticality ?? "—"),
    labels,
  };
}

type SortKey = "table" | "check" | "status" | "created_by" | "modified";
type SortDir = "asc" | "desc";

function SortableHeader({
  label,
  sortKey,
  active,
  direction,
  onSort,
}: {
  label: string;
  sortKey: SortKey;
  active: boolean;
  direction: SortDir;
  onSort: (key: SortKey) => void;
}) {
  const Icon = active ? (direction === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      className="flex items-center gap-1 hover:text-foreground transition-colors"
      onClick={() => onSort(sortKey)}
    >
      {label}
      <Icon className={`h-3 w-3 ${active ? "text-foreground" : "text-muted-foreground/50"}`} />
    </button>
  );
}

function DraftsPage() {
  const { t } = useTranslation();
  const STATUS_OPTIONS = useStatusOptions();
  const navigate = useNavigate();
  const [statusFilter, setStatusFilter] = useState("all");
  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [mySubmissionsOnly, setMySubmissionsOnly] = useState(false);
  const [labelFilter, setLabelFilter] = useState<Set<string>>(new Set());
  const [sort, setSort] = useState<{ key: SortKey; dir: SortDir }>({ key: "modified", dir: "desc" });
  const sortKey = sort.key;
  const sortDir = sort.dir;
  const handleSort = useCallback((key: SortKey) => {
    setSort((prev) => {
      if (prev.key === key) {
        return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
      }
      return { key, dir: key === "modified" ? "desc" : "asc" };
    });
  }, []);
  const [expandedRules, setExpandedRules] = useState<Set<string>>(new Set());
  const toggleExpand = (key: string) =>
    setExpandedRules((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  const pendingActions = usePendingActions();
  const { canCreateRules, canEditRules, canSubmitRules, canApproveRules } = usePermissions();
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  // Authors can only act on rules they themselves drafted; approvers and
  // admins (canApproveRules) can act on anyone's rule. Mirrors the backend
  // ownership check enforced by ``_ensure_owner_or_privileged``.
  const isOwnRule = useCallback(
    (rule: RuleCatalogEntryOut) => {
      if (canApproveRules) return true;
      if (!currentUserEmail) return false;
      return (rule.created_by ?? "").toLowerCase() === currentUserEmail.toLowerCase();
    },
    [canApproveRules, currentUserEmail],
  );

  const { data: rulesResp, isLoading, error } = useListRules(
    statusFilter === "all" ? {} : { status: statusFilter },
  );
  const allRulesRaw: RuleCatalogEntryOut[] = Array.isArray(rulesResp?.data) ? rulesResp.data : [];

  const allRules = useMemo(
    () => allRulesRaw.filter((r) => r.status !== "approved"),
    [allRulesRaw],
  );

  const orphanCount = useMemo(
    () => allRules.filter((r) => !r.rule_id).length,
    [allRules],
  );

  const duplicateInfo = useMemo(() => {
    const IDENTITY_ARGS = new Set([
      "column", "columns", "col_name",
      "expression", "msg", "query",
      "allowed", "forbidden",
      "limit", "min_limit", "max_limit",
      "regex", "date_format", "timestamp_format",
    ]);
    const checkSig = (c: Record<string, unknown>): string => {
      const inner = (c.check ?? c) as Record<string, unknown>;
      const fn = String(inner.function ?? "");
      const rawArgs = (inner.arguments ?? {}) as Record<string, unknown>;
      const idArgs: Record<string, unknown> = {};
      for (const k of [...Object.keys(rawArgs)].sort()) {
        if (IDENTITY_ARGS.has(k)) idArgs[k] = rawArgs[k];
      }
      return JSON.stringify({ arguments: idArgs, function: fn }).toLowerCase();
    };
    const info = new Map<string, string>();
    const sigMap = new Map<string, { ruleId: string; status: string }[]>();
    for (const r of allRulesRaw) {
      if (!r.rule_id) continue;
      if (r.status === "rejected") continue;
      const check = (r.checks?.[0] as Record<string, unknown>) ?? {};
      const sig = `${r.table_fqn}::${checkSig(check)}`;
      if (!sigMap.has(sig)) sigMap.set(sig, []);
      sigMap.get(sig)!.push({ ruleId: r.rule_id, status: r.status ?? "" });
    }
    for (const group of sigMap.values()) {
      if (group.length < 2) continue;
      for (const g of group) {
        const others = group.filter((o) => o.ruleId !== g.ruleId);
        if (others.length === 0) continue;
        const statuses = [...new Set(others.map((o) => o.status))];
        const label = statuses.map((s) => s.replace("_", " ")).join(", ");
        info.set(g.ruleId, label);
      }
    }
    return info;
  }, [allRulesRaw]);

  const { catalogs, schemasByCatalog } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    for (const rule of allRules) {
      if (isCrossTableRule(rule)) {
        catalogSet.add(CROSS_TABLE_CATALOG);
        continue;
      }
      const { catalog, schema } = parseFqn(rule.table_fqn);
      if (catalog) {
        catalogSet.add(catalog);
        if (!schemaMap.has(catalog)) schemaMap.set(catalog, new Set());
        if (schema) schemaMap.get(catalog)!.add(schema);
      }
    }
    return {
      catalogs: Array.from(catalogSet).sort(),
      schemasByCatalog: Object.fromEntries(
        Array.from(schemaMap.entries()).map(([cat, schemas]) => [cat, Array.from(schemas).sort()]),
      ),
    };
  }, [allRules]);

  const rules = useMemo(() => {
    const filtered = allRules.filter((rule) => {
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
      if (mySubmissionsOnly && currentUserEmail) {
        const submitter = rule.updated_by ?? rule.created_by ?? "";
        if (submitter !== currentUserEmail) return false;
      }
      if (labelFilter.size > 0) {
        const matched = rule.checks.some((c) =>
          labelsMatchFilter(getUserMetadata(c as Record<string, unknown>), labelFilter),
        );
        if (!matched) return false;
      }
      return true;
    });

    const statusOrder: Record<string, number> = {
      pending_approval: 0,
      draft: 1,
      rejected: 2,
    };
    const dir = sortDir === "asc" ? 1 : -1;
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case "table":
          cmp = (a.display_name || a.table_fqn).localeCompare(b.display_name || b.table_fqn);
          break;
        case "check": {
          const aCheck = describeCheck(a);
          const bCheck = describeCheck(b);
          cmp = aCheck.localeCompare(bCheck);
          break;
        }
        case "status":
          cmp = (statusOrder[a.status ?? ""] ?? 9) - (statusOrder[b.status ?? ""] ?? 9);
          break;
        case "created_by":
          cmp = (a.updated_by ?? a.created_by ?? "").localeCompare(b.updated_by ?? b.created_by ?? "");
          break;
        case "modified":
          cmp = (a.updated_at ?? a.created_at ?? "").localeCompare(b.updated_at ?? b.created_at ?? "");
          break;
      }
      return cmp * dir;
    });
  }, [allRules, catalogFilter, schemaFilter, mySubmissionsOnly, currentUserEmail, sortKey, sortDir, labelFilter]);

  // Distinct labels seen across all draft rules — used to populate the
  // LabelFilter dropdown.
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

  const availableSchemas = catalogFilter !== "all" ? schemasByCatalog[catalogFilter] || [] : [];

  const handleCatalogChange = (value: string) => {
    setCatalogFilter(value);
    setSchemaFilter("all");
  };

  const queryClient = useQueryClient();
  const invalidateRules = useCallback(
    () => queryClient.invalidateQueries({ queryKey: getListRulesQueryKey() }),
    [queryClient],
  );

  const [repairing, setRepairing] = useState(false);
  const handleRepair = useCallback(async () => {
    setRepairing(true);
    try {
      const resp = await backfillRuleIds();
      toast.success(t("rulesDrafts.repairedToast", { count: resp.data.repaired }));
      invalidateRules();
    } catch {
      toast.error(t("rulesDrafts.failedRepairIds"));
    } finally {
      setRepairing(false);
    }
  }, [invalidateRules, t]);

  const fireAction = useCallback(
    (
      fqn: string,
      action: () => Promise<unknown>,
      successMsg: string,
      errorMsg: string,
    ) => {
      if (_pendingSet.has(fqn)) return;
      _markBusy(fqn);
      action()
        .then(() => {
          toast.success(successMsg);
          invalidateRules();
        })
        .catch((err) => {
          const detail = (err as { body?: { detail?: string } })?.body?.detail ?? "";
          toast.error(detail || errorMsg, { duration: 6000 });
        })
        .finally(() => _clearBusy(fqn));
    },
    [invalidateRules],
  );

  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkBusy, setBulkBusy] = useState(false);

  const toggleSelect = (id: string) =>
    setSelectedIds((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  // Authors can only bulk-act on rules they themselves drafted; approvers
  // and admins (canApproveRules → isOwnRule returns true for everyone) get
  // the previous behaviour. Mirrors the per-row gate further down.
  const selectableRules = useMemo(
    () => rules.filter((r) => r.rule_id && isOwnRule(r)),
    [rules, isOwnRule],
  );

  const toggleSelectAll = () => {
    if (selectedIds.size === selectableRules.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(selectableRules.map((r) => r.rule_id!)));
    }
  };

  const selectedRules = useMemo(
    () => rules.filter((r) => r.rule_id && selectedIds.has(r.rule_id)),
    [rules, selectedIds],
  );

  const bulkAction = useCallback(
    async (
      action: (ruleId: string) => Promise<unknown>,
      successMsg: string,
      errorMsg: string,
    ) => {
      if (bulkBusy || selectedRules.length === 0) return;
      // Defense in depth: even if state somehow contains rules the user
      // doesn't own (manual selectedIds tampering, stale data after a
      // role change, etc.), refuse to act on them. The backend would 403
      // anyway, but filtering here gives a clear, actionable message and
      // avoids partial successes that confuse the operator.
      const ownRules = selectedRules.filter((r) => isOwnRule(r));
      const skippedNotOwned = selectedRules.length - ownRules.length;
      if (ownRules.length === 0) {
        toast.error(
          t("rulesDrafts.bulkCannotAct", { action: errorMsg.toLowerCase() }),
          { duration: 6000 },
        );
        setSelectedIds(new Set());
        return;
      }

      setBulkBusy(true);
      let ok = 0;
      let fail = 0;
      let lastDetail = "";
      for (const rule of ownRules) {
        try {
          await action(rule.rule_id!);
          ok++;
        } catch (err) {
          fail++;
          const detail = (err as { body?: { detail?: string } })?.body?.detail ?? "";
          if (detail) lastDetail = detail;
        }
      }
      setBulkBusy(false);
      setSelectedIds(new Set());
      invalidateRules();
      const skippedSuffix = skippedNotOwned > 0
        ? t("rulesDrafts.bulkSkippedSuffix", { count: skippedNotOwned })
        : "";
      if (fail === 0) {
        toast.success(t("rulesDrafts.bulkSucceededWithSkipped", { count: ok, msg: successMsg, ok, skipped: skippedSuffix }));
      } else {
        const reason = lastDetail ? ` — ${lastDetail}` : "";
        toast.warning(t("rulesDrafts.bulkPartial", { ok, fail, reason, skipped: skippedSuffix }));
      }
    },
    [bulkBusy, selectedRules, invalidateRules, isOwnRule, t],
  );

  const [bulkApproveOpen, setBulkApproveOpen] = useState(false);
  const [bulkRejectOpen, setBulkRejectOpen] = useState(false);
  const handleBulkApprove = () => setBulkApproveOpen(true);
  const confirmBulkApprove = () => {
    setBulkApproveOpen(false);
    bulkAction(approveRule, t("rulesDrafts.bulkApproved"), t("rulesDrafts.bulkApproveAction"));
  };
  const handleBulkReject = () => setBulkRejectOpen(true);
  const confirmBulkReject = () => {
    setBulkRejectOpen(false);
    bulkAction(rejectRule, t("rulesDrafts.bulkRejected"), t("rulesDrafts.bulkRejectAction"));
  };
  const [bulkRevokeOpen, setBulkRevokeOpen] = useState(false);
  const handleBulkRevoke = () => setBulkRevokeOpen(true);
  const confirmBulkRevoke = () => {
    setBulkRevokeOpen(false);
    bulkAction(revokeRule, t("rulesDrafts.bulkRevoked"), t("rulesDrafts.bulkRevokeAction"));
  };
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);
  const handleBulkDelete = () => {
    setBulkDeleteOpen(true);
  };
  const confirmBulkDelete = () => {
    setBulkDeleteOpen(false);
    bulkAction(deleteRuleById, t("rulesDrafts.bulkDeleted"), t("rulesDrafts.bulkDeleteAction"));
  };
  const [bulkSubmitOpen, setBulkSubmitOpen] = useState(false);
  const bulkSubmitEligible = useMemo(
    () =>
      selectedRules.filter(
        // Authors can only submit rules they authored. Filter at the
        // memoised eligible-list so the confirm dialog count and the
        // post-action toast both reflect this.
        (r) => r.status === "draft" && r.rule_id && !duplicateInfo.has(r.rule_id) && isOwnRule(r),
      ),
    [selectedRules, duplicateInfo, isOwnRule],
  );
  const handleBulkSubmit = useCallback(() => {
    if (bulkSubmitEligible.length === 0) {
      toast.warning(
        t("rulesDrafts.bulkAllIneligible", { count: selectedRules.length }),
      );
      return;
    }
    setBulkSubmitOpen(true);
  }, [bulkSubmitEligible, selectedRules, t]);
  const confirmBulkSubmit = useCallback(async () => {
    setBulkSubmitOpen(false);
    const skippedNotOwned = selectedRules.filter((r) => !isOwnRule(r)).length;
    const skippedDuplicate =
      selectedRules.length - bulkSubmitEligible.length - skippedNotOwned;
    setBulkBusy(true);
    let ok = 0;
    let fail = 0;
    let lastDetail = "";
    for (const rule of bulkSubmitEligible) {
      try {
        await submitRuleForApproval(rule.rule_id!);
        ok++;
      } catch (err) {
        fail++;
        const detail = (err as { body?: { detail?: string } })?.body?.detail ?? "";
        if (detail) lastDetail = detail;
      }
    }
    setBulkBusy(false);
    setSelectedIds(new Set());
    invalidateRules();
    const parts: string[] = [];
    if (ok > 0) parts.push(t("rulesDrafts.bulkSubmitted", { count: ok }));
    if (fail > 0) parts.push(lastDetail ? t("rulesDrafts.bulkFailedDetail", { count: fail, detail: lastDetail }) : t("rulesDrafts.bulkFailed", { count: fail }));
    if (skippedDuplicate > 0) parts.push(t("rulesDrafts.bulkSkippedDuplicate", { count: skippedDuplicate }));
    if (skippedNotOwned > 0) parts.push(t("rulesDrafts.bulkSkippedNotOwned", { count: skippedNotOwned }));
    if (fail === 0) toast.success(parts.join(", "));
    else toast.warning(parts.join(", "));
  }, [selectedRules, bulkSubmitEligible, invalidateRules, isOwnRule, t]);

  const ruleKey = (rule: RuleCatalogEntryOut) => rule.rule_id ?? rule.table_fqn;

  const handleRevoke = (rule: RuleCatalogEntryOut) =>
    fireAction(
      ruleKey(rule),
      () => revokeRule(rule.rule_id!),
      t("rulesDrafts.toastRevoked"),
      t("rulesDrafts.toastFailedRevoke"),
    );

  const [singleDeleteTarget, setSingleDeleteTarget] = useState<RuleCatalogEntryOut | null>(null);
  const handleDelete = (rule: RuleCatalogEntryOut) => {
    const key = ruleKey(rule);
    if (_pendingSet.has(key)) return;
    setSingleDeleteTarget(rule);
  };
  const confirmSingleDelete = () => {
    if (!singleDeleteTarget) return;
    const rule = singleDeleteTarget;
    setSingleDeleteTarget(null);
    fireAction(
      ruleKey(rule),
      () => deleteRuleById(rule.rule_id!),
      t("rulesDrafts.toastRuleDeleted"),
      t("rulesDrafts.toastFailedDelete"),
    );
  };

  const handleSubmit = (rule: RuleCatalogEntryOut) =>
    fireAction(
      ruleKey(rule),
      () => submitRuleForApproval(rule.rule_id!),
      t("rulesDrafts.toastSubmitted"),
      t("rulesDrafts.toastFailedSubmit"),
    );

  const handleApprove = (rule: RuleCatalogEntryOut) =>
    fireAction(
      ruleKey(rule),
      () => approveRule(rule.rule_id!),
      t("rulesDrafts.toastApproved"),
      t("rulesDrafts.toastFailedApprove"),
    );

  const handleReject = (rule: RuleCatalogEntryOut) =>
    fireAction(
      ruleKey(rule),
      () => rejectRule(rule.rule_id!),
      t("rulesDrafts.toastRejected"),
      t("rulesDrafts.toastFailedReject"),
    );

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[]} page={t("rulesDrafts.breadcrumb")} />
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">{t("rulesDrafts.title")}</h1>
            <p className="text-muted-foreground">
              {t("rulesDrafts.subtitle")}
            </p>
          </div>
          {canCreateRules && (
            <Button onClick={() => navigate({ to: "/rules/create" })} className="gap-2">
              <Plus className="h-4 w-4" />
              {t("rulesDrafts.createRules")}
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
                  <ClipboardCheck className="h-5 w-5" />
                  {t("rulesDrafts.ruleSets")}
                </CardTitle>
                <CardDescription>
                  {isLoading
                    ? t("common.loading")
                    : `${t("rulesDrafts.rulesCount", { count: rules.length })}${
                        rules.length !== allRules.length ? t("rulesDrafts.filteredFrom", { total: allRules.length }) : ""
                      }`}
                </CardDescription>
              </div>
            </div>

            <div className="flex items-center gap-2 flex-wrap">
              <Select value={catalogFilter} onValueChange={handleCatalogChange}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder={t("rulesDrafts.allCatalogs")} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t("rulesDrafts.allCatalogs")}</SelectItem>
                  {catalogs.map((cat) => (
                    <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={schemaFilter} onValueChange={setSchemaFilter} disabled={catalogFilter === "all"}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder={t("rulesDrafts.allSchemas")} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t("rulesDrafts.allSchemas")}</SelectItem>
                  {availableSchemas.map((sch) => (
                    <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={statusFilter} onValueChange={setStatusFilter}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATUS_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Button
                variant={mySubmissionsOnly ? "default" : "outline"}
                size="sm"
                className="h-9 gap-1.5 text-xs"
                onClick={() => setMySubmissionsOnly((prev) => !prev)}
              >
                <User className="h-3.5 w-3.5" />
                {t("rulesDrafts.mySubmissions")}
              </Button>

              <LabelFilter
                available={availableLabels}
                selected={labelFilter}
                onChange={setLabelFilter}
              />

              {(catalogFilter !== "all" || schemaFilter !== "all" || statusFilter !== "all" || mySubmissionsOnly || labelFilter.size > 0) && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-9 text-xs"
                  onClick={() => {
                    setCatalogFilter("all");
                    setSchemaFilter("all");
                    setStatusFilter("all");
                    setMySubmissionsOnly(false);
                    setLabelFilter(new Set());
                  }}
                >
                  {t("common.clearFilters")}
                </Button>
              )}
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {orphanCount > 0 && !isLoading && (
            <div className="flex items-center gap-3 mb-4 p-3 rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-700">
              <AlertTriangle className="h-4 w-4 text-amber-600 shrink-0" />
              <span className="text-sm text-amber-800 dark:text-amber-300">
                {t("rulesDrafts.orphanWarning", { count: orphanCount })}
              </span>
              <Button
                size="sm"
                variant="outline"
                className="ml-auto gap-1.5 h-7 text-xs border-amber-400 text-amber-700 hover:bg-amber-100 dark:text-amber-300 dark:hover:bg-amber-900"
                disabled={repairing}
                onClick={handleRepair}
              >
                {repairing ? <Loader2 className="h-3 w-3 animate-spin" /> : <AlertTriangle className="h-3 w-3" />}
                {repairing ? t("rulesDrafts.repairing") : t("rulesDrafts.repairIds")}
              </Button>
            </div>
          )}

          {isLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} className="h-14 w-full" />)}
            </div>
          )}

          {error && (
            <p className="text-destructive text-sm">{t("rulesDrafts.failedLoadRules", { error: (error as Error).message })}</p>
          )}

          {!isLoading && !error && rules.length > 0 && (
            <FadeIn duration={0.3}>
              {selectedIds.size > 0 && (
                <div className="flex items-center gap-2 mb-3 p-2.5 rounded-lg bg-muted/60 border">
                  <span className="text-sm font-medium mr-1">
                    {t("rulesDrafts.selectedCount", { count: selectedIds.size })}
                  </span>
                  {bulkBusy && <Loader2 className="h-4 w-4 animate-spin" />}
                  {!bulkBusy && (
                    <>
                      {canSubmitRules && selectedRules.some((r) => r.status === "draft") && (
                        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs" onClick={handleBulkSubmit}>
                          <SendHorizonal className="h-3 w-3" /> {t("rulesDrafts.submit")}
                        </Button>
                      )}
                      {canApproveRules && selectedRules.some((r) => r.status === "pending_approval") && (
                        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-green-600" onClick={handleBulkApprove}>
                          <CheckCircle2 className="h-3 w-3" /> {t("rulesDrafts.approve")}
                        </Button>
                      )}
                      {canApproveRules && selectedRules.some((r) => r.status === "pending_approval") && (
                        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-red-600" onClick={handleBulkReject}>
                          <XCircle className="h-3 w-3" /> {t("rulesDrafts.reject")}
                        </Button>
                      )}
                      {selectedRules.some((r) => r.status === "pending_approval" || r.status === "rejected") && (canApproveRules || selectedRules.some((r) => r.status === "pending_approval" && (r.updated_by ?? r.created_by) === currentUserEmail)) && (
                        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-amber-600" onClick={handleBulkRevoke}>
                          <Undo2 className="h-3 w-3" /> {t("rulesDrafts.revoke")}
                        </Button>
                      )}
                      {canEditRules && (
                        <Button size="sm" variant="outline" className="gap-1 h-7 text-xs text-destructive" onClick={handleBulkDelete}>
                          <Trash2 className="h-3 w-3" /> {t("rulesDrafts.delete")}
                        </Button>
                      )}
                      <Button size="sm" variant="ghost" className="h-7 text-xs ml-auto" onClick={() => setSelectedIds(new Set())}>
                        {t("rulesDrafts.clearSelection")}
                      </Button>
                    </>
                  )}
                </div>
              )}
              <div className="border rounded-lg overflow-x-auto">
                <table className="w-full text-sm" style={{ minWidth: "1100px" }}>
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="p-3 w-10">
                        <Checkbox
                          checked={selectableRules.length > 0 && selectedIds.size === selectableRules.length}
                          onCheckedChange={toggleSelectAll}
                          aria-label={t("rulesDrafts.selectAllAria")}
                        />
                      </th>
                      <th className="text-left p-3 font-medium whitespace-nowrap">
                        <SortableHeader label={t("rulesDrafts.headerTable")} sortKey="table" active={sortKey === "table"} direction={sortDir} onSort={handleSort} />
                      </th>
                      <th className="text-left p-3 font-medium whitespace-nowrap">
                        <SortableHeader label={t("rulesDrafts.headerCheck")} sortKey="check" active={sortKey === "check"} direction={sortDir} onSort={handleSort} />
                      </th>
                      <th className="text-left p-3 font-medium whitespace-nowrap">{t("rulesDrafts.headerLabels")}</th>
                      <th className="text-left p-3 font-medium whitespace-nowrap">
                        <SortableHeader label={t("rulesDrafts.headerStatus")} sortKey="status" active={sortKey === "status"} direction={sortDir} onSort={handleSort} />
                      </th>
                      <th className="text-left p-3 font-medium whitespace-nowrap">
                        <SortableHeader label={t("rulesDrafts.headerCreatedBy")} sortKey="created_by" active={sortKey === "created_by"} direction={sortDir} onSort={handleSort} />
                      </th>
                      <th className="text-left p-3 font-medium whitespace-nowrap">
                        <SortableHeader label={t("rulesDrafts.headerModified")} sortKey="modified" active={sortKey === "modified"} direction={sortDir} onSort={handleSort} />
                      </th>
                      <th className="text-right p-3 font-medium whitespace-nowrap sticky right-0 bg-muted/50">{t("rulesDrafts.headerActions")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rules.map((rule) => {
                      const key = rule.rule_id ?? rule.table_fqn;
                      const busy = pendingActions.has(key);
                      const isDuplicate = rule.rule_id ? duplicateInfo.has(rule.rule_id) : false;
                      const dupLabel = rule.rule_id ? duplicateInfo.get(rule.rule_id) : undefined;
                      const isExpanded = expandedRules.has(key);
                      const details = getCheckDetails(rule);
                      return (
                      <React.Fragment key={key}>
                      <tr
                        className={`border-b last:border-b-0 hover:bg-muted/30 transition-colors cursor-pointer ${isDuplicate ? "bg-amber-50/50" : ""}`}
                        onClick={() => toggleExpand(key)}
                      >
                        <td className="p-3 w-10" onClick={(e) => e.stopPropagation()}>
                          {rule.rule_id && isOwnRule(rule) && (
                            <Checkbox
                              checked={selectedIds.has(rule.rule_id)}
                              onCheckedChange={() => toggleSelect(rule.rule_id!)}
                              aria-label={t("rulesDrafts.selectRowAria", { name: rule.display_name || rule.table_fqn })}
                            />
                          )}
                        </td>
                        <td className="p-3 font-mono text-xs">
                          <span className="flex items-center gap-1.5">
                            {isExpanded ? <ChevronUp className="h-3.5 w-3.5 shrink-0 text-muted-foreground" /> : <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
                            {rule.display_name || rule.table_fqn}
                          </span>
                        </td>
                        <td className="p-3 text-xs font-mono text-muted-foreground">
                          <span className="flex items-center gap-1.5">
                            {describeCheck(rule)}
                            {isDuplicate && (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <AlertTriangle className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    <p>{t("rulesDrafts.duplicateTooltip", { label: dupLabel ?? t("rulesDrafts.duplicateAnotherRule") })}</p>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                          </span>
                        </td>
                        <td className="p-3">
                          {Object.keys(details.labels).length === 0 ? (
                            <span className="text-[10px] italic text-muted-foreground/60">—</span>
                          ) : (
                            <LabelsBadges labels={details.labels} max={3} size="sm" />
                          )}
                        </td>
                        <td className="p-3"><StatusBadge status={rule.status} /></td>
                        <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={rule.updated_by ?? rule.created_by ?? ""}>
                          {rule.updated_by ?? rule.created_by ?? "—"}
                        </td>
                        <td className="p-3 text-muted-foreground text-xs whitespace-nowrap" title={rule.updated_at ?? rule.created_at ?? ""}>
                          {formatDate(rule.updated_at ?? rule.created_at)}
                        </td>
                        <td className="p-3 text-right sticky right-0 bg-background">
                          <div
                            className="flex items-center justify-end gap-1"
                            onClick={(e) => e.stopPropagation()}
                          >
                            {!rule.rule_id && (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <Badge variant="outline" className="gap-1 text-[10px] border-amber-400 text-amber-600">
                                      <AlertTriangle className="h-2.5 w-2.5" />
                                      {t("rulesDrafts.noIdBadge")}
                                    </Badge>
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    <p>{t("rulesDrafts.noIdTooltip")}</p>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                            {rule.status === "draft" && canSubmitRules && rule.rule_id && isOwnRule(rule) && (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <span>
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        disabled={busy || isDuplicate}
                                        onClick={() => handleSubmit(rule)}
                                        className="gap-1 h-7 text-xs"
                                      >
                                        <SendHorizonal className="h-3 w-3 shrink-0" />
                                        {busy ? t("rulesDrafts.submitting") : t("rulesDrafts.submit")}
                                      </Button>
                                    </span>
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    {isDuplicate
                                      ? t("rulesDrafts.cannotSubmitDuplicate", { label: dupLabel ?? t("rulesDrafts.duplicateAnotherRule") })
                                      : t("rulesDrafts.submitTooltip")}
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                            {rule.status === "pending_approval" && canApproveRules && rule.rule_id && (
                              <>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  disabled={busy}
                                  onClick={() => handleApprove(rule)}
                                  className="gap-1 h-7 text-xs text-green-600"
                                >
                                  <CheckCircle2 className="h-3 w-3" />
                                  {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.approveAction")}
                                </Button>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  disabled={busy}
                                  onClick={() => handleReject(rule)}
                                  className="gap-1 h-7 text-xs text-red-600"
                                >
                                  <XCircle className="h-3 w-3" />
                                  {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.rejectAction")}
                                </Button>
                              </>
                            )}
                            {rule.status === "pending_approval" && rule.rule_id && (
                              canApproveRules ||
                              (rule.updated_by ?? rule.created_by) === currentUserEmail
                            ) && (
                              <Button
                                size="sm"
                                variant="ghost"
                                disabled={busy}
                                onClick={() => handleRevoke(rule)}
                                className="h-7 text-xs text-amber-600"
                                title={t("rulesDrafts.revokeTitle")}
                              >
                                <Undo2 className="h-3 w-3" />
                              </Button>
                            )}
                            {rule.status === "rejected" && canApproveRules && rule.rule_id && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={busy}
                                onClick={() => handleRevoke(rule)}
                                className="gap-1 h-7 text-xs text-amber-600"
                                title={t("rulesDrafts.unrejectTitle")}
                              >
                                <Undo2 className="h-3 w-3" />
                                {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.unreject")}
                              </Button>
                            )}
                            {(rule.status === "draft" || rule.status === "rejected") &&
                              canEditRules &&
                              rule.rule_id &&
                              isOwnRule(rule) && (
                                <TooltipProvider>
                                  <Tooltip>
                                    <TooltipTrigger asChild>
                                      <Button
                                        size="sm"
                                        variant="ghost"
                                        disabled={busy}
                                        onClick={() =>
                                          isCrossTableRule(rule)
                                            ? navigate({
                                                to: "/rules/create-sql",
                                                search: { edit: rule.table_fqn, from: "drafts" },
                                              })
                                            : navigate({
                                                to: "/rules/single-table",
                                                search: { table: rule.table_fqn, rule_id: rule.rule_id!, from: "drafts" },
                                              })
                                        }
                                        className="h-7 text-xs"
                                      >
                                        <FileEdit className="h-3 w-3" />
                                      </Button>
                                    </TooltipTrigger>
                                    <TooltipContent>{t("rulesDrafts.editRuleTooltip")}</TooltipContent>
                                  </Tooltip>
                                </TooltipProvider>
                              )}
                            {rule.status === "pending_approval" &&
                              canEditRules &&
                              rule.rule_id &&
                              isOwnRule(rule) && (
                                <TooltipProvider>
                                  <Tooltip>
                                    <TooltipTrigger asChild>
                                      <span>
                                        <Button
                                          size="sm"
                                          variant="ghost"
                                          disabled
                                          className="h-7 text-xs opacity-40"
                                        >
                                          <FileEdit className="h-3 w-3" />
                                        </Button>
                                      </span>
                                    </TooltipTrigger>
                                    <TooltipContent>{t("rulesDrafts.revokeFirstToEdit")}</TooltipContent>
                                  </Tooltip>
                                </TooltipProvider>
                              )}
                            {canEditRules && rule.rule_id && isOwnRule(rule) && (
                              <Button
                                size="sm"
                                variant="ghost"
                                disabled={busy}
                                onClick={() => handleDelete(rule)}
                                className="h-7 text-xs text-destructive"
                              >
                                <Trash2 className="h-3 w-3" />
                              </Button>
                            )}
                          </div>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr className="bg-muted/20">
                          <td colSpan={8} className="p-0">
                            <div className="px-6 py-4 space-y-3 border-l-4 border-primary/20">
                              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs">
                                <div>
                                  <span className="text-muted-foreground font-medium block mb-0.5">{t("rulesDrafts.function")}</span>
                                  <span className="font-mono">{details.fn}</span>
                                </div>
                                <div>
                                  <span className="text-muted-foreground font-medium block mb-0.5">{t("rulesDrafts.criticality")}</span>
                                  <Badge variant={details.criticality === "error" ? "destructive" : "secondary"} className="text-[10px]">
                                    {details.criticality}
                                  </Badge>
                                </div>
                                <div>
                                  <span className="text-muted-foreground font-medium block mb-0.5">{t("rulesDrafts.source")}</span>
                                  <span>{rule.source ?? "—"}</span>
                                </div>
                              </div>
                              {Object.keys(details.args).length > 0 && (
                                <div>
                                  <span className="text-muted-foreground font-medium text-xs block mb-1">{t("rulesDrafts.arguments")}</span>
                                  <div className="flex flex-wrap gap-2">
                                    {Object.entries(details.args).map(([k, v]) => (
                                      <span key={k} className="inline-flex items-center gap-1 bg-muted rounded px-2 py-0.5 text-xs font-mono">
                                        <span className="text-muted-foreground">{k}:</span>{" "}
                                        {Array.isArray(v) ? v.join(", ") : String(v)}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              <div className="flex items-center gap-2 pt-1">
                                {(rule.status === "draft" || rule.status === "rejected") && canEditRules && rule.rule_id && isOwnRule(rule) && (
                                  <Button
                                    size="sm"
                                    variant="outline"
                                    className="gap-1.5 h-7 text-xs"
                                    onClick={() =>
                                      isCrossTableRule(rule)
                                        ? navigate({ to: "/rules/create-sql", search: { edit: rule.table_fqn, from: "drafts" } })
                                        : navigate({ to: "/rules/single-table", search: { table: rule.table_fqn, rule_id: rule.rule_id!, from: "drafts" } })
                                    }
                                  >
                                    <ExternalLink className="h-3 w-3" />
                                    {t("rulesDrafts.editRule")}
                                  </Button>
                                )}
                                {!isOwnRule(rule) && (
                                  <span className="text-[11px] text-muted-foreground italic">
                                    {t("rulesDrafts.authoredBy", { user: rule.created_by ?? t("rulesDrafts.anotherUser") })}
                                  </span>
                                )}
                              </div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                    );
                    })}
                  </tbody>
                </table>
              </div>
            </FadeIn>
          )}

          {!isLoading && !error && rules.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-center">
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
                <ClipboardCheck className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-muted-foreground">{t("rulesDrafts.noDraftsTitle")}</h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                {canCreateRules
                  ? t("rulesDrafts.noDraftsAuthor")
                  : t("rulesDrafts.noDraftsViewer")}
              </p>
              {canCreateRules && (
                <Button onClick={() => navigate({ to: "/rules/create" })} className="mt-4 gap-2">
                  <Plus className="h-4 w-4" />
                  {t("rulesDrafts.createRules")}
                </Button>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <AlertDialog open={!!singleDeleteTarget} onOpenChange={(open) => !open && setSingleDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.deleteTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.deleteConfirmPrefix")}
              <span className="font-medium text-foreground">
                {singleDeleteTarget?.display_name || singleDeleteTarget?.table_fqn}
              </span>
              {t("rulesDrafts.deleteConfirmSuffix")}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmSingleDelete}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesDrafts.delete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkRevokeOpen} onOpenChange={setBulkRevokeOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.revokeBulkTitle", { count: selectedRules.length })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.revokeBulkBody", { count: selectedRules.length })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkRevoke}>
              {t("rulesDrafts.revoke")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkSubmitOpen} onOpenChange={setBulkSubmitOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.submitBulkTitle", { count: bulkSubmitEligible.length })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.submitBulkBody", { eligible: bulkSubmitEligible.length, selected: selectedRules.length })}
              {selectedRules.length - bulkSubmitEligible.length > 0 &&
                t("rulesDrafts.submitBulkSkipped", { count: selectedRules.length - bulkSubmitEligible.length })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkSubmit}>
              {t("rulesDrafts.submit")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkApproveOpen} onOpenChange={setBulkApproveOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.approveBulkTitle", { count: selectedRules.length })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.approveBulkBody", { count: selectedRules.length })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction onClick={confirmBulkApprove}>
              {t("rulesDrafts.approve")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkRejectOpen} onOpenChange={setBulkRejectOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.rejectBulkTitle", { count: selectedRules.length })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.rejectBulkBody", { count: selectedRules.length })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmBulkReject}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesDrafts.reject")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={bulkDeleteOpen} onOpenChange={setBulkDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.deleteBulkTitle", { count: selectedRules.length })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.deleteBulkBody", { count: selectedRules.length })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmBulkDelete}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesDrafts.delete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
