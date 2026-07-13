import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { formatDateShort as formatDate, getUserMetadata } from "@/lib/format-utils";
import { SeverityBadge } from "@/components/RegistryRuleBadges";
import { useTranslation } from "react-i18next";
import { useState, Suspense, useMemo, useCallback } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useQueryClient } from "@tanstack/react-query";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  AlertCircle,
  RotateCcw,
  CheckCircle2,
  XCircle,
  ShieldCheck,
  Boxes,
  Table2,
  GitCompare,
} from "lucide-react";
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
  useListRegistryRules,
  useApproveRegistryRule,
  useRejectRegistryRule,
  useListDataProducts,
  useApproveDataProduct,
  useRejectDataProduct,
  getListDataProductsQueryKey,
  getGetDataProductQueryKey,
  useListMonitoredTables,
  useApproveMonitoredTable,
  useRejectMonitoredTable,
  type RegistryRuleOut,
  type DataProductOut,
  type MonitoredTableSummaryOut,
  type User as UserType,
} from "@/lib/api";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import {
  ApprovalQueueCard,
  ApprovalTh,
  ReadOnlyActionHint,
} from "@/components/drafts/ApprovalQueueCard";
import {
  RegistryRuleDiffDialog,
  MonitoredTableDiffDialog,
  TableSpaceDiffDialog,
  type RegistryDiffTarget,
  type MonitoredTableDiffTarget,
  type TableSpaceDiffTarget,
} from "@/components/drafts/ChangeDiffDialog";
import { invalidateAfterRegistryRuleApprovalChange } from "@/lib/registry-rule-invalidation";
import { usePermissions } from "@/hooks/use-permissions";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";

export const Route = createFileRoute("/_sidebar/rules/drafts")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={DraftsError}>
          <Suspense fallback={<DraftsSkeleton />}>
            <DraftsPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function DraftsError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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

/**
 * Registry rules awaiting approval — a second approval queue alongside the
 * legacy per-table drafts below. Registry rules (dq_rules) go through their
 * own `draft -> pending_approval -> approved` gate before they can ever be
 * applied to a table; this section is what makes Drafts & Review the single
 * place approvers check, instead of having to also visit Rules Registry.
 * See docs/superpowers/specs/2026-07-02-rules-registry-design.md §5.
 */
function RegistryApprovalsSection({
  canApproveRules,
  currentUserEmail,
}: {
  canApproveRules: boolean;
  currentUserEmail: string;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  const { data, isLoading, error } = useListRegistryRules({ status: "pending_approval" });
  const rules: RegistryRuleOut[] = useMemo(() => data?.data ?? [], [data]);

  // Approving/rejecting can re-materialize monitored tables the rule is
  // applied to server-side, so this invalidates the registry rules list
  // (which also feeds this very queue), the monitored-tables list, and
  // every monitored-table detail/versions query — see helper doc for why
  // we can't target specific binding IDs here.
  const invalidate = useCallback(
    () => invalidateAfterRegistryRuleApprovalChange(queryClient),
    [queryClient],
  );

  const approveMutation = useApproveRegistryRule();
  const rejectMutation = useRejectRegistryRule();
  const [pendingRuleId, setPendingRuleId] = useState<string | null>(null);
  const [rejectTarget, setRejectTarget] = useState<RegistryRuleOut | null>(null);

  const runAction = useCallback(
    (ruleId: string, mutate: () => Promise<unknown>, successMsg: string, errorMsg: string) => {
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

  const confirmApprove = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => approveMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesDrafts.registryToastApproved"),
      t("rulesDrafts.registryToastFailedApprove"),
    );
  const confirmReject = (rule: RegistryRuleOut) =>
    runAction(
      rule.rule_id,
      () => rejectMutation.mutateAsync({ ruleId: rule.rule_id }),
      t("rulesDrafts.registryToastRejected"),
      t("rulesDrafts.registryToastFailedReject"),
    );

  const [diffTarget, setDiffTarget] = useState<RegistryDiffTarget | null>(null);

  return (
    <>
      <ApprovalQueueCard
        icon={ShieldCheck}
        title={t("rulesDrafts.registrySectionTitle")}
        description={t("rulesDrafts.registrySectionDescription")}
        isLoading={isLoading}
        error={!!error}
        errorText={error ? t("rulesDrafts.registryFailedLoad", { error: (error as Error).message }) : ""}
        isEmpty={rules.length === 0}
        emptyText={t("rulesDrafts.registryEmptyState")}
        minWidth="900px"
        head={
          <>
            <ApprovalTh>{t("rulesDrafts.headerName")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerDimension")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerSeverity")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerSteward")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerVersion")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerModified")}</ApprovalTh>
            <ApprovalTh align="right">{t("rulesDrafts.headerActions")}</ApprovalTh>
          </>
        }
        rows={rules.map((rule) => {
          const md = getUserMetadata(rule as unknown as Record<string, unknown>);
          const name = md.name || rule.rule_id;
          const dimension = md.dimension;
          const severity = md.severity;
          const author = rule.updated_by ?? rule.created_by ?? rule.steward ?? "";
          const busy = pendingRuleId === rule.rule_id;
          return (
            <tr key={rule.rule_id} className="border-b last:border-b-0 hover:bg-muted/30 transition-colors">
              <td className="p-3 font-mono text-xs">{name}</td>
              <td className="p-3">
                {dimension ? (
                  <Badge variant="outline" className="text-[10px] font-normal">{dimension}</Badge>
                ) : (
                  <span className="text-muted-foreground/50 text-xs">—</span>
                )}
              </td>
              <td className="p-3">
                <SeverityBadge severity={severity ?? ""} />
              </td>
              <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={author}>
                {author || "—"}
              </td>
              <td className="p-3 text-xs text-muted-foreground font-mono">v{rule.version}</td>
              <td className="p-3 text-muted-foreground text-xs whitespace-nowrap" title={rule.updated_at ?? ""}>
                {formatDate(rule.updated_at)}
              </td>
              <td className="p-3 text-right">
                <div className="flex items-center justify-end gap-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() =>
                      setDiffTarget({
                        ruleId: rule.rule_id,
                        name,
                        version: rule.version,
                        definition: rule.definition,
                      })
                    }
                    className="gap-1 h-7 text-xs"
                  >
                    <GitCompare className="h-3 w-3" />
                    {t("rulesDrafts.diff.viewChanges")}
                  </Button>
                  {canApproveRules ? (
                    <>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => confirmApprove(rule)}
                        className="gap-1 h-7 text-xs text-green-600"
                      >
                        <CheckCircle2 className="h-3 w-3" />
                        {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.approveAction")}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => setRejectTarget(rule)}
                        className="gap-1 h-7 text-xs text-red-600"
                      >
                        <XCircle className="h-3 w-3" />
                        {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.rejectAction")}
                      </Button>
                    </>
                  ) : (
                    <ReadOnlyActionHint author={author} currentUserEmail={currentUserEmail} />
                  )}
                </div>
              </td>
            </tr>
          );
        })}
      />

      <RegistryRuleDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

      <AlertDialog open={!!rejectTarget} onOpenChange={(open) => !open && setRejectTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.registryRejectTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.registryRejectBody", {
                name: rejectTarget
                  ? getUserMetadata(rejectTarget as unknown as Record<string, unknown>).name || rejectTarget.rule_id
                  : "",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const target = rejectTarget;
                setRejectTarget(null);
                if (target) confirmReject(target);
              }}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesDrafts.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

/**
 * Monitored Tables awaiting approval (Stream J item 13c). A monitored-table
 * binding carries its own submit-for-review lifecycle
 * (draft -> pending_approval -> approved/rejected), so a pending binding
 * belongs in the same single approvals queue as registry rules and Table
 * Spaces. Driven by ``useListMonitoredTables`` filtered to the
 * ``pending_approval`` status; the change-diff popout reuses the two most
 * recent frozen version snapshots (see MonitoredTableDiffDialog).
 */
function MonitoredTablesApprovalsSection({
  canApproveRules,
  currentUserEmail,
}: {
  canApproveRules: boolean;
  currentUserEmail: string;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  const { data, isLoading, error } = useListMonitoredTables({ status: "pending_approval" });
  const tables: MonitoredTableSummaryOut[] = useMemo(() => data?.data ?? [], [data]);

  const approveMutation = useApproveMonitoredTable();
  const rejectMutation = useRejectMonitoredTable();
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [rejectTarget, setRejectTarget] = useState<MonitoredTableSummaryOut | null>(null);
  const [diffTarget, setDiffTarget] = useState<MonitoredTableDiffTarget | null>(null);

  const runAction = useCallback(
    (bindingId: string, mutate: () => Promise<unknown>, successMsg: string, errorMsg: string) => {
      if (pendingId) return;
      setPendingId(bindingId);
      mutate()
        .then(() => {
          toast.success(successMsg);
          invalidateAfterMonitoredTableChange(queryClient, bindingId);
        })
        .catch((err: unknown) => {
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          toast.error(detail || errorMsg, { duration: 6000 });
        })
        .finally(() => setPendingId(null));
    },
    [pendingId, queryClient],
  );

  const confirmApprove = (row: MonitoredTableSummaryOut) =>
    runAction(
      row.table.binding_id,
      () => approveMutation.mutateAsync({ bindingId: row.table.binding_id }),
      t("rulesDrafts.mtToastApproved"),
      t("rulesDrafts.mtToastFailedApprove"),
    );
  const confirmReject = (row: MonitoredTableSummaryOut) =>
    runAction(
      row.table.binding_id,
      () => rejectMutation.mutateAsync({ bindingId: row.table.binding_id }),
      t("rulesDrafts.mtToastRejected"),
      t("rulesDrafts.mtToastFailedReject"),
    );

  return (
    <>
      <ApprovalQueueCard
        icon={Table2}
        title={t("rulesDrafts.mtSectionTitle")}
        description={t("rulesDrafts.mtSectionDescription")}
        isLoading={isLoading}
        error={!!error}
        errorText={error ? t("rulesDrafts.mtFailedLoad", { error: (error as Error).message }) : ""}
        isEmpty={tables.length === 0}
        emptyText={t("rulesDrafts.mtEmptyState")}
        minWidth="800px"
        head={
          <>
            <ApprovalTh>{t("rulesDrafts.headerTable")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerSteward")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerVersion")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.mtHeaderChecks")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerModified")}</ApprovalTh>
            <ApprovalTh align="right">{t("rulesDrafts.headerActions")}</ApprovalTh>
          </>
        }
        rows={tables.map((row) => {
          const tbl = row.table;
          const busy = pendingId === tbl.binding_id;
          const author = tbl.steward ?? "";
          return (
            <tr key={tbl.binding_id} className="border-b last:border-b-0 hover:bg-muted/30 transition-colors">
              <td className="p-3 font-mono text-xs">{tbl.table_fqn}</td>
              <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={author}>
                {author || "—"}
              </td>
              <td className="p-3 text-xs text-muted-foreground font-mono">v{tbl.version ?? 0}</td>
              <td className="p-3 text-xs text-muted-foreground tabular-nums">{row.check_count ?? 0}</td>
              <td className="p-3 text-muted-foreground text-xs whitespace-nowrap" title={tbl.updated_at ?? ""}>
                {formatDate(tbl.updated_at)}
              </td>
              <td className="p-3 text-right">
                <div className="flex items-center justify-end gap-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() =>
                      setDiffTarget({
                        bindingId: tbl.binding_id,
                        name: tbl.table_fqn,
                        version: tbl.version ?? 0,
                      })
                    }
                    className="gap-1 h-7 text-xs"
                  >
                    <GitCompare className="h-3 w-3" />
                    {t("rulesDrafts.diff.viewChanges")}
                  </Button>
                  {canApproveRules ? (
                    <>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => confirmApprove(row)}
                        className="gap-1 h-7 text-xs text-green-600"
                      >
                        <CheckCircle2 className="h-3 w-3" />
                        {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.approveAction")}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => setRejectTarget(row)}
                        className="gap-1 h-7 text-xs text-red-600"
                      >
                        <XCircle className="h-3 w-3" />
                        {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.rejectAction")}
                      </Button>
                    </>
                  ) : (
                    <ReadOnlyActionHint author={author} currentUserEmail={currentUserEmail} />
                  )}
                </div>
              </td>
            </tr>
          );
        })}
      />

      <MonitoredTableDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

      <AlertDialog open={!!rejectTarget} onOpenChange={(open) => !open && setRejectTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulesDrafts.mtRejectTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("rulesDrafts.mtRejectBody", { name: rejectTarget?.table.table_fqn ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const target = rejectTarget;
                setRejectTarget(null);
                if (target) confirmReject(target);
              }}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesDrafts.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

/**
 * Table Spaces awaiting approval (P21 item 30). A Table Space carries its own
 * submit-for-review lifecycle (draft -> pending_approval -> approved/rejected),
 * so it belongs in the same single approvals queue as registry rules and
 * per-table drafts. Mirrors the registry-rules pending section's shape. This is
 * the SPACE's own lifecycle — distinct from the member-table "Review N pending
 * changes" affordance on the space header (P19-I), which is about member TABLES.
 */
function TableSpacesApprovalsSection({ canApproveRules }: { canApproveRules: boolean }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data, isLoading, error } = useListDataProducts();
  const pending: DataProductOut[] = useMemo(
    () => (data?.data ?? []).filter((p) => p.display_status === "pending_approval"),
    [data],
  );

  const approveMutation = useApproveDataProduct();
  const rejectMutation = useRejectDataProduct();
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [rejectTarget, setRejectTarget] = useState<DataProductOut | null>(null);
  const [diffTarget, setDiffTarget] = useState<TableSpaceDiffTarget | null>(null);

  const invalidate = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
  }, [queryClient]);

  const runAction = useCallback(
    (id: string, mutate: () => Promise<unknown>, successMsg: string, errorMsg: string) => {
      if (pendingId) return;
      setPendingId(id);
      mutate()
        .then(() => {
          toast.success(successMsg);
          queryClient.invalidateQueries({ queryKey: getGetDataProductQueryKey(id) });
          invalidate();
        })
        .catch((err: unknown) => {
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          toast.error(detail || errorMsg, { duration: 6000 });
        })
        .finally(() => setPendingId(null));
    },
    [pendingId, invalidate, queryClient],
  );

  const confirmApprove = (p: DataProductOut) =>
    runAction(
      p.product_id,
      () => approveMutation.mutateAsync({ productId: p.product_id }),
      t("dataProducts.toastApproved"),
      t("dataProducts.toastApproveFailed"),
    );
  const confirmReject = (p: DataProductOut) =>
    runAction(
      p.product_id,
      () => rejectMutation.mutateAsync({ productId: p.product_id }),
      t("dataProducts.toastRejected"),
      t("dataProducts.toastRejectFailed"),
    );

  return (
    <>
      <ApprovalQueueCard
        icon={Boxes}
        title={t("rulesDrafts.tableSpacesSectionTitle")}
        description={t("rulesDrafts.tableSpacesSectionDescription")}
        isLoading={isLoading}
        error={!!error}
        errorText={error ? t("rulesDrafts.tableSpacesFailedLoad", { error: (error as Error).message }) : ""}
        isEmpty={pending.length === 0}
        emptyText={t("rulesDrafts.tableSpacesEmptyState")}
        minWidth="760px"
        head={
          <>
            <ApprovalTh>{t("rulesDrafts.headerName")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerSteward")}</ApprovalTh>
            <ApprovalTh>{t("dataProducts.colTables")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerVersion")}</ApprovalTh>
            <ApprovalTh>{t("rulesDrafts.headerModified")}</ApprovalTh>
            <ApprovalTh align="right">{t("rulesDrafts.headerActions")}</ApprovalTh>
          </>
        }
        rows={pending.map((p) => {
          const busy = pendingId === p.product_id;
          return (
            <tr key={p.product_id} className="border-b last:border-b-0 hover:bg-muted/30 transition-colors">
              <td className="p-3">
                <button
                  type="button"
                  className="font-medium hover:underline text-left"
                  onClick={() =>
                    navigate({ to: "/table-spaces/$productId", params: { productId: p.product_id } })
                  }
                >
                  {p.name}
                </button>
              </td>
              <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={p.steward ?? ""}>
                {p.steward || "—"}
              </td>
              <td className="p-3 text-xs text-muted-foreground tabular-nums">{p.member_count ?? 0}</td>
              <td className="p-3 text-xs text-muted-foreground font-mono">v{p.version ?? 0}</td>
              <td className="p-3 text-muted-foreground text-xs whitespace-nowrap" title={p.updated_at ?? ""}>
                {formatDate(p.updated_at)}
              </td>
              <td className="p-3 text-right">
                <div className="flex items-center justify-end gap-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => setDiffTarget({ productId: p.product_id, name: p.name })}
                    className="gap-1 h-7 text-xs"
                  >
                    <GitCompare className="h-3 w-3" />
                    {t("rulesDrafts.diff.viewChanges")}
                  </Button>
                  {canApproveRules ? (
                    <>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => confirmApprove(p)}
                        className="gap-1 h-7 text-xs text-green-600"
                      >
                        <CheckCircle2 className="h-3 w-3" />
                        {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.approveAction")}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => setRejectTarget(p)}
                        className="gap-1 h-7 text-xs text-red-600"
                      >
                        <XCircle className="h-3 w-3" />
                        {busy ? t("rulesDrafts.ellipsis") : t("rulesDrafts.rejectAction")}
                      </Button>
                    </>
                  ) : (
                    <ReadOnlyActionHint />
                  )}
                </div>
              </td>
            </tr>
          );
        })}
      />

      <TableSpaceDiffDialog target={diffTarget} onClose={() => setDiffTarget(null)} />

      <AlertDialog open={!!rejectTarget} onOpenChange={(open) => !open && setRejectTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.rejectConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("dataProducts.rejectConfirmDescription", { name: rejectTarget?.name ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const target = rejectTarget;
                setRejectTarget(null);
                if (target) confirmReject(target);
              }}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {t("rulesDrafts.rejectAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function DraftsPage() {
  const { t } = useTranslation();
  const { canApproveRules } = usePermissions();
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  return (
    <FadeIn>
      <div className="space-y-6">
        <div className="space-y-2">
          <PageBreadcrumb items={[]} page={t("rulesDrafts.breadcrumb")} />
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("rulesDrafts.title")}</h1>
            <p className="text-sm text-muted-foreground mt-1">{t("rulesDrafts.subtitle")}</p>
          </div>
        </div>

        <RegistryApprovalsSection canApproveRules={canApproveRules} currentUserEmail={currentUserEmail} />

        <MonitoredTablesApprovalsSection canApproveRules={canApproveRules} currentUserEmail={currentUserEmail} />

        <TableSpacesApprovalsSection canApproveRules={canApproveRules} />
      </div>
    </FadeIn>
  );
}
