// Policies tab on the rule page: this rule's policies as clickable cards, with the
// policy form opened in place for a new or existing policy.
import { useState, type KeyboardEvent, type MouseEvent, type ReactNode } from "react";
import { Trans, useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Loader2, Plus, RefreshCw, RotateCcw, ShieldCheck, Trash2 } from "lucide-react";
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
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { LabelColorDefinition } from "@/components/RegistryRuleBadges";
import {
  getListRulePoliciesQueryKey,
  useDeleteRulePolicy,
  useListRulePolicies,
  useRescanRulePolicy,
  type RegistryRuleOut,
  type RulePolicyOut,
} from "@/lib/api";
import { extractApiError } from "@/lib/api-error";
import { cn } from "@/lib/utils";
import { PolicyForm } from "./PolicyForm";
import {
  alignColumnConditions,
  draftFromPolicy,
  emptyPolicyDraft,
  scopeSummary,
  type PolicyDraft,
} from "./policy-form-state";

function ModifyGate({ allowed, children }: { allowed: boolean; children: ReactNode }) {
  const { t } = useTranslation();
  if (allowed) return <>{children}</>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-not-allowed">{children}</span>
      </TooltipTrigger>
      <TooltipContent>{t("rulePolicies.needModify")}</TooltipContent>
    </Tooltip>
  );
}

function PolicyCard({
  policy,
  canModify,
  onOpen,
  onDelete,
}: {
  policy: RulePolicyOut;
  canModify: boolean;
  onOpen: () => void;
  onDelete: () => void;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const rescan = useRescanRulePolicy();
  const matched = policy.tables_matched ?? 0;

  const onRescan = (e: MouseEvent) => {
    e.stopPropagation();
    toast.promise(rescan.mutateAsync({ policyId: policy.policy_id }), {
      loading: t("rulePolicies.rescanning", { name: policy.name }),
      success: (res) => {
        void queryClient.invalidateQueries({ queryKey: getListRulePoliciesQueryKey() });
        const { attached, registered, registration_capped } = res.data;
        const details = [
          registered > 0 ? t("rulePolicies.rescanRegistered", { count: registered }) : null,
          registration_capped ? t("rulePolicies.rescanCapped") : null,
        ].filter((d): d is string => d !== null);
        return {
          message: t("rulePolicies.rescanDone", { count: attached }),
          description: details.length > 0 ? details.join(" ") : undefined,
        };
      },
      error: (err) => extractApiError(err, t("rulePolicies.rescanFailed")),
    });
  };

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.target !== e.currentTarget) return;
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      onOpen();
    }
  };

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={onOpen}
      onKeyDown={onKeyDown}
      aria-label={t("rulePolicies.openPolicy", { name: policy.name })}
      className={cn(
        "flex cursor-pointer flex-col gap-3 px-4 py-4 transition-colors md:flex-row md:items-start md:justify-between",
        "hover:bg-muted/40 focus-visible:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:ring-inset",
      )}
    >
      <div className="min-w-0 space-y-1.5">
        <div className="font-medium">{policy.name}</div>
        {policy.description && <p className="text-sm text-muted-foreground">{policy.description}</p>}
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
          <span className="font-mono">{scopeSummary(policy.scope, t)}</span>
          <span>
            <Trans
              i18nKey="rulePolicies.tablesMatched"
              count={matched}
              values={{ count: matched }}
              components={[<span key="n" className="font-medium text-foreground tabular-nums" />]}
            />
          </span>
        </div>
      </div>
      <div className="flex shrink-0 items-center gap-1" onClick={(e) => e.stopPropagation()}>
        <ModifyGate allowed={canModify}>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 px-2 text-xs"
            onClick={onRescan}
            disabled={!canModify || rescan.isPending}
          >
            <RefreshCw className={cn("h-3.5 w-3.5", rescan.isPending && "animate-spin")} /> {t("rulePolicies.rescan")}
          </Button>
        </ModifyGate>
        <ModifyGate allowed={canModify}>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 px-2 text-xs text-destructive hover:text-destructive"
            onClick={(e) => {
              e.stopPropagation();
              onDelete();
            }}
            disabled={!canModify}
          >
            <Trash2 className="h-3.5 w-3.5" /> {t("rulePolicies.delete")}
          </Button>
        </ModifyGate>
      </div>
    </div>
  );
}

export function PoliciesTab({
  rule,
  readOnly,
  labelDefinitions,
}: {
  rule: RegistryRuleOut;
  /** The caller can't modify this rule: policies open read-only and can't be added or changed. */
  readOnly: boolean;
  labelDefinitions: LabelColorDefinition[];
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const canModify = !readOnly;
  const [editing, setEditing] = useState<PolicyDraft | null>(null);
  const [pendingDelete, setPendingDelete] = useState<RulePolicyOut | null>(null);

  const listQuery = useListRulePolicies({ rule_id: rule.rule_id });
  const policies = listQuery.data?.data ?? [];
  const deleteMutation = useDeleteRulePolicy();

  if (editing) {
    const takenNames = policies.filter((p) => p.policy_id !== editing.policyId).map((p) => p.name);
    return (
      <PolicyForm
        key={editing.policyId ?? "new"}
        rule={rule}
        initial={editing}
        readOnly={readOnly}
        takenNames={takenNames}
        labelDefinitions={labelDefinitions}
        onClose={() => setEditing(null)}
      />
    );
  }

  const startNew = () => setEditing(emptyPolicyDraft(rule));
  const newButton = canModify ? (
    <Button onClick={startNew} className="gap-1.5">
      <Plus className="h-4 w-4" /> {t("rulePolicies.newPolicy")}
    </Button>
  ) : (
    <ModifyGate allowed={false}>
      <Button disabled className="gap-1.5">
        <Plus className="h-4 w-4" /> {t("rulePolicies.newPolicy")}
      </Button>
    </ModifyGate>
  );

  const confirmDelete = () => {
    const target = pendingDelete;
    if (!target) return;
    setPendingDelete(null);
    deleteMutation.mutate(
      { policyId: target.policy_id },
      {
        onSuccess: () => {
          toast.success(t("rulePolicies.deleted", { name: target.name }));
          void queryClient.invalidateQueries({ queryKey: getListRulePoliciesQueryKey() });
        },
        onError: (err) => toast.error(extractApiError(err, t("rulePolicies.deleteFailed"))),
      },
    );
  };

  return (
    <div className="space-y-4 pt-2">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold">{t("rulePolicies.listTitle")}</h2>
          <p className="text-sm text-muted-foreground">{t("rulePolicies.listDescription")}</p>
        </div>
        {policies.length > 0 && newButton}
      </div>

      {listQuery.isLoading ? (
        <div className="flex items-center gap-2 py-10 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> {t("common.loading")}
        </div>
      ) : listQuery.isError ? (
        <div className="flex items-center gap-2 rounded-lg border px-4 py-6 text-sm text-muted-foreground">
          {t("rulePolicies.loadFailed")}
          <Button variant="outline" size="sm" className="h-7 gap-1.5" onClick={() => void listQuery.refetch()}>
            <RotateCcw className="h-3 w-3" /> {t("common.retry")}
          </Button>
        </div>
      ) : policies.length === 0 ? (
        <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed px-6 py-14 text-center">
          <div className="rounded-full bg-primary/10 p-3">
            <ShieldCheck className="h-6 w-6 text-primary" />
          </div>
          <div className="space-y-1">
            <div className="font-medium">{t("rulePolicies.emptyTitle")}</div>
            <p className="max-w-md text-sm text-muted-foreground">{t("rulePolicies.emptyDescription")}</p>
          </div>
          {newButton}
        </div>
      ) : (
        <div className="divide-y overflow-hidden rounded-lg border bg-card">
          {policies.map((p) => (
            <PolicyCard
              key={p.policy_id}
              policy={p}
              canModify={canModify}
              onOpen={() => {
                const draft = draftFromPolicy(p);
                setEditing({ ...draft, columnConditions: alignColumnConditions(rule, draft.columnConditions) });
              }}
              onDelete={() => setPendingDelete(p)}
            />
          ))}
        </div>
      )}

      <AlertDialog open={!!pendingDelete} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rulePolicies.deleteTitle", { name: pendingDelete?.name ?? "" })}</AlertDialogTitle>
            <AlertDialogDescription>{t("rulePolicies.deleteDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction className="bg-destructive text-white hover:bg-destructive/90" onClick={confirmDelete}>
              {t("rulePolicies.deleteConfirm")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
