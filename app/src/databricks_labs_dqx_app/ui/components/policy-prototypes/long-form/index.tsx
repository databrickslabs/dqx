// THROWAWAY prototype — variant A "Long form": Policies tab list + faithful ABAC single-page form.
// Mock data only (../mock.ts). Not production code; hardcoded English is intentional.
import { useState, type ReactNode } from "react";
import { Pencil, Plus, RefreshCw, ShieldCheck, Trash2, User } from "lucide-react";
import { toast } from "sonner";
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
import { CURRENT_USER, MOCK_POLICIES, emptyDraft, mockPreview, type PolicyDraft } from "../mock";
import { ActionBadge, ruleById } from "./primitives";
import { PolicyForm, toastSaved } from "./policy-form";

function scopeSummary(scope: PolicyDraft["scope"]): string {
  const byCatalog = new Map<string, string[]>();
  for (const s of scope.filter((x) => x.catalog)) {
    byCatalog.set(s.catalog, [...(byCatalog.get(s.catalog) ?? []), s.schema]);
  }
  return [...byCatalog.entries()]
    .map(([cat, schemas]) => `${cat} · ${schemas.includes("*") ? "all schemas" : schemas.join(", ")}`)
    .join("; ");
}

function OwnerGate({ allowed, children }: { allowed: boolean; children: ReactNode }) {
  if (allowed) return <>{children}</>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-not-allowed">{children}</span>
      </TooltipTrigger>
      <TooltipContent>Only the policy owner or an admin can edit</TooltipContent>
    </Tooltip>
  );
}

function PolicyRow({
  policy,
  currentRuleId,
  onEdit,
  onDelete,
}: {
  policy: PolicyDraft;
  currentRuleId: string;
  onEdit: () => void;
  onDelete: () => void;
}) {
  const canEdit = policy.owner === CURRENT_USER;
  const matched = mockPreview(policy).matched_monitored;

  const rescan = () => {
    const p = mockPreview(policy);
    toast.promise(new Promise((resolve) => setTimeout(resolve, 900)), {
      loading: `Re-scanning "${policy.name}"…`,
      success: `Re-scan finished · ${p.matched_monitored} tables matched, ${p.attachments} ${policy.action === "suggest" ? "suggestions" : "attachments"}`,
    });
  };

  return (
    <div className="flex flex-col gap-3 px-4 py-4 md:flex-row md:items-start md:justify-between">
      <div className="min-w-0 space-y-1.5">
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-medium">{policy.name}</span>
          <ActionBadge action={policy.action} />
        </div>
        {policy.description && <p className="text-sm text-muted-foreground">{policy.description}</p>}
        <div className="flex flex-wrap items-center gap-1.5 pt-0.5">
          {policy.rule_ids.map((id) => (
            <span
              key={id}
              className={
                id === currentRuleId
                  ? "rounded-md border border-primary/30 bg-primary/10 px-1.5 py-0.5 text-[11px] font-medium text-primary"
                  : "rounded-md border bg-muted/40 px-1.5 py-0.5 text-[11px] text-muted-foreground"
              }
            >
              {ruleById(id)?.name ?? id}
            </span>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
          <span className="font-mono">{scopeSummary(policy.scope)}</span>
          <span className="inline-flex items-center gap-1">
            <User className="h-3 w-3" />
            {policy.owner}
          </span>
          <span>
            <span className="font-medium text-foreground tabular-nums">{matched}</span> {matched === 1 ? "table" : "tables"}{" "}
            matched
          </span>
        </div>
      </div>
      <div className="flex shrink-0 items-center gap-1">
        <Button variant="ghost" size="sm" className="gap-1.5" onClick={rescan}>
          <RefreshCw className="h-3.5 w-3.5" /> Re-scan
        </Button>
        <OwnerGate allowed={canEdit}>
          <Button variant="ghost" size="sm" className="gap-1.5" onClick={onEdit} disabled={!canEdit}>
            <Pencil className="h-3.5 w-3.5" /> Edit
          </Button>
        </OwnerGate>
        <OwnerGate allowed={canEdit}>
          <Button
            variant="ghost"
            size="sm"
            className="gap-1.5 text-destructive hover:text-destructive"
            onClick={onDelete}
            disabled={!canEdit}
          >
            <Trash2 className="h-3.5 w-3.5" /> Delete
          </Button>
        </OwnerGate>
      </div>
    </div>
  );
}

export function Prototype({ ruleId }: { ruleId: string }) {
  const [policies, setPolicies] = useState<PolicyDraft[]>(() => MOCK_POLICIES.map((p) => structuredClone(p)));
  const [editing, setEditing] = useState<{ draft: PolicyDraft; isEdit: boolean } | null>(null);
  const [pendingDelete, setPendingDelete] = useState<PolicyDraft | null>(null);

  const forRule = policies.filter((p) => p.rule_ids.includes(ruleId));

  if (editing) {
    const takenNames = policies
      .filter((p) => p.policy_id !== editing.draft.policy_id)
      .map((p) => p.name.trim().toLowerCase());
    return (
      <PolicyForm
        initial={editing.draft}
        isEdit={editing.isEdit}
        takenNames={takenNames}
        onCancel={() => setEditing(null)}
        onSave={(draft) => {
          const saved: PolicyDraft = { ...draft, policy_id: draft.policy_id ?? `p-${Date.now()}` };
          setPolicies((prev) =>
            editing.isEdit ? prev.map((p) => (p.policy_id === saved.policy_id ? saved : p)) : [...prev, saved],
          );
          toastSaved(saved, editing.isEdit, mockPreview(saved).matched_monitored);
          setEditing(null);
        }}
      />
    );
  }

  const startNew = () => setEditing({ draft: emptyDraft([ruleId]), isEdit: false });

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold">Policies</h2>
          <p className="text-sm text-muted-foreground">Policies that attach this rule to matching tables and columns.</p>
        </div>
        {forRule.length > 0 && (
          <Button onClick={startNew} className="gap-1.5">
            <Plus className="h-4 w-4" /> New policy
          </Button>
        )}
      </div>

      {forRule.length === 0 ? (
        <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed px-6 py-14 text-center">
          <div className="rounded-full bg-primary/10 p-3">
            <ShieldCheck className="h-6 w-6 text-primary" />
          </div>
          <div className="space-y-1">
            <div className="font-medium">No policies use this rule</div>
            <p className="max-w-md text-sm text-muted-foreground">
              A policy attaches approved rules to every table and column that matches its scope and conditions.
            </p>
          </div>
          <Button onClick={startNew} className="gap-1.5">
            <Plus className="h-4 w-4" /> New policy
          </Button>
        </div>
      ) : (
        <div className="divide-y rounded-lg border bg-card">
          {forRule.map((p) => (
            <PolicyRow
              key={p.policy_id}
              policy={p}
              currentRuleId={ruleId}
              onEdit={() => setEditing({ draft: structuredClone(p), isEdit: true })}
              onDelete={() => setPendingDelete(p)}
            />
          ))}
        </div>
      )}

      <AlertDialog open={!!pendingDelete} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{pendingDelete?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              Rules already attached by this policy stay on their tables. No new matches will be made.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={() => {
                if (!pendingDelete) return;
                setPolicies((prev) => prev.filter((p) => p.policy_id !== pendingDelete.policy_id));
                toast.success(`Deleted "${pendingDelete.name}"`);
                setPendingDelete(null);
              }}
            >
              Delete policy
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
