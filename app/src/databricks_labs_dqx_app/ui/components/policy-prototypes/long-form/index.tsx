// THROWAWAY prototype — variant A "Long form": Policies tab list + faithful ABAC single-page form.
// A policy is 1:1 with a rule. Mock data only (./model.ts). Not production code; hardcoded English is intentional.
import { useState, type KeyboardEvent, type MouseEvent, type ReactNode } from "react";
import { Plus, RefreshCw, ShieldCheck, Trash2 } from "lucide-react";
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
import { cn } from "@/lib/utils";
import { MOCK_POLICIES, canModifyRule, copyForRule, emptyPolicy, mockPreview, scopeSummary, type Policy } from "./model";
import { ActionBadge } from "./primitives";
import { PolicyForm } from "./policy-form";

function OwnerGate({ allowed, children }: { allowed: boolean; children: ReactNode }) {
  if (allowed) return <>{children}</>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-not-allowed">{children}</span>
      </TooltipTrigger>
      <TooltipContent>You need Modify on this rule to change its policies</TooltipContent>
    </Tooltip>
  );
}

function PolicyCard({ policy, onOpen, onDelete }: { policy: Policy; onOpen: () => void; onDelete: () => void }) {
  const canEdit = canModifyRule(policy.rule_id);
  const matched = mockPreview(policy).matched_monitored;

  const rescan = (e: MouseEvent) => {
    e.stopPropagation();
    const p = mockPreview(policy);
    toast.promise(new Promise((resolve) => setTimeout(resolve, 900)), {
      loading: `Re-scanning "${policy.name}"…`,
      success: `Re-scan finished · ${p.matched_monitored} tables matched, ${p.attachments} ${policy.action === "suggest" ? "suggestions" : "attachments"}`,
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
      aria-label={`Open policy ${policy.name}`}
      className={cn(
        "flex cursor-pointer flex-col gap-3 px-4 py-4 transition-colors md:flex-row md:items-start md:justify-between",
        "hover:bg-muted/40 focus-visible:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:ring-inset",
      )}
    >
      <div className="min-w-0 space-y-1.5">
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-medium">{policy.name}</span>
          <ActionBadge action={policy.action} />
        </div>
        {policy.description && <p className="text-sm text-muted-foreground">{policy.description}</p>}
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
          <span className="font-mono">{scopeSummary(policy.scope)}</span>
          <span>
            <span className="font-medium text-foreground tabular-nums">{matched}</span> {matched === 1 ? "table" : "tables"}{" "}
            matched
          </span>
        </div>
      </div>
      <div className="flex shrink-0 items-center gap-1" onClick={(e) => e.stopPropagation()}>
        <Button variant="ghost" size="sm" className="h-7 gap-1.5 px-2 text-xs" onClick={rescan}>
          <RefreshCw className="h-3.5 w-3.5" /> Re-scan
        </Button>
        <OwnerGate allowed={canEdit}>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 px-2 text-xs text-destructive hover:text-destructive"
            onClick={(e) => {
              e.stopPropagation();
              onDelete();
            }}
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
  const [policies, setPolicies] = useState<Policy[]>(() => MOCK_POLICIES.map((p) => structuredClone(p)));
  const [editing, setEditing] = useState<{ policy: Policy; isEdit: boolean } | null>(null);
  const [pendingDelete, setPendingDelete] = useState<Policy | null>(null);

  const forRule = policies.filter((p) => p.rule_id === ruleId);

  if (editing) {
    // Names are unique per rule.
    const takenNames = forRule
      .filter((p) => p.policy_id !== editing.policy.policy_id)
      .map((p) => p.name.trim().toLowerCase());
    return (
      <PolicyForm
        initial={editing.policy}
        isEdit={editing.isEdit}
        readOnly={!canModifyRule(editing.policy.rule_id)}
        takenNames={takenNames}
        onCancel={() => setEditing(null)}
        onSave={(policy, alsoFor) => {
          const stamp = Date.now();
          const saved: Policy = { ...policy, policy_id: policy.policy_id ?? `p-${stamp}` };
          if (editing.isEdit) {
            setPolicies((prev) => prev.map((p) => (p.policy_id === saved.policy_id ? saved : p)));
            toast.success(`Saved "${saved.name}"`);
          } else {
            const copies = alsoFor.map((rid, i) => ({ ...copyForRule(saved, rid), policy_id: `p-${stamp}-${i}` }));
            setPolicies((prev) => [...prev, saved, ...copies]);
            const matched = mockPreview(saved).matched_monitored;
            toast.success(copies.length > 0 ? `Created ${copies.length + 1} policies` : `Created "${saved.name}"`, {
              description: `Evaluating ${matched} matching ${matched === 1 ? "table" : "tables"} in the background.`,
            });
          }
          setEditing(null);
        }}
      />
    );
  }

  const startNew = () => setEditing({ policy: emptyPolicy(ruleId), isEdit: false });

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
            <div className="font-medium">No policies for this rule</div>
            <p className="max-w-md text-sm text-muted-foreground">
              A policy attaches this rule to every table and column that matches its scope and conditions.
            </p>
          </div>
          <Button onClick={startNew} className="gap-1.5">
            <Plus className="h-4 w-4" /> New policy
          </Button>
        </div>
      ) : (
        <div className="divide-y overflow-hidden rounded-lg border bg-card">
          {forRule.map((p) => (
            <PolicyCard
              key={p.policy_id}
              policy={p}
              onOpen={() => setEditing({ policy: structuredClone(p), isEdit: true })}
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
