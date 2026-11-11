// THROWAWAY prototype — variant C "Split preview": the Policies tab as a
// master-detail workspace. List of policies, or (New/Edit) the form on the left
// and a sticky live-impact pane on the right. Mock data only, no API calls.
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { toast } from "sonner";
import { ArrowLeft, Pencil, Plus, RefreshCw, Trash2, Waypoints } from "lucide-react";
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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CURRENT_USER, MOCK_POLICIES, emptyDraft, mockPreview, type PolicyDraft, type PolicyPreview } from "../mock";
import { ImpactPane } from "./ImpactPane";
import { PolicyForm, type FormErrors } from "./PolicyForm";
import { ActionPill, previewKey, ruleById, scopeSummary } from "./shared";

const NOT_OWNER = "Only the policy owner or an admin can edit";

function OwnerGate({ allowed, children }: { allowed: boolean; children: ReactNode }) {
  if (allowed) return <>{children}</>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-not-allowed">{children}</span>
      </TooltipTrigger>
      <TooltipContent>{NOT_OWNER}</TooltipContent>
    </Tooltip>
  );
}

// ── List mode ────────────────────────────────────────────────────────────────

function PolicyList({
  policies,
  onNew,
  onEdit,
  onDelete,
}: {
  policies: PolicyDraft[];
  onNew: () => void;
  onEdit: (p: PolicyDraft) => void;
  onDelete: (p: PolicyDraft) => void;
}) {
  const [confirm, setConfirm] = useState<PolicyDraft | null>(null);
  const [scanning, setScanning] = useState<string | null>(null);
  const counts = useMemo(() => new Map(policies.map((p) => [p.policy_id, mockPreview(p).matched_monitored])), [policies]);

  const rescan = (p: PolicyDraft) => {
    setScanning(p.policy_id ?? null);
    setTimeout(() => {
      setScanning(null);
      const pv = mockPreview(p);
      toast.success(`Re-scanned “${p.name}”`, {
        description: `${pv.matched_monitored} tables matched · ${pv.attachments} ${p.action === "suggest" ? "suggestions" : "attachments"} · ${pv.ambiguous} ambiguous`,
      });
    }, 700);
  };

  if (policies.length === 0) {
    return (
      <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed bg-card px-6 py-14 text-center">
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-primary/10">
          <Waypoints className="h-5 w-5 text-primary" />
        </div>
        <div className="space-y-1">
          <div className="text-sm font-semibold">No policies include this rule</div>
          <p className="max-w-md text-sm text-muted-foreground">
            A policy attaches rules to every table and column that matches its scope and tag conditions.
          </p>
        </div>
        <Button size="sm" className="gap-1.5" onClick={onNew}>
          <Plus className="h-4 w-4" /> New policy
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-end justify-between gap-4">
        <div>
          <div className="text-sm font-semibold">Policies</div>
          <p className="text-xs text-muted-foreground">Policies that include this rule, and how many monitored tables they match.</p>
        </div>
        <Button size="sm" className="gap-1.5" onClick={onNew}>
          <Plus className="h-4 w-4" /> New policy
        </Button>
      </div>
      <div className="rounded-lg border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">Name</TableHead>
              <TableHead>Action</TableHead>
              <TableHead>Scope</TableHead>
              <TableHead className="text-right">Rules</TableHead>
              <TableHead>Owner</TableHead>
              <TableHead className="text-right">Tables matched</TableHead>
              <TableHead className="w-[1%] pr-4 text-right">
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {policies.map((p) => {
              const mine = p.owner === CURRENT_USER;
              const names = p.rule_ids.map((id) => ruleById(id)?.name ?? id);
              return (
                <TableRow key={p.policy_id}>
                  <TableCell className="max-w-[16rem] pl-4">
                    <div className="truncate font-medium">{p.name}</div>
                    {p.description && <div className="truncate text-xs text-muted-foreground">{p.description}</div>}
                  </TableCell>
                  <TableCell>
                    <ActionPill action={p.action} />
                  </TableCell>
                  <TableCell className="font-mono text-xs">{scopeSummary(p.scope)}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="cursor-default underline decoration-dotted underline-offset-4">{p.rule_ids.length}</span>
                      </TooltipTrigger>
                      <TooltipContent>{names.join(", ")}</TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">{mine ? "You" : p.owner}</TableCell>
                  <TableCell className="text-right tabular-nums">{counts.get(p.policy_id)}</TableCell>
                  <TableCell className="pr-4">
                    <div className="flex items-center justify-end gap-0.5">
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button variant="ghost" size="icon-sm" onClick={() => rescan(p)} disabled={scanning === p.policy_id} aria-label="Re-scan">
                            <RefreshCw className={scanning === p.policy_id ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Re-scan matching tables</TooltipContent>
                      </Tooltip>
                      <OwnerGate allowed={mine}>
                        <Button variant="ghost" size="icon-sm" disabled={!mine} onClick={() => onEdit(p)} aria-label="Edit">
                          <Pencil className="h-4 w-4" />
                        </Button>
                      </OwnerGate>
                      <OwnerGate allowed={mine}>
                        <Button variant="ghost" size="icon-sm" disabled={!mine} onClick={() => setConfirm(p)} aria-label="Delete">
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </OwnerGate>
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>

      <AlertDialog open={!!confirm} onOpenChange={(o) => !o && setConfirm(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete “{confirm?.name}”?</AlertDialogTitle>
            <AlertDialogDescription>
              Rules it already attached stay on their tables. No new matches will be attached or suggested.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={() => {
                if (confirm) onDelete(confirm);
                setConfirm(null);
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

// ── Edit mode ────────────────────────────────────────────────────────────────

function validate(d: PolicyDraft, others: PolicyDraft[]): FormErrors {
  const e: FormErrors = {};
  const name = d.name.trim();
  if (!name) e.name = "Name is required.";
  else if (others.some((p) => p.policy_id !== d.policy_id && p.name.trim().toLowerCase() === name.toLowerCase()))
    e.name = "A policy with this name already exists.";
  if (d.rule_ids.length === 0) e.rules = "Add at least one rule.";
  if (!d.scope.some((s) => s.catalog)) e.scope = "Pick at least one catalog.";
  return e;
}

/** Simulated debounced preview call (~300 ms). Keeps the previous result for deltas. */
function useLivePreview(draft: PolicyDraft) {
  const [preview, setPreview] = useState<PolicyPreview>(() => mockPreview(draft));
  const [prev, setPrev] = useState<PolicyPreview | null>(null);
  const [loading, setLoading] = useState(false);
  const lastKey = useRef(previewKey(draft));
  const current = useRef(preview);
  const key = previewKey(draft);

  useEffect(() => {
    if (key === lastKey.current) return;
    lastKey.current = key;
    setLoading(true);
    const h = setTimeout(() => {
      const next = mockPreview(draft);
      if (JSON.stringify(next) !== JSON.stringify(current.current)) {
        setPrev(current.current);
        current.current = next;
        setPreview(next);
      }
      setLoading(false);
    }, 300);
    return () => clearTimeout(h);
    // draft is captured via key; key covers every preview-relevant field
  }, [key]);

  return { preview, prev, loading };
}

function PolicyEditor({
  initial,
  others,
  onCancel,
  onSave,
}: {
  initial: PolicyDraft;
  others: PolicyDraft[];
  onCancel: () => void;
  onSave: (d: PolicyDraft) => void;
}) {
  const [draft, setDraft] = useState<PolicyDraft>(initial);
  const [submitted, setSubmitted] = useState(false);
  const { preview, prev, loading } = useLivePreview(draft);
  const isNew = !initial.policy_id;
  const allErrors = validate(draft, others);
  const errors = submitted ? allErrors : {};
  const errorCount = Object.keys(allErrors).length;

  const save = () => {
    setSubmitted(true);
    if (errorCount > 0) return;
    onSave(draft);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" className="-ml-2 gap-1.5 text-muted-foreground" onClick={onCancel}>
          <ArrowLeft className="h-4 w-4" /> Policies
        </Button>
        <span className="text-muted-foreground">/</span>
        <h2 className="truncate text-base font-semibold">{isNew ? "New policy" : draft.name || initial.name}</h2>
        {!isNew && <ActionPill action={draft.action} />}
      </div>

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,58fr)_minmax(0,42fr)]">
        {/* LEFT: form + sticky footer */}
        <div className="min-w-0">
          <PolicyForm draft={draft} update={(fn) => setDraft((d) => fn(d))} errors={errors} />
          <div className="sticky bottom-0 z-20 -mx-1 mt-4 flex items-center gap-2 border-t bg-background/95 px-1 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
            <Button onClick={save}>{isNew ? "Create policy" : "Save"}</Button>
            <Button variant="outline" onClick={onCancel}>
              Cancel
            </Button>
            <span className="ml-auto text-xs text-muted-foreground">
              {submitted && errorCount > 0 ? (
                <span className="text-destructive">
                  Fix {errorCount} issue{errorCount === 1 ? "" : "s"} above
                </span>
              ) : draft.action === "auto_apply" ? (
                `Saving attaches ${preview.attachments} rule${preview.attachments === 1 ? "" : "s"} in the background`
              ) : (
                `Saving creates ${preview.attachments} suggestion${preview.attachments === 1 ? "" : "s"}`
              )}
            </span>
          </div>
        </div>

        {/* RIGHT: sticky live impact */}
        <div className="min-w-0 lg:sticky lg:top-12 lg:h-[calc(100vh-4rem)] lg:self-start">
          <ImpactPane draft={draft} preview={preview} prev={prev} loading={loading} />
        </div>
      </div>
    </div>
  );
}

// ── Root ─────────────────────────────────────────────────────────────────────

export function Prototype({ ruleId }: { ruleId: string }) {
  const [policies, setPolicies] = useState<PolicyDraft[]>(MOCK_POLICIES);
  const [editing, setEditing] = useState<PolicyDraft | null>(null);
  const forRule = policies.filter((p) => p.rule_ids.includes(ruleId));

  if (editing) {
    return (
      <PolicyEditor
        initial={editing}
        others={policies}
        onCancel={() => setEditing(null)}
        onSave={(d) => {
          const isNew = !d.policy_id;
          const saved: PolicyDraft = isNew ? { ...d, policy_id: `p-${Date.now()}`, owner: CURRENT_USER } : d;
          setPolicies((ps) => (isNew ? [...ps, saved] : ps.map((p) => (p.policy_id === saved.policy_id ? saved : p))));
          const pv = mockPreview(saved);
          toast.success(isNew ? `Policy “${saved.name}” created` : `Policy “${saved.name}” saved`, {
            description: saved.rule_ids.includes(ruleId)
              ? `Evaluating ${pv.matched_monitored} monitored tables in the background.`
              : "It no longer includes this rule, so it isn't listed here.",
          });
          setEditing(null);
        }}
      />
    );
  }

  return (
    <PolicyList
      policies={forRule}
      onNew={() => setEditing(emptyDraft([ruleId]))}
      onEdit={(p) => setEditing(structuredClone(p))}
      onDelete={(p) => {
        setPolicies((ps) => ps.filter((x) => x.policy_id !== p.policy_id));
        toast.success(`Policy “${p.name}” deleted`);
      }}
    />
  );
}
