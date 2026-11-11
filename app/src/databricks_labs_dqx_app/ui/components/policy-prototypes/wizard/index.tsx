// THROWAWAY prototype — variant B "Wizard sheet" for rule policies.
// Policies tab body + a right-hand Sheet with a stepped wizard. Mock data only (../mock.ts).
import { useMemo, useState, type ReactElement } from "react";
import { toast } from "sonner";
import { Check, Lightbulb, Pencil, Plus, RefreshCw, ScanSearch, Trash2, User, Zap } from "lucide-react";
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
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { CURRENT_USER, MOCK_POLICIES, emptyDraft, mockPreview, type PolicyDraft } from "../mock";
import { STEPS, hasSlots, normalizeDraft, ruleById, selectedRules, summarize, validateStep, type StepKey } from "./model";
import { NoColumnsNote, StepColumns, StepReview, StepRules, StepWhere } from "./steps";

const NOT_OWNER = "Only the policy owner or an admin can edit";

function ActionBadge({ action }: { action: PolicyDraft["action"] }) {
  return action === "auto_apply" ? (
    <Badge variant="outline" className="gap-1 border-primary/40 bg-primary/10 text-primary">
      <Zap /> Apply automatically
    </Badge>
  ) : (
    <Badge variant="outline" className="gap-1 border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300">
      <Lightbulb /> Suggest only
    </Badge>
  );
}

function OwnerGated({ allowed, children }: { allowed: boolean; children: ReactElement }) {
  if (allowed) return children;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-not-allowed">{children}</span>
      </TooltipTrigger>
      <TooltipContent>{NOT_OWNER}</TooltipContent>
    </Tooltip>
  );
}

// ── Policy card ─────────────────────────────────────────────────────────

function PolicyCard({
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
  const others = selectedRules(policy).filter((r) => r.rule_id !== currentRuleId);

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-semibold">{policy.name}</h3>
            <ActionBadge action={policy.action} />
          </div>
          {policy.description && <p className="text-sm text-muted-foreground">{policy.description}</p>}
          <p className="text-sm">{summarize(policy)}.</p>
        </div>
        <div className="flex shrink-0 items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            className="gap-1.5"
            onClick={() =>
              toast.success(`Re-scanned “${policy.name}”`, {
                description: `${matched} monitored tables checked. No new matches.`,
              })
            }
          >
            <RefreshCw className="h-3.5 w-3.5" /> Re-scan
          </Button>
          <OwnerGated allowed={canEdit}>
            <Button variant="outline" size="sm" className="gap-1.5" disabled={!canEdit} onClick={onEdit}>
              <Pencil className="h-3.5 w-3.5" /> Edit
            </Button>
          </OwnerGated>
          <OwnerGated allowed={canEdit}>
            <Button
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0 text-muted-foreground hover:text-destructive"
              disabled={!canEdit}
              onClick={onDelete}
              aria-label="Delete policy"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </OwnerGated>
        </div>
      </div>
      <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 border-t pt-3 text-xs text-muted-foreground">
        <span className="inline-flex items-center gap-1">
          <User className="h-3.5 w-3.5" /> {policy.owner}
        </span>
        <span className="inline-flex items-center gap-1">
          <ScanSearch className="h-3.5 w-3.5" /> {matched} {matched === 1 ? "table" : "tables"} matched
        </span>
        {others.length > 0 && <span>Also applies: {others.map((r) => r.name).join(", ")}</span>}
      </div>
    </div>
  );
}

// ── Stepper ─────────────────────────────────────────────────────────────

function Stepper({
  current,
  reachable,
  skipped,
  onJump,
}: {
  current: number;
  reachable: (i: number) => boolean;
  skipped: (i: number) => boolean;
  onJump: (i: number) => void;
}) {
  return (
    <ol className="flex items-center gap-1 border-b px-6 py-3">
      {STEPS.map((s, i) => {
        const done = i < current;
        const active = i === current;
        const canJump = !active && reachable(i);
        const isSkipped = skipped(i);
        return (
          <li key={s.key} className="flex min-w-0 flex-1 items-center gap-1">
            <button
              type="button"
              disabled={!canJump}
              onClick={() => onJump(i)}
              aria-current={active ? "step" : undefined}
              className={cn(
                "flex min-w-0 items-center gap-2 rounded-md px-1.5 py-1 text-left",
                canJump && "hover:bg-muted",
                !canJump && !active && "cursor-default",
              )}
            >
              <span
                className={cn(
                  "flex h-6 w-6 shrink-0 items-center justify-center rounded-full border text-xs font-semibold",
                  active && "border-primary bg-primary text-primary-foreground",
                  done && !isSkipped && "border-primary bg-primary/10 text-primary",
                  done && isSkipped && "border-dashed text-muted-foreground",
                  !active && !done && "text-muted-foreground",
                )}
              >
                {done && !isSkipped ? <Check className="h-3.5 w-3.5" /> : i + 1}
              </span>
              <span className="min-w-0">
                <span
                  className={cn(
                    "block truncate text-sm",
                    active ? "font-semibold" : done ? "font-medium" : "text-muted-foreground",
                  )}
                >
                  {s.label}
                </span>
                <span className="block truncate text-[11px] text-muted-foreground">
                  {isSkipped ? "Not needed" : s.hint}
                </span>
              </span>
            </button>
            {i < STEPS.length - 1 && <span aria-hidden className={cn("h-px flex-1", done ? "bg-primary/50" : "bg-border")} />}
          </li>
        );
      })}
    </ol>
  );
}

// ── Wizard sheet ────────────────────────────────────────────────────────

function PolicyWizard({
  initial,
  takenNames,
  onCancel,
  onSubmit,
}: {
  initial: PolicyDraft;
  takenNames: string[];
  onCancel: () => void;
  onSubmit: (p: PolicyDraft) => void;
}) {
  const isEdit = !!initial.policy_id;
  const [draft, setDraft] = useState<PolicyDraft>(initial);
  const [step, setStep] = useState(0);
  const [maxReached, setMaxReached] = useState(isEdit ? STEPS.length - 1 : 0);
  const [showErrors, setShowErrors] = useState(false);

  const key = STEPS[step].key;
  const columnsNeeded = hasSlots(draft);
  const isSkipped = (i: number) => STEPS[i].key === "columns" && !columnsNeeded;
  const errors = useMemo(() => validateStep(key, draft, takenNames), [key, draft, takenNames]);
  const errorCount = Object.keys(errors).length;
  const stepValid = (i: number) => Object.keys(validateStep(STEPS[i].key, draft, takenNames)).length === 0;

  const nextIndex = (i: number) => (STEPS[i + 1] && isSkipped(i + 1) ? i + 2 : i + 1);
  const prevIndex = (i: number) => (i - 1 >= 0 && isSkipped(i - 1) ? i - 2 : i - 1);

  const goto = (i: number) => {
    setShowErrors(false);
    setStep(i);
    setMaxReached((m) => Math.max(m, i));
  };
  const reachable = (i: number) => {
    if (i < step) return true;
    if (i > maxReached) return false;
    for (let j = step; j < i; j++) if (!stepValid(j)) return false;
    return true;
  };
  const next = () => {
    if (errorCount > 0) {
      setShowErrors(true);
      return;
    }
    goto(nextIndex(step));
  };
  const submit = () => {
    for (let i = 0; i < STEPS.length - 1; i++) {
      if (!stepValid(i)) {
        goto(i);
        setShowErrors(true);
        return;
      }
    }
    onSubmit(normalizeDraft(draft));
  };

  const isLast = key === "review";
  const stepErrors = showErrors ? errors : {};

  return (
    <>
      <SheetHeader className="border-b px-6 py-4">
        <SheetTitle className="text-lg">{isEdit ? `Edit policy` : "New policy"}</SheetTitle>
        <SheetDescription>
          {isEdit
            ? "Changes apply on the next evaluation. Rules already attached stay attached."
            : "Attach rules to matching tables and columns across your catalogs."}
        </SheetDescription>
      </SheetHeader>
      <Stepper current={step} reachable={reachable} skipped={isSkipped} onJump={goto} />

      <div className="flex-1 overflow-y-auto px-6 py-6">
        {key === "rules" && <StepRules draft={draft} setDraft={setDraft} errors={stepErrors} />}
        {key === "where" && <StepWhere draft={draft} setDraft={setDraft} errors={stepErrors} />}
        {key === "columns" &&
          (columnsNeeded ? (
            <StepColumns draft={draft} setDraft={setDraft} errors={stepErrors} />
          ) : (
            <NoColumnsNote />
          ))}
        {key === "review" && (
          <StepReview draft={draft} goTo={(s: StepKey) => goto(STEPS.findIndex((x) => x.key === s))} />
        )}
      </div>

      <div className="flex items-center gap-2 border-t bg-background px-6 py-3">
        <Button variant="ghost" onClick={onCancel}>
          Cancel
        </Button>
        <div className="ml-auto flex items-center gap-3">
          {showErrors && errorCount > 0 && (
            <span className="text-xs font-medium text-destructive">
              {errorCount === 1 ? "Fix 1 issue to continue" : `Fix ${errorCount} issues to continue`}
            </span>
          )}
          {key === "where" && !columnsNeeded && !showErrors && (
            <span className="text-xs text-muted-foreground">Column step not needed for these rules</span>
          )}
          {step > 0 && (
            <Button variant="outline" onClick={() => goto(prevIndex(step))}>
              Back
            </Button>
          )}
          {isLast ? (
            <Button onClick={submit}>{isEdit ? "Save" : "Create policy"}</Button>
          ) : (
            <Button onClick={next}>Next</Button>
          )}
        </div>
      </div>
    </>
  );
}

// ── Policies tab ────────────────────────────────────────────────────────

export function Prototype({ ruleId }: { ruleId: string }) {
  const [policies, setPolicies] = useState<PolicyDraft[]>(MOCK_POLICIES);
  const [editing, setEditing] = useState<PolicyDraft | null>(null);
  const [sheetOpen, setSheetOpen] = useState(false);
  const [deleting, setDeleting] = useState<PolicyDraft | null>(null);
  const rule = ruleById(ruleId);
  const forRule = policies.filter((p) => p.rule_ids.includes(ruleId));

  const openNew = () => {
    setEditing(emptyDraft([ruleId]));
    setSheetOpen(true);
  };
  const openEdit = (p: PolicyDraft) => {
    setEditing(structuredClone(p));
    setSheetOpen(true);
  };
  const submit = (p: PolicyDraft) => {
    const preview = mockPreview(p);
    const isEdit = !!p.policy_id;
    const saved: PolicyDraft = isEdit ? p : { ...p, policy_id: `p-${Date.now()}`, owner: CURRENT_USER };
    setPolicies((prev) => (isEdit ? prev.map((x) => (x.policy_id === p.policy_id ? saved : x)) : [...prev, saved]));
    setSheetOpen(false);
    const verb = p.action === "auto_apply" ? "attachments" : "suggestions";
    toast.success(isEdit ? `Saved “${p.name}”` : `Created “${p.name}”`, {
      description: `Evaluating ${preview.matched_monitored} monitored tables: ${preview.attachments} ${verb}${
        preview.to_register ? `, ${preview.to_register} tables registered` : ""
      }.`,
    });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold">Policies</h2>
          <p className="text-sm text-muted-foreground">
            Policies that apply {rule ? `“${rule.name}”` : "this rule"} to matching tables and columns.
          </p>
        </div>
        {forRule.length > 0 && (
          <Button className="gap-1.5" onClick={openNew}>
            <Plus className="h-4 w-4" /> New policy
          </Button>
        )}
      </div>

      {forRule.length === 0 ? (
        <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed px-6 py-12 text-center">
          <div className="flex h-10 w-10 items-center justify-center rounded-full bg-primary/10 text-primary">
            <ScanSearch className="h-5 w-5" />
          </div>
          <div className="space-y-1">
            <p className="font-medium">No policies yet</p>
            <p className="max-w-md text-sm text-muted-foreground">
              A policy attaches this rule to every table or column that matches its scope and tags, now and as new
              ones appear.
            </p>
          </div>
          <Button className="gap-1.5" onClick={openNew}>
            <Plus className="h-4 w-4" /> New policy
          </Button>
        </div>
      ) : (
        <div className="space-y-3">
          {forRule.map((p) => (
            <PolicyCard
              key={p.policy_id}
              policy={p}
              currentRuleId={ruleId}
              onEdit={() => openEdit(p)}
              onDelete={() => setDeleting(p)}
            />
          ))}
        </div>
      )}

      <Sheet open={sheetOpen} onOpenChange={setSheetOpen}>
        <SheetContent className="w-full gap-0 p-0 sm:max-w-3xl">
          {editing && (
            <PolicyWizard
              key={editing.policy_id ?? "new"}
              initial={editing}
              takenNames={policies.filter((p) => p.policy_id !== editing.policy_id).map((p) => p.name)}
              onCancel={() => setSheetOpen(false)}
              onSubmit={submit}
            />
          )}
        </SheetContent>
      </Sheet>

      <AlertDialog open={!!deleting} onOpenChange={(o) => !o && setDeleting(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete “{deleting?.name}”?</AlertDialogTitle>
            <AlertDialogDescription>
              The policy stops matching new tables and columns. Rules it already attached stay on their tables.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={() => {
                if (!deleting) return;
                setPolicies((prev) => prev.filter((p) => p.policy_id !== deleting.policy_id));
                toast.success(`Deleted “${deleting.name}”`);
                setDeleting(null);
              }}
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
