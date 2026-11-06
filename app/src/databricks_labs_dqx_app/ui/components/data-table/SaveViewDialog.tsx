import { useEffect, useId, useState, type FormEvent, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import type { SavedViewOut } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { canSubmitViewName, MAX_VIEW_NAME_LENGTH, viewNameError, type ViewNameError } from "./saved-views";
import { isDuplicateViewNameError, type SaveAsNewInput } from "./useSavedViews";

/** Wraps a disabled control so its *reason* still shows as a tooltip (disabled elements get no pointer events). */
export function DisabledReason({
  reason,
  className,
  children,
}: {
  reason: string | null;
  className?: string;
  children: ReactNode;
}) {
  if (!reason) return <>{children}</>;
  return (
    <Tooltip delayDuration={0}>
      <TooltipTrigger asChild>
        <span tabIndex={0} className={cn("outline-none", className)}>
          {children}
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">{reason}</TooltipContent>
    </Tooltip>
  );
}

/** The Name field shared by the Save as new and Rename dialogs, with its inline error. */
export function ViewNameField({
  value,
  onChange,
  error,
}: {
  value: string;
  onChange: (value: string) => void;
  error: ViewNameError | null;
}) {
  const { t } = useTranslation();
  const id = useId();
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>
        {t("savedViews.name")} <span className="text-destructive">*</span>
      </Label>
      <Input
        id={id}
        value={value}
        autoFocus
        required
        placeholder={t("savedViews.namePlaceholder")}
        maxLength={MAX_VIEW_NAME_LENGTH + 20}
        aria-invalid={error !== null}
        aria-describedby={error ? `${id}-error` : undefined}
        onChange={(e) => onChange(e.target.value)}
      />
      {error && (
        <p id={`${id}-error`} className="text-xs text-red-600 dark:text-red-400">
          {t(`savedViews.nameErrors.${error}`)}
        </p>
      )}
    </div>
  );
}

/**
 * Name validation state for a view-name dialog: the client-side error shown as
 * the user types ("required" only after a submit attempt) plus a server 409.
 */
export function useViewNameState(
  initial: string,
  open: boolean,
  scope: SavedViewOut["scope"],
  views: readonly SavedViewOut[],
  excludeId?: string,
) {
  const [name, setName] = useState(initial);
  const [submitted, setSubmitted] = useState(false);
  const [serverDuplicate, setServerDuplicate] = useState(false);
  const [pending, setPending] = useState(false);

  useEffect(() => {
    if (!open) return;
    setName(initial);
    setSubmitted(false);
    setServerDuplicate(false);
    setPending(false);
  }, [open, initial]);

  const clientError = viewNameError(name, scope, views, excludeId);
  const shownError: ViewNameError | null = serverDuplicate
    ? "duplicate"
    : clientError === "required" && !submitted
      ? null
      : clientError;

  /** Validates, then runs *action*; true when it succeeded. */
  const submit = async (action: () => Promise<void>): Promise<boolean> => {
    setSubmitted(true);
    if (!canSubmitViewName(name) || clientError) return false;
    setPending(true);
    try {
      await action();
      return true;
    } catch (error) {
      if (isDuplicateViewNameError(error)) setServerDuplicate(true);
      return false;
    } finally {
      setPending(false);
    }
  };

  const changeName = (value: string) => {
    setName(value);
    setServerDuplicate(false);
  };

  return { name, changeName, error: shownError, pending, canSubmit: canSubmitViewName(name), submit };
}

type Scope = SaveAsNewInput["scope"];

function ScopeOption({
  checked,
  disabled,
  label,
  onSelect,
}: {
  checked: boolean;
  disabled: boolean;
  label: string;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      role="radio"
      aria-checked={checked}
      disabled={disabled}
      onClick={onSelect}
      className="flex items-center gap-2 py-0.5 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring/50 rounded-sm disabled:cursor-not-allowed disabled:opacity-50"
    >
      <span
        aria-hidden
        className="flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-primary"
      >
        {checked && <span className="h-2 w-2 rounded-full bg-primary" />}
      </span>
      {label}
    </button>
  );
}

function CheckOption({
  checked,
  disabled,
  label,
  onChange,
}: {
  checked: boolean;
  disabled: boolean;
  label: string;
  onChange: (checked: boolean) => void;
}) {
  const id = useId();
  return (
    <div className={cn("flex items-center gap-2", disabled && "opacity-50")}>
      <Checkbox id={id} checked={checked} disabled={disabled} onCheckedChange={onChange} />
      <Label htmlFor={id} className={cn("font-normal", disabled && "cursor-not-allowed")}>
        {label}
      </Label>
    </div>
  );
}

export interface SaveViewDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  isAdmin: boolean;
  /** Every view on the page, for the duplicate-name check. */
  views: readonly SavedViewOut[];
  onSave: (input: SaveAsNewInput) => Promise<void>;
}

/** "Save as new view": name, who can see it, and whether it becomes a default. */
export function SaveViewDialog({ open, onOpenChange, isAdmin, views, onSave }: SaveViewDialogProps) {
  const { t } = useTranslation();
  const [scope, setScope] = useState<Scope>("user");
  const [makeMyDefault, setMakeMyDefault] = useState(false);
  const [makeOrgDefault, setMakeOrgDefault] = useState(false);
  const nameState = useViewNameState("", open, scope, views);

  useEffect(() => {
    if (!open) return;
    setScope("user");
    setMakeMyDefault(false);
    setMakeOrgDefault(false);
  }, [open]);

  const orgDefaultEnabled = isAdmin && scope === "org";
  const orgDefaultReason = !isAdmin
    ? t("savedViews.reasons.orgDefaultAdminsOnly")
    : scope !== "org"
      ? t("savedViews.orgDefaultNeedsEveryone")
      : null;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const ok = await nameState.submit(() =>
      onSave({ name: nameState.name, scope, makeMyDefault, makeOrgDefault: orgDefaultEnabled && makeOrgDefault }),
    );
    if (ok) onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={handleSubmit} className="space-y-4">
          <DialogHeader>
            <DialogTitle>{t("savedViews.dialogTitle")}</DialogTitle>
          </DialogHeader>
          <ViewNameField value={nameState.name} onChange={nameState.changeName} error={nameState.error} />
          <div className="space-y-1.5">
            <p className="text-sm font-medium">{t("savedViews.visibleTo")}</p>
            <div role="radiogroup" aria-label={t("savedViews.visibleTo")} className="flex flex-col items-start gap-1">
              <ScopeOption
                checked={scope === "user"}
                disabled={false}
                label={t("savedViews.onlyMe")}
                onSelect={() => setScope("user")}
              />
              <DisabledReason reason={isAdmin ? null : t("savedViews.everyoneAdminsOnly")}>
                <ScopeOption
                  checked={scope === "org"}
                  disabled={!isAdmin}
                  label={t("savedViews.everyone")}
                  onSelect={() => setScope("org")}
                />
              </DisabledReason>
            </div>
          </div>
          <div className="flex flex-col items-start gap-2">
            <CheckOption
              checked={makeMyDefault}
              disabled={false}
              label={t("savedViews.makeMyDefault")}
              onChange={setMakeMyDefault}
            />
            <DisabledReason reason={orgDefaultReason}>
              <CheckOption
                checked={orgDefaultEnabled && makeOrgDefault}
                disabled={!orgDefaultEnabled}
                label={t("savedViews.makeOrgDefault")}
                onChange={setMakeOrgDefault}
              />
            </DisabledReason>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" size="sm" onClick={() => onOpenChange(false)}>
              {t("common.cancel")}
            </Button>
            <Button type="submit" size="sm" disabled={nameState.pending || !nameState.canSubmit}>
              {t("savedViews.save")}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
